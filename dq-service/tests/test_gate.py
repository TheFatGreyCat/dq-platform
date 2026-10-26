import copy

import pytest

from models import AuditReport, RUNTIME_CONF, WriteReport
from policy import evaluate

U = "11111111-1111-1111-1111-111111111111"
H = "sha256:" + "a" * 64
VERSIONS = {"spark_version": "3.5.1", "scala_version": "2.12.18", "java_major": 17, "hadoop_version": "3.3.4",
            "jars": {k: H for k in ("delta_spark", "delta_storage", "hadoop_aws", "deequ", "dq_client")}}


def ref(name="raw"):
    return {"uri": f"s3a://b/x/{name}", "table_id": U, "version": 0, "schema_hash": H}


def prule(rid, check, action="BLOCK", required=True, kind_type="DECIMAL"):
    return {"rule_id": rid, "metric_definition_hash": H, "measurement_definition_hash": H, "metric_name": None,
            "value_type": kind_type, "resolved_check": check, "action": action, "required": required}


def make_plan(rules, *, workflow="WRITE_GATE", cmin=1, clean_min=1, ratio="0.05"):
    return {"workflow": workflow,
            "policy_revision": {"payload": {"min_candidate_rows": cmin, "min_clean_rows": clean_min,
                                            "max_quarantine_ratio": ratio}},
            "stage_bindings": [{"scope": "PARTITION", "stage": "CANDIDATE", "rules": rules}]}


def ok(rid, kind, name, value, **kw):
    return {"rule_id": rid, "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
            "kind": kind, "measurement_status": "OK", "metric_name": name, "metric_value": value, **kw}


def err(rid, kind="size"):
    return {"rule_id": rid, "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
            "kind": kind, "measurement_status": "ERROR",
            "error": {"code": "STORAGE_UNAVAILABLE", "params": {}}}


def write_report(results, candidate=100, quarantine=3, status="COMPLETE", errors=()):
    complete = status == "COMPLETE"
    return WriteReport.model_validate({
        "attempt_id": U, "dataset_id": "d", "plan_hash": H, "suite_revision_id": U, "policy_revision_id": U,
        "engine": "e", "interpreter_version": "1", "runtime_conf": RUNTIME_CONF, "runtime_versions": VERSIONS,
        "parameters": {}, "results": results, "raw_reference": ref(),
        "classified_reference": ref("c") if complete else None,
        "business_schema_hash": H, "classified_schema_hash": H if complete else None,
        "classification_status": status, "candidate_rows": candidate,
        "valid_rows": candidate - quarantine if complete else None,
        "quarantine_rows": quarantine if complete else None,
        "clean_selector": "size(_dq_errors)=0" if complete else None,
        "quarantine_selector": "size(_dq_errors)>0" if complete else None,
        "errors": list(errors) or ([] if complete else [err("x")["error"]])})


def audit_report(results):
    return AuditReport.model_validate({
        "attempt_id": U, "dataset_id": "d", "plan_hash": H, "suite_revision_id": U, "policy_revision_id": U,
        "engine": "e", "interpreter_version": "1", "runtime_conf": RUNTIME_CONF, "runtime_versions": VERSIONS,
        "parameters": {}, "results": results, "audit_reference": ref("a"), "measured_rows": 5,
        "classification_status": "NOT_APPLICABLE"})


SIZE = {"kind": "size", "assert": {"op": ">=", "value": "1"}}
COMPLETE = {"kind": "complete", "column": "v", "assert": {"op": "==", "value": "1"}}


def codes(a):
    return [r["code"] for r in a.reasons]


def test_100_3_allows():
    plan = make_plan([prule("q", COMPLETE, "QUARANTINE")])
    a = evaluate(plan, write_report([ok("q", "complete", "Completeness", "0.97")]))
    assert (a.decision, a.assessment, a.reasons) == ("ALLOW", None, [])
    assert a.rule_outcomes == {"q": "VIOLATION"}  # quarantine rule không veto


def test_100_6_denies_on_ratio():
    a = evaluate(make_plan([]), write_report([ok("s", "size", "Size", 100)], quarantine=6))
    assert a.decision == "DENY" and codes(a) == ["QUARANTINE_RATIO_EXCEEDED"]


def test_ratio_boundary_is_inclusive():
    a = evaluate(make_plan([]), write_report([ok("s", "size", "Size", 100)], quarantine=5))
    assert a.decision == "ALLOW"


def test_clean_minimum_not_candidate_minimum():
    plan = make_plan([], cmin=1000, clean_min=1000, ratio="1")
    a = evaluate(plan, write_report([ok("s", "size", "Size", 1000)], candidate=1000, quarantine=30))
    assert a.decision == "DENY" and codes(a) == ["MIN_CLEAN_ROWS"]
    a = evaluate(make_plan([], cmin=1000, clean_min=1, ratio="1"), write_report([ok("s", "size", "Size", 1)], candidate=999, quarantine=0))
    assert codes(a) == ["MIN_CANDIDATE_ROWS"]


def test_zero_candidates_ratio_zero_allowed_only_when_complete():
    plan = make_plan([], cmin=0, clean_min=0, ratio="0")
    assert evaluate(plan, write_report([ok("s", "size", "Size", 0)], candidate=0, quarantine=0)).decision == "ALLOW"
    a = evaluate(plan, write_report([ok("s", "size", "Size", 0)], candidate=0, status="INCOMPLETE"))
    assert a.decision == "DENY" and "CLASSIFICATION_INCOMPLETE" in codes(a)


def test_required_error_denies_optional_error_warns():
    plan = make_plan([prule("a", SIZE, required=True, kind_type="INTEGER"), prule("b", SIZE, required=False, kind_type="INTEGER")])
    a = evaluate(plan, write_report([err("a"), ok("b", "size", "Size", 5)]))
    assert a.decision == "DENY" and codes(a) == ["REQUIRED_MEASUREMENT_ERROR"] and a.rule_outcomes["a"] == "ERROR"
    a = evaluate(plan, write_report([ok("a", "size", "Size", 5), err("b")]))
    assert a.decision == "ALLOW" and codes(a) == ["OPTIONAL_MEASUREMENT_ERROR"]


def test_observe_violation_warns_block_violation_denies():
    plan = make_plan([prule("o", SIZE, "OBSERVE", False), prule("b", SIZE, "BLOCK", True)])
    a = evaluate(plan, write_report([ok("o", "size", "Size", 0), ok("b", "size", "Size", 5)]))
    assert a.decision == "ALLOW" and codes(a) == ["RULE_VIOLATION_OBSERVE"]
    a = evaluate(plan, write_report([ok("o", "size", "Size", 5), ok("b", "size", "Size", 0)]))
    assert a.decision == "DENY" and codes(a) == ["RULE_VIOLATION_BLOCK"]


def test_missing_result_is_error():
    a = evaluate(make_plan([prule("a", SIZE, kind_type="INTEGER")]), write_report([ok("zz", "size", "Size", 1)]))
    assert a.rule_outcomes == {"a": "ERROR"} and a.decision == "DENY"


@pytest.mark.parametrize("assertion,value,expected", [
    ({"op": ">=", "value": "5"}, 5, "PASS"), ({"op": ">", "value": "5"}, 5, "VIOLATION"),
    ({"op": "<=", "value": "5"}, 5, "PASS"), ({"op": "<", "value": "5"}, 5, "VIOLATION"),
    ({"op": "==", "value": "0.5"}, "0.5", "PASS"), ({"op": "==", "value": "0.5"}, "0.50001", "VIOLATION"),
    ({"op": "between", "min": "1", "max": "2"}, "1", "PASS"), ({"op": "between", "min": "1", "max": "2"}, "2", "PASS"),
    ({"op": "between", "min": "1", "max": "2"}, "2.01", "VIOLATION"),
])
def test_assertion_operators(assertion, value, expected):
    name = "Size" if isinstance(value, int) else "Mean"
    kind = "size" if isinstance(value, int) else "mean"
    check = {"kind": kind, "assert": assertion, **({} if kind == "size" else {"column": "v"})}
    a = evaluate(make_plan([prule("r", check)]), write_report([ok("r", kind, name, value)]))
    assert a.rule_outcomes["r"] == expected


def test_assertion_less_rule_passes_when_measured():
    check = {"kind": "unique", "columns": ["id"], "assert": None}
    a = evaluate(make_plan([prule("u", check)]), write_report([ok("u", "unique", "Uniqueness", "0.1")]))
    assert a.rule_outcomes["u"] == "PASS"


def test_unparseable_assertion_literal_is_error_not_exception():
    check = {"kind": "size", "assert": {"op": ">=", "value": {"param": "x"}}}
    a = evaluate(make_plan([prule("r", check, kind_type="INTEGER")]), write_report([ok("r", "size", "Size", 1)]))
    assert a.rule_outcomes["r"] == "ERROR" and a.decision == "DENY"


def test_incomplete_denies():
    a = evaluate(make_plan([]), write_report([ok("s", "size", "Size", 1)], status="INCOMPLETE"))
    assert a.decision == "DENY" and set(codes(a)) == {"CLASSIFICATION_INCOMPLETE", "CLIENT_ERROR"}


def test_schema_type_recomputed_ignoring_client_matches():
    check = {"kind": "schema_type", "column": "amount", "type": "decimal"}
    plan = make_plan([prule("t", check, kind_type="SCHEMA")])

    def run(actual):
        ev = {"column": "amount", "actual_type": actual, "matches": True}  # client nói True
        res = {"rule_id": "t", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
               "kind": "schema_type", "measurement_status": "OK", "schema_evidence": ev}
        return evaluate(plan, write_report([res]))
    assert run("decimal(18,2)").rule_outcomes["t"] == "PASS"
    a = run("string")
    assert a.rule_outcomes["t"] == "VIOLATION" and a.decision == "DENY"


def test_schema_columns_exact_and_subset():
    def run(exact, actual):
        check = {"kind": "schema_columns", "columns": ["a", "b"], "exact": exact}
        res = {"rule_id": "c", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
               "kind": "schema_columns", "measurement_status": "OK",
               "schema_evidence": {"actual_columns": actual, "matches": False}}
        return evaluate(make_plan([prule("c", check, kind_type="SCHEMA")]), write_report([res])).rule_outcomes["c"]
    assert run(False, ["a", "b", "c"]) == "PASS"
    assert run(True, ["a", "b", "c"]) == "VIOLATION"
    assert run(True, ["B", "a"]) == "PASS"  # caseSensitive=false
    assert run(False, ["a"]) == "VIOLATION"


def _audit_plan(*rules):
    plan = make_plan(list(rules), workflow="AUDIT_TABLE")
    plan["stage_bindings"][0]["stage"] = "AUDIT_SNAPSHOT"
    return plan


def _audit_ok(rid, value):
    r = ok(rid, "size", "Size", value)
    r["stage"] = "AUDIT_SNAPSHOT"
    return r


def test_audit_pass_violation_error():
    plan = _audit_plan(prule("a", SIZE, "BLOCK", True, "INTEGER"), prule("o", SIZE, "OBSERVE", False, "INTEGER"))
    a = evaluate(plan, audit_report([_audit_ok("a", 3), _audit_ok("o", 3)]))
    assert (a.decision, a.assessment) == (None, "PASS")
    a = evaluate(plan, audit_report([_audit_ok("a", 3), _audit_ok("o", 0)]))
    assert (a.decision, a.assessment) == (None, "VIOLATION") and codes(a) == ["RULE_VIOLATION_OBSERVE"]
    bad = err("a"); bad["stage"] = "AUDIT_SNAPSHOT"
    a = evaluate(plan, audit_report([bad, _audit_ok("o", 3)]))
    assert (a.decision, a.assessment) == (None, "ERROR")
    bad_o = err("o"); bad_o["stage"] = "AUDIT_SNAPSHOT"
    a = evaluate(plan, audit_report([_audit_ok("a", 3), bad_o]))
    assert a.assessment == "PASS" and codes(a) == ["OPTIONAL_MEASUREMENT_ERROR"]


def test_plan_not_mutated():
    plan = make_plan([prule("a", SIZE, kind_type="INTEGER")])
    before = copy.deepcopy(plan)
    evaluate(plan, write_report([ok("a", "size", "Size", 1)]))
    assert plan == before
