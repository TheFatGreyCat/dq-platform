import pytest

from budget import LIMIT, activation_estimate, check_budget, max_report_bytes
from canonical import canonical_bytes
from errors import ApiError
from models import Policy, Registration, RUNTIME_CONF, Suite, WriteReport

U = "11111111-1111-1111-1111-111111111111"
H = "sha256:" + "a" * 64
MANIFEST = {"format": "dq-compat-v1", "spark_version": "3.5.1", "scala_version": "2.12.18", "java_major": 17,
            "hadoop_version": "3.3.4", "jars": {k: H for k in ("delta_spark", "delta_storage", "hadoop_aws", "deequ", "dq_client")}}


def registration(ncols=3):
    t = lambda i, u: {"uri": u, "table_id": i, "schema_hash": H, "partition_schema_hash": H, "set_transaction_retention": None}
    cols = [{"name": f"c{i}", "type": "string", "nullable": True} for i in range(ncols)]
    return Registration.model_validate({
        "owner": "o", "silver": t(U, "s3a://b/silver"), "quarantine": t("22222222-2222-2222-2222-222222222222", "s3a://b/q"),
        "candidate_root": "s3a://b/cand", "allowed_storage_roots": ["s3a://b"],
        "business_schema": cols + [{"name": "dt", "type": "date", "nullable": False}],
        "partition_schema": [{"name": "dt", "type": "date", "nullable": False}], "history_context": "ctx",
        "retention": {"candidate_duration_seconds": None, "report_duration_seconds": None, "retry_window_seconds": 60,
                      "transaction_metadata_duration_seconds": None, "cleanup_enabled": False}})


def suite(n, check):
    return Suite.model_validate({"format": "dq-rules/v3", "rules": [
        {"rule_id": f"r{i}", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE", "check": check}
        for i in range(n)]})


def policy(n):
    return Policy.model_validate({"rules": [{"rule_id": f"r{i}", "action": "BLOCK", "required": True} for i in range(n)],
                                  "max_quarantine_ratio": "0.05", "anomaly": {"enabled": False}})


def test_realistic_suite_admitted():
    n = 20
    est = activation_estimate(registration(), suite(n, {"kind": "complete", "column": "c0"}), policy(n))
    assert 0 < est < LIMIT
    check_budget(est)


def test_worst_case_500_schema_rules_rejected():
    n = 500
    est = activation_estimate(registration(999), suite(n, {"kind": "schema_columns", "columns": ["c0"]}), policy(n))
    assert est > LIMIT
    with pytest.raises(ApiError) as e:
        check_budget(est)
    assert (e.value.code, e.value.field) == ("INVALID_CONFIG", "payload_budget")


def test_check_budget_boundary():
    check_budget(LIMIT)
    with pytest.raises(ApiError):
        check_budget(LIMIT + 1)


def _plan(rules_checks, parameters=None, root="s3a://b/cand/exec"):
    rules = [{"rule_id": f"r{i}", "resolved_check": c, "action": "BLOCK", "required": True}
             for i, c in enumerate(rules_checks)]
    return {"workflow": "WRITE_GATE", "dataset_id": "ds", "engine": "spark", "interpreter_version": "1",
            "stage_bindings": [{"scope": "PARTITION", "stage": "CANDIDATE", "rules": rules}],
            "parameters": parameters or {}, "registration_snapshot": {
                "business_schema": [{"name": "c0", "type": "string", "nullable": True}], "silver": {"uri": "s3a://b/silver"}},
            "compat_manifest": {"payload": MANIFEST}, "candidate_locations": {"execution_root": root}}


def _ref(uri):
    return {"uri": uri, "table_id": U, "version": 2**63 - 1, "schema_hash": H}


def _err():
    return {"code": "CAST_OVERFLOW", "params": {"rule_id": "r0", "column": "c0", "exception_class": "a" * 256}}


def test_estimate_bounds_largest_valid_report():
    checks = [{"kind": "schema_columns", "columns": ["c0"]}, {"kind": "mean", "column": "c0"},
              {"kind": "size"}, {"kind": "schema_type", "column": "c0", "type": "string"}]
    plan = _plan(checks, {"p": "x" * 1024})
    results = [
        {"rule_id": "r0", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
         "kind": "schema_columns", "measurement_status": "OK", "failed_rows": 2**63 - 1,
         "schema_evidence": {"actual_columns": ["\u00e9" * 128] * 1000, "matches": False}},
        {"rule_id": "r1", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
         "kind": "mean", "measurement_status": "OK", "metric_name": "Mean", "metric_value": "-" + "9" * 383},
        {"rule_id": "r2", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
         "kind": "size", "measurement_status": "OK", "metric_name": "Size", "metric_value": -(2**63)},
        {"rule_id": "r3", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE",
         "kind": "schema_type", "measurement_status": "ERROR", "error": _err()},
    ]
    root = plan["candidate_locations"]["execution_root"] + "/" + U
    report = WriteReport.model_validate({
        "attempt_id": U, "dataset_id": "ds", "plan_hash": H, "suite_revision_id": U, "policy_revision_id": U,
        "engine": "spark", "interpreter_version": "1", "runtime_conf": RUNTIME_CONF,
        "runtime_versions": {k: v for k, v in MANIFEST.items() if k != "format"},
        "parameters": {"p": "x" * 1024}, "results": results,
        "raw_reference": _ref(root + "/raw"), "classified_reference": _ref(root + "/classified"),
        "business_schema_hash": H, "classified_schema_hash": H, "classification_status": "COMPLETE",
        "candidate_rows": 2**63 - 1, "valid_rows": 2**63 - 2, "quarantine_rows": 1,
        "clean_selector": "size(_dq_errors)=0", "quarantine_selector": "size(_dq_errors)>0", "errors": [_err()] * 50})
    actual = len(canonical_bytes(report.model_dump(mode="json", by_alias=True)))
    assert actual <= max_report_bytes(plan)


def test_estimate_grows_with_errors_and_parameters():
    small = max_report_bytes(_plan([{"kind": "size"}]))
    assert max_report_bytes(_plan([{"kind": "size"}], {"p": "x" * 1024})) > small
