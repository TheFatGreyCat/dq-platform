import copy
import json

import pytest
from pydantic import TypeAdapter, ValidationError

import models
from errors import ClientError
from models import (Assertion, BindingRequest, ExecutionRequest, MeasurementResult, OutcomeRequest, Policy,
                    ProcessStop, Registration, Suite, WriteReport, check_policy_for_suite)

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
U3 = "33333333-3333-3333-3333-333333333333"
H = "sha256:" + "a" * 64


def rule(rid="r1", check=None, **kw):
    return {"rule_id": rid, "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": check or {"kind": "complete", "column": "value"}, **kw}


def suite(*rules, parameters=None):
    return {"format": "dq-rules/v3", "parameters": parameters or {}, "rules": list(rules) or [rule()]}


def policy(**kw):
    return {"rules": [{"rule_id": "r1", "action": "QUARANTINE", "required": True}], "max_quarantine_ratio": "0.05",
            "anomaly": {"enabled": False}, **kw}


def ref(uri="s3a://bucket/silver", **kw):
    return {"uri": uri, "table_id": U1, "version": 0, "schema_hash": H, "set_transaction_retention": None, **kw}


def test_complete_defaults_assertion_to_one_and_dump_keeps_alias():
    s = Suite.model_validate(suite())
    assert s.rules[0].check.assert_ == Assertion(op="==", value="1")
    assert s.model_dump(mode="json", by_alias=True)["rules"][0]["check"]["assert"] == {
        "op": "==", "value": "1", "min": None, "max": None}


@pytest.mark.parametrize("bad", [True, 1.5, "1e3", "NaN", "Infinity", "-0", "01", "1.50", "1.", "x" * 400])
def test_numeric_bounds_reject_non_canonical(bad):
    chk = {"kind": "size", "assert": {"op": ">=", "value": bad}}
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(check=chk)))


def test_integer_and_decimal_string_bounds_accepted_and_range_order_checked():
    ok = {"kind": "range", "column": "amount", "min": "0", "max": 100}
    Suite.model_validate(suite(rule(check=ok)))
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(check={**ok, "min": "101"})))


def test_rule_count_ids_and_extra_fields():
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(*[rule(f"r{i}") for i in range(501)]))
    Suite.model_validate(suite(*[rule(f"r{i}") for i in range(500)]))
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule("a"), rule("a")))
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(severity="error")))
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule("é")))


def test_parameters_declared_exactly_and_batch_reserved():
    chk = {"kind": "sql_metric", "sql": "select count(*) from ${batch} where d=${day}", "assert": {"op": "==", "value": {"param": "n"}}}
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(check=chk), parameters={"n": "x"}))
    Suite.model_validate(suite(rule(check=chk), parameters={"n": "x", "day": "d"}))
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(), parameters={"batch": "x"}))


def test_stage_must_match_workflow():
    with pytest.raises(ValidationError):
        Suite.model_validate(suite(rule(), {**rule("a2"), "stage": "AUDIT_SNAPSHOT"}))
    with pytest.raises(ValidationError):
        Suite.model_validate({**suite(), "workflow": "AUDIT_TABLE"})
    Suite.model_validate({**suite(), "workflow": "WRITE_GATE"})


def test_policy_ratio_bounds_and_float_rejected():
    Policy.model_validate(policy(max_quarantine_ratio="1"))
    for bad in ("1.01", "-0.1", 0.05):
        with pytest.raises(ValidationError):
            Policy.model_validate(policy(max_quarantine_ratio=bad))
    with pytest.raises(ValidationError):
        Policy.model_validate(policy(min_clean_rows=True))
    Policy.model_validate(policy(anomaly={"enabled": True, "max_points": 20, "lower": "0.8", "upper": "1.2", "bootstrap": False}))
    with pytest.raises(ValidationError):
        Policy.model_validate(policy(anomaly={"enabled": True, "max_points": 20, "lower": "1.3", "upper": "1.2", "bootstrap": False}))


def test_quarantine_only_row_level_candidate():
    p = Policy.model_validate(policy())
    check_policy_for_suite(Suite.model_validate(suite()), p)
    with pytest.raises(ValueError):
        check_policy_for_suite(Suite.model_validate(suite(rule(check={"kind": "mean", "column": "a", "assert": {"op": ">", "value": "0"}}))), p)
    with pytest.raises(ValueError):
        check_policy_for_suite(Suite.model_validate(suite(rule(stage="CLEAN"))), p)
    with pytest.raises(ValueError):
        check_policy_for_suite(Suite.model_validate(suite(rule(), rule("r2"))), p)


@pytest.mark.parametrize("uri", [
    "s3a://user:pw@bucket/x", "s3a://bucket/x?a=1", "s3a://bucket/x#f", "s3a://bucket/../x", "S3A://bucket/%2E%2e/x",
    "s3a://bucket/%2fx", "http://bucket/x", "s3a://bucket//x"])
def test_snapshot_uri_rejected(uri):
    with pytest.raises(ValidationError):
        models.SnapshotRef.model_validate(ref(uri))


def test_snapshot_retention_must_be_null_and_version_not_bool():
    models.SnapshotRef.model_validate(ref())
    with pytest.raises(ValidationError):
        models.SnapshotRef.model_validate(ref(set_transaction_retention="1"))
    with pytest.raises(ValidationError):
        models.SnapshotRef.model_validate(ref(version=True))


def registration(**kw):
    t = lambda i, u: {"uri": u, "table_id": i, "schema_hash": H, "partition_schema_hash": H, "set_transaction_retention": None}
    return {"owner": "o", "silver": t(U1, "s3a://b/silver"), "quarantine": t(U2, "s3a://b/q"),
            "candidate_root": "s3a://b/cand", "allowed_storage_roots": ["s3a://b"],
            "business_schema": [{"name": "id", "type": "long", "nullable": False}, {"name": "dt", "type": "date", "nullable": False}],
            "partition_schema": [{"name": "dt", "type": "date", "nullable": False}], "history_context": "ctx",
            "retention": {"candidate_duration_seconds": None, "report_duration_seconds": None, "retry_window_seconds": 60,
                          "transaction_metadata_duration_seconds": None, "cleanup_enabled": False}, **kw}


def test_registration_rules():
    Registration.model_validate(registration())
    cols = lambda *n: [{"name": x, "type": "string", "nullable": True} for x in n]
    for bad in (dict(business_schema=cols("A", "a")), dict(business_schema=cols("_dq_errors")),
                dict(candidate_root="s3a://other/c"),
                dict(partition_schema=[{"name": "dt", "type": "string", "nullable": False}]),
                dict(retention={**registration()["retention"], "retry_window_seconds": 0})):
        with pytest.raises(ValidationError):
            Registration.model_validate(registration(**bad))


def exec_req(**kw):
    return {"dataset_id": "ds", "workflow": "WRITE_GATE", "purpose": "default", "event_time": "2026-10-20T00:00:00Z",
            "parameters": {"a": "x-1"}, "history_context": "ctx", "partition_values": {"dt": "2026-10-20"},
            "input_snapshot": None, "target_bases": {"silver": ref(), "quarantine": ref("s3a://b/q")},
            "compat_manifest_id": U3, **kw}


def test_execution_request_shape():
    ExecutionRequest.model_validate(exec_req())
    for bad in (dict(parameters={"a": "x;drop"}), dict(partition_values={"dt": "2026-02-30"}),
                dict(input_snapshot=ref()), dict(event_time="2026-10-20T00:00:00"), dict(event_origin="X")):
        with pytest.raises(ValidationError):
            ExecutionRequest.model_validate(exec_req(**bad))
    with pytest.raises(ValidationError):
        ExecutionRequest.model_validate(exec_req(workflow="AUDIT_TABLE"))


def test_process_stop_confirmed_needs_reference():
    ProcessStop(confirmed_stopped=False)
    with pytest.raises(ValidationError):
        ProcessStop(confirmed_stopped=True, reference="  ")


def ok(**kw):
    return {"rule_id": "r1", "measurement_definition_hash": H, "scope": "PARTITION", "stage": "CANDIDATE", "kind": "size",
            "measurement_status": "OK", "metric_name": "Size", "metric_value": 100, **kw}


def test_measurement_result_shapes():
    MeasurementResult.model_validate(ok())
    MeasurementResult.model_validate(ok(kind="mean", metric_name="Mean", metric_value="10.5"))
    err = {"code": "DRIVER_OOM", "params": {"exception_class": "java.lang.OutOfMemoryError"}}
    MeasurementResult.model_validate({**ok(metric_name=None, metric_value=None), "measurement_status": "ERROR", "error": err})
    bad = [ok(metric_value=True), ok(metric_value="100"), ok(metric_name="Mean"), ok(metric_value=None),
           ok(kind="mean", metric_name="Mean", metric_value=10), ok(metric_value="NaN"),
           ok(error=err), ok(schema_evidence={"column": "a", "actual_type": "long", "matches": True}),
           {**ok(), "measurement_status": "ERROR"}, {**ok(), "message": "x"},
           ok(kind="schema_type", metric_name=None, metric_value=None, schema_evidence={"actual_columns": [], "matches": True})]
    for b in bad:
        with pytest.raises(ValidationError):
            MeasurementResult.model_validate(b)
    MeasurementResult.model_validate(ok(kind="schema_type", metric_name=None, metric_value=None,
                                        schema_evidence={"column": "a", "actual_type": "long", "matches": True}))


def versions():
    return {"spark_version": "3.5.1", "scala_version": "2.12.18", "java_major": 17, "hadoop_version": "3.3.4",
            "jars": {k: H for k in ("delta_spark", "delta_storage", "hadoop_aws", "deequ", "dq_client")}}


def write_report(**kw):
    return {"attempt_id": U1, "dataset_id": "ds", "plan_hash": H, "suite_revision_id": U1, "policy_revision_id": U2,
            "engine": "deequ", "interpreter_version": "1", "runtime_conf": dict(models.RUNTIME_CONF),
            "runtime_versions": versions(), "parameters": {}, "results": [ok()],
            "raw_reference": ref(), "classified_reference": ref(), "business_schema_hash": H, "classified_schema_hash": H,
            "classification_status": "COMPLETE", "candidate_rows": 100, "valid_rows": 97, "quarantine_rows": 3,
            "clean_selector": "size(_dq_errors)=0", "quarantine_selector": "size(_dq_errors)>0", "errors": [], **kw}


def test_write_report_counts_and_conf():
    WriteReport.model_validate(write_report())
    conf = dict(models.RUNTIME_CONF, **{"spark.sql.ansi.enabled": "true"})
    for bad in (dict(valid_rows=98), dict(runtime_conf=conf), dict(runtime_conf={}), dict(classified_reference=None),
                dict(candidate_rows=True), dict(status="PASS"), dict(results=[ok(), ok()])):
        with pytest.raises(ValidationError):
            WriteReport.model_validate(write_report(**bad))
    WriteReport.model_validate(write_report(
        classification_status="INCOMPLETE", classified_reference=None, classified_schema_hash=None, valid_rows=None,
        quarantine_rows=None, clean_selector=None, quarantine_selector=None,
        errors=[{"code": "SERVICE_UNAVAILABLE", "params": {}}]))


def test_errors_capped_at_50():
    e = {"code": "SERVICE_UNAVAILABLE", "params": {}}
    WriteReport.model_validate(write_report(errors=[e] * 50))
    with pytest.raises(ValidationError):
        WriteReport.model_validate(write_report(errors=[e] * 51))


@pytest.mark.parametrize("err", [
    {"code": "SQL_ANALYSIS_ERROR", "params": {"rule_id": "r1", "exception_class": "x y"}},
    {"code": "CAST_OVERFLOW", "params": {"exception_class": "a.B"}},
    {"code": "COLUMN_NOT_FOUND", "params": {"rule_id": "r1", "column": "c", "exception_class": "a.B"}},
    {"code": "DRIVER_OOM", "params": {}},
    {"code": "RUNTIME_MISMATCH", "params": {"component": "bogus", "expected": "a", "actual": "b"}},
    {"code": "RUNTIME_MISMATCH", "params": {"component": "spark.sql.ansi.enabled", "expected": "a b", "actual": "b"}},
    {"code": "MADE_UP", "params": {}}, {"code": "SERVICE_UNAVAILABLE", "params": {"message": "boom"}}])
def test_client_error_per_code_params(err):
    with pytest.raises(ValidationError):
        TypeAdapter(ClientError).validate_python(err)


def test_client_error_valid_and_omits_nulls():
    e = TypeAdapter(ClientError).validate_python({"code": "STORAGE_UNAVAILABLE", "params": {"exception_class": "java.io.IOException"}})
    assert e.model_dump(mode="json") == {"code": "STORAGE_UNAVAILABLE", "params": {"exception_class": "java.io.IOException"}}


def test_outcome_transitions_shape():
    base = {"expected_outcome_version": 0}
    OutcomeRequest.model_validate({**base, "state": "IN_PROGRESS", "intent": {"txn_app_id": f"dq:{U1}:silver", "txn_version": 1}})
    exact = {"table_id": U1, "version": 3, "metadata_hash": H, "evidence": "EXACT"}
    OutcomeRequest.model_validate({**base, "state": "SUCCEEDED", "commit": exact, "rows": 97})
    txn_only = {"table_id": U1, "version": None, "metadata_hash": None, "evidence": "TXN_ONLY"}
    for bad in ({"state": "SUCCEEDED"}, {"state": "UNKNOWN", "commit": exact}, {"state": "IN_PROGRESS"},
                {"state": "FAILED"}, {"state": "SUCCEEDED", "commit": {**exact, "metadata_hash": None}},
                {"state": "SUCCEEDED", "commit": {**exact, "evidence": "TXN_ONLY"}},
                {"state": "SUCCEEDED", "commit": txn_only}, {"state": "NOT_APPLICABLE"}):
        with pytest.raises(ValidationError):
            OutcomeRequest.model_validate({**base, **bad})
    rec = {"current_base_version": 1, "writer_stop": {"confirmed_stopped": True, "reference": "r"},
           "txn_evidence": {"table_id": U1, "snapshot_version": 4, "txn_app_id": f"dq:{U1}:silver", "txn_version": 1}}
    OutcomeRequest.model_validate({**base, "state": "SUCCEEDED", "commit": txn_only, "reason": "why", "recovery": rec})


def test_misc_requests_forbid_extras_and_bool_ints():
    with pytest.raises(ValidationError):
        BindingRequest.model_validate({"expected_binding_version": True, "suite_id": U1, "suite_revision_id": U1,
                                       "policy_revision_id": U2, "reason": "r"})
    with pytest.raises(ValidationError):
        models.RetireRequest.model_validate({"expected_binding_version": 0, "reason": "r", "x": 1})
    with pytest.raises(ValidationError):
        models.PublicationClose.model_validate({"reason_code": "UNRESOLVABLE_LOG_EXPIRED", "reason": "r",
                                                "writer_stop": {"confirmed_stopped": False, "reference": None}, "evidence": []})
    f = {"scan_id": U1, "page": 0, "final_page": True, "scheduler_stop_reference": "r",
         "scans": [{"destination": "silver", "table_id": U1, "snapshot_version": 1},
                   {"destination": "quarantine", "table_id": U2, "snapshot_version": 1}], "tokens": []}
    models.RestoreFindings.model_validate(f)
    with pytest.raises(ValidationError):
        models.RestoreFindings.model_validate({**f, "scans": f["scans"][:1] * 2})
    with pytest.raises(ValidationError):
        models.RestoreFindings.model_validate({**f, "tokens": [{"destination": "silver", "txn_app_id": "a", "txn_version": 1,
                                                                "commit_version": None, "partition_values": None}] * 2001})


def test_manifest_format_and_hash_shape():
    models.CompatManifest.model_validate({"format": "dq-compat-v1", **versions()})
    v = versions()
    v["jars"]["deequ"] = "abc"
    with pytest.raises(ValidationError):
        models.CompatManifest.model_validate({"format": "dq-compat-v1", **v})
    with pytest.raises(ValidationError):
        models.CompatManifest.model_validate({"format": "dq-compat-v2", **versions()})
