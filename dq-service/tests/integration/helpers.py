"""Synthetic fixtures for HTTP integration tests (real PostgreSQL via DQ_TEST_DATABASE_URL)."""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone

from canonical import schema_hash
from models import RUNTIME_CONF

DT = "2026-10-20"
H0 = "sha256:" + "0" * 64
BUSINESS = [{"name": "id", "type": "long", "nullable": True}, {"name": "value", "type": "string", "nullable": True},
            {"name": "amount", "type": "decimal(18,2)", "nullable": True},
            {"name": "event_time", "type": "timestamp", "nullable": True},
            {"name": "dt", "type": "date", "nullable": True}]
VERSIONS = {"spark_version": "3.5.1", "scala_version": "2.12.18", "java_major": 17, "hadoop_version": "3.3.4",
            "jars": {k: "sha256:" + hashlib.sha256(k.encode()).hexdigest()
                     for k in ("delta_spark", "delta_storage", "hadoop_aws", "deequ", "dq_client")}}
ROLES = ("viewer", "editor", "approver", "operator")


def token_registry_dict(datasets: list[str]) -> tuple[dict, dict]:
    """principals: editor, approver, operator, viewer, job_gate, job_audit (per dataset grants), other_job."""
    tokens, principals = {}, []

    def add(name, grants):
        tok = f"tok-{name}-{uuid.uuid4().hex}"
        tokens[name] = tok
        principals.append({"principal": name, "token_sha256": hashlib.sha256(tok.encode()).hexdigest(),
                           "grants": grants})
    for r in ROLES:
        add(r, [{"dataset_id": d, "role": r, "workflow": None, "purpose": None} for d in datasets])
    add("editor2", [{"dataset_id": d, "role": "editor", "workflow": None, "purpose": None} for d in datasets]
        + [{"dataset_id": d, "role": "approver", "workflow": None, "purpose": None} for d in datasets])
    add("job_gate", [{"dataset_id": d, "role": "job", "workflow": "WRITE_GATE", "purpose": "default"}
                     for d in datasets])
    add("job_audit", [{"dataset_id": d, "role": "job", "workflow": "AUDIT_TABLE", "purpose": "nightly"}
                      for d in datasets])
    add("job_other", [{"dataset_id": datasets[0], "role": "job", "workflow": "AUDIT_TABLE", "purpose": "other"}])
    return {"principals": principals}, tokens


def registration(retry=3600, cleanup=False, duration=None) -> dict:
    sil, q = str(uuid.uuid4()), str(uuid.uuid4())
    root = f"s3a://dq-test/{uuid.uuid4().hex}"
    biz_hash = schema_hash([{**c, "metadata": {}} for c in BUSINESS])
    return {"owner": "team-dq", "candidate_root": f"{root}/candidates", "allowed_storage_roots": [root],
            "silver": {"uri": f"{root}/silver", "table_id": sil, "schema_hash": biz_hash,
                       "partition_schema_hash": H0, "set_transaction_retention": None},
            "quarantine": {"uri": f"{root}/quarantine", "table_id": q, "schema_hash": H0,
                           "partition_schema_hash": H0, "set_transaction_retention": None},
            "business_schema": BUSINESS, "partition_schema": [{"name": "dt", "type": "date", "nullable": True}],
            "history_context": "daily",
            "retention": {"candidate_duration_seconds": duration, "report_duration_seconds": None,
                          "retry_window_seconds": retry, "transaction_metadata_duration_seconds": None,
                          "cleanup_enabled": cleanup}}


def gate_suite(sql=False) -> dict:
    rules = [
        {"rule_id": "candidate_min", "dimension": "completeness", "scope": "PARTITION", "stage": "CANDIDATE",
         "check": {"kind": "size", "assert": {"op": ">=", "value": 1}}},
        {"rule_id": "value_complete", "dimension": "completeness", "scope": "PARTITION", "stage": "CANDIDATE",
         "check": {"kind": "complete", "column": "value"}},
        {"rule_id": "amount_range", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
         "check": {"kind": "range", "column": "amount", "min": "0", "max": "100"}},
        {"rule_id": "clean_min", "dimension": "completeness", "scope": "PARTITION", "stage": "CLEAN",
         "check": {"kind": "size", "assert": {"op": ">=", "value": 1}}},
        {"rule_id": "amount_type", "dimension": "validity", "scope": "PARTITION", "stage": "RAW",
         "check": {"kind": "schema_type", "column": "amount", "type": "decimal"}},
    ]
    if sql:
        rules.append({"rule_id": "ts_in_partition", "dimension": "consistency", "scope": "PARTITION",
                      "stage": "CANDIDATE", "check": {"kind": "sql_predicate", "columns": ["event_time"],
                                                     "predicate": "event_time IS NOT NULL"}})
    return {"format": "dq-rules/v3", "parameters": {}, "rules": rules}


def gate_policy(suite: dict, ratio="0.05", anomaly=None) -> dict:
    actions = {"candidate_min": "BLOCK", "value_complete": "QUARANTINE", "amount_range": "QUARANTINE",
               "clean_min": "BLOCK", "amount_type": "BLOCK", "ts_in_partition": "QUARANTINE"}
    return {"rules": [{"rule_id": r["rule_id"], "action": actions[r["rule_id"]], "required": True}
                      for r in suite["rules"]],
            "min_candidate_rows": 1, "min_clean_rows": 1, "max_quarantine_ratio": ratio,
            "anomaly": anomaly or {"enabled": False}}


def snap(ref: dict, version: int) -> dict:
    return {"uri": ref["uri"], "table_id": ref["table_id"], "version": version, "schema_hash": ref["schema_hash"],
            "set_transaction_retention": None}


def execution_request(dataset: str, reg: dict, manifest_id: str, *, event_time=None, previous=None) -> dict:
    return {"dataset_id": dataset, "workflow": "WRITE_GATE", "purpose": "default",
            "event_time": (event_time or datetime(2026, 10, 20, 23, tzinfo=timezone.utc)).isoformat(),
            "parameters": {}, "history_context": "daily", "partition_values": {"dt": DT}, "input_snapshot": None,
            "target_bases": {"silver": snap(reg["silver"], 3), "quarantine": snap(reg["quarantine"], 1)},
            "compat_manifest_id": manifest_id, "previous_execution_id": previous}


def result_for(rule: dict, stage: dict, *, value=None, status="OK", evidence=None, error=None) -> dict:
    kind = rule["kind"]
    base = {"rule_id": rule["rule_id"], "measurement_definition_hash": rule["measurement_definition_hash"],
            "scope": stage["scope"], "stage": stage["stage"], "kind": kind, "measurement_status": status,
            "metric_name": None, "metric_value": None, "schema_evidence": None, "error": None, "failed_rows": None}
    if status == "ERROR":
        base["error"] = error or {"code": "STORAGE_UNAVAILABLE", "params": {}}
        return base
    if kind in ("schema_columns", "schema_type"):
        base["schema_evidence"] = evidence
        return base
    base["metric_name"] = rule["metric_name"]
    base["metric_value"] = value
    return base


def write_report(plan: dict, attempt_id: str, *, candidate=100, quarantine=3, overrides=None) -> dict:
    overrides = overrides or {}
    clean = candidate - quarantine
    values = {"candidate_min": candidate, "clean_min": clean,
              "value_complete": "0.98", "amount_range": "0.98", "ts_in_partition": "0.99"}
    results = []
    for sb in plan["stage_bindings"]:
        for r in sb["rules"]:
            if r["rule_id"] in overrides:
                results.append({**result_for(r, sb), **overrides[r["rule_id"]]})
            elif r["kind"] == "schema_type":
                results.append(result_for(r, sb, evidence={"column": "amount", "actual_type": "decimal(18,2)",
                                                           "matches": True}))
            else:
                results.append(result_for(r, sb, value=values[r["rule_id"]]))
    root = plan["candidate_locations"]["execution_root"]
    ref = lambda kind: {"uri": f"{root}/{attempt_id}/{kind}", "table_id": str(uuid.uuid4()), "version": 0,  # noqa
                        "schema_hash": H0, "set_transaction_retention": None}
    return {"attempt_id": attempt_id, "dataset_id": plan["dataset_id"], "plan_hash": plan["plan_hash"],
            "suite_revision_id": plan["suite_revision"]["revision_id"],
            "policy_revision_id": plan["policy_revision"]["revision_id"], "engine": plan["engine"],
            "interpreter_version": plan["interpreter_version"], "runtime_conf": dict(RUNTIME_CONF),
            "runtime_versions": VERSIONS, "parameters": plan["parameters"], "results": results,
            "raw_reference": ref("raw"), "classified_reference": ref("classified"), "business_schema_hash": H0,
            "classified_schema_hash": H0, "classification_status": "COMPLETE", "candidate_rows": candidate,
            "valid_rows": clean, "quarantine_rows": quarantine, "clean_selector": "size(_dq_errors)=0",
            "quarantine_selector": "size(_dq_errors)>0", "errors": []}


class FixedClock:
    def __init__(self, start=datetime(2026, 10, 21, 1, tzinfo=timezone.utc)):
        self.now = start

    def __call__(self):
        self.now += timedelta(microseconds=1)
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)
