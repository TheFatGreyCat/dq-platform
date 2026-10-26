"""
Ước lượng cận trên kích thước report (canonical UTF-8 JSON) để từ chối cấu hình không thể vừa trần 1 MiB
ngay lúc activate / tạo plan, thay vì để runtime gặp 413. Cận trên được đo bằng chính canonical_bytes trên
một report "xấu nhất" dựng từ plan: mỗi rule lấy max(hình OK, hình ERROR), schema evidence tối đa, 50 errors.
"""
from __future__ import annotations

from pydantic import BaseModel

from canonical import canonical_bytes
from errors import ApiError, RUNTIME_COMPONENTS
from models import METRIC_NAMES, RUNTIME_CONF

LIMIT = 1_048_576
MAX_URI = 2048
MAX_PARAM_VALUE = 1024
MAX_ID = 128
MAX_ERRORS = 50
MAX_SCHEMA_COLUMNS = 1000
MAX_DECIMAL = 384

_CTRL = "\x01"          # 1 ký tự -> "\u0001" = 6 byte sau escape: xấu nhất cho free-text mọi ký tự
_WIDE = "\U00010000"    # 4 byte UTF-8, không bị escape: xấu nhất cho URI (không chứa control char)
_UUID = "0" * 36
_HASH = "sha256:" + "0" * 64
_INT64 = -(2**63)
_FQCN = "a" * 256


def _dump(o):
    return o.model_dump(mode="json", by_alias=True) if isinstance(o, BaseModel) else o


def _size(o) -> int:
    return len(canonical_bytes(o))


def _error_variants(rule_id: str, column: str) -> list[dict]:
    comp = max(RUNTIME_COMPONENTS, key=len)
    return [
        {"code": "CAST_OVERFLOW", "params": {"rule_id": rule_id, "column": column, "exception_class": _FQCN}},
        {"code": "SQL_ANALYSIS_ERROR", "params": {"rule_id": rule_id, "exception_class": _FQCN}},
        {"code": "UNCLASSIFIED_ENGINE_ERROR", "params": {"rule_id": rule_id, "exception_class": _FQCN}},
        {"code": "COLUMN_NOT_FOUND", "params": {"rule_id": rule_id, "column": column}},
        {"code": "RUNTIME_MISMATCH", "params": {"component": comp, "expected": "a" * 128, "actual": "a" * 128}},
        {"code": "STORAGE_UNAVAILABLE", "params": {"exception_class": _FQCN}},
    ]


def _worst_error(rule_id: str, column: str) -> dict:
    return max(_error_variants(rule_id, column), key=_size)


def _result_variants(rule: dict, scope: str, stage: str, column: str) -> list[dict]:
    check = rule["resolved_check"]
    kind = check["kind"]
    base = {"rule_id": rule["rule_id"], "measurement_definition_hash": _HASH, "scope": scope, "stage": stage,
            "kind": kind, "failed_rows": 2**63 - 1}
    err = {**base, "measurement_status": "ERROR", "metric_name": None, "metric_value": None,
           "schema_evidence": None, "error": _worst_error(rule["rule_id"], column)}
    if kind == "schema_columns":
        ev = {"actual_columns": [_CTRL * MAX_ID] * MAX_SCHEMA_COLUMNS, "matches": False}
    elif kind == "schema_type":
        ev = {"column": check["column"], "actual_type": _CTRL * MAX_ID, "matches": False}
    else:
        ev = None
    if ev is not None:
        ok = {**base, "measurement_status": "OK", "metric_name": None, "metric_value": None,
              "schema_evidence": ev, "error": None}
    else:
        name = METRIC_NAMES[kind]
        value = _INT64 if name == "Size" else "-" + "9" * (MAX_DECIMAL - 1)
        ok = {**base, "measurement_status": "OK", "metric_name": name, "metric_value": value,
              "schema_evidence": None, "error": None}
    return [ok, err]


def _ref(uri: str) -> dict:
    return {"uri": uri, "table_id": _UUID, "version": 2**63 - 1, "schema_hash": _HASH, "set_transaction_retention": None}


def _longest(strings, default: str = "a") -> str:
    return max(strings, key=lambda s: len(canonical_bytes(s)), default=default)


def max_report_bytes(plan: dict) -> int:
    """Cận trên byte của canonical WriteReport/AuditReport hợp lệ cho plan này."""
    rules = [(b, r) for b in plan["stage_bindings"] for r in b["rules"]]
    column = _longest(c["name"] for c in plan["registration_snapshot"]["business_schema"])
    longest_rule = _longest(r["rule_id"] for _, r in rules)
    results = sum(max(_size(v) for v in _result_variants(r, b["scope"], b["stage"], column)) for b, r in rules)
    results += max(len(rules) - 1, 0) + 2  # dấu phẩy + ngoặc mảng

    manifest = {k: v for k, v in plan["compat_manifest"]["payload"].items() if k != "format"}
    common = {
        "attempt_id": _UUID, "dataset_id": plan["dataset_id"], "plan_hash": _HASH,
        "suite_revision_id": _UUID, "policy_revision_id": _UUID,
        "engine": plan["engine"], "interpreter_version": plan["interpreter_version"],
        "runtime_conf": RUNTIME_CONF, "runtime_versions": manifest, "parameters": plan["parameters"],
        "results": [],
    }
    if plan["workflow"] == "AUDIT_TABLE":
        snap = plan.get("input_snapshot") or plan["registration_snapshot"]["silver"]
        specific = {"audit_reference": _ref(snap["uri"]), "measured_rows": 2**63 - 1,
                    "classification_status": "NOT_APPLICABLE"}
    else:
        root = plan["candidate_locations"]["execution_root"].rstrip("/") + "/" + _UUID
        specific = {
            "raw_reference": _ref(root + "/raw"), "classified_reference": _ref(root + "/classified"),
            "business_schema_hash": _HASH, "classified_schema_hash": _HASH, "classification_status": "INCOMPLETE",
            "candidate_rows": 2**63 - 1, "valid_rows": 2**63 - 1, "quarantine_rows": 2**63 - 1,
            "clean_selector": "size(_dq_errors)=0", "quarantine_selector": "size(_dq_errors)>0",
            "errors": [_worst_error(longest_rule, column)] * MAX_ERRORS,
        }
    return _size({**common, **specific}) + results


def check_budget(n: int) -> None:
    if n > LIMIT:
        raise ApiError("INVALID_CONFIG", "payload_budget")


def activation_estimate(registration, suite, policy) -> int:
    """Cận trên report cho cấu hình sắp activate: URI tối đa 2048, tham số tối đa 1024 byte, UUID đủ dài."""
    reg, suite, policy = _dump(registration), _dump(suite), _dump(policy)
    action = {p["rule_id"]: p for p in policy["rules"]}
    wide = _WIDE * MAX_URI
    rules = [{"rule_id": r["rule_id"], "resolved_check": r["check"], "action": action[r["rule_id"]]["action"],
              "required": action[r["rule_id"]]["required"], "scope": r["scope"], "stage": r["stage"]}
             for r in suite["rules"]]
    bindings: dict[tuple, dict] = {}
    for r in rules:
        b = bindings.setdefault((r["scope"], r["stage"]), {"scope": r["scope"], "stage": r["stage"], "rules": []})
        b["rules"].append(r)
    workflow = suite.get("workflow") or ("AUDIT_TABLE" if any(r["stage"] == "AUDIT_SNAPSHOT" for r in rules) else "WRITE_GATE")
    plan = {
        "workflow": workflow, "dataset_id": "a" * MAX_ID, "engine": "a" * MAX_ID, "interpreter_version": "a" * MAX_ID,
        "stage_bindings": list(bindings.values()),
        "parameters": {name: "a" * MAX_PARAM_VALUE for name in suite.get("parameters", {})},
        "registration_snapshot": {"business_schema": reg["business_schema"], "silver": {"uri": wide}},
        "compat_manifest": {"payload": {"spark_version": "a" * 64, "scala_version": "a" * 64, "java_major": 1000,
                                         "hadoop_version": "a" * 64,
                                         "jars": {k: _HASH for k in ("delta_spark", "delta_storage", "hadoop_aws",
                                                                     "deequ", "dq_client")}}},
        # execution_root + "/" + uuid + "/classified" phải còn trong cận URI 2048 ký tự
        "candidate_locations": {"execution_root": _WIDE * (MAX_URI - len("/" + _UUID + "/classified"))},
        "input_snapshot": {"uri": wide},
    }
    return max_report_bytes(plan)
