"""
Gate thuần: plan đã pin + report đã validate -> quyết định. Không I/O, không fallback policy; mọi so sánh bằng Decimal/số nguyên.
Client chỉ gửi số đo; assertion, action, ngưỡng và schema `matches` đều do server tính lại.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from models import AuditReport, WriteReport

# tên kiểu Spark (đã bỏ tham số) -> họ kiểu của schema_type
_TYPE_FAMILY = {
    "string": "string", "boolean": "boolean", "int": "int", "integer": "int", "long": "long", "bigint": "long",
    "float": "float", "real": "float", "double": "double", "decimal": "decimal", "numeric": "decimal",
    "date": "date", "timestamp": "timestamp",
}
_OPS = {
    ">=": lambda v, a: v >= a["value"], ">": lambda v, a: v > a["value"],
    "<=": lambda v, a: v <= a["value"], "<": lambda v, a: v < a["value"],
    "==": lambda v, a: v == a["value"], "between": lambda v, a: a["min"] <= v <= a["max"],
}
_MESSAGES = {
    "RULE_VIOLATION_BLOCK": "Rule BLOCK bị vi phạm.",
    "RULE_VIOLATION_OBSERVE": "Rule OBSERVE bị vi phạm (chỉ cảnh báo).",
    "REQUIRED_MEASUREMENT_ERROR": "Rule bắt buộc không đo được.",
    "OPTIONAL_MEASUREMENT_ERROR": "Rule tuỳ chọn không đo được (cảnh báo).",
    "QUARANTINE_RATIO_EXCEEDED": "Tỉ lệ quarantine vượt ngưỡng policy.",
    "MIN_CANDIDATE_ROWS": "Số dòng candidate dưới mức tối thiểu của policy.",
    "MIN_CLEAN_ROWS": "Số dòng sạch dưới mức tối thiểu của policy.",
    "CLASSIFICATION_INCOMPLETE": "Phân loại dòng chưa hoàn tất.",
    "CLIENT_ERROR": "Client báo lỗi khi đánh giá (cảnh báo).",
}
_VETO = {"RULE_VIOLATION_BLOCK", "REQUIRED_MEASUREMENT_ERROR", "QUARANTINE_RATIO_EXCEEDED", "MIN_CANDIDATE_ROWS",
         "MIN_CLEAN_ROWS", "CLASSIFICATION_INCOMPLETE"}


@dataclass
class GateAssessment:
    decision: str | None
    assessment: str | None
    reasons: list[dict] = field(default_factory=list)
    rule_outcomes: dict[str, str] = field(default_factory=dict)


def _reason(code: str, rule_id: str | None = None) -> dict:
    return {"code": code, "rule_id": rule_id, "message": _MESSAGES[code]}


def _decimal(v) -> Decimal | None:
    """Số hữu hạn hoặc None (bool, NaN, Inf, chuỗi không phải số đều không hợp lệ)."""
    if v is None or isinstance(v, bool):
        return None
    try:
        d = Decimal(str(v))
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def _assertion(raw: dict) -> dict | None:
    out = {}
    for k in ("value", "min", "max"):
        if raw.get(k) is not None:
            out[k] = _decimal(raw[k])
            if out[k] is None:
                return None
    return out


def _family(t: str) -> str | None:
    return _TYPE_FAMILY.get(t.split("(", 1)[0].strip().lower())


def _schema_outcome(check: dict, evidence) -> str:
    if check["kind"] == "schema_columns":
        actual = {c.lower() for c in evidence.actual_columns}
        want = {c.lower() for c in check["columns"]}
        ok = actual == want if check.get("exact") else want <= actual
    else:
        ok = evidence.column.lower() == check["column"].lower() and _family(evidence.actual_type) == check["type"]
    return "PASS" if ok else "VIOLATION"


def _rule_outcome(check: dict, result) -> str:
    if result is None or result.measurement_status != "OK":
        return "ERROR"
    if check["kind"] in ("schema_columns", "schema_type"):
        return _schema_outcome(check, result.schema_evidence)
    value = _decimal(result.metric_value)
    if value is None:
        return "ERROR"
    raw = check.get("assert")
    if raw is None:
        return "PASS"
    assertion = _assertion(raw)
    if assertion is None:
        return "ERROR"
    return "PASS" if _OPS[raw["op"]](value, assertion) else "VIOLATION"


def _classify_rules(plan: dict, report) -> tuple[dict[str, str], list[dict], list[dict]]:
    """-> (rule_outcomes, reasons, rule_info[{rule_id, action, required, outcome}]) theo thứ tự rule_id."""
    results = {r.rule_id: r for r in report.results}
    rules = sorted((r for b in plan["stage_bindings"] for r in b["rules"]), key=lambda r: r["rule_id"])
    outcomes, reasons, info = {}, [], []
    for r in rules:
        rid = r["rule_id"]
        outcome = _rule_outcome(r["resolved_check"], results.get(rid))
        outcomes[rid] = outcome
        info.append({"rule_id": rid, "action": r["action"], "required": r["required"], "outcome": outcome})
        if outcome == "ERROR":
            reasons.append(_reason("REQUIRED_MEASUREMENT_ERROR" if r["required"] else "OPTIONAL_MEASUREMENT_ERROR", rid))
        elif outcome == "VIOLATION" and r["action"] != "QUARANTINE":
            reasons.append(_reason(f"RULE_VIOLATION_{r['action']}", rid))
    return outcomes, reasons, info


def _audit(outcomes: dict[str, str], reasons: list[dict], info: list[dict]) -> GateAssessment:
    if any(i["outcome"] == "ERROR" and i["required"] for i in info):
        verdict = "ERROR"
    elif "VIOLATION" in outcomes.values():
        verdict = "VIOLATION"
    else:
        verdict = "PASS"
    return GateAssessment(None, verdict, reasons, outcomes)


def _write(plan: dict, report: WriteReport, outcomes: dict[str, str], reasons: list[dict]) -> GateAssessment:
    policy = plan["policy_revision"]["payload"]
    if report.classification_status != "COMPLETE":
        reasons.append(_reason("CLASSIFICATION_INCOMPLETE"))
    if report.errors:
        reasons.append(_reason("CLIENT_ERROR"))
    if report.candidate_rows < policy["min_candidate_rows"]:
        reasons.append(_reason("MIN_CANDIDATE_ROWS"))
    if report.classification_status == "COMPLETE":
        if report.valid_rows < policy["min_clean_rows"]:
            reasons.append(_reason("MIN_CLEAN_ROWS"))
        # q/c > ratio  <=>  q > ratio*c, chính xác bằng Fraction; candidate=0 cho ratio 0 (q=0 do COMPLETE)
        if Fraction(report.quarantine_rows) > Fraction(policy["max_quarantine_ratio"]) * report.candidate_rows:
            reasons.append(_reason("QUARANTINE_RATIO_EXCEEDED"))
    deny = any(r["code"] in _VETO for r in reasons)
    return GateAssessment("DENY" if deny else "ALLOW", None, reasons, outcomes)


def evaluate(plan: dict, report: WriteReport | AuditReport) -> GateAssessment:
    outcomes, reasons, info = _classify_rules(plan, report)
    if plan["workflow"] == "AUDIT_TABLE":
        return _audit(outcomes, reasons, info)
    return _write(plan, report, outcomes, reasons)
