"""Canonical JSON + hashing for protocol v3. Stdlib only; vectors shared with Scala (protocol-v3.json)."""
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

SEMANTICS_VERSION = "dq-semantics-v3-1:dq-runtime-v1"
CANONICALIZATION_VERSION = "v1"

# kind -> (metric_name | None, value_type)
METRIC_NAMES = {
    "size": ("Size", "INTEGER"),
    "complete": ("Completeness", "DECIMAL"),
    "completeness": ("Completeness", "DECIMAL"),
    "unique": ("Uniqueness", "DECIMAL"),
    "pattern": ("Compliance", "DECIMAL"),
    "range": ("Compliance", "DECIMAL"),
    "in_set": ("Compliance", "DECIMAL"),
    "sql_predicate": ("Compliance", "DECIMAL"),
    "mean": ("Mean", "DECIMAL"),
    "sql_metric": ("SqlMetric", "DECIMAL"),
    "schema_columns": (None, "SCHEMA"),
    "schema_type": (None, "SCHEMA"),
}

_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$")


def _no_dupes(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError("duplicate key")
        out[k] = v
    return out


def _bad_constant(name):
    raise ValueError("non-finite number")


def parse_json(body: bytes):
    text = body.decode("utf-8")  # strict: invalid UTF-8 raises UnicodeDecodeError (a ValueError)
    if text.startswith("\ufeff"):
        raise ValueError("BOM not allowed")
    return json.loads(text, object_pairs_hook=_no_dupes, parse_float=Decimal, parse_constant=_bad_constant)


def canonical_decimal(value) -> str:
    if isinstance(value, bool):
        raise TypeError("bool is not a decimal")
    try:
        d = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError, TypeError) as e:
        raise ValueError("invalid decimal") from e
    if not d.is_finite():
        raise ValueError("non-finite decimal")
    if d == 0:
        return "0"
    return format(d.normalize(), "f")


def _render(v, out: list):
    if v is None:
        out.append("null")
    elif v is True:
        out.append("true")
    elif v is False:
        out.append("false")
    elif isinstance(v, int):
        out.append(str(v))
    elif isinstance(v, Decimal):
        out.append(canonical_decimal(v))
    elif isinstance(v, float):
        raise TypeError("float not allowed in canonical JSON")
    elif isinstance(v, str):
        out.append(json.dumps(v, ensure_ascii=False))
    elif isinstance(v, (list, tuple)):
        out.append("[")
        for i, x in enumerate(v):
            if i:
                out.append(",")
            _render(x, out)
        out.append("]")
    elif isinstance(v, dict):
        out.append("{")
        for i, k in enumerate(sorted(v)):
            if not isinstance(k, str):
                raise TypeError("object keys must be strings")
            if i:
                out.append(",")
            out.append(json.dumps(k, ensure_ascii=False) + ":")
            _render(v[k], out)
        out.append("}")
    else:
        raise TypeError(f"unsupported type {type(v).__name__}")


def canonical_bytes(value) -> bytes:
    out: list = []
    _render(value, out)
    return "".join(out).encode("utf-8")


def content_hash(value) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def canonical_uuid(s) -> str:
    return str(uuid.UUID(str(s)))


def canonical_timestamp(value) -> str:
    if isinstance(value, str):
        if not _TS.match(value):
            raise ValueError("invalid timestamp")
        value = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive timestamp")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _measurement_fields(check: dict) -> dict:
    return {k: v for k, v in check.items() if k != "assert"}


def metric_hash(check: dict, semantics_version: str = SEMANTICS_VERSION) -> str:
    return content_hash({"check": _measurement_fields(check), "semantics_version": semantics_version})


def measurement_hash(resolved_check: dict, semantics_version: str = SEMANTICS_VERSION) -> str:
    return content_hash({"check": _measurement_fields(resolved_check), "semantics_version": semantics_version})


def sort_rules(rules: list) -> list:
    return sorted(rules, key=lambda r: r["rule_id"])


def sort_context(events: list) -> list:
    return sorted(events, key=lambda e: (e["event_time"], e["execution_id"]))


def schema_hash(fields: list) -> str:
    """Hash of the ordered Spark StructType JSON shape (field order, nullability, metadata preserved)."""
    return content_hash({"type": "struct", "fields": [
        {"name": f["name"], "type": f["type"], "nullable": f["nullable"], "metadata": f.get("metadata", {})}
        for f in fields]})
