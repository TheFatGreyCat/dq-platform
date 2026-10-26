import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import canonical as c

VECTORS = json.loads((Path(__file__).parents[2] / "jvm/dq-client/src/test/resources/protocol-v3.json").read_text("utf-8"))


@pytest.mark.parametrize("v", VECTORS["canonical_vectors"], ids=lambda v: v["name"])
def test_shared_vectors(v):
    o = c.parse_json(v["json"].encode())
    assert c.canonical_bytes(o).decode() == v["canonical"]
    assert c.content_hash(o) == v["hash"]


@pytest.mark.parametrize("v", VECTORS["hash_vectors"], ids=lambda v: v["name"])
def test_shared_metric_hashes(v):
    assert c.metric_hash(v["check"], VECTORS["semantics_version"]) == v["metric_hash"]


def test_hand_checked_canonical_forms():
    o = c.parse_json(b'{"b":1,"a":[true,null,"\xc3\xa9"],"x":1.50,"y":-0.0,"z":1E+2}')
    assert c.canonical_bytes(o) == '{"a":[true,null,"é"],"b":1,"x":1.5,"y":0,"z":100}'.encode()


def test_decimals_table():
    for raw, want in VECTORS["decimals"].items():
        assert c.canonical_decimal(raw) == want
    assert c.canonical_decimal(Decimal("-0.0")) == "0"
    assert c.canonical_decimal(5) == "5"


@pytest.mark.parametrize("bad", VECTORS["rejected_json"])
def test_parse_rejects(bad):
    with pytest.raises(ValueError):
        c.parse_json(bad.encode())


def test_parse_rejects_bad_utf8_and_bom():
    with pytest.raises(ValueError):
        c.parse_json(b'{"a":"\xff"}')
    with pytest.raises(ValueError):
        c.parse_json(b'\xef\xbb\xbf{"a":1}')


def test_parse_types():
    o = c.parse_json(b'{"i":3,"d":1.5}')
    assert type(o["i"]) is int and isinstance(o["d"], Decimal)


def test_canonical_bytes_rejects_float_and_bad_decimal():
    with pytest.raises(TypeError):
        c.canonical_bytes({"a": 1.5})
    with pytest.raises(ValueError):
        c.canonical_decimal("NaN")
    with pytest.raises(ValueError):
        c.canonical_decimal("abc")


def test_uuid_and_timestamp():
    assert c.canonical_uuid("AB3F2C1E-0000-4000-8000-000000000001") == "ab3f2c1e-0000-4000-8000-000000000001"
    tz = timezone(timedelta(hours=7))
    assert c.canonical_timestamp(datetime(2026, 10, 20, 19, 0, tzinfo=tz)) == "2026-10-20T12:00:00.000000Z"
    assert c.canonical_timestamp("2026-10-20T12:00:00Z") == "2026-10-20T12:00:00.000000Z"
    with pytest.raises(ValueError):
        c.canonical_timestamp(datetime(2026, 10, 20, 12))
    with pytest.raises(ValueError):
        c.canonical_timestamp("2026-10-20T12:00:00")


def test_metric_hash_ignores_assertion_but_not_semantics():
    a = {"kind": "range", "column": "x", "min": "0", "max": "5", "assert": {"op": ">=", "value": "1"}}
    b = {**a, "assert": {"op": "==", "value": "1"}}
    assert c.metric_hash(a) == c.metric_hash(b) == c.measurement_hash(a)
    assert c.metric_hash(a) != c.metric_hash(a, "other")
    assert c.metric_hash(a) != c.metric_hash({**a, "max": "6"})


def test_metric_names_and_sorting():
    assert c.METRIC_NAMES["size"] == ("Size", "INTEGER")
    assert c.METRIC_NAMES["schema_type"] == (None, "SCHEMA")
    assert [r["rule_id"] for r in c.sort_rules([{"rule_id": "b"}, {"rule_id": "a"}])] == ["a", "b"]
    ev = [{"event_time": "t2", "execution_id": "a"}, {"event_time": "t1", "execution_id": "z"}, {"event_time": "t1", "execution_id": "b"}]
    assert [e["execution_id"] for e in c.sort_context(ev)] == ["b", "z", "a"]


@pytest.mark.parametrize("v", VECTORS["schema_vectors"], ids=lambda v: v["name"])
def test_schema_hash_vectors(v):
    assert c.schema_hash(v["fields"]) == v["schema_hash"]
    assert c.schema_hash(list(reversed(v["fields"]))) != v["schema_hash"]


def test_escapes_exact():
    assert c.canonical_bytes({"k\x01\"\n": "\t\x1f\x7f😀"}) == b'{"k\\u0001\\"\\n":"\\t\\u001f\x7f\xf0\x9f\x98\x80"}'
