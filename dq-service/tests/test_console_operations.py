import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

import console_operations as ops
from errors import ApiError

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeStore:
    clock = staticmethod(lambda: T0)
    new_id = staticmethod(uuid.uuid4)

    @staticmethod
    def require_dataset_row(c, ds):
        return {}


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class FakeConn:
    def __init__(self, rows=()):
        self.rows, self.stmts = list(rows), []

    def execute(self, stmt):
        self.stmts.append(stmt)
        return FakeResult(self.rows)


def sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect()))


def test_settings_defaults_and_decimal_rendering():
    assert ops.settings_view(None)["version"] == 0 and ops.settings_view(None)["enabled"] is False
    row = dict(version=2, enabled=True, quarantine_ratio_threshold=Decimal("0.50"), no_report_after_seconds=60,
               violation_rate_threshold=None, violation_rate_min_reports=3, window_seconds=300, updated_at=T0,
               updated_by="op")
    v = ops.settings_view(row)
    assert v["quarantine_ratio_threshold"] == "0.5" and v["violation_rate_threshold"] is None
    assert v["updated_at"] == "2026-01-01T00:00:00.000000Z"


@pytest.mark.parametrize("bad", [
    {"expected_version": 0, "enabled": True, "quarantine_ratio_threshold": "1.5"},
    {"expected_version": 0, "enabled": True, "no_report_after_seconds": 59},
    {"expected_version": 0, "enabled": True, "window_seconds": 299},
    {"expected_version": 0, "enabled": "yes"},
    {"expected_version": 0, "enabled": True, "webhook_url": "http://x"},
    {"enabled": True},
])
def test_settings_body_rejects_invalid(bad):
    with pytest.raises(ValidationError):
        ops.AlertSettingsPut.model_validate(bad)


def test_ack_body_limits():
    assert ops.AlertAck.model_validate({"expected_version": 1}).note is None
    for bad in ({"expected_version": 0}, {"expected_version": 1, "note": "x" * 2049},
                {"expected_version": 1, "note": "a\x00"}, {"expected_version": 1, "extra": 1}):
        with pytest.raises(ValidationError):
            ops.AlertAck.model_validate(bad)


def test_list_alerts_scopes_dataset_and_filters():
    c = FakeConn()
    ops.list_alerts(c, FakeStore, "ds", "unacknowledged", "NO_REPORT", 10, None)
    q = sql(c.stmts[0])
    assert "dataset_id = %(dataset_id_1)s" in q and "acknowledged_at IS NULL" in q and "LIMIT" in q
    assert "details" in q and "lease_until" not in q


def test_list_filters_and_cursor_validated():
    c = FakeConn()
    for kw in ({"state": "x", "kind": None}, {"state": None, "kind": "x"}):
        with pytest.raises(ApiError):
            ops.list_alerts(c, FakeStore, "ds", kw["state"], kw["kind"], 10, None)
    with pytest.raises(ApiError):
        ops.list_alerts(c, FakeStore, "ds", None, None, 10, ["2026-01-01T00:00:00", str(uuid.uuid4())])
    with pytest.raises(ApiError):
        ops.list_audit_events(c, FakeStore, "ds", None, 10, ["2026-01-01T00:00:00Z", "nope"])
    assert not c.stmts


def test_acknowledge_unknown_id_is_404():
    with pytest.raises(ApiError) as e:
        ops.acknowledge(FakeConn(), FakeStore, "op", "ds", "not-a-uuid", ops.AlertAck(expected_version=1))
    assert e.value.code == "NOT_FOUND"
