import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.dialects import postgresql

from errors import ApiError
from store import Store

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
EID = uuid.UUID(int=7)


class FakeConn:
    def __init__(self):
        self.stmts = []

    def execute(self, stmt, *a, **kw):
        self.stmts.append(stmt)


def make_store(purpose_row=None) -> Store:
    s = Store.__new__(Store)  # no engine: unit-level seam
    s.clock = lambda: T0
    s.new_id = lambda: EID
    s.tx_retry = lambda fn: fn(s.conn)
    s.conn = FakeConn()
    s.require_dataset_row = lambda c, d: {}
    s.lock = lambda *a: None
    s._purpose = lambda *a, **kw: purpose_row
    s._event = lambda *a: None
    return s


def audit_stmts(c):
    return [x for x in c.stmts if x.table.name == "console_audit_events"]


def test_record_console_audit_inserts_one_row_with_clock_and_id():
    s = make_store()
    s.record_console_audit(s.conn, dataset_id="ds", actor="op", action="a.b", resource_type="r",
                           resource_id="1", details={"k": "v"})
    (stmt,) = s.conn.stmts
    assert stmt.table.name == "console_audit_events"
    p = stmt.compile(dialect=postgresql.dialect()).params
    assert p["event_id"] == EID and p["occurred_at"] == T0 and p["details"] == {"k": "v"}
    assert (p["dataset_id"], p["actor"], p["action"], p["resource_type"], p["resource_id"]) == ("ds", "op", "a.b", "r", "1")


def test_register_purpose_audits_only_new_rows():
    s = make_store()
    s._purpose = lambda *a, **kw: None
    calls = iter([None, {"workflow": "W", "purpose": "p", "dataset_id": "ds"}])
    s._purpose = lambda *a, **kw: next(calls)
    s.purpose_view = lambda row: {}
    s.register_purpose("op", "ds", "AUDIT_TABLE", "p", None)
    (stmt,) = audit_stmts(s.conn)
    p = stmt.compile(dialect=postgresql.dialect()).params
    assert p["action"] == "purpose.register" and p["resource_id"] == "AUDIT_TABLE/p"
    assert p["details"] == {"state_after": "ACTIVE"}


def test_register_purpose_replay_and_conflict_write_no_audit():
    s = make_store({"description": "d", "dataset_id": "ds", "workflow": "W", "purpose": "p"})
    s.purpose_view = lambda row: {}
    assert s.register_purpose("op", "ds", "W", "p", "d") == (200, {})
    with pytest.raises(ApiError):
        s.register_purpose("op", "ds", "W", "p", "other")
    assert not audit_stmts(s.conn)
