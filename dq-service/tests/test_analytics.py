from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.dialects import postgresql

import analytics as an

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
T1 = T0 + timedelta(days=1)
H = "sha256:" + "a" * 64


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeConn:
    """Returns queued row lists in call order and records the statements."""

    def __init__(self, *results):
        self.results, self.stmts = list(results), []

    def execute(self, stmt):
        self.stmts.append(stmt)
        return FakeResult(self.results.pop(0))


def sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": False}))


def test_fold_executions_zero_fills_and_ratio():
    out = an.fold_executions([("COMPLETED", 2), ("FAILED", 1)])
    assert out["total"] == "3" and out["by_state"]["PLANNED"] == "0" and out["by_state"]["FAILED"] == "1"
    assert out["failed_ratio"] == an.canonical_decimal(Decimal(1) / Decimal(3))
    assert an.fold_executions([])["failed_ratio"] is None


def test_fold_decisions_exact_ratio_and_null():
    assert an.fold_decisions([("ALLOW", 3), ("DENY", 1)]) == {"ALLOW": "3", "DENY": "1", "deny_ratio": "0.25"}
    assert an.fold_decisions([])["deny_ratio"] is None


def test_fold_rules_failed_rows_only_from_ok():
    out = an.fold_rules([("OK", 3, Decimal(10)), ("ERROR", 1, None)])
    assert out == {"total": "4", "OK": "3", "ERROR": "1", "error_ratio": "0.25", "failed_rows": "10"}
    assert an.fold_rules([])["error_ratio"] is None


def test_unknown_key_rejected():
    with pytest.raises(KeyError):
        an.fold_decisions([("MAYBE", 1)])


def test_aggregate_keeps_workflows_separate():
    conn = FakeConn([("WRITE_GATE", "COMPLETED", 4), ("AUDIT_TABLE", "FAILED", 1)],
                    [("ALLOW", 3), ("DENY", 1)],
                    [("OK", 5, 7), ("ERROR", 5, None)])
    out = an.aggregate_dataset(conn, "ds", T0, T1)
    wg, au = out["workflows"]["WRITE_GATE"], out["workflows"]["AUDIT_TABLE"]
    assert wg["executions"]["total"] == "4" and wg["decisions"]["deny_ratio"] == "0.25" and "rules" not in wg
    assert au["executions"]["total"] == "1" and au["rules"]["failed_rows"] == "7" and "decisions" not in au
    assert out["window"] == {"start": "2026-01-01T00:00:00.000000Z", "end": "2026-01-02T00:00:00.000000Z"}


def test_aggregate_workflow_filter_skips_other_queries():
    conn = FakeConn([], [])
    out = an.aggregate_dataset(conn, "ds", T0, T1, workflow="AUDIT_TABLE", purpose="p")
    assert list(out["workflows"]) == ["AUDIT_TABLE"] and len(conn.stmts) == 2
    assert "gate_decisions" not in sql(conn.stmts[1])
    conn = FakeConn([], [])
    an.aggregate_dataset(conn, "ds", T0, T1, workflow="WRITE_GATE")
    assert "rule_results" not in "".join(sql(s) for s in conn.stmts)


def test_statements_use_half_open_utc_window_and_workflow_guard():
    s = sql(an.rules_stmt("ds", T0, T1))
    assert "executions.event_time >= %(event_time_1)s" in s and "executions.event_time < %(event_time_2)s" in s
    assert "executions.workflow = %(workflow_1)s" in s
    assert an.rules_stmt("ds", T0, T1).compile().params["workflow_1"] == "AUDIT_TABLE"
    assert an.decisions_stmt("ds", T0, T1).compile().params["workflow_1"] == "WRITE_GATE"


def test_window_validation_and_offset_normalised():
    with pytest.raises(ValueError):
        an.aggregate_dataset(FakeConn(), "ds", T0.replace(tzinfo=None), T1)
    with pytest.raises(ValueError):
        an.aggregate_dataset(FakeConn(), "ds", T1, T0)
    with pytest.raises(ValueError):
        an.aggregate_dataset(FakeConn(), "ds", T0, T1, workflow="OTHER")
    plus7 = timezone(timedelta(hours=7))
    out = an.aggregate_dataset(FakeConn([], []), "ds", T0.astimezone(plus7), T1, workflow="WRITE_GATE")
    assert out["window"]["start"] == "2026-01-01T00:00:00.000000Z"


def row(i, value):
    return (f"00000000-0000-0000-0000-00000000000{i}", f"10000000-0000-0000-0000-00000000000{i}",
            T0 + timedelta(hours=i), value)


def test_rule_history_orders_oldest_first_and_flags_truncation():
    newest_first = [row(3, Decimal("3.50")), row(2, Decimal("2")), row(1, Decimal("1"))]
    out = an.rule_history(FakeConn(newest_first), "ds", H, "PARTITION", "CANDIDATE", H, T0, T1, 2)
    assert out["truncated"] is True
    assert [p["value"] for p in out["points"]] == ["2", "3.5"]
    assert out["points"][0]["event_time"] == "2026-01-01T02:00:00.000000Z"
    full = an.rule_history(FakeConn(newest_first[:2]), "ds", H, "PARTITION", "CANDIDATE", H, T0, T1, 2)
    assert full["truncated"] is False and len(full["points"]) == 2


def test_rule_history_statement_filters_and_validation():
    conn = FakeConn([])
    an.rule_history(conn, "ds", H, "PARTITION", "CANDIDATE", H, T0, T1, 5)
    params = conn.stmts[0].compile().params
    assert params["workflow_1"] == "AUDIT_TABLE" and params["param_1"] == 6
    s = sql(conn.stmts[0])
    assert "metric_observations.event_time >= " in s and "metric_observations.event_time < " in s
    assert "ORDER BY metric_observations.event_time DESC, metric_observations.execution_id DESC" in s
    for bad in (0, an.MAX_HISTORY_LIMIT + 1):
        with pytest.raises(ValueError):
            an.rule_history(FakeConn(), "ds", H, "PARTITION", "CANDIDATE", H, T0, T1, bad)
    with pytest.raises(ValueError):
        an.rule_history(FakeConn(), "ds", H, "PARTITION", "CANDIDATE", H, T0, T1, 1, workflow="X")
