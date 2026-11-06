"""Read-only analytics over the execution schema (SQLAlchemy Core, no app/route coupling).

Windows are UTC half-open [start, end) on executions.event_time (history: metric_observations.event_time).
Counts are exact integers rendered as strings; ratios are canonical Decimal strings or None when the
denominator is 0. WRITE_GATE (gate decisions) and AUDIT_TABLE (measured rule results) are aggregated by
separate queries and reported under separate workflow keys; they are never summed together.
Pagination is server-side only: rule_history returns the newest `limit` points plus a `truncated` flag.
"""
from datetime import datetime, timezone
from decimal import Decimal

import sqlalchemy as sa

from canonical import canonical_decimal, canonical_timestamp
from schema import executions as ex, gate_decisions as gd, metric_observations as mo, rule_results as rr

WRITE_GATE, AUDIT = "WRITE_GATE", "AUDIT_TABLE"
WORKFLOWS = (WRITE_GATE, AUDIT)
STATES = ("PLANNED", "RUNNING", "REPORTED", "COMPLETED", "FAILED", "CANCELLED")
DECISIONS = ("ALLOW", "DENY")
MEASUREMENT_STATUSES = ("OK", "ERROR")
MAX_HISTORY_LIMIT = 10_000


def _utc_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    for v in (start, end):
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("naive datetime")
    s, e = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if s >= e:
        raise ValueError("empty window")
    return s, e


def _count(n) -> str:
    return str(int(n or 0))


def _ratio(num, den) -> str | None:
    return canonical_decimal(Decimal(int(num)) / Decimal(int(den))) if den else None


def _tally(rows, keys) -> dict[str, int]:
    """rows of (key, n) -> {key: n} zero-filled over `keys`; unknown keys are a schema violation."""
    out = dict.fromkeys(keys, 0)
    for k, n in rows:
        out[k] += int(n)
    return out


def fold_executions(rows) -> dict:
    by_state = _tally(rows, STATES)
    total = sum(by_state.values())
    return {"total": _count(total), "by_state": {k: _count(v) for k, v in by_state.items()},
            "failed_ratio": _ratio(by_state["FAILED"], total)}


def fold_decisions(rows) -> dict:
    d = _tally(rows, DECISIONS)
    return {"ALLOW": _count(d["ALLOW"]), "DENY": _count(d["DENY"]),
            "deny_ratio": _ratio(d["DENY"], d["ALLOW"] + d["DENY"])}


def fold_rules(rows) -> dict:
    """rows of (measurement_status, n, failed_rows_sum)."""
    st = _tally(((s, n) for s, n, _ in rows), MEASUREMENT_STATUSES)
    total = sum(st.values())
    failed = sum(int(f or 0) for s, _, f in rows if s == "OK")
    return {"total": _count(total), "OK": _count(st["OK"]), "ERROR": _count(st["ERROR"]),
            "error_ratio": _ratio(st["ERROR"], total), "failed_rows": _count(failed)}


def _execution_filter(dataset_id, start, end, workflow, purpose) -> list:
    conds = [ex.c.dataset_id == dataset_id, ex.c.event_time >= start, ex.c.event_time < end]
    if workflow:
        conds.append(ex.c.workflow == workflow)
    if purpose:
        conds.append(ex.c.purpose == purpose)
    return conds


def executions_stmt(dataset_id, start, end, workflow=None, purpose=None):
    return (sa.select(ex.c.workflow, ex.c.state, sa.func.count())
            .where(*_execution_filter(dataset_id, start, end, workflow, purpose))
            .group_by(ex.c.workflow, ex.c.state))


def decisions_stmt(dataset_id, start, end, purpose=None):
    return (sa.select(gd.c.decision, sa.func.count())
            .select_from(gd.join(ex, ex.c.execution_id == gd.c.execution_id))
            .where(*_execution_filter(dataset_id, start, end, WRITE_GATE, purpose))
            .group_by(gd.c.decision))


def rules_stmt(dataset_id, start, end, purpose=None):
    return (sa.select(rr.c.measurement_status, sa.func.count(), sa.func.sum(rr.c.failed_rows))
            .select_from(rr.join(ex, ex.c.execution_id == rr.c.execution_id))
            .where(*_execution_filter(dataset_id, start, end, AUDIT, purpose))
            .group_by(rr.c.measurement_status))


def aggregate_dataset(conn, dataset_id: str, start: datetime, end: datetime,
                      workflow: str | None = None, purpose: str | None = None) -> dict:
    """Per-workflow aggregates for one dataset; `workflow` limits which sections are present."""
    if workflow is not None and workflow not in WORKFLOWS:
        raise ValueError(f"unknown workflow {workflow!r}")
    s, e = _utc_window(start, end)
    per_wf: dict[str, list] = {w: [] for w in WORKFLOWS}
    for w, state, n in conn.execute(executions_stmt(dataset_id, s, e, workflow, purpose)).all():
        per_wf[w].append((state, n))
    out = {w: {"executions": fold_executions(per_wf[w])} for w in WORKFLOWS if workflow in (None, w)}
    if WRITE_GATE in out:
        out[WRITE_GATE]["decisions"] = fold_decisions(conn.execute(decisions_stmt(dataset_id, s, e, purpose)).all())
    if AUDIT in out:
        out[AUDIT]["rules"] = fold_rules(conn.execute(rules_stmt(dataset_id, s, e, purpose)).all())
    return {"dataset_id": dataset_id, "window": {"start": canonical_timestamp(s), "end": canonical_timestamp(e)},
            "workflows": out}


def history_stmt(dataset_id, metric_definition_hash, scope, stage, history_identity_hash, start, end, limit,
                 workflow=AUDIT):
    return (sa.select(mo.c.observation_id, mo.c.execution_id, mo.c.event_time, mo.c.value)
            .select_from(mo.join(ex, ex.c.execution_id == mo.c.execution_id))
            .where(ex.c.dataset_id == dataset_id, ex.c.workflow == workflow,
                   mo.c.metric_definition_hash == metric_definition_hash, mo.c.scope == scope,
                   mo.c.stage == stage, mo.c.history_identity_hash == history_identity_hash,
                   mo.c.event_time >= start, mo.c.event_time < end)
            .order_by(mo.c.event_time.desc(), mo.c.execution_id.desc())
            .limit(limit + 1))


def rule_history(conn, dataset_id: str, metric_definition_hash: str, scope: str, stage: str,
                 history_identity_hash: str, start: datetime, end: datetime, limit: int,
                 workflow: str = AUDIT) -> dict:
    """Newest `limit` observations in the window, returned oldest-first. `workflow` (default AUDIT_TABLE)
    keeps WRITE_GATE and AUDIT measured points from mixing; `truncated` signals older points were cut."""
    if workflow not in WORKFLOWS:
        raise ValueError(f"unknown workflow {workflow!r}")
    if not 1 <= limit <= MAX_HISTORY_LIMIT:
        raise ValueError("limit out of range")
    s, e = _utc_window(start, end)
    rows = conn.execute(history_stmt(dataset_id, metric_definition_hash, scope, stage, history_identity_hash,
                                     s, e, limit, workflow)).all()
    return {"truncated": len(rows) > limit,
            "points": [{"observation_id": str(r[0]), "execution_id": str(r[1]),
                        "event_time": canonical_timestamp(r[2]), "value": canonical_decimal(r[3])}
                       for r in reversed(rows[:limit])]}
