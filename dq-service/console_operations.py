"""Console operations over the v3 tables: alert settings, alert inbox, acknowledgement, audit events.

Pure SQLAlchemy Core functions taking an open connection; authorization, idempotency and HTTP live in app.py.
Views expose safe metadata only (no webhook URL, response body, SQL, raw payload or storage URI).
"""
from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Callable

import sqlalchemy as sa
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictBool, StrictInt

import schema as t
from canonical import canonical_decimal
from errors import ApiError
from store import Store, sid, ts

ALERT_KINDS = ("REPORT_ATTENTION", "NO_REPORT", "QUARANTINE_RATIO", "VIOLATION_RATE")
ALERT_STATES = ("unacknowledged", "acknowledged")
NOTE_MAX = 2048

_Ratio = Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)]


def _no_nul(v: str) -> str:
    if "\x00" in v:
        raise ValueError("NUL")
    return v


class AlertSettingsPut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: StrictInt = Field(ge=0)  # 0 = settings not created yet
    enabled: StrictBool
    quarantine_ratio_threshold: _Ratio | None = None
    no_report_after_seconds: StrictInt | None = Field(default=None, ge=60, le=604800)
    violation_rate_threshold: _Ratio | None = None
    violation_rate_min_reports: StrictInt = Field(default=1, ge=1)
    window_seconds: StrictInt = Field(default=3600, ge=300, le=604800)


class AlertAck(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: StrictInt = Field(ge=1)
    note: Annotated[str, Field(max_length=NOTE_MAX), AfterValidator(_no_nul)] | None = None


def _dec(v) -> str | None:
    return None if v is None else canonical_decimal(v)


def settings_view(row: dict | None) -> dict:
    """Missing row -> disabled defaults at version 0 (the baseline for expected_version)."""
    if row is None:
        return {"version": 0, "enabled": False, "quarantine_ratio_threshold": None, "no_report_after_seconds": None,
                "violation_rate_threshold": None, "violation_rate_min_reports": 1, "window_seconds": 3600,
                "updated_at": None, "updated_by": None}
    return {"version": row["version"], "enabled": row["enabled"],
            "quarantine_ratio_threshold": _dec(row["quarantine_ratio_threshold"]),
            "no_report_after_seconds": row["no_report_after_seconds"],
            "violation_rate_threshold": _dec(row["violation_rate_threshold"]),
            "violation_rate_min_reports": row["violation_rate_min_reports"],
            "window_seconds": row["window_seconds"], "updated_at": ts(row["updated_at"]),
            "updated_by": row["updated_by"]}


def alert_view(row) -> dict:
    return {"alert_id": sid(row["alert_id"]), "dataset_id": row["dataset_id"], "kind": row["kind"],
            "execution_id": sid(row["execution_id"]), "rule_id": row["rule_id"],
            "window_start": ts(row["window_start"]), "window_end": ts(row["window_end"]),
            "owner_snapshot": row["owner_snapshot"], "details": row["details"], "created_at": ts(row["created_at"]),
            "acknowledged_at": ts(row["acknowledged_at"]), "acknowledged_by": row["acknowledged_by"],
            "acknowledgement_note": row["acknowledgement_note"], "version": row["version"],
            "delivery_state": row["delivery_state"], "delivery_attempts": row["delivery_attempts"],
            "delivery_error_code": row["delivery_error_code"]}


def audit_view(row) -> dict:
    return {"event_id": sid(row["event_id"]), "dataset_id": row["dataset_id"], "actor": row["actor"],
            "action": row["action"], "resource_type": row["resource_type"], "resource_id": row["resource_id"],
            "occurred_at": ts(row["occurred_at"]), "details": row["details"]}


def _row(c, ds: str):
    return c.execute(sa.select(t.console_alert_settings).where(t.console_alert_settings.c.dataset_id == ds)
                     .with_for_update()).mappings().first()


def record_audit(c, *, event_id: uuid.UUID, at: datetime, dataset_id: str, actor: str, action: str,
                 resource_type: str, resource_id: str, details: dict) -> None:
    c.execute(t.console_audit_events.insert().values(
        event_id=event_id, dataset_id=dataset_id, actor=actor, action=action, resource_type=resource_type,
        resource_id=resource_id, occurred_at=at, details=details))


def get_settings(c, store: Store, dataset_id: str) -> dict:
    store.require_dataset_row(c, dataset_id)
    return settings_view(c.execute(sa.select(t.console_alert_settings).where(
        t.console_alert_settings.c.dataset_id == dataset_id)).mappings().first())


def put_settings(c, store: Store, actor: str, dataset_id: str, b: AlertSettingsPut) -> tuple[int, dict, str]:
    """Optimistic-concurrency upsert + audit event in one transaction. Returns (status, view, resource_id)."""
    store.require_dataset_row(c, dataset_id)
    Store.lock(c, "alert_settings", dataset_id)
    cur = _row(c, dataset_id)
    if (cur["version"] if cur else 0) != b.expected_version:
        raise ApiError("STATE_CONFLICT", "expected_version")
    now = store.clock()
    values = dict(version=b.expected_version + 1, enabled=b.enabled,
                  quarantine_ratio_threshold=b.quarantine_ratio_threshold,
                  no_report_after_seconds=b.no_report_after_seconds,
                  violation_rate_threshold=b.violation_rate_threshold,
                  violation_rate_min_reports=b.violation_rate_min_reports, window_seconds=b.window_seconds,
                  updated_at=now, updated_by=actor)
    tbl = t.console_alert_settings
    if cur is None:
        c.execute(tbl.insert().values(dataset_id=dataset_id, **values))
    else:
        c.execute(tbl.update().where(tbl.c.dataset_id == dataset_id).values(**values))
    event_id = store.new_id()
    record_audit(c, event_id=event_id, at=now, dataset_id=dataset_id, actor=actor, action="ALERT_SETTINGS_UPDATED",
                 resource_type="alert_settings", resource_id=dataset_id, details={"version": values["version"]})
    return 200, get_settings(c, store, dataset_id), str(event_id)


def _keyset(col_time, col_id, cursor, to_id: Callable[[str], object]):
    """Descending keyset predicate on (time, id) from a validated [time, id] cursor."""
    try:
        at = datetime.fromisoformat(cursor[0])
        if at.tzinfo is None:
            raise ValueError("naive cursor")
        return sa.tuple_(col_time, col_id) < sa.tuple_(at, to_id(cursor[1]))
    except ValueError:
        raise ApiError("INVALID_CONFIG", "cursor")


def _page(rows: list, limit: int, key: Callable[[object], list]):
    return rows[:limit], (key(rows[limit - 1]) if len(rows) > limit else None)


def list_alerts(c, store: Store, dataset_id: str, state: str | None, kind: str | None, limit: int,
                cursor: list | None) -> tuple[list[dict], list | None]:
    if state is not None and state not in ALERT_STATES:
        raise ApiError("INVALID_CONFIG", "state")
    if kind is not None and kind not in ALERT_KINDS:
        raise ApiError("INVALID_CONFIG", "kind")
    store.require_dataset_row(c, dataset_id)
    a, d = t.console_alerts, t.console_alert_deliveries
    q = (sa.select(a, d.c.state.label("delivery_state"), d.c.attempts.label("delivery_attempts"),
                   d.c.last_error_code.label("delivery_error_code"))
         .select_from(a.outerjoin(d, d.c.alert_id == a.c.alert_id)).where(a.c.dataset_id == dataset_id))
    if state == "unacknowledged":
        q = q.where(a.c.acknowledged_at.is_(None))
    elif state == "acknowledged":
        q = q.where(a.c.acknowledged_at.is_not(None))
    if kind is not None:
        q = q.where(a.c.kind == kind)
    if cursor is not None:
        q = q.where(_keyset(a.c.created_at, a.c.alert_id, cursor, uuid.UUID))
    rows = c.execute(q.order_by(a.c.created_at.desc(), a.c.alert_id.desc()).limit(limit + 1)).mappings().all()
    page, nxt = _page(rows, limit, lambda r: [ts(r["created_at"]), sid(r["alert_id"])])
    return [alert_view(r) for r in page], nxt


def acknowledge(c, store: Store, actor: str, dataset_id: str, alert_id: str, b: AlertAck) -> tuple[int, dict, str]:
    """First successful ack only: already acknowledged or stale expected_version -> 409 STATE_CONFLICT."""
    try:
        aid = uuid.UUID(alert_id)
    except ValueError:
        raise ApiError("NOT_FOUND")
    a, d = t.console_alerts, t.console_alert_deliveries
    cur = c.execute(sa.select(a).where(a.c.alert_id == aid, a.c.dataset_id == dataset_id)
                    .with_for_update()).mappings().first()
    if cur is None:
        raise ApiError("NOT_FOUND")
    if cur["acknowledged_at"] is not None or cur["version"] != b.expected_version:
        raise ApiError("STATE_CONFLICT", "expected_version")
    now = store.clock()
    c.execute(a.update().where(a.c.alert_id == aid).values(
        acknowledged_at=now, acknowledged_by=actor, acknowledgement_note=b.note, version=cur["version"] + 1))
    record_audit(c, event_id=store.new_id(), at=now, dataset_id=dataset_id, actor=actor, action="ALERT_ACKNOWLEDGED",
                 resource_type="alert", resource_id=alert_id, details={"kind": cur["kind"]})
    row = c.execute(sa.select(a, d.c.state.label("delivery_state"), d.c.attempts.label("delivery_attempts"),
                              d.c.last_error_code.label("delivery_error_code"))
                    .select_from(a.outerjoin(d, d.c.alert_id == a.c.alert_id))
                    .where(a.c.alert_id == aid)).mappings().one()
    return 200, alert_view(row), alert_id


def list_audit_events(c, store: Store, dataset_id: str, action: str | None, limit: int,
                      cursor: list | None) -> tuple[list[dict], list | None]:
    store.require_dataset_row(c, dataset_id)
    e = t.console_audit_events
    q = sa.select(e).where(e.c.dataset_id == dataset_id)
    if action is not None:
        q = q.where(e.c.action == action)
    if cursor is not None:
        q = q.where(_keyset(e.c.occurred_at, e.c.event_id, cursor, uuid.UUID))
    rows = c.execute(q.order_by(e.c.occurred_at.desc(), e.c.event_id.desc()).limit(limit + 1)).mappings().all()
    page, nxt = _page(rows, limit, lambda r: [ts(r["occurred_at"]), sid(r["event_id"])])
    return [audit_view(r) for r in page], nxt
