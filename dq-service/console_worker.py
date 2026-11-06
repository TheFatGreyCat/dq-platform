"""Console alert worker: durable alert evaluation + webhook delivery over the v3 tables.

Fixed poll loop; clock comes from Store.clock and the HTTP transport is injectable. The worker only writes
console_alerts / console_alert_deliveries, so a delivery failure can never change a gate decision.
Logs carry fixed codes only (no webhook URL, request/response body, SQL or exception text).
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

import requests
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

import schema as t
from store import Store, sid, ts

log = logging.getLogger("dq.console_worker")

POLL_SECONDS = 15.0
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = (30, 60, 120, 240)  # delay after failed attempt 1..4; attempt 5 failing -> FAILED
LEASE_SECONDS = 60  # > HTTP_TIMEOUT so a live claim is never stolen
HTTP_TIMEOUT = 5
BATCH = 50
STALE_STATES = ("PLANNED", "RUNNING")

# transport(url, json_body, headers) -> HTTP status code; raises on network failure
Transport = Callable[[str, dict, dict], int]


def post_transport(url: str, body: dict, headers: dict) -> int:
    r = requests.post(url, json=body, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=False, stream=True)
    try:
        return r.status_code
    finally:
        r.close()  # response body is never read or stored


# ---------- pure rules ----------
def last_completed_window(now: datetime, window_seconds: int) -> tuple[datetime, datetime]:
    """Most recent fully elapsed UTC tumbling window aligned to the Unix epoch."""
    end = int(now.timestamp()) // window_seconds * window_seconds
    return (datetime.fromtimestamp(end - window_seconds, timezone.utc), datetime.fromtimestamp(end, timezone.utc))


def rate_exceeds(violations: int, reports: int, min_reports: int, threshold: Decimal) -> bool:
    return reports > 0 and reports >= min_reports and Decimal(violations) / Decimal(reports) > threshold


def delivery_result(transport: Transport, url: str, alert_id: str, body: dict, attempts: int,
                    now: datetime) -> dict[str, Any]:
    """Column values to store after one delivery attempt (`attempts` already counts this one)."""
    try:
        status = transport(url, body, {"Idempotency-Key": alert_id})
        ok, code = 200 <= status < 300, "WEBHOOK_HTTP_ERROR"
    except Exception:  # noqa: BLE001 - any transport failure is a retryable network error, detail not stored
        ok, code = False, "WEBHOOK_NETWORK_ERROR"
    if ok:
        return {"state": "DELIVERED", "delivered_at": now, "next_attempt_at": None, "lease_until": None,
                "last_error_code": None}
    if attempts >= MAX_ATTEMPTS:
        return {"state": "FAILED", "next_attempt_at": None, "lease_until": None, "last_error_code": code}
    return {"state": "PENDING", "next_attempt_at": now + timedelta(seconds=BACKOFF_SECONDS[attempts - 1]),
            "lease_until": None, "last_error_code": code}


def alert_body(row) -> dict:
    return {"alert_id": sid(row["alert_id"]), "dataset_id": row["dataset_id"], "kind": row["kind"],
            "execution_id": sid(row["execution_id"]), "rule_id": row["rule_id"],
            "window_start": ts(row["window_start"]), "window_end": ts(row["window_end"]),
            "owner": row["owner_snapshot"], "details": row["details"], "created_at": ts(row["created_at"])}


# ---------- worker ----------
class ConsoleWorker:
    def __init__(self, store: Store, webhook_url: str = "", transport: Transport = post_transport):
        self.store, self.url, self.transport = store, webhook_url, transport

    # --- evaluation ---
    def evaluate(self) -> None:
        self.store.tx(self._evaluate)

    def _evaluate(self, c: sa.Connection) -> None:
        now = self.store.clock()
        s = t.console_alert_settings
        for st in c.execute(sa.select(s).where(s.c.enabled.is_(True))).mappings().all():
            owner = c.execute(sa.select(t.datasets.c.registration["owner"].as_string())
                              .where(t.datasets.c.dataset_id == st["dataset_id"])).scalar_one_or_none()
            if owner is None:
                continue
            for kind, key, execution_id, window, details in self._breaches(c, st, now):
                self._raise(c, st["dataset_id"], owner, now, kind, key, execution_id, window, details)

    def _breaches(self, c, st, now):
        if st["no_report_after_seconds"] is not None:
            yield from self._no_report(c, st, now)
        if st["quarantine_ratio_threshold"] is not None:
            yield from self._quarantine(c, st)
        if st["violation_rate_threshold"] is not None:
            yield from self._violation_rate(c, st, now)

    @staticmethod
    def _no_alert_yet(c_execution_id, prefix: str):
        a = t.console_alerts
        return ~sa.exists().where(a.c.alert_key == prefix + sa.cast(c_execution_id, sa.Text))

    def _no_report(self, c, st, now):
        e, secs = t.executions, st["no_report_after_seconds"]
        rows = c.execute(sa.select(e.c.execution_id, e.c.state).where(
            e.c.dataset_id == st["dataset_id"], e.c.state.in_(STALE_STATES),
            e.c.created_at <= now - timedelta(seconds=secs), e.c.created_at >= st["updated_at"],
            self._no_alert_yet(e.c.execution_id, "NO_REPORT:")).limit(BATCH)).all()
        for eid, state in rows:
            yield ("NO_REPORT", f"NO_REPORT:{eid}", eid, None,
                   {"execution_state": state, "no_report_after_seconds": secs})

    def _quarantine(self, c, st):
        e, r, thr = t.executions, t.accepted_reports, st["quarantine_ratio_threshold"]
        cand, quar = r.c.payload["candidate_rows"].as_integer(), r.c.payload["quarantine_rows"].as_integer()
        rows = c.execute(sa.select(e.c.execution_id, cand, quar).select_from(r.join(e, e.c.execution_id == r.c.execution_id))
                         .where(e.c.dataset_id == st["dataset_id"], e.c.workflow == "WRITE_GATE",
                                r.c.payload["classification_status"].as_string() == "COMPLETE",
                                r.c.accepted_at >= st["updated_at"], cand > 0,
                                sa.cast(quar, sa.Numeric) > thr * sa.cast(cand, sa.Numeric),
                                self._no_alert_yet(e.c.execution_id, "QUARANTINE_RATIO:")).limit(BATCH)).all()
        for eid, cr, qr in rows:
            yield ("QUARANTINE_RATIO", f"QUARANTINE_RATIO:{eid}", eid, None,
                   {"candidate_rows": cr, "quarantine_rows": qr, "threshold": str(thr)})

    def _violation_rate(self, c, st, now):
        # ponytail: only the latest completed window is checked each poll (one COUNT per dataset per poll);
        # a worker outage spanning a whole window skips it. Add a per-dataset watermark if catch-up is needed.
        start, end = last_completed_window(now, st["window_seconds"])
        if end <= st["updated_at"]:
            return
        e, r = t.executions, t.accepted_reports
        in_dataset = sa.exists().where(e.c.execution_id == r.c.execution_id, e.c.dataset_id == st["dataset_id"])
        total, bad = c.execute(sa.select(
            sa.func.count(), sa.func.count().filter(r.c.assessment == "VIOLATION")
        ).select_from(r).where(in_dataset, r.c.accepted_at >= start, r.c.accepted_at < end)).one()
        if rate_exceeds(bad, total, st["violation_rate_min_reports"], st["violation_rate_threshold"]):
            yield ("VIOLATION_RATE", f"VIOLATION_RATE:{st['dataset_id']}:{ts(start)}", None, (start, end),
                   {"reports": total, "violations": bad, "threshold": str(st["violation_rate_threshold"]),
                    "window_seconds": st["window_seconds"]})

    def _raise(self, c, dataset_id, owner, now, kind, key, execution_id, window, details) -> None:
        alert_id = self.store.new_id()
        got = c.execute(pg_insert(t.console_alerts).values(
            alert_id=alert_id, dataset_id=dataset_id, alert_key=key, kind=kind, execution_id=execution_id,
            rule_id=None, window_start=window[0] if window else None, window_end=window[1] if window else None,
            owner_snapshot=owner, details=details, created_at=now, version=1,
        ).on_conflict_do_nothing(index_elements=["alert_key"]).returning(t.console_alerts.c.alert_id)).first()
        if got is None:
            return
        c.execute(t.console_alert_deliveries.insert().values(
            alert_id=alert_id, state="PENDING" if self.url else "DISABLED", attempts=0,
            next_attempt_at=now if self.url else None))

    # --- delivery ---
    def deliver(self) -> None:
        if not self.url:
            return
        for row in self.store.tx(self._claim):
            now = self.store.clock()
            values = delivery_result(self.transport, self.url, sid(row["alert_id"]), alert_body(row),
                                     row["delivery_attempts"], now)
            d = t.console_alert_deliveries
            self.store.tx(lambda c: c.execute(d.update().where(
                d.c.alert_id == row["alert_id"], d.c.state == "PENDING").values(**values)))

    def _claim(self, c: sa.Connection) -> list:
        d, a, now = t.console_alert_deliveries, t.console_alerts, self.store.clock()
        expired = sa.or_(d.c.lease_until.is_(None), d.c.lease_until < now)
        # attempts exhausted while a worker died mid-flight: finish them instead of leaving PENDING forever
        c.execute(d.update().where(d.c.state == "PENDING", d.c.attempts >= MAX_ATTEMPTS, expired).values(
            state="FAILED", next_attempt_at=None, lease_until=None,
            last_error_code=sa.func.coalesce(d.c.last_error_code, "WEBHOOK_NETWORK_ERROR")))
        rows = c.execute(sa.select(a, d.c.attempts.label("delivery_attempts"))
                         .select_from(d.join(a, a.c.alert_id == d.c.alert_id))
                         .where(d.c.state == "PENDING", d.c.attempts < MAX_ATTEMPTS, d.c.next_attempt_at <= now, expired)
                         .order_by(d.c.next_attempt_at).limit(BATCH)
                         .with_for_update(skip_locked=True, of=d)).mappings().all()
        claimed = [{**r, "delivery_attempts": r["delivery_attempts"] + 1} for r in rows]
        if claimed:
            c.execute(d.update().where(d.c.alert_id.in_([r["alert_id"] for r in claimed])).values(
                attempts=d.c.attempts + 1, lease_until=now + timedelta(seconds=LEASE_SECONDS)))
        return claimed

    # --- loop ---
    def tick(self) -> None:
        self.evaluate()
        self.deliver()

    def run(self, stop: Callable[[], bool], sleep: Callable[[float], None] = time.sleep,
            interval: float = POLL_SECONDS) -> None:
        while not stop():
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - keep polling; detail may contain SQL/URL so only a code is logged
                log.error("console worker tick failed code=WORKER_TICK_FAILED")
            sleep(interval)


def main() -> None:
    url = os.environ.get("DATABASE_URL")
    if not url and os.environ.get("DATABASE_URL_FILE"):
        with open(os.environ["DATABASE_URL_FILE"], encoding="utf-8") as f:
            url = f.read().strip()
    if not url:
        raise SystemExit("DATABASE_URL required")
    store = Store(url)
    store.check_schema()
    ConsoleWorker(store, os.environ.get("DQ_ALERT_WEBHOOK_URL", "")).run(lambda: False)


if __name__ == "__main__":
    main()
