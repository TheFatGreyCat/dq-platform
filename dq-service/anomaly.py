"""
Advisory anomaly (WP6): frozen history selection at plan time + one bounded batch call to the loopback
Deequ worker at report time. Never part of the gate decision.

select_history(conn, ...) runs inside the plan-creation REPEATABLE READ transaction; the frozen
contexts are stored in the immutable plan, so report retries never re-query history.
AnomalyClient.evaluate(items) is called OUTSIDE any DB transaction with a 5 s total budget.
"""
from __future__ import annotations

import time
from typing import Any

import requests
import sqlalchemy as sa

from canonical import SEMANTICS_VERSION, canonical_decimal, canonical_timestamp, content_hash

DETECTOR_TIMEOUT_SECONDS = 5.0
MAX_ITEMS = 20


def history_identity(dataset_id: str, table_id: str, history_context: str, scope: str, stage: str,
                     metric_definition_hash: str, engine: str, interpreter_version: str) -> str:
    return content_hash({
        "dataset_id": dataset_id, "table_id": table_id, "history_context": history_context, "scope": scope,
        "stage": stage, "metric_definition_hash": metric_definition_hash, "engine": engine,
        "interpreter_version": interpreter_version, "semantics_version": SEMANTICS_VERSION,
    })


def planned_metrics(stage_bindings: list[dict]) -> list[dict]:
    """Distinct numeric (metric_definition_hash, scope, stage); contradictory name/type for one hash -> error."""
    seen: dict[tuple, dict] = {}
    for sb in stage_bindings:
        for r in sb["rules"]:
            if r["value_type"] == "SCHEMA":
                continue
            key = (r["metric_definition_hash"], sb["scope"], sb["stage"])
            cur = {"metric_definition_hash": key[0], "scope": key[1], "stage": key[2],
                   "metric_name": r["metric_name"], "value_type": r["value_type"]}
            if key in seen and (seen[key]["metric_name"], seen[key]["value_type"]) != (cur["metric_name"],
                                                                                         cur["value_type"]):
                raise ValueError("metric hash có metric_name/value_type mâu thuẫn")
            seen[key] = cur
    return [seen[k] for k in sorted(seen)]


_HISTORY_SQL = sa.text("""
SELECT mo.execution_id::text AS execution_id, mo.event_time, mo.value
FROM metric_observations mo
JOIN accepted_reports ar ON ar.execution_id = mo.execution_id
WHERE mo.history_identity_hash = :h
  AND mo.event_time < :event_time
  AND ar.accepted_at < :cutoff
  AND (SELECT e.eligible FROM metric_eligibility_events e
       WHERE e.observation_id = mo.observation_id AND e.effective_at <= :cutoff
       ORDER BY e.effective_at DESC, e.event_id DESC LIMIT 1) IS TRUE
ORDER BY mo.event_time DESC, mo.execution_id DESC
LIMIT :n
""")


def select_history(conn, *, dataset_id: str, table_id: str, history_context: str, engine: str,
                   interpreter_version: str, stage_bindings: list[dict], event_time, cutoff,
                   max_points: int) -> list[dict]:
    contexts = []
    for m in planned_metrics(stage_bindings):
        h = history_identity(dataset_id, table_id, history_context, m["scope"], m["stage"],
                             m["metric_definition_hash"], engine, interpreter_version)
        rows = conn.execute(_HISTORY_SQL, {"h": h, "event_time": event_time, "cutoff": cutoff,
                                           "n": max_points}).mappings().all()
        points = [{"execution_id": r["execution_id"], "event_time": canonical_timestamp(r["event_time"]),
                   "value": canonical_decimal(r["value"])} for r in reversed(rows)]
        contexts.append({**m, "history_identity_hash": h, "points": points,
                         "baseline_hash": content_hash(points)})
    return contexts


class AnomalyClient:
    """HTTP client to the loopback worker; never raises — failures become explicit ERROR codes."""

    def __init__(self, url: str, session: requests.Session | None = None):
        self.url = url.rstrip("/")
        self.session = session or requests.Session()

    def health(self) -> bool:
        try:
            return self.session.get(f"{self.url}/health", timeout=1.0).status_code == 200
        except requests.RequestException:
            return False

    def evaluate(self, items: list[dict]) -> list[dict] | str:
        """Returns results aligned with items, or an error code string for the whole batch."""
        if not items:
            return []
        if len(items) > MAX_ITEMS:
            return "INVALID_REQUEST"
        deadline = time.monotonic() + DETECTOR_TIMEOUT_SECONDS
        try:
            resp = self.session.post(f"{self.url}/v1/evaluate", json={"items": items},
                                     timeout=(min(1.0, DETECTOR_TIMEOUT_SECONDS), DETECTOR_TIMEOUT_SECONDS))
        except requests.Timeout:
            return "WORKER_TIMEOUT"
        except requests.RequestException:
            return "WORKER_UNAVAILABLE"
        if time.monotonic() > deadline:
            return "WORKER_TIMEOUT"
        if resp.status_code != 200:
            return "WORKER_UNAVAILABLE"
        try:
            body = resp.json()
            results = body["results"]
        except (ValueError, KeyError, TypeError):
            return "INVALID_RESPONSE"
        if not isinstance(results, list) or len(results) != len(items):
            return "INVALID_RESPONSE"
        return results


def build_observations(policy_anomaly: dict, contexts: list[dict], results_by_key: dict[tuple, dict],
                       client: AnomalyClient) -> list[dict]:
    """Observation payloads (one per frozen context). Disabled -> []."""
    if not policy_anomaly.get("enabled"):
        return []
    lower, upper = policy_anomaly["lower"], policy_anomaly["upper"]
    obs: list[dict | None] = []
    items, idx = [], []
    for c in contexts:
        key = (c["metric_definition_hash"], c["scope"], c["stage"])
        r = results_by_key.get(key)
        base = {"metric_definition_hash": key[0], "scope": key[1], "stage": key[2], "metric_name": c["metric_name"],
                "baseline_hash": c["baseline_hash"], "baseline_execution_ids": [p["execution_id"] for p in c["points"]]}
        if r is None or r["measurement_status"] != "OK":
            obs.append({**base, "status": "ERROR", "evidence": {"code": "MEASUREMENT_UNAVAILABLE"}})
            continue
        value = r["metric_value"]
        items.append({"metric_definition_hash": key[0], "scope": key[1], "stage": key[2], "lower": lower,
                      "upper": upper, "baseline": [{"execution_id": p["execution_id"], "event_time": p["event_time"],
                                                    "value": p["value"]} for p in c["points"]],
                      "current": canonical_decimal(value) if not isinstance(value, str) else value})
        idx.append(len(obs))
        obs.append(base)
    out = client.evaluate(items) if items else []
    for j, pos in enumerate(idx):
        base = obs[pos]
        if isinstance(out, str):
            obs[pos] = {**base, "status": "ERROR", "evidence": {"code": out}}
            continue
        res = out[j]
        ok_identity = isinstance(res, dict) and all(res.get(k) == base[k] for k in
                                                    ("metric_definition_hash", "scope", "stage"))
        status = res.get("status") if isinstance(res, dict) else None
        if not ok_identity or status not in ("NORMAL", "ANOMALY", "INSUFFICIENT_HISTORY", "ERROR"):
            obs[pos] = {**base, "status": "ERROR", "evidence": {"code": "INVALID_RESPONSE"}}
            continue
        evidence = res.get("evidence") if status != "ERROR" else {"code": (res.get("error") or {}).get("code",
                                                                                                    "INVALID_RESPONSE")}
        if not isinstance(evidence, dict):
            obs[pos] = {**base, "status": "ERROR", "evidence": {"code": "INVALID_RESPONSE"}}
            continue
        obs[pos] = {**base, "status": status, "evidence": evidence,
                    "detector_version": str(res.get("detector_version", ""))[:128]}
    return [o for o in obs if o is not None]


def eligible_on_success(policy_anomaly: dict, anomaly_status: str | None, event_origin: str) -> bool:
    """Baseline eligibility after full exact publication / completed audit."""
    if event_origin != "SCHEDULED":
        return False
    if not policy_anomaly.get("enabled"):
        return True
    if anomaly_status == "NORMAL":
        return True
    return anomaly_status == "INSUFFICIENT_HISTORY" and bool(policy_anomaly.get("bootstrap"))


DETECTOR_VERSION = "deequ-2.0.21-spark-3.5:RelativeRateOfChange:order1"


def _unused(_: Any) -> None:  # pragma: no cover
    return None
