"""Advisory anomaly over real PostgreSQL + the real loopback JVM worker (DQ_TEST_ANOMALY_WORKER_URL)."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from anomaly import AnomalyClient
from app import create_app
from auth import TokenRegistry
from store import Store

from .helpers import FixedClock, token_registry_dict, write_report
from . import test_execution_acceptance as T

ANOMALY = {"enabled": True, "max_points": 5, "lower": "0.8", "upper": "1.2", "bootstrap": True}


@pytest.fixture
def worker_url():
    url = os.environ.get("DQ_TEST_ANOMALY_WORKER_URL")
    if not url:
        pytest.skip("DQ_TEST_ANOMALY_WORKER_URL chưa đặt: bỏ qua test worker thật")
    return url


@pytest.fixture
def env(fresh_database_url, worker_url):
    reg_dict, tokens = token_registry_dict([T.DS, T.DS2])
    store = Store(fresh_database_url, clock=FixedClock())
    app = create_app(store, TokenRegistry.from_dict(reg_dict), AnomalyClient(worker_url))
    with TestClient(app) as client:
        yield client, tokens


def _gate_with_anomaly(client, tokens):
    reg, mid, sid, rev, _ = T.setup_gate(client, tokens)
    p = client.post(f"/api/v2/datasets/{T.DS}/suites/{sid}/policy-revisions",
                    json=T.gate_policy(T.gate_suite(), anomaly=ANOMALY), headers=T.H(tokens, "editor", T.k()))
    assert p.status_code == 201, p.text
    b = client.put(f"/api/v2/datasets/{T.DS}/purposes/WRITE_GATE/default/binding",
                   json={"expected_binding_version": 1, "suite_id": sid, "suite_revision_id": rev["revision_id"],
                         "policy_revision_id": p.json()["revision_id"], "reason": "enable anomaly"},
                   headers=T.H(tokens, "approver", T.k()))
    assert b.status_code == 200, b.text
    return reg, mid


def _run_day(client, tokens, reg, mid, day: int, candidate: int, version: int):
    eid, plan, aid = T.start(client, tokens, reg, mid, event_time=datetime(2026, 10, day, 23, tzinfo=timezone.utc))
    r = client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid, candidate=candidate),
                    headers=T.H(tokens, "job_gate"))
    assert r.status_code == 200, r.text
    acc = r.json()
    if acc["decision"]["decision"] == "ALLOW":
        assert T.publish_ok(client, tokens, reg, eid, silver_version=version, q_version=version).status_code == 200
    return eid, plan, acc


def _size_obs(client, tokens, eid):
    items = client.get(f"/api/v2/executions/{eid}/anomalies", headers=T.H(tokens, "viewer")).json()["items"]
    return [o for o in items if o["metric_name"] == "Size" and o["stage"] == "CANDIDATE"]


def test_100_to_130_is_advisory_anomaly_while_gate_allows(env):
    client, tokens = env
    reg, mid = _gate_with_anomaly(client, tokens)
    e1, plan1, a1 = _run_day(client, tokens, reg, mid, 18, 100, 4)
    first = _size_obs(client, tokens, e1)
    assert [o["status"] for o in first] == ["INSUFFICIENT_HISTORY"]
    e2, plan2, a2 = _run_day(client, tokens, reg, mid, 19, 130, 5)
    # frozen history in plan 2 is exactly day 1 (bootstrap-eligible after full publication)
    ctx = [c for c in plan2["history_snapshot"]["contexts"] if c["metric_name"] == "Size" and c["stage"] == "CANDIDATE"]
    assert [p["execution_id"] for p in ctx[0]["points"]] == [e1]
    assert a2["decision"]["decision"] == "ALLOW"
    obs = _size_obs(client, tokens, e2)
    assert obs[0]["status"] == "ANOMALY"
    ev = obs[0]["evidence"]
    assert (ev["previous"], ev["current"], ev["lower"], ev["upper"]) == ("100", "130", "0.8", "1.2")
    assert ev["ratio"] == "1.3"
    # anomaly point is not auto-eligible -> day 3 plan baseline still only day 1
    e3, plan3, _ = _run_day(client, tokens, reg, mid, 20, 105, 6)
    ctx3 = [c for c in plan3["history_snapshot"]["contexts"]
            if c["metric_name"] == "Size" and c["stage"] == "CANDIDATE"][0]
    assert [p["execution_id"] for p in ctx3["points"]] == [e1]
    assert _size_obs(client, tokens, e3)[0]["status"] == "NORMAL"
    # approver override makes day 2 eligible for later plans; frozen observation of day 3 does not change
    over = client.post(f"/api/v2/executions/{e2}/metrics/{obs[0]['observation_id']}/eligibility",
                       json={"eligible": True, "reason": "known promo"}, headers=T.H(tokens, "approver", T.k()))
    assert over.status_code == 201, over.text
    assert _size_obs(client, tokens, e3)[0]["status"] == "NORMAL"
    _, plan4, _ = _run_day(client, tokens, reg, mid, 21, 104, 7)
    ctx4 = [c for c in plan4["history_snapshot"]["contexts"]
            if c["metric_name"] == "Size" and c["stage"] == "CANDIDATE"][0]
    assert [p["execution_id"] for p in ctx4["points"]] == [e1, e2, e3]


def test_worker_unavailable_is_bounded_error_not_veto(fresh_database_url):
    reg_dict, tokens = token_registry_dict([T.DS, T.DS2])
    store = Store(fresh_database_url, clock=FixedClock())
    app = create_app(store, TokenRegistry.from_dict(reg_dict), AnomalyClient("http://127.0.0.1:9"))
    with TestClient(app) as client:
        reg, mid = _gate_with_anomaly(client, tokens)
        eid, _, acc = _run_day(client, tokens, reg, mid, 18, 100, 4)
        assert acc["decision"]["decision"] == "ALLOW"
        obs = _size_obs(client, tokens, eid)
        assert obs[0]["status"] == "ERROR" and obs[0]["evidence"] == {"code": "WORKER_UNAVAILABLE"}
