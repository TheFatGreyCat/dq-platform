"""Restore reconciliation + pin-aware retention over real HTTP app + PostgreSQL 16."""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from app import create_app
from auth import TokenRegistry
from store import Store

from . import test_execution_acceptance as T
from .helpers import FixedClock, token_registry_dict, write_report


def _app(url, reg_dict, clock, restore=False):
    return create_app(Store(url, clock=clock), TokenRegistry.from_dict(reg_dict), T.NoWorker(), restore_mode=restore)


@pytest.fixture
def world(fresh_database_url):
    reg_dict, tokens = token_registry_dict([T.DS, T.DS2])
    clock = FixedClock()
    with TestClient(_app(fresh_database_url, reg_dict, clock)) as client:
        reg, mid, *_ = T.setup_gate(client, tokens)
        eid, plan, aid = T.start(client, tokens, reg, mid)
        client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid), headers=T.H(tokens, "job_gate"))
        assert T.publish_ok(client, tokens, reg, eid).status_code == 200
    return fresh_database_url, reg_dict, tokens, clock, reg, mid, eid


def _findings(reg, scan_id, page, final, tokens_=(), silver_v=4, q_v=2):
    return {"scan_id": scan_id, "page": page, "final_page": final, "scheduler_stop_reference": "sched:stopped",
            "scans": [{"destination": "silver", "table_id": reg["silver"]["table_id"], "snapshot_version": silver_v},
                      {"destination": "quarantine", "table_id": reg["quarantine"]["table_id"],
                       "snapshot_version": q_v}],
            "tokens": list(tokens_)}


def test_restore_mode_blocks_jobs_until_complete_paged_scan(world):
    url, reg_dict, tokens, clock, reg, mid, eid = world
    with TestClient(_app(url, reg_dict, clock, restore=True)) as client:
        view = client.get(f"/api/v2/datasets/{T.DS}/restore", headers=T.H(tokens, "viewer")).json()
        assert view["open"] is not None
        rid = view["open"]["restore_id"]
        r = client.post("/api/v2/executions", json=T.execution_request(T.DS, reg, mid),
                        headers=T.H(tokens, "job_gate", T.k()))
        assert r.status_code == 503 and r.json()["error"]["code"] == "RESTORE_RECONCILING"
        # restart with flag still set does not open a duplicate incident
    with TestClient(_app(url, reg_dict, clock, restore=True)) as client:
        assert client.get(f"/api/v2/datasets/{T.DS}/restore",
                          headers=T.H(tokens, "viewer")).json()["open"]["restore_id"] == rid
        scan = str(uuid.uuid4())
        orphan_app = f"dq:{uuid.uuid4()}:silver"
        tok = [{"destination": "silver", "txn_app_id": f"dq:{eid}:silver", "txn_version": 1, "commit_version": 4,
                "partition_values": {"dt": T.DT}},
               {"destination": "silver", "txn_app_id": orphan_app, "txn_version": 1, "commit_version": 5,
                "partition_values": {"dt": "2026-10-19"}},
               {"destination": "quarantine", "txn_app_id": "foreign-writer", "txn_version": 3, "commit_version": None,
                "partition_values": None}]
        p0 = client.post(f"/api/v2/datasets/{T.DS}/restore/findings", json=_findings(reg, scan, 0, False, tok),
                         headers=T.H(tokens, "operator", T.k()))
        assert p0.status_code == 200, p0.text
        body = p0.json()
        assert [k["execution_id"] for k in body["known"]] == [eid]
        assert sorted(o["scope"] for o in body["orphans"]) == ["DATASET", "PARTITION"]
        done = {"reason": "restored", "reconcile_reference": "ticket-1"}
        # page 0 is not the final page -> cannot complete
        c0 = client.post(f"/api/v2/datasets/{T.DS}/restore/complete", json=done,
                         headers=T.H(tokens, "operator", T.k()))
        assert c0.status_code == 409
        # skipped page rejected
        skip = client.post(f"/api/v2/datasets/{T.DS}/restore/findings", json=_findings(reg, scan, 2, True),
                           headers=T.H(tokens, "operator", T.k()))
        assert skip.status_code == 409
        p1 = client.post(f"/api/v2/datasets/{T.DS}/restore/findings", json=_findings(reg, scan, 1, True),
                         headers=T.H(tokens, "operator", T.k()))
        assert p1.status_code == 200
        c1 = client.post(f"/api/v2/datasets/{T.DS}/restore/complete", json=done,
                         headers=T.H(tokens, "operator", T.k()))
        assert c1.status_code == 200, c1.text
        assert client.get(f"/api/v2/datasets/{T.DS}/restore", headers=T.H(tokens, "viewer")).json()["open"] is None
        pub = client.get(f"/api/v2/datasets/{T.DS}/publications?dt=2026-10-19", headers=T.H(tokens, "viewer")).json()
        assert sorted(o["scope"] for o in pub["restore_orphans"]) == ["DATASET", "PARTITION"], pub


def test_restore_rejects_snapshot_older_than_recorded_commit(world):
    url, reg_dict, tokens, clock, reg, mid, eid = world
    with TestClient(_app(url, reg_dict, clock, restore=True)) as client:
        scan = str(uuid.uuid4())
        r = client.post(f"/api/v2/datasets/{T.DS}/restore/findings",
                        json=_findings(reg, scan, 0, True, silver_v=3), headers=T.H(tokens, "operator", T.k()))
        assert r.status_code == 200 and r.json()["blocked"] == "TARGET_NOT_INITIALIZED"
        c = client.post(f"/api/v2/datasets/{T.DS}/restore/complete",
                        json={"reason": "x", "reconcile_reference": "y"}, headers=T.H(tokens, "operator", T.k()))
        assert c.status_code == 409 and c.json()["error"]["code"] == "TARGET_NOT_INITIALIZED"


def test_retention_pins_and_cleanup(fresh_database_url):
    reg_dict, tokens = token_registry_dict([T.DS, T.DS2])
    clock = FixedClock()
    with TestClient(_app(fresh_database_url, reg_dict, clock)) as client:
        reg = T.registration(duration=3600, cleanup=True)
        assert client.put(f"/api/v2/datasets/{T.DS}", json=reg, headers=T.H(tokens, "editor")).status_code == 200
        # reuse setup_gate pieces with this registration
        _, mid, *_ = T.setup_gate(client, tokens, dataset=T.DS2)
        reg2 = client.get(f"/api/v2/datasets/{T.DS2}", headers=T.H(tokens, "viewer")).json()["registration"]
        reg2 = {**reg2, "retention": {**reg2["retention"], "candidate_duration_seconds": 3600, "cleanup_enabled": True}}
        assert client.put(f"/api/v2/datasets/{T.DS2}", json=reg2, headers=T.H(tokens, "editor")).status_code == 200
        # E1: denied (terminal, stop unconfirmed) ; E2: published (current reference)
        e1, p1, a1 = T.start(client, tokens, reg2, mid, dataset=T.DS2)
        client.post(f"/api/v2/executions/{e1}/reports", json=write_report(p1, a1, quarantine=6),
                    headers=T.H(tokens, "job_gate"))
        e2, p2, a2 = T.start(client, tokens, reg2, mid, dataset=T.DS2)
        client.post(f"/api/v2/executions/{e2}/reports", json=write_report(p2, a2), headers=T.H(tokens, "job_gate"))
        assert T.publish_ok(client, tokens, reg2, e2).status_code == 200
        url = f"/api/v2/datasets/{T.DS2}/retention"
        assert client.get(url, headers=T.H(tokens, "viewer")).status_code == 403

        def items():
            return {i["execution_id"]: i for i in client.get(url, headers=T.H(tokens, "operator")).json()["items"]}
        it = items()
        assert it[e1]["pinned"] and it[e1]["reason"] == "WRITER_UNCONFIRMED"
        assert it[e2]["pinned"]
        for e, a in ((e1, a1), (e2, a2)):
            client.post(f"/api/v2/executions/{e}/attempts/{a}/stop-confirmation",
                        json={"reference": "local-process:x:exit=0"}, headers=T.H(tokens, "job_gate"))
        it = items()
        assert it[e1]["pinned"] and it[e1]["reason"] == "NOT_EXPIRED"
        clock.advance(7200)
        it = items()
        assert it[e1]["pinned"] is False and it[e2]["reason"] == "REFERENCED"
        body = {"execution_id": e1, "attempt_id": a1, "uri": it[e1]["uri"], "stop_reference": "sched:stopped"}
        r = client.post(f"/api/v2/datasets/{T.DS2}/cleanup-records", json=body, headers=T.H(tokens, "operator", T.k()))
        assert r.status_code == 201, r.text
        pinned = {"execution_id": e2, "attempt_id": a2, "uri": it[e2]["uri"], "stop_reference": "sched:stopped"}
        assert client.post(f"/api/v2/datasets/{T.DS2}/cleanup-records", json=pinned,
                           headers=T.H(tokens, "operator", T.k())).status_code == 409
