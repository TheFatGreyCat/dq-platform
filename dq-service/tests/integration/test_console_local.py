"""Tests for local synthetic console smoke seed and CLI lifecycle."""
from __future__ import annotations

import json
import os
import stat
import threading
import time
import urllib.request
from pathlib import Path

import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import create_app
from auth import TokenRegistry
from store import Store

from .console_local import (
    DS_EMPTY,
    DS_MAIN,
    DS_OTHER,
    DS_OVERFLOW,
    VIEWER_PRINCIPAL,
    console_token_registry,
    main,
    reserve_socket,
    seed_console,
)
from .helpers import FixedClock
from .test_execution_acceptance import H, NoWorker

pytestmark = pytest.mark.skipif(
    not os.environ.get("DQ_TEST_DATABASE_URL"),
    reason="DQ_TEST_DATABASE_URL is not set: skipping PostgreSQL integration tests",
)


def test_seed_console_executions_and_verdicts(fresh_database_url):
    store = Store(fresh_database_url, clock=FixedClock())
    reg_data, tokens = console_token_registry()
    app = create_app(store, TokenRegistry.from_dict(reg_data), NoWorker())

    with TestClient(app) as client:
        res = seed_console(client, tokens)

        # Expected counts
        assert res["counts"]["total"] == 51
        assert res["counts"]["gate"] == 25
        assert res["counts"]["audit"] == 26
        assert res["counts"]["audit_nightly"] == 25
        assert res["counts"]["audit_other"] == 1
        assert len(res["gate_ids"]) == 25
        assert len(res["audit_ids"]) == 26
        assert len(res["all_execution_ids"]) == 51

        # Query all main dataset executions
        resp = client.get(f"/api/v2/datasets/{DS_MAIN}/executions?limit=100", headers=H(tokens, "viewer"))
        assert resp.status_code == 200, resp.text
        items = resp.json()["items"]
        assert len(items) == 51

        # Keyset ordering: strictly ascending event_time
        event_times = [item["plan"]["event_time"] for item in items]
        assert event_times == sorted(event_times)
        assert len(set(event_times)) == 51

        # 25 WRITE_GATE followed by 26 AUDIT_TABLE
        for item in items[:25]:
            assert item["workflow"] == "WRITE_GATE"
            assert item["purpose"] == "default"
        for item in items[25:50]:
            assert item["workflow"] == "AUDIT_TABLE"
            assert item["purpose"] == "nightly"
        assert items[50]["workflow"] == "AUDIT_TABLE"
        assert items[50]["purpose"] == "other"

        # Gate execution 0: plain ALLOW (published)
        rep_allow = client.get(f"/api/v2/executions/{res['allow_id']}/report", headers=H(tokens, "viewer"))
        assert rep_allow.status_code == 200
        assert rep_allow.json()["accepted"]["decision"]["decision"] == "ALLOW"
        assert rep_allow.json()["report"]["candidate_rows"] == 100
        assert rep_allow.json()["report"]["quarantine_rows"] == 3
        ex_allow = client.get(f"/api/v2/executions/{res['allow_id']}", headers=H(tokens, "viewer")).json()
        assert ex_allow["publication_state"] == "SUCCEEDED"

        # Gate execution 1: PARTIAL
        ex_partial = client.get(f"/api/v2/executions/{res['partial_id']}", headers=H(tokens, "viewer")).json()
        assert ex_partial["publication_state"] == "PARTIAL"

        # Gate execution 2: CLOSED_UNRESOLVED
        ex_closed = client.get(f"/api/v2/executions/{res['closed_unresolved_id']}", headers=H(tokens, "viewer")).json()
        assert ex_closed["resolution"] == "CLOSED_UNRESOLVED"
        assert ex_closed["unresolved"] is True

        # Gate execution 3: pending report (202)
        rep_gate_pending = client.get(f"/api/v2/executions/{res['gate_pending_id']}/report", headers=H(tokens, "viewer"))
        assert rep_gate_pending.status_code == 202
        assert rep_gate_pending.json()["state"] == "STARTED"

        # Gate execution 4: DENY
        rep_deny = client.get(f"/api/v2/executions/{res['deny_ids'][0]}/report", headers=H(tokens, "viewer"))
        assert rep_deny.status_code == 200
        assert rep_deny.json()["accepted"]["decision"]["decision"] == "DENY"

        # Audit execution 0: PASS (measured_rows=5)
        rep_pass = client.get(f"/api/v2/executions/{res['audit_pass_ids'][0]}/report", headers=H(tokens, "viewer"))
        assert rep_pass.status_code == 200
        assert rep_pass.json()["accepted"]["assessment"] == "PASS"
        assert rep_pass.json()["report"]["measured_rows"] == 5

        # Audit execution 1: VIOLATION (measured_rows=0)
        rep_viol = client.get(f"/api/v2/executions/{res['audit_violation_id']}/report", headers=H(tokens, "viewer"))
        assert rep_viol.status_code == 200
        assert rep_viol.json()["accepted"]["assessment"] == "VIOLATION"
        assert rep_viol.json()["report"]["measured_rows"] == 0

        # Audit execution 2: ERROR
        rep_err = client.get(f"/api/v2/executions/{res['audit_error_id']}/report", headers=H(tokens, "viewer"))
        assert rep_err.status_code == 200
        assert rep_err.json()["accepted"]["assessment"] == "ERROR"

        # Audit execution 3: pending report (202)
        rep_audit_pending = client.get(f"/api/v2/executions/{res['audit_pending_id']}/report", headers=H(tokens, "viewer"))
        assert rep_audit_pending.status_code == 202
        assert rep_audit_pending.json()["state"] == "STARTED"

        # Empty dataset: zero executions
        empty_ex = client.get(f"/api/v2/datasets/{DS_EMPTY}/executions", headers=H(tokens, "viewer")).json()
        assert len(empty_ex["items"]) == 0

        # Scope datasets registered
        assert client.get(f"/api/v2/datasets/{DS_OTHER}", headers=H(tokens, "viewer")).status_code == 200
        assert client.get(f"/api/v2/datasets/{DS_OVERFLOW}", headers=H(tokens, "viewer")).status_code == 200

        # Viewer principal
        me = client.get("/api/v2/me", headers=H(tokens, "viewer")).json()
        assert me["principal"] == VIEWER_PRINCIPAL

        # Viewer other: denied on main dataset, allowed on silver.other
        assert client.get(f"/api/v2/datasets/{DS_MAIN}/executions", headers=H(tokens, "viewer_other")).status_code == 403
        assert client.get(f"/api/v2/datasets/{DS_OTHER}/executions", headers=H(tokens, "viewer_other")).status_code == 200


def test_server_stays_available_after_testclient_closes(fresh_database_url):
    store = Store(fresh_database_url, clock=FixedClock())
    reg_data, tokens = console_token_registry()
    app = create_app(store, TokenRegistry.from_dict(reg_data), NoWorker())

    with TestClient(app) as client:
        seed_console(client, tokens)

    # Server stays available via another client / network after seeding TestClient closes
    with TestClient(app) as client2:
        me = client2.get("/api/v2/me", headers=H(tokens, "viewer"))
        assert me.status_code == 200
        assert me.json()["principal"] == VIEWER_PRINCIPAL
        ds = client2.get("/api/v2/datasets", headers=H(tokens, "viewer"))
        assert ds.status_code == 200

    # Start uvicorn server with pre-bound socket in a background thread
    sock = reserve_socket(0)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    t.start()
    try:
        time.sleep(0.3)
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/v2/me",
            headers={"Authorization": f"Bearer {tokens['viewer']}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["principal"] == VIEWER_PRINCIPAL
    finally:
        server.should_exit = True
        t.join(timeout=3)
        try:
            sock.close()
        except Exception:
            pass
        store.engine.dispose()


def test_cli_rejects_existing_output_dir(tmp_path):
    existing = tmp_path / "already_exists"
    existing.mkdir()
    marker = existing / "marker.txt"
    marker.write_text("untouched")

    rc = main(["--output", str(existing), "--port", "0"])
    assert rc == 1
    assert marker.read_text() == "untouched"


def test_cli_rejects_occupied_port(tmp_path):
    sock = reserve_socket(0)
    occupied_port = sock.getsockname()[1]
    out_dir = tmp_path / "should_not_exist"
    try:
        rc = main(["--output", str(out_dir), "--port", str(occupied_port)])
        assert rc == 1
        assert not out_dir.exists()
    finally:
        sock.close()


def test_cli_mode_permissions_and_state_json_safety(tmp_path, fresh_database_url, monkeypatch):
    out_dir = tmp_path / "cli_run"
    monkeypatch.setattr("integration.console_local.init_isolated_db", lambda s_url, name: fresh_database_url)
    monkeypatch.setenv("DQ_TEST_DATABASE_URL", "postgresql://safe-test-placeholder")
    monkeypatch.setattr(uvicorn.Server, "run", lambda self, sockets=None: None)

    rc = main(["--output", str(out_dir), "--port", "0"])
    assert rc == 0

    # Output directory permissions: 0700
    assert stat.S_IMODE(out_dir.stat().st_mode) == 0o700
    tokens_dir = out_dir / "tokens"
    assert stat.S_IMODE(tokens_dir.stat().st_mode) == 0o700

    # Token files permissions: 0600
    token_files = list(tokens_dir.glob("*.token"))
    assert len(token_files) >= 8
    for tf in token_files:
        assert stat.S_IMODE(tf.stat().st_mode) == 0o600

    # state.json permissions: 0600
    state_file = out_dir / "state.json"
    assert state_file.exists()
    assert stat.S_IMODE(state_file.stat().st_mode) == 0o600

    # Safe state: no database URLs, passwords, or raw token strings
    state_text = state_file.read_text()
    assert "postgresql:" not in state_text
    assert "Bearer" not in state_text
    for tf in token_files:
        tok = tf.read_text().strip()
        assert tok not in state_text

    state_data = json.loads(state_text)
    assert state_data["api_url"].startswith("http://127.0.0.1:")
    assert state_data["database_name"].startswith("dqconsole_")
    assert state_data["expected_counts"]["total"] == 51
    assert state_data["expected_counts"]["gate"] == 25
    assert state_data["expected_counts"]["audit"] == 26


def test_cli_missing_prerequisites(tmp_path, monkeypatch):
    out_dir = tmp_path / "missing_prereqs"
    monkeypatch.delenv("DQ_TEST_DATABASE_URL", raising=False)
    rc = main(["--output", str(out_dir), "--port", "0"])
    assert rc == 1
    assert not out_dir.exists()
