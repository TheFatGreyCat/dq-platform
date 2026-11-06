"""Synthetic console smoke seed and CLI lifecycle for local readiness."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import sqlalchemy as sa
import uvicorn
from fastapi.testclient import TestClient

from anomaly import AnomalyClient
from app import create_app
from auth import TokenRegistry
from models import RUNTIME_CONF
from store import Store

try:
    from integration.helpers import (
        DT,
        H0,
        VERSIONS,
        FixedClock,
        execution_request,
        gate_policy,
        gate_suite,
        registration,
        result_for,
        snap,
        write_report,
    )
    from integration.test_execution_acceptance import (
        H,
        NoWorker,
        exact,
        intent,
        k,
        outcome,
        publish_ok,
        setup_gate,
    )
except ImportError:
    from .helpers import (  # type: ignore
        DT,
        H0,
        VERSIONS,
        FixedClock,
        execution_request,
        gate_policy,
        gate_suite,
        registration,
        result_for,
        snap,
        write_report,
    )
    from .test_execution_acceptance import (  # type: ignore
        H,
        NoWorker,
        exact,
        intent,
        k,
        outcome,
        publish_ok,
        setup_gate,
    )

POSTGRES_DIR = Path(__file__).resolve().parents[3] / "postgres"
DS_MAIN = "silver.acceptance"
DS_EMPTY = "silver.empty"
DS_OTHER = "silver.other"
DS_OVERFLOW = "silver.console_" + "x" * 80
VIEWER_PRINCIPAL = "console_viewer_" + "x" * 80


def _url_with_db(url: str, db: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{db}"))


def _libpq(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def psql(url: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["psql", _libpq(url), "-v", "ON_ERROR_STOP=1", "-X", "-q", *args],
        capture_output=True,
        text=True,
        cwd=POSTGRES_DIR,
    )


def reserve_socket(port: int = 0) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(128)
    return sock


def init_isolated_db(server_url: str, db_name: str) -> str:
    if shutil.which("psql") is None:
        raise RuntimeError("Prerequisite missing: psql executable not found in PATH")
    admin = sa.create_engine(server_url, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as c:
            c.execute(sa.text(f'CREATE DATABASE "{db_name}"'))
    finally:
        admin.dispose()

    db_url = _url_with_db(server_url, db_name)
    init_sql = POSTGRES_DIR / "init.sql"
    if not init_sql.exists():
        raise RuntimeError(f"Database schema file not found at {init_sql}")
    done = psql(db_url, "-f", str(init_sql))
    if done.returncode != 0:
        raise RuntimeError(f"Database schema initialization failed via psql (exit code {done.returncode})")
    return db_url


def console_token_registry() -> tuple[dict, dict]:
    tokens: dict[str, str] = {}
    principals: list[dict] = []
    datasets = [DS_MAIN, DS_EMPTY, DS_OTHER, DS_OVERFLOW]

    def add(key: str, principal_name: str, grants: list[dict]):
        tok = f"tok-{key}-{uuid.uuid4().hex}"
        tokens[key] = tok
        principals.append({
            "principal": principal_name,
            "token_sha256": hashlib.sha256(tok.encode()).hexdigest(),
            "grants": grants,
        })

    add("viewer", VIEWER_PRINCIPAL, [
        {"dataset_id": d, "role": "viewer", "workflow": None, "purpose": None} for d in datasets
    ])
    for r in ("editor", "approver", "operator"):
        add(r, r, [
            {"dataset_id": d, "role": r, "workflow": None, "purpose": None} for d in datasets
        ])
    add("editor2", "editor2", [
        {"dataset_id": d, "role": "editor", "workflow": None, "purpose": None} for d in datasets
    ] + [
        {"dataset_id": d, "role": "approver", "workflow": None, "purpose": None} for d in datasets
    ])
    add("job_gate", "job_gate", [
        {"dataset_id": d, "role": "job", "workflow": "WRITE_GATE", "purpose": "default"}
        for d in (DS_MAIN, DS_OTHER)
    ])
    add("job_audit", "job_audit", [
        {"dataset_id": d, "role": "job", "workflow": "AUDIT_TABLE", "purpose": "nightly"}
        for d in (DS_MAIN, DS_OTHER)
    ])
    add("job_other", "job_other", [
        {"dataset_id": DS_MAIN, "role": "job", "workflow": "AUDIT_TABLE", "purpose": "other"}
    ])
    add("viewer_other", "viewer_other", [
        {"dataset_id": DS_OTHER, "role": "viewer", "workflow": None, "purpose": None}
    ])
    return {"principals": principals}, tokens


def write_audit_report(
    plan: dict,
    attempt_id: str,
    *,
    measured_rows: int = 5,
    status: str = "OK",
    error: dict | None = None,
) -> dict:
    sb = plan["stage_bindings"][0]
    r = sb["rules"][0]
    if status == "ERROR":
        results = [result_for(r, sb, status="ERROR", error=error or {"code": "STORAGE_UNAVAILABLE", "params": {}})]
    else:
        results = [result_for(r, sb, value=measured_rows, status="OK")]
    return {
        "attempt_id": attempt_id,
        "dataset_id": plan["dataset_id"],
        "plan_hash": plan["plan_hash"],
        "suite_revision_id": plan["suite_revision"]["revision_id"],
        "policy_revision_id": plan["policy_revision"]["revision_id"],
        "engine": plan["engine"],
        "interpreter_version": plan["interpreter_version"],
        "runtime_conf": dict(RUNTIME_CONF),
        "runtime_versions": VERSIONS,
        "parameters": plan["parameters"],
        "results": results,
        "audit_reference": plan["input_snapshot"],
        "measured_rows": measured_rows,
        "classification_status": "NOT_APPLICABLE",
    }


def seed_console(client: TestClient, tokens: dict[str, str]) -> dict:
    reg_main, mid, _, _, _ = setup_gate(client, tokens, DS_MAIN, ratio="0.05", retry=3600)

    for ds in (DS_EMPTY, DS_OTHER, DS_OVERFLOW):
        res = client.put(
            f"/api/v2/datasets/{ds}",
            json=registration(retry=3600, cleanup=False),
            headers=H(tokens, "editor", k()),
        )
        assert res.status_code == 200, res.text

    audit_suite = {
        "format": "dq-rules/v3",
        "workflow": "AUDIT_TABLE",
        "parameters": {},
        "rules": [
            {
                "rule_id": "audit_size",
                "dimension": "completeness",
                "scope": "TABLE",
                "stage": "AUDIT_SNAPSHOT",
                "check": {"kind": "size", "assert": {"op": ">=", "value": 1}},
            }
        ],
    }
    audit_policy = {
        "rules": [{"rule_id": "audit_size", "action": "BLOCK", "required": True}],
        "min_candidate_rows": 0,
        "min_clean_rows": 0,
        "max_quarantine_ratio": "0",
        "anomaly": {"enabled": False},
    }

    s_audit = client.post(
        f"/api/v2/datasets/{DS_MAIN}/suites",
        json={"name": "audit", "workflow": "AUDIT_TABLE", "description": None},
        headers=H(tokens, "editor", k()),
    )
    assert s_audit.status_code == 201, s_audit.text
    sid_audit = s_audit.json()["suite_id"]

    r_audit = client.post(
        f"/api/v2/datasets/{DS_MAIN}/suites/{sid_audit}/revisions",
        json=audit_suite,
        headers=H(tokens, "editor", k()),
    )
    assert r_audit.status_code == 201, r_audit.text
    revid_audit = r_audit.json()["revision_id"]

    p_audit = client.post(
        f"/api/v2/datasets/{DS_MAIN}/suites/{sid_audit}/policy-revisions",
        json=audit_policy,
        headers=H(tokens, "editor", k()),
    )
    assert p_audit.status_code == 201, p_audit.text
    polid_audit = p_audit.json()["policy_revision_id"] if "policy_revision_id" in p_audit.json() else p_audit.json()["revision_id"]

    assert client.put(
        f"/api/v2/datasets/{DS_MAIN}/purposes/AUDIT_TABLE/nightly",
        json={"description": None},
        headers=H(tokens, "editor"),
    ).status_code in (200, 201)
    b_nightly = client.put(
        f"/api/v2/datasets/{DS_MAIN}/purposes/AUDIT_TABLE/nightly/binding",
        json={
            "expected_binding_version": 0,
            "suite_id": sid_audit,
            "suite_revision_id": revid_audit,
            "policy_revision_id": polid_audit,
            "reason": "audit nightly",
        },
        headers=H(tokens, "approver", k()),
    )
    assert b_nightly.status_code == 200, b_nightly.text

    assert client.put(
        f"/api/v2/datasets/{DS_MAIN}/purposes/AUDIT_TABLE/other",
        json={"description": None},
        headers=H(tokens, "editor"),
    ).status_code in (200, 201)
    b_other = client.put(
        f"/api/v2/datasets/{DS_MAIN}/purposes/AUDIT_TABLE/other/binding",
        json={
            "expected_binding_version": 0,
            "suite_id": sid_audit,
            "suite_revision_id": revid_audit,
            "policy_revision_id": polid_audit,
            "reason": "audit other",
        },
        headers=H(tokens, "approver", k()),
    )
    assert b_other.status_code == 200, b_other.text

    base_time = datetime(2026, 10, 20, 0, 0, 0, tzinfo=timezone.utc)
    gate_eids: list[str] = []
    audit_eids: list[str] = []
    execution_records: list[dict] = []

    # 25 WRITE_GATE executions
    for i in range(25):
        event_time = (base_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        req = execution_request(DS_MAIN, reg_main, mid, event_time=event_time)
        e = client.post("/api/v2/executions", json=req, headers=H(tokens, "job_gate", k()))
        assert e.status_code == 202, e.text
        eid = e.json()["execution_id"]
        plan = e.json()["plan"]
        gate_eids.append(eid)

        a = client.post(
            f"/api/v2/executions/{eid}/attempts",
            json={"runner_identity": f"runner-gate-{i}", "image_digest": None, "previous_attempt_id": None, "previous_stop": None},
            headers=H(tokens, "job_gate"),
        )
        assert a.status_code == 201, a.text
        aid = a.json()["attempt"]["attempt_id"]

        if i == 0:
            rep = write_report(plan, aid, candidate=100, quarantine=3)
            r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
            assert r.status_code == 200, r.text
            pub = publish_ok(client, tokens, reg_main, eid, silver_version=4, q_version=2, q_rows=3)
            assert pub.status_code == 200, pub.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "WRITE_GATE",
                "purpose": "default",
                "event_time": event_time,
                "verdict": "ALLOW",
                "state": "COMPLETED",
                "publication_state": "SUCCEEDED",
                "unresolved": False,
            })
        elif i == 1:
            rep = write_report(plan, aid, candidate=100, quarantine=3)
            r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
            assert r.status_code == 200, r.text
            outcome(client, tokens, eid, "quarantine", {"state": "IN_PROGRESS", "expected_outcome_version": 0, "intent": intent(eid, "quarantine")})
            outcome(client, tokens, eid, "quarantine", {**exact(reg_main, "quarantine", 2, 3), "expected_outcome_version": 1})
            outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0, "intent": intent(eid, "silver")})
            u = outcome(client, tokens, eid, "silver", {"state": "UNKNOWN", "expected_outcome_version": 1, "reason": "timeout"})
            assert u.status_code == 200 and u.json()["publication_state"] == "PARTIAL", u.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "WRITE_GATE",
                "purpose": "default",
                "event_time": event_time,
                "verdict": "ALLOW",
                "state": "REPORTED",
                "publication_state": "PARTIAL",
                "unresolved": False,
            })
        elif i == 2:
            rep = write_report(plan, aid, candidate=100, quarantine=3)
            r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
            assert r.status_code == 200, r.text
            outcome(client, tokens, eid, "quarantine", {"state": "IN_PROGRESS", "expected_outcome_version": 0, "intent": intent(eid, "quarantine")})
            outcome(client, tokens, eid, "quarantine", {**exact(reg_main, "quarantine", 2, 3), "expected_outcome_version": 1})
            outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0, "intent": intent(eid, "silver")})
            u = outcome(client, tokens, eid, "silver", {"state": "UNKNOWN", "expected_outcome_version": 1, "reason": "timeout"})
            assert u.status_code == 200, u.text
            stop = client.post(
                f"/api/v2/executions/{eid}/attempts/{aid}/stop-confirmation",
                json={"reference": "local-process:r:1:exit=0"},
                headers=H(tokens, "job_gate"),
            )
            assert stop.status_code in (200, 201), stop.text
            close = {
                "reason_code": "UNRESOLVABLE_LOG_EXPIRED",
                "reason": "log expired",
                "writer_stop": {"confirmed_stopped": True, "reference": "sched:stop"},
                "evidence": [{
                    "destination": "silver",
                    "table_id": reg_main["silver"]["table_id"],
                    "snapshot_version": 9,
                    "checked_through_version": None,
                    "txn_present": False,
                }],
            }
            c = client.post(f"/api/v2/executions/{eid}/publication/close-unresolved", json=close, headers=H(tokens, "operator", k()))
            assert c.status_code == 200 and c.json()["resolution"] == "CLOSED_UNRESOLVED", c.text
            chk = client.get(f"/api/v2/executions/{eid}", headers=H(tokens, "viewer"))
            assert chk.status_code == 200 and chk.json()["unresolved"] is True, chk.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "WRITE_GATE",
                "purpose": "default",
                "event_time": event_time,
                "verdict": "ALLOW",
                "state": "COMPLETED",
                "publication_state": "PARTIAL",
                "unresolved": True,
                "resolution": "CLOSED_UNRESOLVED",
            })
        elif i == 3:
            execution_records.append({
                "execution_id": eid,
                "workflow": "WRITE_GATE",
                "purpose": "default",
                "event_time": event_time,
                "verdict": None,
                "state": "STARTED",
                "publication_state": "NOT_STARTED",
                "unresolved": False,
            })
        else:
            rep = write_report(plan, aid, candidate=100, quarantine=6)
            r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
            assert r.status_code == 200, r.text
            assert r.json()["decision"]["decision"] == "DENY", r.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "WRITE_GATE",
                "purpose": "default",
                "event_time": event_time,
                "verdict": "DENY",
                "state": "COMPLETED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })

    # 26 AUDIT_TABLE executions
    for j in range(26):
        event_time = (base_time + timedelta(minutes=25 + j)).strftime("%Y-%m-%dT%H:%M:%SZ")
        purpose = "other" if j == 25 else "nightly"
        who = "job_other" if j == 25 else "job_audit"
        req = {
            "dataset_id": DS_MAIN,
            "workflow": "AUDIT_TABLE",
            "purpose": purpose,
            "event_time": event_time,
            "parameters": {},
            "history_context": "daily",
            "partition_values": None,
            "input_snapshot": snap(reg_main["silver"], 3),
            "target_bases": None,
            "compat_manifest_id": mid,
            "previous_execution_id": None,
        }
        e = client.post("/api/v2/executions", json=req, headers=H(tokens, who, k()))
        assert e.status_code == 202, e.text
        eid = e.json()["execution_id"]
        plan = e.json()["plan"]
        audit_eids.append(eid)

        a = client.post(
            f"/api/v2/executions/{eid}/attempts",
            json={"runner_identity": f"runner-audit-{j}", "image_digest": None, "previous_attempt_id": None, "previous_stop": None},
            headers=H(tokens, who),
        )
        assert a.status_code == 201, a.text
        aid = a.json()["attempt"]["attempt_id"]

        if j == 0:
            rep = write_audit_report(plan, aid, measured_rows=5, status="OK")
            res = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, who))
            assert res.status_code == 200 and res.json()["assessment"] == "PASS", res.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "AUDIT_TABLE",
                "purpose": purpose,
                "event_time": event_time,
                "verdict": "PASS",
                "state": "COMPLETED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })
        elif j == 1:
            rep = write_audit_report(plan, aid, measured_rows=0, status="OK")
            res = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, who))
            assert res.status_code == 200 and res.json()["assessment"] == "VIOLATION", res.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "AUDIT_TABLE",
                "purpose": purpose,
                "event_time": event_time,
                "verdict": "VIOLATION",
                "state": "COMPLETED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })
        elif j == 2:
            rep = write_audit_report(plan, aid, measured_rows=0, status="ERROR")
            res = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, who))
            assert res.status_code == 200 and res.json()["assessment"] == "ERROR", res.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "AUDIT_TABLE",
                "purpose": purpose,
                "event_time": event_time,
                "verdict": "ERROR",
                "state": "FAILED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })
        elif j == 3:
            execution_records.append({
                "execution_id": eid,
                "workflow": "AUDIT_TABLE",
                "purpose": purpose,
                "event_time": event_time,
                "verdict": None,
                "state": "STARTED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })
        else:
            rep = write_audit_report(plan, aid, measured_rows=5, status="OK")
            res = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, who))
            assert res.status_code == 200 and res.json()["assessment"] == "PASS", res.text
            execution_records.append({
                "execution_id": eid,
                "workflow": "AUDIT_TABLE",
                "purpose": purpose,
                "event_time": event_time,
                "verdict": "PASS",
                "state": "COMPLETED",
                "publication_state": "NOT_APPLICABLE",
                "unresolved": False,
            })

    return {
        "datasets": {
            "main": DS_MAIN,
            "empty": DS_EMPTY,
            "other": DS_OTHER,
            "visual_fixture": DS_OVERFLOW,
        },
        "gate_ids": gate_eids,
        "audit_ids": audit_eids,
        "audit_nightly_ids": audit_eids[:25],
        "audit_other_ids": [audit_eids[25]],
        "allow_id": gate_eids[0],
        "partial_id": gate_eids[1],
        "closed_unresolved_id": gate_eids[2],
        "gate_pending_id": gate_eids[3],
        "deny_ids": gate_eids[4:],
        "audit_pass_ids": [audit_eids[0]] + audit_eids[4:],
        "audit_violation_id": audit_eids[1],
        "audit_error_id": audit_eids[2],
        "audit_pending_id": audit_eids[3],
        "all_execution_ids": gate_eids + audit_eids,
        "executions": execution_records,
        "counts": {
            "total": 51,
            "gate": 25,
            "audit": 26,
            "audit_nightly": 25,
            "audit_other": 1,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DQ Console local synthetic smoke server")
    parser.add_argument("--output", required=True, help="Private output directory for tokens and state")
    parser.add_argument("--port", type=int, default=0, help="Loopback port to bind (default: 0)")
    args = parser.parse_args(argv)

    output_dir = Path(args.output).resolve()
    if output_dir.exists():
        sys.stderr.write(f"Error: Output directory already exists: {output_dir}\n")
        return 1

    try:
        sock = reserve_socket(args.port)
    except Exception:
        sys.stderr.write("Error: Failed to reserve requested loopback socket\n")
        return 1

    allocated_port = sock.getsockname()[1]

    if shutil.which("psql") is None:
        sock.close()
        sys.stderr.write("Error: psql command not found in PATH\n")
        return 1

    server_url = os.environ.get("DQ_TEST_DATABASE_URL")
    if not server_url:
        sock.close()
        sys.stderr.write("Error: DQ_TEST_DATABASE_URL is not set\n")
        return 1

    try:
        output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
        output_dir.chmod(0o700)
        tokens_dir = output_dir / "tokens"
        tokens_dir.mkdir(mode=0o700, exist_ok=True)
        tokens_dir.chmod(0o700)
    except Exception as e:
        sock.close()
        sys.stderr.write(f"Error: Failed to create output directory: {e}\n")
        return 1

    run_id = f"dqconsole_{uuid.uuid4().hex[:12]}"
    db_name = run_id
    store = None
    try:
        db_url = init_isolated_db(server_url, db_name)
    except Exception:
        sock.close()
        sys.stderr.write(f"Error: Database initialization failed for owned database {db_name} in {output_dir}\n")
        return 1

    try:
        registry_data, tokens = console_token_registry()
        token_files = {}
        for role, tok in tokens.items():
            t_file = tokens_dir / f"{role}.token"
            t_file.write_text(tok)
            t_file.chmod(0o600)
            token_files[role] = str(t_file)

        store = Store(db_url, clock=FixedClock())
        token_registry = TokenRegistry.from_dict(registry_data)
        app = create_app(store, token_registry, NoWorker())

        with TestClient(app) as client:
            seed_res = seed_console(client, tokens)

        state_data = {
            "api_url": f"http://127.0.0.1:{allocated_port}",
            "database_name": db_name,
            "run_id": run_id,
            "datasets": seed_res["datasets"],
            "token_files": token_files,
            "expected_counts": seed_res["counts"],
            "gate_ids": seed_res["gate_ids"],
            "audit_ids": seed_res["audit_ids"],
            "audit_nightly_ids": seed_res["audit_nightly_ids"],
            "audit_other_ids": seed_res["audit_other_ids"],
            "allow_id": seed_res["allow_id"],
            "partial_id": seed_res["partial_id"],
            "closed_unresolved_id": seed_res["closed_unresolved_id"],
            "gate_pending_id": seed_res["gate_pending_id"],
            "deny_ids": seed_res["deny_ids"],
            "audit_pass_ids": seed_res["audit_pass_ids"],
            "audit_violation_id": seed_res["audit_violation_id"],
            "audit_error_id": seed_res["audit_error_id"],
            "audit_pending_id": seed_res["audit_pending_id"],
            "executions": seed_res["executions"],
        }
        state_file = output_dir / "state.json"
        state_file.write_text(json.dumps(state_data, indent=2))
        state_file.chmod(0o600)

        print(f"URL: http://127.0.0.1:{allocated_port}")
        print(f"RUN_ID: {run_id}")
        print(f"STATE: {state_file}")
        sys.stdout.flush()

        config = uvicorn.Config(app, host="127.0.0.1", port=allocated_port, log_level="warning")
        server = uvicorn.Server(config)
        try:
            server.run(sockets=[sock])
        except (KeyboardInterrupt, SystemExit):
            pass
        return 0
    except Exception:
        sys.stderr.write(f"Error during console smoke setup for owned database {db_name} in {output_dir}\n")
        return 1
    finally:
        try:
            sock.close()
        except Exception:
            pass
        if store is not None:
            store.engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
