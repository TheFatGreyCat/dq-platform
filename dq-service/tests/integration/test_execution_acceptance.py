"""Registry -> execution -> attempt -> report -> outcomes over real HTTP app + PostgreSQL 16."""
from __future__ import annotations

import json
import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from anomaly import AnomalyClient
from app import create_app
from auth import TokenRegistry
from store import Store

from .helpers import (DT, VERSIONS, FixedClock, execution_request, gate_policy, gate_suite, registration,
                      token_registry_dict, write_report)

DS, DS2 = "silver.acceptance", "silver.other"


class NoWorker(AnomalyClient):
    def __init__(self):
        super().__init__("http://127.0.0.1:9")

    def health(self):
        return False


@pytest.fixture
def env(fresh_database_url):
    database_url = fresh_database_url
    reg_dict, tokens = token_registry_dict([DS, DS2])
    clock = FixedClock()
    store = Store(database_url, clock=clock)
    app = create_app(store, TokenRegistry.from_dict(reg_dict), NoWorker())
    with TestClient(app) as client:
        yield client, tokens, clock, store


def H(tokens, who, key=None):
    h = {"Authorization": f"Bearer {tokens[who]}"}
    if key:
        h["Idempotency-Key"] = key
    return h


def k():
    return uuid.uuid4().hex


def setup_gate(client, tokens, dataset=DS, *, ratio="0.05", sql=False, retry=3600):
    reg = registration(retry=retry)
    assert client.put(f"/api/v2/datasets/{dataset}", json=reg, headers=H(tokens, "editor")).status_code == 200
    m = client.post("/api/v2/compat-manifests", json={"format": "dq-compat-v1", **VERSIONS},
                    headers=H(tokens, "editor", k()))
    assert m.status_code in (200, 201), m.text
    mid = m.json()["manifest_id"]
    assert client.put(f"/api/v2/datasets/{dataset}/compat-manifests/{mid}/acceptance",
                      json={"harness_reference": "file:///tmp/dq-wp1-h1/result.json"},
                      headers=H(tokens, "approver", k())).status_code == 200
    s = client.post(f"/api/v2/datasets/{dataset}/suites", json={"name": "gate", "workflow": "WRITE_GATE",
                                                                "description": None},
                    headers=H(tokens, "editor", k()))
    assert s.status_code == 201, s.text
    sid = s.json()["suite_id"]
    suite = gate_suite(sql=sql)
    r = client.post(f"/api/v2/datasets/{dataset}/suites/{sid}/revisions", json=suite, headers=H(tokens, "editor", k()))
    assert r.status_code == 201, r.text
    p = client.post(f"/api/v2/datasets/{dataset}/suites/{sid}/policy-revisions", json=gate_policy(suite, ratio),
                    headers=H(tokens, "editor", k()))
    assert p.status_code == 201, p.text
    assert client.put(f"/api/v2/datasets/{dataset}/purposes/WRITE_GATE/default", json={"description": None},
                      headers=H(tokens, "editor")).status_code == 201
    b = client.put(f"/api/v2/datasets/{dataset}/purposes/WRITE_GATE/default/binding",
                   json={"expected_binding_version": 0, "suite_id": sid, "suite_revision_id": r.json()["revision_id"],
                         "policy_revision_id": p.json()["revision_id"], "reason": "initial"},
                   headers=H(tokens, "approver", k()))
    assert b.status_code == 200, b.text
    return reg, mid, sid, r.json(), p.json()


def start(client, tokens, reg, mid, dataset=DS, **kw):
    e = client.post("/api/v2/executions", json=execution_request(dataset, reg, mid, **kw),
                    headers=H(tokens, "job_gate", k()))
    assert e.status_code == 202, e.text
    eid = e.json()["execution_id"]
    a = client.post(f"/api/v2/executions/{eid}/attempts", json={"runner_identity": "runner-1", "image_digest": None,
                                                                "previous_attempt_id": None, "previous_stop": None},
                    headers=H(tokens, "job_gate"))
    assert a.status_code == 201, a.text
    return eid, e.json()["plan"], a.json()["attempt"]["attempt_id"]


def outcome(client, tokens, eid, dest, body, who="job_gate"):
    base = {"intent": None, "commit": None, "rows": None, "reason": None, "error": None, "recovery": None}
    return client.put(f"/api/v2/executions/{eid}/write-outcomes/{dest}", json={**base, **body},
                      headers=H(tokens, who))


def exact(reg, dest, version, rows):
    return {"state": "SUCCEEDED", "commit": {"table_id": reg[dest]["table_id"], "version": version,
                                             "metadata_hash": "sha256:" + "1" * 64, "evidence": "EXACT"}, "rows": rows}


def intent(eid, dest):
    return {"txn_app_id": f"dq:{eid}:{dest}", "txn_version": 1}


def publish_ok(client, tokens, reg, eid, silver_version=4, q_version=2, q_rows=3):
    if q_rows:
        assert outcome(client, tokens, eid, "quarantine", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                                           "intent": intent(eid, "quarantine")}).status_code == 200
        r = outcome(client, tokens, eid, "quarantine", {**exact(reg, "quarantine", q_version, q_rows),
                                                        "expected_outcome_version": 1})
        assert r.status_code == 200, r.text
    assert outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                                   "intent": intent(eid, "silver")}).status_code == 200
    return outcome(client, tokens, eid, "silver", {**exact(reg, "silver", silver_version, 97),
                                                   "expected_outcome_version": 1})


def test_allow_100_3_publish_and_identical_retry(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    assert plan["partition_predicate"] == f"dt = DATE '{DT}'"
    rep = write_report(plan, aid)
    r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
    assert r.status_code == 200, r.text
    acc = r.json()
    assert acc["decision"]["decision"] == "ALLOW" and acc["publication_state"] == "NOT_STARTED"
    again = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
    assert again.status_code == 200 and again.json() == acc
    assert client.get(f"/api/v2/executions/{eid}", headers=H(tokens, "viewer")).json()["state"] == "REPORTED"
    # silver before quarantine is refused
    early = outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                                    "intent": intent(eid, "silver")})
    assert early.status_code == 409
    done = publish_ok(client, tokens, reg, eid)
    assert done.status_code == 200 and done.json()["publication_state"] == "SUCCEEDED"
    view = client.get(f"/api/v2/executions/{eid}", headers=H(tokens, "viewer")).json()
    assert view["state"] == "COMPLETED"
    pub = client.get(f"/api/v2/datasets/{DS}/publications?dt={DT}", headers=H(tokens, "viewer")).json()
    assert pub["current"]["execution_id"] == eid and pub["current"]["quarantine_rows"] == 3
    # identical terminal success retry -> 200 unchanged; changed request -> 409
    same = outcome(client, tokens, eid, "silver", {**exact(reg, "silver", 4, 97), "expected_outcome_version": 1})
    assert same.status_code == 200 and same.json()["outcome"]["outcome_version"] == 2
    changed = outcome(client, tokens, eid, "silver", {**exact(reg, "silver", 5, 97), "expected_outcome_version": 2})
    assert changed.status_code == 409
    # duplicate report with different content after acceptance -> 409
    rep2 = write_report(plan, aid, quarantine=4)
    assert client.post(f"/api/v2/executions/{eid}/reports", json=rep2, headers=H(tokens, "job_gate")).status_code == 409


def test_deny_100_6_has_no_publication(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    r = client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid, quarantine=6),
                    headers=H(tokens, "job_gate"))
    assert r.status_code == 200
    d = r.json()["decision"]
    assert d["decision"] == "DENY" and "QUARANTINE_RATIO_EXCEEDED" in [x["code"] for x in d["reasons"]]
    view = client.get(f"/api/v2/executions/{eid}", headers=H(tokens, "viewer")).json()
    assert view["state"] == "COMPLETED" and view["publication_state"] == "NOT_APPLICABLE"
    assert outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                                   "intent": intent(eid, "silver")}).status_code == 409


@pytest.mark.parametrize("mutate,status", [
    (lambda rep: rep["results"].pop(), 422),                                     # missing rule
    (lambda rep: rep.__setitem__("message", "x"), 422),                          # unknown field
    (lambda rep: next(r for r in rep["results"] if r["kind"] == "complete").__setitem__("metric_value", None),
     422),                                                                       # OK null
    (lambda rep: rep.__setitem__("plan_hash", "sha256:" + "f" * 64), 409),       # plan mismatch
    (lambda rep: rep["runtime_conf"].__setitem__("spark.sql.ansi.enabled", "true"), 422),
])
def test_malformed_reports_rejected_without_decision(env, mutate, status):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    rep = write_report(plan, aid)
    mutate(rep)
    r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
    assert r.status_code == status, r.text
    assert "x" not in json.dumps(r.json()["error"].get("field", ""))
    assert client.get(f"/api/v2/executions/{eid}/decision", headers=H(tokens, "viewer")).status_code == 202


def test_nan_and_duplicate_keys_rejected(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    body = json.dumps(write_report(plan, aid))
    for raw in (body.replace('"0.98"', 'NaN', 1), body[:-1] + ', "errors": []}'):
        r = client.post(f"/api/v2/executions/{eid}/reports", content=raw.encode(),
                        headers={**H(tokens, "job_gate"), "Content-Type": "application/json"})
        assert r.status_code == 422, r.text


def test_required_error_is_accepted_as_deny(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    rep = write_report(plan, aid, overrides={"amount_range": {"measurement_status": "ERROR", "metric_name": None,
                                                              "metric_value": None, "error": {
                                                                  "code": "CAST_OVERFLOW", "params": {
                                                                      "rule_id": "amount_range",
                                                                      "exception_class": "java.lang.ArithmeticException"}}}})
    r = client.post(f"/api/v2/executions/{eid}/reports", json=rep, headers=H(tokens, "job_gate"))
    assert r.status_code == 200 and r.json()["decision"]["decision"] == "DENY"


def test_auth_boundaries(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    assert client.get(f"/api/v2/executions/{eid}").status_code == 401
    assert client.get(f"/api/v2/executions/{eid}", headers={"Authorization": "Bearer nope"}).status_code == 401
    # viewer cannot terminate; audit job cannot touch gate execution
    t = {"state": "CANCELLED", "reason": "x", "error": None, "process_stop": {"confirmed_stopped": False,
                                                                              "reference": None}}
    assert client.post(f"/api/v2/executions/{eid}/terminate", json=t, headers=H(tokens, "viewer", k())).status_code == 403
    assert client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid),
                       headers=H(tokens, "job_audit")).status_code == 403
    assert client.get(f"/api/v2/executions/{eid}", headers=H(tokens, "job_other")).status_code == 403
    # wrong-purpose job cannot create execution
    req = execution_request(DS, reg, mid)
    assert client.post("/api/v2/executions", json=req, headers=H(tokens, "job_audit", k())).status_code == 403
    assert client.get("/api/v1/suites/x", headers=H(tokens, "viewer")).status_code == 404


def test_sql_self_approval_forbidden(env):
    client, tokens, _, _ = env
    reg = registration()
    client.put(f"/api/v2/datasets/{DS2}", json=reg, headers=H(tokens, "editor2"))
    s = client.post(f"/api/v2/datasets/{DS2}/suites", json={"name": "g", "workflow": "WRITE_GATE", "description": None},
                    headers=H(tokens, "editor2", k())).json()
    suite = gate_suite(sql=True)
    r = client.post(f"/api/v2/datasets/{DS2}/suites/{s['suite_id']}/revisions", json=suite,
                    headers=H(tokens, "editor2", k())).json()
    assert r["contains_sql"] is True
    # duplicate content by another principal keeps the original creator
    dup = client.post(f"/api/v2/datasets/{DS2}/suites/{s['suite_id']}/revisions", json=suite,
                      headers=H(tokens, "editor", k()))
    assert dup.status_code == 200 and dup.json()["created_by"] == "editor2"
    p = client.post(f"/api/v2/datasets/{DS2}/suites/{s['suite_id']}/policy-revisions", json=gate_policy(suite),
                    headers=H(tokens, "editor2", k())).json()
    client.put(f"/api/v2/datasets/{DS2}/purposes/WRITE_GATE/default", json={"description": None},
               headers=H(tokens, "editor2"))
    body = {"expected_binding_version": 0, "suite_id": s["suite_id"], "suite_revision_id": r["revision_id"],
            "policy_revision_id": p["revision_id"], "reason": "x"}
    url = f"/api/v2/datasets/{DS2}/purposes/WRITE_GATE/default/binding"
    assert client.put(url, json=body, headers=H(tokens, "editor2", k())).status_code == 403
    assert client.put(url, json=body, headers=H(tokens, "approver", k())).status_code == 200
    # stale CAS
    assert client.put(url, json=body, headers=H(tokens, "approver", k())).status_code == 409


def test_report_vs_terminate_race_one_wins(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    barrier = threading.Barrier(2)
    out = {}

    def report():
        barrier.wait()
        out["r"] = client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid),
                               headers=H(tokens, "job_gate")).status_code

    def term():
        barrier.wait()
        out["t"] = client.post(f"/api/v2/executions/{eid}/terminate",
                               json={"state": "FAILED", "reason": "x", "error": None,
                                     "process_stop": {"confirmed_stopped": False, "reference": None}},
                               headers=H(tokens, "operator", k())).status_code
    ts = [threading.Thread(target=report), threading.Thread(target=term)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    assert sorted(out.values()) == [200, 409], out


def test_failed_attempt_keeps_pin_and_late_report_rejected(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    r = client.post(f"/api/v2/executions/{eid}/terminate",
                    json={"state": "FAILED", "reason": "driver exit", "error": None,
                          "process_stop": {"confirmed_stopped": False, "reference": None}},
                    headers=H(tokens, "job_gate", k()))
    assert r.status_code == 200 and r.json()["attempts"][0]["process_stop"] == "UNCONFIRMED"
    assert client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid),
                       headers=H(tokens, "job_gate")).status_code == 409
    sc = client.post(f"/api/v2/executions/{eid}/attempts/{aid}/stop-confirmation",
                     json={"reference": "local-process:run:123:exit=1"}, headers=H(tokens, "job_gate"))
    assert sc.status_code == 200 and sc.json()["process_stop"] == "CONFIRMED"


def test_partial_then_abandon_and_close_unresolved_rules(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    eid, plan, aid = start(client, tokens, reg, mid)
    client.post(f"/api/v2/executions/{eid}/reports", json=write_report(plan, aid), headers=H(tokens, "job_gate"))
    outcome(client, tokens, eid, "quarantine", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                                "intent": intent(eid, "quarantine")})
    outcome(client, tokens, eid, "quarantine", {**exact(reg, "quarantine", 2, 3), "expected_outcome_version": 1})
    outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                            "intent": intent(eid, "silver")})
    u = outcome(client, tokens, eid, "silver", {"state": "UNKNOWN", "expected_outcome_version": 1, "reason": "timeout"})
    assert u.status_code == 200 and u.json()["publication_state"] == "PARTIAL"
    # UNKNOWN cannot be abandoned
    assert client.post(f"/api/v2/executions/{eid}/publication/abandon", json={"reason": "x", "error": None},
                       headers=H(tokens, "job_gate", k())).status_code == 409
    # resume needs confirmed writer stop
    rs = outcome(client, tokens, eid, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 2,
                                                 "intent": intent(eid, "silver"),
                                                 "recovery": {"absence": {"table_id": reg["silver"]["table_id"],
                                                                          "checked_through_version": 3},
                                                              "current_base_version": 3, "writer_stop": None,
                                                              "txn_evidence": None}})
    assert rs.status_code == 409 and rs.json()["error"]["code"] == "WRITER_UNCONFIRMED"
    # attempt stop confirmation then close-unresolved (log expired, no txn)
    client.post(f"/api/v2/executions/{eid}/attempts/{aid}/stop-confirmation",
                json={"reference": "local-process:r:1:exit=0"}, headers=H(tokens, "job_gate"))
    close = {"reason_code": "UNRESOLVABLE_LOG_EXPIRED", "reason": "log expired",
             "writer_stop": {"confirmed_stopped": True, "reference": "sched:stop"},
             "evidence": [{"destination": "silver", "table_id": reg["silver"]["table_id"], "snapshot_version": 9,
                           "checked_through_version": None, "txn_present": True}]}
    url = f"/api/v2/executions/{eid}/publication/close-unresolved"
    assert client.post(url, json=close, headers=H(tokens, "operator", k())).json()["error"]["code"] == \
        "COMMIT_EVIDENCE_PRESENT"
    close["evidence"][0]["txn_present"] = False
    c = client.post(url, json=close, headers=H(tokens, "operator", k()))
    assert c.status_code == 200 and c.json()["resolution"] == "CLOSED_UNRESOLVED"
    assert outcome(client, tokens, eid, "silver", {**exact(reg, "silver", 4, 97), "expected_outcome_version": 2},
                   who="operator").status_code == 409
    pub = client.get(f"/api/v2/datasets/{DS}/publications?dt={DT}", headers=H(tokens, "viewer")).json()
    assert pub["current"] is None and pub["unresolved_execution_id"] == eid


def test_newer_publication_not_regressed_by_old_callback(env):
    client, tokens, _, _ = env
    reg, mid, *_ = setup_gate(client, tokens)
    e1, p1, a1 = start(client, tokens, reg, mid)
    client.post(f"/api/v2/executions/{e1}/reports", json=write_report(p1, a1), headers=H(tokens, "job_gate"))
    outcome(client, tokens, e1, "quarantine", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                               "intent": intent(e1, "quarantine")})
    outcome(client, tokens, e1, "quarantine", {**exact(reg, "quarantine", 2, 3), "expected_outcome_version": 1})
    outcome(client, tokens, e1, "silver", {"state": "IN_PROGRESS", "expected_outcome_version": 0,
                                           "intent": intent(e1, "silver")})
    e2, p2, a2 = start(client, tokens, reg, mid)
    client.post(f"/api/v2/executions/{e2}/reports", json=write_report(p2, a2), headers=H(tokens, "job_gate"))
    assert publish_ok(client, tokens, reg, e2, silver_version=6, q_version=3).status_code == 200
    # late E1 success callback for older silver version 4
    late = outcome(client, tokens, e1, "silver", {**exact(reg, "silver", 4, 97), "expected_outcome_version": 1})
    assert late.status_code == 200
    pub = client.get(f"/api/v2/datasets/{DS}/publications?dt={DT}", headers=H(tokens, "viewer")).json()
    assert pub["current"]["execution_id"] == e2 and pub["current"]["silver"]["commit_version"] == 6


def test_payload_budget_rejects_activation(env):
    client, tokens, _, _ = env
    reg = registration()
    client.put(f"/api/v2/datasets/{DS2}", json=reg, headers=H(tokens, "editor"))
    s = client.post(f"/api/v2/datasets/{DS2}/suites", json={"name": "big", "workflow": "WRITE_GATE",
                                                            "description": None},
                    headers=H(tokens, "editor", k())).json()
    rules = [{"rule_id": f"cols_{i}", "dimension": "validity", "scope": "PARTITION", "stage": "RAW",
              "check": {"kind": "schema_columns", "columns": ["id"], "exact": False}} for i in range(500)]
    suite = {"format": "dq-rules/v3", "parameters": {}, "rules": rules}
    r = client.post(f"/api/v2/datasets/{DS2}/suites/{s['suite_id']}/revisions", json=suite,
                    headers=H(tokens, "editor", k())).json()
    pol = {"rules": [{"rule_id": x["rule_id"], "action": "BLOCK", "required": True} for x in rules],
           "min_candidate_rows": 1, "min_clean_rows": 1, "max_quarantine_ratio": "0", "anomaly": {"enabled": False}}
    p = client.post(f"/api/v2/datasets/{DS2}/suites/{s['suite_id']}/policy-revisions", json=pol,
                    headers=H(tokens, "editor", k())).json()
    client.put(f"/api/v2/datasets/{DS2}/purposes/WRITE_GATE/default", json={"description": None},
               headers=H(tokens, "editor"))
    b = client.put(f"/api/v2/datasets/{DS2}/purposes/WRITE_GATE/default/binding",
                   json={"expected_binding_version": 0, "suite_id": s["suite_id"], "suite_revision_id": r["revision_id"],
                         "policy_revision_id": p["revision_id"], "reason": "x"}, headers=H(tokens, "approver", k()))
    assert b.status_code == 422 and b.json()["error"]["field"] == "payload_budget"


def test_body_over_1mib_413(env):
    client, tokens, _, _ = env
    r = client.post("/api/v2/compat-manifests", content=b"{" + b" " * (1_048_577) + b"}",
                    headers={**H(tokens, "editor", k()), "Content-Type": "application/json"})
    assert r.status_code == 413
