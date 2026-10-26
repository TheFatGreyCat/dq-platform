"""Launcher + parity against a state-driven fake dq-service (real local HTTP server)."""
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))
import parity_check  # noqa: E402

TOKEN = "synthetic-token-not-a-secret-0001"
REG = {"silver": {"table_id": "bbbbbbbb-0000-0000-0000-000000000001"},
       "quarantine": {"table_id": "aaaaaaaa-0000-0000-0000-000000000002"}}


class Fake:
    def __init__(self):
        self.executions = []   # execution views
        self.confirmations = []  # (execution, attempt, reference)
        self.reports = {}      # id -> (accepted, report, rules)

    def handler(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _auth(self):
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    self._send(401, {"error": {"code": "UNAUTHENTICATED"}})
                    return False
                return True

            def do_GET(self):
                if not self._auth():
                    return
                p = self.path.split("?")[0]
                if p == "/api/v2/datasets/d1":
                    return self._send(200, {"registration": REG, "registration_hash": "x"})
                if p == "/api/v2/datasets/d1/executions":
                    return self._send(200, {"items": fake.executions, "next_cursor": None})
                parts = p.split("/")
                if len(parts) >= 6 and parts[3] == "executions" and parts[4] in fake.reports:
                    acc, rep, rules = fake.reports[parts[4]]
                    if parts[5] == "report":
                        return self._send(200, {"accepted": acc, "report": rep})
                    if parts[5] == "rules":
                        return self._send(200, {"items": rules, "next_cursor": None})
                if len(parts) == 5 and parts[3] == "executions" and parts[4] in fake.reports:
                    return self._send(200, {"state": "COMPLETED", "publication_state": "SUCCEEDED", "unresolved": False})
                self._send(404, {"error": {"code": "NOT_FOUND"}})

            def do_POST(self):
                if not self._auth():
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                parts = self.path.split("/")
                if parts[-1] == "stop-confirmation":
                    eid, aid = parts[4], parts[6]
                    fake.confirmations.append((eid, aid, body["reference"]))
                    for e in fake.executions:
                        if e["execution_id"] == eid:
                            for a in e["attempts"]:
                                if a["attempt_id"] == aid:
                                    a["process_stop"] = "CONFIRMED"
                    return self._send(200, {})
                self._send(404, {})

        return H


@pytest.fixture
def fake():
    f = Fake()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), f.handler())
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    f.url = f"http://127.0.0.1:{srv.server_port}"
    yield f
    srv.shutdown()


def launch(fake, slot, *cmd, token=TOKEN):
    env = {**os.environ, "DQ_API_TOKEN": token}
    return subprocess.Popen([sys.executable, str(SCRIPTS / "run_dataset_job.py"), "--dataset", "d1", "--slot-dir", str(slot),
                             "--service-url", fake.url, "--command", *cmd],
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


CHILD = ("import json,os,sys,time;"
         "open(os.environ['DQ_RUN_STATE_FILE'],'w').write(json.dumps({'execution_id':'e-new','attempt_id':'a-new'}));"
         "os.chmod(os.environ['DQ_RUN_STATE_FILE'],0o600);"
         "g=sys.argv[1];s=sys.argv[2];open(s,'w').close();"
         "[time.sleep(0.05) for _ in range(200) if not os.path.exists(g)];"
         "sys.exit(int(sys.argv[3]))")


def test_second_launcher_refused_while_first_holds_slot_then_confirmation_sent(fake, tmp_path):
    go, started = tmp_path / "go", tmp_path / "started"
    first = launch(fake, tmp_path / "slot", sys.executable, "-c", CHILD, str(go), str(started), "0")
    for _ in range(200):
        if started.exists():
            break
        time.sleep(0.05)
    assert started.exists()
    second = launch(fake, tmp_path / "slot", sys.executable, "-c", "print('should not run')")
    out2, err2 = second.communicate(timeout=20)
    assert second.returncode == 2 and "should not run" not in out2
    go.touch()
    out1, err1 = first.communicate(timeout=20)
    assert first.returncode == 0
    assert len(fake.confirmations) == 1
    eid, aid, ref = fake.confirmations[0]
    assert (eid, aid) == ("e-new", "a-new")
    assert ref.startswith("local-process:") and ref.endswith(":exit=0")
    for stream in (out1, err1, out2, err2):
        assert TOKEN not in stream and ref not in stream


def test_child_exit_code_propagates_and_is_in_reference(fake, tmp_path):
    go, started = tmp_path / "go", tmp_path / "started"
    go.touch()
    p = launch(fake, tmp_path / "slot", sys.executable, "-c", CHILD, str(go), str(started), "3")
    p.communicate(timeout=20)
    assert p.returncode == 3
    assert fake.confirmations[0][2].endswith(":exit=3")


def _exec(state, unresolved=False, stop="CONFIRMED", eid="e-old"):
    return {"execution_id": eid, "state": state, "unresolved": unresolved,
            "attempts": [{"attempt_id": "a-old", "process_stop": stop}]}


@pytest.mark.parametrize("ex", [_exec("REPORTED"), _exec("COMPLETED", unresolved=True),
                                _exec("FAILED", stop="UNCONFIRMED"), _exec("FAILED", stop=None)])
def test_prior_unresolved_execution_refuses_spawn(fake, tmp_path, ex):
    fake.executions.append(ex)
    marker = tmp_path / "ran"
    p = launch(fake, tmp_path / "slot", sys.executable, "-c", f"open({str(marker)!r},'w')")
    out, err = p.communicate(timeout=20)
    assert p.returncode == 2 and not marker.exists()
    assert "e-old" in err and TOKEN not in out + err


def test_clean_history_allows_spawn(fake, tmp_path):
    fake.executions.append(_exec("COMPLETED"))
    marker = tmp_path / "ran"
    p = launch(fake, tmp_path / "slot", sys.executable, "-c", f"open({str(marker)!r},'w')")
    p.communicate(timeout=20)
    assert marker.exists()  # no run-state written -> no confirmation, exit propagates child's 0
    assert fake.confirmations == []


def test_bad_token_refused_without_spawn(fake, tmp_path):
    marker = tmp_path / "ran"
    p = launch(fake, tmp_path / "slot", sys.executable, "-c", f"open({str(marker)!r},'w')", token="wrong-synthetic")
    out, err = p.communicate(timeout=20)
    assert p.returncode == 2 and not marker.exists() and "wrong-synthetic" not in out + err


# ---------------- k8s mode ----------------

KTOKEN = "synthetic-sa-token-not-a-secret"


class Kube:
    def __init__(self):
        self.phases = []      # list of phase-lists returned per poll; last one repeats
        self.status = 200
        self.requests = []    # (path, auth)


@pytest.fixture
def kube(tmp_path):
    k = Kube()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            k.requests.append((self.path, self.headers.get("Authorization")))
            phases = k.phases.pop(0) if len(k.phases) > 1 else (k.phases[0] if k.phases else [])
            data = json.dumps({"items": [{"status": {"phase": p}} for p in phases]}).encode()
            self.send_response(k.status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    k.url = f"http://127.0.0.1:{srv.server_port}"
    k.sa = tmp_path / "sa"
    k.sa.mkdir()
    (k.sa / "token").write_text(KTOKEN)
    (k.sa / "ca.crt").write_text("")
    yield k
    srv.shutdown()


def klaunch(fake, kube, tmp_path, wait="2", run_key="run-key_1.a", env_extra=None, cmd=None):
    env = {**os.environ, "DQ_API_TOKEN": TOKEN, "POD_NAME": "driver-0", "POD_NAMESPACE": "dq-ns",
           "DQ_K8S_API_URL": kube.url, "DQ_K8S_SA_DIR": str(kube.sa), "DQ_RUN_KEY": run_key, **(env_extra or {})}
    for k_, v in list(env.items()):
        if v is None:
            del env[k_]
    go = tmp_path / "go"
    go.touch()
    cmd = cmd or [sys.executable, "-c", CHILD, str(go), str(tmp_path / "started"), "0"]
    return subprocess.run([sys.executable, str(SCRIPTS / "run_dataset_job.py"), "--dataset", "d1", "--slot-dir",
                           str(tmp_path / "slot"), "--service-url", fake.url, "--k8s-executor-wait", wait,
                           "--command", *cmd], env=env, capture_output=True, text=True, timeout=30)


def test_k8s_confirms_after_executors_stop_with_pod_reference(fake, kube, tmp_path):
    kube.phases = [["Running", "Pending"], ["Running", "Succeeded"], ["Succeeded", "Failed"]]
    p = klaunch(fake, kube, tmp_path)
    assert p.returncode == 0, p.stderr
    assert fake.confirmations == [("e-new", "a-new", "k8s-pod:dq-ns/driver-0:run-key_1.a:exit=0")]
    assert len(kube.requests) == 3
    path, auth = kube.requests[0]
    assert path == "/api/v1/namespaces/dq-ns/pods?labelSelector=dq-run-key%3Drun-key_1.a"
    assert auth == f"Bearer {KTOKEN}"
    assert KTOKEN not in p.stdout + p.stderr and TOKEN not in p.stdout + p.stderr


def test_k8s_no_executor_pods_counts_as_stopped_and_exit_code_propagates(fake, kube, tmp_path):
    kube.phases = [[]]
    go = tmp_path / "go"
    go.touch()
    p = klaunch(fake, kube, tmp_path, cmd=[sys.executable, "-c", CHILD, str(go), str(tmp_path / "s"), "3"])
    assert p.returncode == 3
    assert fake.confirmations[0][2] == "k8s-pod:dq-ns/driver-0:run-key_1.a:exit=3"


@pytest.mark.parametrize("phase", ["Running", "Pending", "Unknown"])
def test_k8s_timeout_with_live_executor_never_confirms(fake, kube, tmp_path, phase):
    kube.phases = [[phase]]
    p = klaunch(fake, kube, tmp_path, wait="1")
    assert p.returncode == 2
    assert fake.confirmations == []
    assert "refused: executors not confirmed stopped" in p.stderr
    assert KTOKEN not in p.stdout + p.stderr and kube.url not in p.stdout + p.stderr


def test_k8s_timeout_keeps_child_exit_code(fake, kube, tmp_path):
    kube.phases = [["Running"]]
    go = tmp_path / "go"
    go.touch()
    p = klaunch(fake, kube, tmp_path, wait="0.5", cmd=[sys.executable, "-c", CHILD, str(go), str(tmp_path / "s"), "5"])
    assert p.returncode == 5 and fake.confirmations == []


def test_k8s_api_error_never_confirms(fake, kube, tmp_path):
    kube.status = 500
    p = klaunch(fake, kube, tmp_path, wait="1")
    assert p.returncode == 2 and fake.confirmations == []
    assert "refused: executors not confirmed stopped" in p.stderr


def test_k8s_api_unreachable_never_confirms(fake, kube, tmp_path):
    p = klaunch(fake, kube, tmp_path, wait="1", env_extra={"DQ_K8S_API_URL": "http://127.0.0.1:9"})
    assert p.returncode == 2 and fake.confirmations == []
    assert "refused: executors not confirmed stopped" in p.stderr


@pytest.mark.parametrize("key", ["bad key", "-lead", "trail-", "x" * 64, "a/b"])
def test_k8s_invalid_run_key_refused_before_launch(fake, kube, tmp_path, key):
    marker = tmp_path / "ran"
    p = klaunch(fake, kube, tmp_path, run_key=key, cmd=[sys.executable, "-c", f"open({str(marker)!r},'w')"])
    assert p.returncode == 2 and not marker.exists() and fake.confirmations == [] and kube.requests == []


@pytest.mark.parametrize("missing", ["POD_NAME", "POD_NAMESPACE"])
def test_k8s_requires_pod_identity(fake, kube, tmp_path, missing):
    marker = tmp_path / "ran"
    p = klaunch(fake, kube, tmp_path, env_extra={missing: None}, cmd=[sys.executable, "-c", f"open({str(marker)!r},'w')"])
    assert p.returncode == 2 and not marker.exists()


def test_k8s_requires_service_account_files(fake, kube, tmp_path):
    (kube.sa / "ca.crt").unlink()
    marker = tmp_path / "ran"
    p = klaunch(fake, kube, tmp_path, cmd=[sys.executable, "-c", f"open({str(marker)!r},'w')"])
    assert p.returncode == 2 and not marker.exists()


# ---------------- parity ----------------
def _doc(value="97", rows=97, decision="ALLOW"):
    rules = [{"rule_id": "r1", "scope": "CANDIDATE", "stage": "CLEAN", "kind": "size", "measurement_status": "OK",
              "metric_name": "Size", "metric_value": value, "schema_evidence": None, "failed_rows": None, "error": None}]
    report = {"candidate_rows": 100, "valid_rows": rows, "quarantine_rows": 3, "classification_status": "COMPLETE",
              "errors": []}
    acc = {"decision": {"decision": decision, "reasons": []}, "assessment": None, "accepted_at": "t"}
    return acc, report, rules


def _run_parity(fake, tmp_path, *extra):
    out = tmp_path / "out.json"
    env = {**os.environ, "DQ_API_TOKEN": TOKEN}
    r = subprocess.run([sys.executable, str(SCRIPTS / "parity_check.py"), "--python-execution", "p", "--scala-execution", "s",
                        "--service-url", fake.url, "--output", str(out), *extra], env=env, capture_output=True, text=True)
    return r, out


def test_parity_equal_executions_pass_even_with_decimal_formatting(fake, tmp_path):
    fake.reports["p"], fake.reports["s"] = _doc("97"), _doc("97.0")
    r, out = _run_parity(fake, tmp_path, "--expect-candidate", "100", "--expect-quarantined", "3")
    assert r.returncode == 0, r.stdout
    assert json.loads(out.read_text())["status"] == "PASS"


def test_parity_detects_value_count_and_decision_drift(fake, tmp_path):
    fake.reports["p"], fake.reports["s"] = _doc("97"), _doc("96", rows=96, decision="DENY")
    r, out = _run_parity(fake, tmp_path)
    assert r.returncode == 1
    problems = json.loads(out.read_text())["problems"]
    assert any("metric_value" in x for x in problems) and any("valid_rows" in x for x in problems)
    assert any(x.startswith("decision") for x in problems)


def test_parity_missing_report_exit_2(fake, tmp_path):
    fake.reports["p"] = _doc()
    r, _ = _run_parity(fake, tmp_path)
    assert r.returncode == 2
