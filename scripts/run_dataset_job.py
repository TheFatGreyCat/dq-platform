#!/usr/bin/env python3
"""Serialized launcher for one dataset's Silver/quarantine table pair (stdlib only).

  run_dataset_job.py --dataset D --slot-dir DIR --service-url URL [--token-file F] --command CMD ARGS...

Token: env DQ_API_TOKEN, or --token-file (mode 0600). Never printed.
Holds an exclusive flock on <slot-dir>/<sha256(sorted silver+quarantine table UUIDs)>.lock while the child runs.
Refuses (exit 2, message names only the execution id) when the service shows a REPORTED/unresolved execution
or an attempt whose writer process stop is not CONFIRMED.

Child contract (env): DQ_RUN_KEY (stable per invocation) and DQ_RUN_STATE_FILE (<slot-dir>/<lockname>.run.json).
As soon as the child opens an attempt it MUST write {"execution_id","attempt_id"} to DQ_RUN_STATE_FILE (0600).
After the child exits, this launcher POSTs stop-confirmation reference
`local-process:<run-key>:<pid>:exit=<code>`. Missing run-state = no confirmation (operator proof required).

Exit: child's exit code; 2 on refusal/slot busy/config/service error or failed stop confirmation.
Single host / shared slot-dir only: flock is not a distributed lock.

K8s mode (--k8s-executor-wait SECONDS): Spark driver = this pod (client mode), executors = pods labelled
dq-run-key=<DQ_RUN_KEY> (label set via spark.kubernetes.executor.label.dq-run-key by the workflow). Requires env
POD_NAME/POD_NAMESPACE and in-cluster SA files <DQ_K8S_SA_DIR or /var/run/secrets/kubernetes.io/serviceaccount>/{token,ca.crt};
API base from KUBERNETES_SERVICE_HOST/PORT (DQ_K8S_API_URL overrides, e.g. plain http in tests). DQ_RUN_KEY must be a valid
k8s label value (checked before launch). After the child exits and BEFORE confirming, the pods with that label are polled
until none is Pending/Running/Unknown (Succeeded/Failed/absent = stopped) or the timeout; timeout/API error = no confirmation
("refused: executors not confirmed stopped"; exit = child code or 2). Reference: `k8s-pod:<ns>/<pod>:<run-key>:exit=<code>`.
The flock is still taken but is pod-local; cross-pod mutual exclusion comes from Argo Workflows `synchronization.mutex`
plus the service's unresolved-execution refusal (check_clear).
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import signal
import ssl
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

SA_DIR_DEFAULT = "/var/run/secrets/kubernetes.io/serviceaccount"
LABEL_VALUE = re.compile(r"[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?")
RUNNING_PHASES = {"Pending", "Running", "Unknown"}
POLL_SEC = 0.5
KILL_GRACE_SEC = 10


class Refused(Exception):
    pass


def _api(base: str, token: str, method: str, path: str, body: dict | None = None, key: str | None = None, ctx=None):
    headers = {"Authorization": f"Bearer {token}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(base.rstrip("/") + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:  # never surface body/URL (may carry secrets)
        raise Refused(f"service error {method} status={e.code}") from None
    except (urllib.error.URLError, OSError, ValueError):
        raise Refused(f"service unreachable {method}") from None


def read_token(token_file: str | None) -> str:
    if token_file:
        if stat.S_IMODE(os.stat(token_file).st_mode) & 0o077:
            raise Refused("token file must be mode 0600")
        with open(token_file) as f:
            tok = f.read().strip()
    else:
        tok = os.environ.get("DQ_API_TOKEN", "").strip()
    if not tok:
        raise Refused("no token (DQ_API_TOKEN or --token-file)")
    return tok


def lock_name(registration: dict) -> str:
    ids = sorted([registration["silver"]["table_id"], registration["quarantine"]["table_id"]])
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def check_clear(base: str, token: str, dataset: str) -> None:
    """Refuse if any execution is REPORTED/unresolved or any attempt writer is not confirmed stopped."""
    cursor = None
    while True:
        q = "?limit=500" + (f"&cursor={urllib.parse.quote(cursor)}" if cursor else "")
        page = _api(base, token, "GET", f"/api/v2/datasets/{urllib.parse.quote(dataset, safe='')}/executions{q}")
        for e in page["items"]:
            blocked = e["state"] == "REPORTED" or e.get("unresolved") or any(
                a.get("process_stop") != "CONFIRMED" for a in e.get("attempts", []))
            if blocked:
                raise Refused(f"unresolved execution {e['execution_id']}")
        cursor = page.get("next_cursor")
        if not cursor:
            return


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--slot-dir", required=True)
    ap.add_argument("--service-url", required=True)
    ap.add_argument("--token-file")
    ap.add_argument("--k8s-executor-wait", type=float, metavar="SECONDS",
                    help="k8s mode: wait this long for labelled executor pods to stop before confirming")
    ap.add_argument("--command", nargs=argparse.REMAINDER, required=True)
    a = ap.parse_args(argv)
    if not a.command:
        ap.error("--command needs a program")
    try:
        return _launch(a)
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 2


def _launch(a) -> int:
    k8s = _k8s_config() if a.k8s_executor_wait is not None else None
    token = read_token(a.token_file)
    reg = _api(a.service_url, token, "GET", f"/api/v2/datasets/{urllib.parse.quote(a.dataset, safe='')}")["registration"]
    os.makedirs(a.slot_dir, mode=0o700, exist_ok=True)
    name = lock_name(reg)
    lock_fd = os.open(os.path.join(a.slot_dir, name + ".lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock_fd)
        raise Refused("slot busy") from None
    try:
        check_clear(a.service_url, token, a.dataset)
        if k8s and not LABEL_VALUE.fullmatch(os.environ.get("DQ_RUN_KEY") or "x"):
            raise Refused("DQ_RUN_KEY is not a valid k8s label value")
        run_state = os.path.join(a.slot_dir, name + ".run.json")
        if os.path.exists(run_state):
            os.unlink(run_state)
        run_key = os.environ.get("DQ_RUN_KEY") or uuid.uuid4().hex
        env = {**os.environ, "DQ_RUN_KEY": run_key, "DQ_RUN_STATE_FILE": run_state}
        code, pid = _run(a.command, env)
        return _confirm_stop(a, token, run_state, run_key, pid, code, k8s)
    finally:
        os.close(lock_fd)  # releases flock only after the child is gone


def _run(command: list[str], env: dict) -> tuple[int, int]:
    child = subprocess.Popen(command, env=env)

    def forward(signum, _frame):
        if child.poll() is None:
            child.send_signal(signal.SIGTERM)
            # SIGKILL escalation only for the owned child
            signal.signal(signal.SIGALRM, lambda *_: child.poll() is None and child.kill())
            signal.alarm(KILL_GRACE_SEC)

    old = {s: signal.signal(s, forward) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        return child.wait(), child.pid
    finally:
        signal.alarm(0)
        for s, h in old.items():
            signal.signal(s, h)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


def _confirm_stop(a, token: str, run_state: str, run_key: str, pid: int, code: int, k8s: dict | None = None) -> int:
    try:
        with open(run_state) as f:
            st = json.load(f)
        eid, aid = st["execution_id"], st["attempt_id"]
    except (OSError, ValueError, KeyError):
        print("refused: no run-state from child; stop confirmation needs operator proof", file=sys.stderr)
        return code or 2
    if k8s:
        try:
            _wait_executors_stopped(k8s, run_key, a.k8s_executor_wait)
        except Refused:
            print("refused: executors not confirmed stopped", file=sys.stderr)
            return code or 2
        ref = f"k8s-pod:{k8s['namespace']}/{k8s['pod']}:{run_key}:exit={code}"
    else:
        ref = f"local-process:{run_key}:{pid}:exit={code}"
    _api(a.service_url, token, "POST", f"/api/v2/executions/{eid}/attempts/{aid}/stop-confirmation",
         {"reference": ref}, key=f"stop-{run_key}-{aid}"[:128])
    return code


def _k8s_config() -> dict:
    pod, ns = os.environ.get("POD_NAME"), os.environ.get("POD_NAMESPACE")
    if not pod or not ns:
        raise Refused("k8s mode needs POD_NAME and POD_NAMESPACE")
    sa = os.environ.get("DQ_K8S_SA_DIR") or SA_DIR_DEFAULT
    token_path, ca_path = os.path.join(sa, "token"), os.path.join(sa, "ca.crt")
    if not (os.path.isfile(token_path) and os.path.isfile(ca_path)):
        raise Refused("k8s mode needs in-cluster service account token and ca.crt")
    base = os.environ.get("DQ_K8S_API_URL")
    if not base:
        host, port = os.environ.get("KUBERNETES_SERVICE_HOST"), os.environ.get("KUBERNETES_SERVICE_PORT")
        if not host or not port:
            raise Refused("k8s mode needs KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT")
        base = f"https://{f'[{host}]' if ':' in host else host}:{port}"
    return {"pod": pod, "namespace": ns, "token_path": token_path, "ca_path": ca_path, "base": base}


def _wait_executors_stopped(k8s: dict, run_key: str, timeout: float) -> None:
    """Return once no pod labelled dq-run-key=<run_key> is Pending/Running/Unknown; Refused on timeout/API error."""
    ctx = ssl.create_default_context(cafile=k8s["ca_path"]) if k8s["base"].startswith("https:") else None
    path = (f"/api/v1/namespaces/{urllib.parse.quote(k8s['namespace'], safe='')}/pods"
            f"?labelSelector={urllib.parse.quote('dq-run-key=' + run_key, safe='')}")
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(k8s["token_path"]) as f:
                sa_token = f.read().strip()
            pods = _api(k8s["base"], sa_token, "GET", path, ctx=ctx)["items"]
            running = any((p.get("status") or {}).get("phase", "Unknown") in RUNNING_PHASES for p in pods)
        except (OSError, KeyError, TypeError, AttributeError):
            raise Refused("kube api unusable") from None
        if not running:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise Refused("executors still running")
        time.sleep(min(POLL_SEC, remaining))


if __name__ == "__main__":
    sys.exit(main())
