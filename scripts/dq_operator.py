#!/usr/bin/env python3
"""Operator recovery CLI for the dq-service v2 API (stdlib only).

The operator token is read from --token-file (mode 0600) and is never printed or put on argv.
Evidence/body files are JSON produced by the JVM reconcile tooling and are sent verbatim.
"""
import argparse
import hashlib
import json
import os
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "/api/v2"
POLL_STATES = {"IN_PROGRESS", "UNKNOWN"}


def read_token(path):
    st = os.stat(path)
    if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        sys.exit("token file must be mode 0600")
    with open(path, encoding="utf-8") as f:
        token = f.read().strip()
    if not token:
        sys.exit("token file is empty")
    return token


def call(args, token, method, path, body=None, key=None):
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    req = urllib.request.Request(args.service_url.rstrip("/") + API + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=args.timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            code = json.loads(e.read())["error"]["code"]
        except Exception:
            code = "UNKNOWN"
        return e.code, {"error": {"code": code}}
    except (urllib.error.URLError, OSError):
        sys.exit("service unreachable")


def idem(*parts):
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:48]


def emit(status, payload):
    print(json.dumps({"http_status": status, "result": payload}, sort_keys=True))
    return 0 if 200 <= status < 300 else 2


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cmd_reconcile(args, token):
    """Evidence file: {"outcomes": {"<destination>": <OutcomeRequest>}}; PUT in quarantine, silver order."""
    ev = load(args.evidence_file).get("outcomes")
    if not isinstance(ev, dict) or not ev or set(ev) - {"quarantine", "silver"}:
        sys.exit("evidence file needs outcomes for quarantine and/or silver")
    last = (0, None)
    for dest in ("quarantine", "silver"):
        if dest not in ev:
            continue
        status, res = call(args, token, "PUT", f"/executions/{urllib.parse.quote(args.execution, safe='')}"
                           f"/write-outcomes/{dest}", ev[dest])
        last = (status, res)
        if not 200 <= status < 300:
            return emit(status, res)
    # bounded wait for the execution to show no unresolved destination
    deadline = time.monotonic() + args.wait
    while True:
        status, view = call(args, token, "GET", f"/executions/{urllib.parse.quote(args.execution, safe='')}")
        if status != 200:
            return emit(status, view)
        pending = {d: o["state"] for d, o in (view.get("outcomes") or {}).items() if o["state"] in POLL_STATES}
        if not pending:
            return emit(*last)
        if time.monotonic() >= deadline:
            print(json.dumps({"unresolved": pending}), file=sys.stderr)
            return 3
        time.sleep(args.interval)


def post_body(args, token, suffix):
    body = load(args.body_file)
    path = f"/executions/{urllib.parse.quote(args.execution, safe='')}/publication/{suffix}"
    key = args.idempotency_key or idem(path, json.dumps(body, sort_keys=True))
    return emit(*call(args, token, "POST", path, body, key))


def _q(s):
    return urllib.parse.quote(s, safe="")


def cmd_retention(args, token):
    """All retention pages -> --output (input for the Maintenance spark job)."""
    items, cursor = [], None
    while True:
        qs = "?limit=500" + (f"&cursor={_q(cursor)}" if cursor else "")
        status, page = call(args, token, "GET", f"/datasets/{_q(args.dataset)}/retention{qs}")
        if status != 200:
            return emit(status, page)
        items += page["items"]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump({"items": items}, f)
    return emit(200, {"items": len(items), "deletable": sum(1 for i in items if not i["pinned"])})


def cmd_cleanup_records(args, token):
    """Removed-items JSON from Maintenance -> one cleanup record per item (idempotent per attempt+uri)."""
    last = (200, [])
    out = []
    for item in load(args.removed_file):
        body = {"execution_id": item["execution_id"], "attempt_id": item["attempt_id"], "uri": item["uri"],
                "stop_reference": args.stop_reference}
        status, res = call(args, token, "POST", f"/datasets/{_q(args.dataset)}/cleanup-records", body,
                           idem("cleanup", item["attempt_id"], item["uri"]))
        if not 200 <= status < 300:
            return emit(status, res)
        out.append(res)
        last = (status, out)
    return emit(*last)


def cmd_seed(args, token):
    """Seed envelope {dataset,workflow,purpose,suite,policy}: suite + revisions + purpose as editor,
    then binding as approver (separate token; SQL suites cannot be self-approved)."""
    env = load(args.fixture)
    ds, wf, purpose = env["dataset"], env["workflow"], env["purpose"]
    base = f"/datasets/{_q(ds)}"
    steps = []

    def step(method, path, body, key=None, tok=token, ok=(200, 201)):
        status, res = call(args, tok, method, path, body, key)
        steps.append({"path": path, "http_status": status})
        if status not in ok:
            raise SystemExit(emit(status, {"steps": steps, "error": res.get("error") if isinstance(res, dict) else None}))
        return res

    s = step("POST", f"{base}/suites", {"name": args.suite_name or purpose, "workflow": wf, "description": None},
             idem("seed-suite", ds, args.suite_name or purpose))
    sid = s["suite_id"]
    rev = step("POST", f"{base}/suites/{sid}/revisions", env["suite"], idem("seed-rev", sid, json.dumps(env["suite"], sort_keys=True)))
    pol = step("POST", f"{base}/suites/{sid}/policy-revisions", env["policy"],
               idem("seed-pol", sid, json.dumps(env["policy"], sort_keys=True)))
    status, cur = call(args, token, "GET", f"{base}/purposes/{wf}/{_q(purpose)}")
    if status == 404:
        cur = step("PUT", f"{base}/purposes/{wf}/{_q(purpose)}", {"description": None})
    elif status != 200:
        return emit(status, cur)
    if args.approver_token_file:
        bind = {"expected_binding_version": cur["binding_version"], "suite_id": sid,
                "suite_revision_id": rev["revision_id"], "policy_revision_id": pol["revision_id"],
                "reason": args.reason}
        step("PUT", f"{base}/purposes/{wf}/{_q(purpose)}/binding", bind,
             idem("seed-bind", ds, wf, purpose, rev["revision_id"], pol["revision_id"]),
             tok=read_token(args.approver_token_file))
    return emit(200, {"steps": steps, "suite_id": sid, "suite_revision_id": rev["revision_id"],
                      "policy_revision_id": pol["revision_id"], "bound": bool(args.approver_token_file)})


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--service-url", default=os.environ.get("DQ_SERVICE_URL", "http://127.0.0.1:8000"))
    p.add_argument("--token-file", required=True, help="file (0600) holding the operator token")
    p.add_argument("--timeout", type=float, default=30.0)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("reconcile", help="PUT verified outcome evidence, then wait (bounded) for resolution")
    r.add_argument("--execution", required=True)
    r.add_argument("--evidence-file", required=True)
    r.add_argument("--wait", type=float, default=30.0, help="max seconds to wait; 0 = no wait")
    r.add_argument("--interval", type=float, default=2.0)
    r.set_defaults(fn=cmd_reconcile)

    for name, helptext in (("close-unresolved", "close UNKNOWN destinations (PublicationClose JSON)"),
                           ("abandon", "abandon publication (AbandonRequest JSON)")):
        c = sub.add_parser(name, help=helptext)
        c.add_argument("--execution", required=True)
        c.add_argument("--body-file", required=True)
        c.add_argument("--idempotency-key")
        c.set_defaults(fn=lambda a, t, n=name: post_body(a, t, n))

    rt = sub.add_parser("retention", help="dump all retention items (operator) to a 0600 JSON file")
    rt.add_argument("--dataset", required=True)
    rt.add_argument("--output", required=True)
    rt.set_defaults(fn=cmd_retention)

    cr = sub.add_parser("cleanup-records", help="record candidates removed by the Maintenance job")
    cr.add_argument("--dataset", required=True)
    cr.add_argument("--removed-file", required=True)
    cr.add_argument("--stop-reference", required=True, help="scheduler proof the dataset slot was held/stopped")
    cr.set_defaults(fn=cmd_cleanup_records)

    sd = sub.add_parser("seed", help="register suite/revisions/purpose from a seed envelope; bind if approver given")
    sd.add_argument("--fixture", required=True)
    sd.add_argument("--suite-name")
    sd.add_argument("--approver-token-file", help="approver token (0600); omit to leave binding for an approver")
    sd.add_argument("--reason", default="seed")
    sd.set_defaults(fn=cmd_seed)

    args = p.parse_args(argv)
    return args.fn(args, read_token(args.token_file))


if __name__ == "__main__":
    sys.exit(main())
