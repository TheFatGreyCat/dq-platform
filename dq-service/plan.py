"""
Pure plan compilation: pinned registration + suite/policy revisions + request -> immutable ExecutionPlan dict
(shape: local wire contract / plan Phụ lục B). No I/O except what the caller passes in.
"""
from __future__ import annotations

import copy
import re
from datetime import datetime, timedelta

from canonical import (CANONICALIZATION_VERSION, METRIC_NAMES, SEMANTICS_VERSION, canonical_timestamp,
                       content_hash, measurement_hash, metric_hash)
from models import RUNTIME_CONF

ENGINE = "deequ-2.0.21-spark-3.5"
INTERPRETER_VERSION = "dq-client-0.1.0"
RUNTIME_PROFILE = {"version": "dq-runtime-v1", "conf": RUNTIME_CONF}
STAGE_ORDER = {"RAW": 0, "CANDIDATE": 1, "CLEAN": 2, "AUDIT_SNAPSHOT": 3}
REFERENCE_ROLE = {"RAW": "raw", "CANDIDATE": "candidate", "CLEAN": "classified_clean", "AUDIT_SNAPSHOT": "audit"}
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
DEFAULT_ASSERT_KINDS = ("complete", "range", "in_set")


def _resolve_bound(b, params: dict):
    if isinstance(b, dict) and set(b) == {"param"}:
        return params[b["param"]]
    return b


def _substitute(text: str, params: dict) -> str:
    return _PLACEHOLDER.sub(lambda m: m.group(0) if m.group(1) == "batch" else params[m.group(1)], text)


def resolve_check(check: dict, params: dict) -> dict:
    """Template check (as stored, aliases dumped) -> resolved check with defaults + literal parameter values."""
    out = copy.deepcopy(check)
    if out["kind"] in DEFAULT_ASSERT_KINDS and out.get("assert") is None:
        out["assert"] = {"op": "==", "value": "1"}
    a = out.get("assert")
    if a is not None:
        out["assert"] = {k: _resolve_bound(v, params) for k, v in a.items() if v is not None}
    for k in ("min", "max"):
        if k in out:
            out[k] = _resolve_bound(out[k], params)
    for k in ("predicate", "sql"):
        if k in out and isinstance(out[k], str):
            out[k] = _substitute(out[k], params)
    return {k: v for k, v in out.items() if v is not None}


def template_check(check: dict) -> dict:
    out = copy.deepcopy(check)
    if out["kind"] in DEFAULT_ASSERT_KINDS and out.get("assert") is None:
        out["assert"] = {"op": "==", "value": "1"}
    if out.get("assert") is not None:
        out["assert"] = {k: v for k, v in out["assert"].items() if v is not None}
    return {k: v for k, v in out.items() if v is not None}


def stage_bindings(suite: dict, policy: dict, params: dict) -> list[dict]:
    actions = {p["rule_id"]: p for p in policy["rules"]}
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in suite["rules"]:
        tmpl = template_check(r["check"])
        resolved = resolve_check(r["check"], params)
        name, vtype = METRIC_NAMES[r["check"]["kind"]]
        p = actions[r["rule_id"]]
        groups.setdefault((r["stage"], r["scope"]), []).append({
            "rule_id": r["rule_id"], "kind": r["check"]["kind"],
            "metric_definition_hash": metric_hash(tmpl, SEMANTICS_VERSION),
            "measurement_definition_hash": measurement_hash(resolved, SEMANTICS_VERSION),
            "metric_name": name, "value_type": vtype, "resolved_check": resolved,
            "action": p["action"], "required": p["required"]})
    return [{"scope": scope, "stage": stage, "reference_role": REFERENCE_ROLE[stage],
             "rules": sorted(rules, key=lambda x: x["rule_id"])}
            for (stage, scope), rules in sorted(groups.items(), key=lambda kv: (STAGE_ORDER[kv[0][0]], kv[0][1]))]


def partition_predicate(dt: str) -> str:
    return f"dt = DATE '{dt}'"


def compile_plan(*, execution_id: str, dataset_id: str, registration: dict, binding: dict, suite_rev: dict,
                 policy_rev: dict, manifest: dict, request: dict, execution_timestamp: datetime,
                 history_snapshot: dict) -> dict:
    wf = request["workflow"]
    params = dict(request["parameters"])
    ret = registration["retention"]
    retain_until = None
    if ret.get("candidate_duration_seconds"):
        retain_until = canonical_timestamp(execution_timestamp + timedelta(seconds=ret["candidate_duration_seconds"]))
    pv = request.get("partition_values")
    plan = {
        "execution_id": execution_id,
        "dataset_id": dataset_id,
        "registration_snapshot": registration,
        "binding": binding,
        "suite_revision": {"revision_id": suite_rev["revision_id"], "content_hash": suite_rev["content_hash"],
                           "payload": suite_rev["payload"]},
        "policy_revision": {"revision_id": policy_rev["revision_id"], "content_hash": policy_rev["content_hash"],
                            "payload": policy_rev["payload"]},
        "workflow": wf,
        "write_operation": "OVERWRITE_PARTITION" if wf == "WRITE_GATE" else None,
        "partition_values": pv,
        "partition_predicate": partition_predicate(pv["dt"]) if pv else None,
        "stage_bindings": stage_bindings(suite_rev["payload"], policy_rev["payload"], params),
        "parameters": params,
        "engine": ENGINE,
        "interpreter_version": INTERPRETER_VERSION,
        "runtime_profile": RUNTIME_PROFILE,
        "compat_manifest": {"manifest_id": manifest["manifest_id"], "content_hash": manifest["content_hash"],
                            "payload": manifest["payload"]},
        "event_time": canonical_timestamp(request["event_time"]),
        "event_origin": request.get("event_origin") or "SCHEDULED",
        "previous_execution_id": request.get("previous_execution_id"),
        "execution_timestamp": canonical_timestamp(execution_timestamp),
        "target_bases": request.get("target_bases"),
        "input_snapshot": request.get("input_snapshot"),
        "candidate_locations": ({"execution_root": f"{registration['candidate_root'].rstrip('/')}/{dataset_id}/"
                                                   f"{execution_id}"} if wf == "WRITE_GATE" else None),
        "history_snapshot": history_snapshot,
        "retention": {"policy": ret, "retain_until": retain_until, "recoverable_until": None},
        "canonicalization_version": CANONICALIZATION_VERSION,
    }
    plan["plan_hash"] = content_hash(plan)
    return plan


def plan_rules(plan: dict) -> dict[str, tuple[dict, dict]]:
    return {r["rule_id"]: (sb, r) for sb in plan["stage_bindings"] for r in sb["rules"]}
