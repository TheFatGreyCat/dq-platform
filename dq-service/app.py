"""
DQ control plane HTTP API v2 (metadata/measurement only; no data rows, no Spark).

Launch: `uvicorn app:app_from_env --factory`. Env: DATABASE_URL or DATABASE_URL_FILE, DQ_TOKEN_REGISTRY_FILE,
DQ_ANOMALY_WORKER_URL (default http://127.0.0.1:8081), DQ_RESTORE_MODE=1 (open restore markers on startup),
DQ_ALERT_WEBHOOK_URL (optional). Importing this module has no side effects.
"""
from __future__ import annotations

import base64
import binascii
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

import analytics
import console_operations as ops
from anomaly import AnomalyClient, build_observations, history_identity, select_history
from auth import READ_ROLES, Principal, TokenRegistry, require_dataset, require_execution, require_read
from budget import check_budget, max_report_bytes
from canonical import canonical_decimal, canonical_timestamp, content_hash, parse_json
from errors import ApiError, check_client_error_refs, from_validation_error
from models import (AbandonRequest, AttemptCreate, AttemptFail, AuditReport, BindingRequest, ClearRequest,
                    CompatManifest, EligibilityRequest, ExecutionRequest, ManifestAcceptance, OutcomeRequest, Policy,
                    PublicationClose, PurposeRegister, Registration, RestoreComplete, RestoreFindings, RetireRequest,
                    RevisionValidationRequest, StopConfirmation, Suite, SuiteCreate, TerminateRequest, WORKFLOW_STAGES,
                    WriteReport, check_parameter_values, check_policy_for_suite)
from plan import compile_plan, plan_rules
from policy import evaluate
from store import Store, sid, ts

log = logging.getLogger("dq-service")
MAX_BODY = 1_048_576
API = "/api/v2"
_STATES = ("PLANNED", "RUNNING", "REPORTED", "COMPLETED", "FAILED", "CANCELLED")


class BodyLimit:
    """ASGI middleware: count streamed bytes before any decode; >1 MiB -> 413 (also without Content-Length)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        total = 0
        too_big = False

        async def limited():
            nonlocal total, too_big
            msg = await receive()
            if msg["type"] == "http.request":
                total += len(msg.get("body", b""))
                if total > MAX_BODY:
                    too_big = True
                    raise _TooLarge()
            return msg

        for k, v in scope.get("headers", []):
            if k == b"content-length" and v.isdigit() and int(v) > MAX_BODY:
                return await _send_error(send, ApiError("PAYLOAD_TOO_LARGE"))
        try:
            await self.app(scope, limited, send)
        except _TooLarge:
            if too_big:
                await _send_error(send, ApiError("PAYLOAD_TOO_LARGE"))


class _TooLarge(Exception):
    pass


async def _send_error(send, err: ApiError):
    body = json.dumps(err.body()).encode()
    await send({"type": "http.response.start", "status": err.status,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
    await send({"type": "http.response.body", "body": body})


def _jr(status: int, body: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content=body)


def _cursor_encode(v: Any) -> str | None:
    if v is None:
        return None
    return base64.urlsafe_b64encode(json.dumps(v).encode()).decode().rstrip("=")


def _cursor_decode(s: str | None):
    if s is None:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))
    except (ValueError, json.JSONDecodeError):
        raise ApiError("INVALID_CONFIG", "cursor")
def _page_cursor(value, expected):
    if value is None:
        return None
    if expected is str:
        valid = isinstance(value, str) and 1 <= len(value) <= 128
    elif expected is int:
        valid = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    else:
        valid = isinstance(value, list) and len(value) == 2 and all(
            isinstance(part, str) and len(part) <= 128 for part in value)
    if not valid:
        raise ApiError("INVALID_CONFIG", "cursor")
    return value


def _limit(v: str | None) -> int:
    if v is None:
        return 100
    if not v.isdigit() or not 1 <= int(v) <= 500:
        raise ApiError("INVALID_CONFIG", "limit")
    return int(v)


def create_app(store: Store, token_registry: TokenRegistry, anomaly_client: AnomalyClient, *,
               restore_mode: bool = False, alert_webhook_url: str = "") -> FastAPI:
    app = FastAPI(title="DQ Service", version="2.0.0", docs_url=None, redoc_url=None, openapi_url=None)

    @app.on_event("startup")
    def _startup():
        store.check_schema()
        store.sync_all_grants(token_registry)
        if restore_mode:
            store.open_restore_all("system:restore-mode")

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError):
        return _jr(exc.status, exc.body())

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, exc: Exception):
        log.error("unexpected error class=%s", type(exc).__name__)
        e = ApiError("SERVICE_UNAVAILABLE")
        return _jr(e.status, e.body())

    from fastapi.exceptions import RequestValidationError
    from starlette.exceptions import HTTPException as StarletteHTTPException

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException):
        code = {404: "NOT_FOUND", 405: "NOT_FOUND"}.get(exc.status_code, "INVALID_CONFIG")
        e = ApiError(code)
        return _jr(e.status, e.body())

    @app.exception_handler(RequestValidationError)
    async def _rv(_: Request, exc: RequestValidationError):
        e = ApiError("INVALID_CONFIG")
        return _jr(e.status, e.body())

    # ---------- helpers ----------
    def principal(request: Request) -> Principal:
        return token_registry.authenticate(request.headers.get("authorization"))
    def collection_page(request: Request, expected=str):
        return _page_cursor(_cursor_decode(request.query_params.get("cursor")), expected), _limit(
            request.query_params.get("limit"))

    async def body_json(request: Request) -> Any:
        raw = await request.body()
        try:
            return parse_json(raw)
        except (ValueError, UnicodeDecodeError):
            raise ApiError("INVALID_CONFIG")

    def model(cls: type[BaseModel], data: Any, code: str = "INVALID_CONFIG"):
        try:
            return cls.model_validate(data)
        except ValidationError as e:
            raise from_validation_error(e, code)

    def idem_key(request: Request, required: bool = True) -> str | None:
        k = request.headers.get("idempotency-key")
        if k is None:
            if required:
                raise ApiError("INVALID_CONFIG", "Idempotency-Key")
            return None
        if not (1 <= len(k) <= 128) or not k.isascii() or not k.isprintable():
            raise ApiError("INVALID_CONFIG", "Idempotency-Key")
        return k

    def analytics_window(request: Request) -> tuple[datetime, datetime]:
        now = datetime.now(timezone.utc)
        raw_from = request.query_params.get("from")
        raw_to = request.query_params.get("to")
        try:
            end = datetime.fromisoformat(raw_to.replace("Z", "+00:00")) if raw_to else now
            start = datetime.fromisoformat(raw_from.replace("Z", "+00:00")) if raw_from else end - timedelta(days=7)
        except (AttributeError, ValueError):
            raise ApiError("INVALID_CONFIG", "from")
        if start.tzinfo is None or end.tzinfo is None:
            raise ApiError("INVALID_CONFIG", "from")
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
        if start >= end or end - start > timedelta(days=90):
            raise ApiError("INVALID_CONFIG", "to")
        return start, end

    def analytics_filters(request: Request) -> tuple[str | None, str | None]:
        workflow = request.query_params.get("workflow")
        purpose = request.query_params.get("purpose")
        if workflow is not None and workflow not in ("WRITE_GATE", "AUDIT_TABLE"):
            raise ApiError("INVALID_CONFIG", "workflow")
        if purpose and workflow is None:
            raise ApiError("INVALID_CONFIG", "purpose")
        return workflow, purpose
        k = request.headers.get("idempotency-key")
        if k is None:
            if required:
                raise ApiError("INVALID_CONFIG", "Idempotency-Key")
            return None
        if not (1 <= len(k) <= 128) or not k.isascii() or not k.isprintable():
            raise ApiError("INVALID_CONFIG", "Idempotency-Key")
        return k

    def exec_ctx(p: Principal, execution_id: str, op: str) -> tuple[dict, str]:
        e = store.execution_row(execution_id)
        return e, require_execution(p, e, op)

    # ---------- console discovery ----------
    @app.get(API + "/me")
    def get_me(request: Request):
        p = principal(request)
        grants = sorted(({"dataset_id": g.dataset_id, "role": g.role, "workflow": g.workflow, "purpose": g.purpose}
                         for g in p.grants), key=lambda g: (g["dataset_id"], g["role"], g["workflow"] or "",
                                                            g["purpose"] or ""))
        return {"principal": p.name, "grants": grants}

    @app.get(API + "/datasets")
    def list_datasets(request: Request):
        p = principal(request)
        ids = {g.dataset_id for g in p.grants if g.role in READ_ROLES}
        after, limit = collection_page(request)
        items, nxt = store.list_datasets(ids, limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/datasets/{dataset_id}/suites")
    def list_suites(dataset_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        after, limit = collection_page(request)
        items, nxt = store.list_suites(dataset_id, limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/datasets/{dataset_id}/suites/{suite_id}/revisions")
    def list_suite_revisions(dataset_id: str, suite_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        after, limit = collection_page(request, int)
        items, nxt = store.list_revisions(dataset_id, suite_id, "suite", limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/datasets/{dataset_id}/suites/{suite_id}/policy-revisions")
    def list_policy_revisions(dataset_id: str, suite_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        after, limit = collection_page(request, int)
        items, nxt = store.list_revisions(dataset_id, suite_id, "policy", limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/datasets/{dataset_id}/purposes")
    def list_purposes(dataset_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        after, limit = collection_page(request, list)
        items, nxt = store.list_purposes(dataset_id, limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/datasets/{dataset_id}/compat-manifests")
    def list_accepted_manifests(dataset_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        after, limit = collection_page(request)
        items, nxt = store.list_accepted_manifests(dataset_id, limit, after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}
    # ---------- health ----------
    @app.get("/health")
    def health():
        db = store.healthy()
        worker = anomaly_client.health()
        return _jr(200 if db else 503, {"status": "ok" if db else "degraded", "database": db, "anomaly_worker": worker})

    # ---------- datasets ----------
    @app.put(API + "/datasets/{dataset_id}")
    async def put_dataset(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        reg = model(Registration, await body_json(request))
        if not _ident(dataset_id):
            raise ApiError("INVALID_CONFIG", "dataset_id")
        return _jr(*store.put_dataset(p.name, dataset_id, reg, token_registry))

    @app.get(API + "/datasets/{dataset_id}")
    def get_dataset(dataset_id: str, request: Request):
        p = principal(request)
        require_read(p, dataset_id)
        row = store.tx(lambda c: store.require_dataset_row(c, dataset_id))
        return store.registration_view(row)

    # ---------- manifests ----------
    @app.post(API + "/compat-manifests")
    async def post_manifest(request: Request):
        p = principal(request)
        if not any(g.role == "editor" for g in p.grants):
            raise ApiError("FORBIDDEN")
        m = model(CompatManifest, await body_json(request))
        return _jr(*store.create_manifest(p.name, m, idem_key(request)))

    @app.get(API + "/compat-manifests/{manifest_id}")
    def get_manifest(manifest_id: str, request: Request):
        principal(request)
        _uuid_or_404(manifest_id)
        return store.get_manifest(manifest_id)

    @app.put(API + "/datasets/{dataset_id}/compat-manifests/{manifest_id}/acceptance")
    async def accept_manifest(dataset_id: str, manifest_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"approver"})
        _uuid_or_404(manifest_id)
        b = model(ManifestAcceptance, await body_json(request))
        return _jr(*store.accept_manifest(p.name, dataset_id, manifest_id, b.harness_reference, idem_key(request)))

    # ---------- suites ----------
    @app.post(API + "/datasets/{dataset_id}/suites")
    async def create_suite(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        b = model(SuiteCreate, await body_json(request))
        return _jr(*store.create_suite(p.name, dataset_id, b.model_dump(mode="json"), idem_key(request)))

    @app.get(API + "/datasets/{dataset_id}/suites/{suite_id}")
    def get_suite(dataset_id: str, suite_id: str, request: Request):
        require_read(principal(request), dataset_id)
        return store.get_suite(dataset_id, suite_id)

    @app.post(API + "/datasets/{dataset_id}/suites/{suite_id}/validate")
    async def validate_suite_revision(dataset_id: str, suite_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        suite_meta = store.get_suite(dataset_id, suite_id)
        b = model(RevisionValidationRequest, await body_json(request))
        if b.suite.workflow is not None and b.suite.workflow != suite_meta["workflow"]:
            raise ApiError("INVALID_CONFIG", "suite.workflow")
        expected_stages = WORKFLOW_STAGES[suite_meta["workflow"]]
        if any(r.stage not in expected_stages for r in b.suite.rules):
            raise ApiError("INVALID_CONFIG", "suite.rules")
        if b.policy is not None:
            try:
                check_policy_for_suite(b.suite, b.policy)
            except ValueError:
                raise ApiError("INVALID_CONFIG", "policy")
        return {"valid": True, "contains_sql": b.suite.contains_sql}

    @app.post(API + "/datasets/{dataset_id}/suites/{suite_id}/revisions")
    async def create_suite_revision(dataset_id: str, suite_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        s = model(Suite, await body_json(request))
        return _jr(*store.create_revision(p.name, dataset_id, suite_id, "suite", s, idem_key(request)))

    @app.post(API + "/datasets/{dataset_id}/suites/{suite_id}/policy-revisions")
    async def create_policy_revision(dataset_id: str, suite_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        pol = model(Policy, await body_json(request))
        return _jr(*store.create_revision(p.name, dataset_id, suite_id, "policy", pol, idem_key(request)))

    @app.get(API + "/datasets/{dataset_id}/suites/{suite_id}/revisions/{revision_id}")
    def get_suite_revision(dataset_id: str, suite_id: str, revision_id: str, request: Request):
        require_read(principal(request), dataset_id)
        return store.get_revision(dataset_id, suite_id, revision_id, "suite")

    @app.get(API + "/datasets/{dataset_id}/suites/{suite_id}/policy-revisions/{revision_id}")
    def get_policy_revision(dataset_id: str, suite_id: str, revision_id: str, request: Request):
        require_read(principal(request), dataset_id)
        return store.get_revision(dataset_id, suite_id, revision_id, "policy")

    # ---------- purposes ----------
    def _wf(workflow: str) -> str:
        if workflow not in ("WRITE_GATE", "AUDIT_TABLE"):
            raise ApiError("NOT_FOUND")
        return workflow

    @app.put(API + "/datasets/{dataset_id}/purposes/{workflow}/{purpose}")
    async def register_purpose(dataset_id: str, workflow: str, purpose: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"editor"})
        b = model(PurposeRegister, await body_json(request))
        if not _ident(purpose):
            raise ApiError("INVALID_CONFIG", "purpose")
        return _jr(*store.register_purpose(p.name, dataset_id, _wf(workflow), purpose, b.description))

    @app.get(API + "/datasets/{dataset_id}/purposes/{workflow}/{purpose}")
    def get_purpose(dataset_id: str, workflow: str, purpose: str, request: Request):
        p = principal(request)
        if not (p.roles(dataset_id) & READ_ROLES or p.job_grant(dataset_id, workflow, purpose)):
            raise ApiError("FORBIDDEN")
        return store.get_purpose(dataset_id, _wf(workflow), purpose)

    @app.get(API + "/datasets/{dataset_id}/purposes/{workflow}/{purpose}/events")
    def purpose_events(dataset_id: str, workflow: str, purpose: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, READ_ROLES)
        after = _cursor_decode(request.query_params.get("cursor"))
        if after is not None and not isinstance(after, int):
            raise ApiError("INVALID_CONFIG", "cursor")
        items, nxt = store.purpose_events(dataset_id, _wf(workflow), purpose, _limit(request.query_params.get("limit")),
                                          after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.put(API + "/datasets/{dataset_id}/purposes/{workflow}/{purpose}/binding")
    async def bind(dataset_id: str, workflow: str, purpose: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"approver"})
        b = model(BindingRequest, await body_json(request))
        return _jr(*store.activate_binding(p.name, dataset_id, _wf(workflow), purpose, b.model_dump(mode="json"),
                                          idem_key(request)))

    @app.post(API + "/datasets/{dataset_id}/purposes/{workflow}/{purpose}/retire")
    async def retire(dataset_id: str, workflow: str, purpose: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"approver"})
        b = model(RetireRequest, await body_json(request))
        return _jr(*store.retire_purpose(p.name, dataset_id, _wf(workflow), purpose, b.model_dump(mode="json"),
                                        idem_key(request)))

    # ---------- executions ----------
    def _compile(c, ctx) -> dict:
        req = ctx["request"]
        reg = ctx["registration"]
        suite = Suite.model_validate(ctx["suite_rev"]["payload"])
        try:
            check_parameter_values(suite, req["parameters"])
        except ValueError:
            raise ApiError("INVALID_CONFIG", "parameters")
        if req["history_context"] != reg["history_context"]:
            raise ApiError("INVALID_CONFIG", "history_context")
        if req["workflow"] == "WRITE_GATE":
            for d in ("silver", "quarantine"):
                base, reg_t = req["target_bases"][d], reg[d]
                if (base["uri"], base["table_id"]) != (reg_t["uri"], reg_t["table_id"]):
                    raise ApiError("INVALID_CONFIG", f"target_bases.{d}")
                if base["schema_hash"] != reg_t["schema_hash"]:
                    raise ApiError("INVALID_CONFIG", f"target_bases.{d}.schema_hash")
        else:
            snap, reg_t = req["input_snapshot"], reg["silver"]
            if (snap["uri"], snap["table_id"], snap["schema_hash"]) != (reg_t["uri"], reg_t["table_id"],
                                                                         reg_t["schema_hash"]):
                raise ApiError("INVALID_CONFIG", "input_snapshot")
        m = ctx["manifest"]
        plan = compile_plan(
            execution_id=ctx["execution_id"], dataset_id=req["dataset_id"], registration=reg,
            binding={"workflow": req["workflow"], "purpose": req["purpose"],
                     "binding_version": ctx["purpose"]["binding_version"], "suite_id": sid(ctx["suite"]["suite_id"]),
                     "suite_name": ctx["suite"]["name"]},
            suite_rev={"revision_id": sid(ctx["suite_rev"]["revision_id"]),
                       "content_hash": ctx["suite_rev"]["content_hash"], "payload": ctx["suite_rev"]["payload"]},
            policy_rev={"revision_id": sid(ctx["policy_rev"]["revision_id"]),
                        "content_hash": ctx["policy_rev"]["content_hash"], "payload": ctx["policy_rev"]["payload"]},
            manifest={"manifest_id": sid(m["manifest_id"]), "content_hash": m["content_hash"], "payload": m["payload"]},
            request=req, execution_timestamp=ctx["now"], history_snapshot=ctx["history_snapshot"])
        check_budget(max_report_bytes(plan))
        return plan

    def _history(c, ctx) -> dict:
        from plan import ENGINE, INTERPRETER_VERSION, stage_bindings
        req = ctx["request"]
        anomaly_cfg = ctx["policy_rev"]["payload"]["anomaly"]
        cutoff = ctx["now"]
        if not anomaly_cfg.get("enabled"):
            return {"cutoff": canonical_timestamp(cutoff), "contexts": []}
        sb = stage_bindings(ctx["suite_rev"]["payload"], ctx["policy_rev"]["payload"], req["parameters"])
        contexts = select_history(
            c, dataset_id=req["dataset_id"], table_id=ctx["registration"]["silver"]["table_id"],
            history_context=req["history_context"], engine=ENGINE, interpreter_version=INTERPRETER_VERSION,
            stage_bindings=sb, event_time=datetime.fromisoformat(req["event_time"].replace("Z", "+00:00")),
            cutoff=cutoff, max_points=anomaly_cfg["max_points"])
        if len(contexts) > 20:
            raise ApiError("INVALID_CONFIG", "anomaly")
        if len(json.dumps({"items": contexts})) > MAX_BODY:
            raise ApiError("INVALID_CONFIG", "payload_budget")
        return {"cutoff": canonical_timestamp(cutoff), "contexts": contexts}

    @app.post(API + "/executions")
    async def create_execution(request: Request):
        p = principal(request)
        req = model(ExecutionRequest, await body_json(request))
        if not p.job_grant(req.dataset_id, req.workflow, req.purpose):
            if p.roles(req.dataset_id) or p.job_purposes(req.dataset_id):
                raise ApiError("FORBIDDEN")
            raise ApiError("NOT_FOUND")
        return _jr(*store.create_execution(p.name, req, idem_key(request), compile_fn=_compile, history_fn=_history))

    @app.get(API + "/datasets/{dataset_id}/executions")
    def list_executions(dataset_id: str, request: Request):
        p = principal(request)
        require_read(p, dataset_id)
        states = request.query_params.getlist("state")
        if any(s not in _STATES for s in states):
            raise ApiError("INVALID_CONFIG", "state")
        workflows = request.query_params.getlist("workflow")
        workflow = workflows[0] if len(workflows) == 1 else None
        if workflows and (len(workflows) != 1 or workflow not in ("WRITE_GATE", "AUDIT_TABLE")):
            raise ApiError("INVALID_CONFIG", "workflow")
        purposes = None if p.roles(dataset_id) & READ_ROLES else p.job_purposes(dataset_id)
        cur = _cursor_decode(request.query_params.get("cursor"))
        if cur is not None and not (isinstance(cur, list) and len(cur) == 2):
            raise ApiError("INVALID_CONFIG", "cursor")
        items, nxt = store.list_executions(dataset_id, states, purposes, _limit(request.query_params.get("limit")),
                                           tuple(cur) if cur else None, workflow=workflow)
        return {"items": items, "next_cursor": _cursor_encode(list(nxt) if nxt else None)}

    @app.get(API + "/datasets/{dataset_id}/analytics")
    def dataset_analytics(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        start, end = analytics_window(request)
        workflow, purpose = analytics_filters(request)
        return store.tx(lambda c: analytics.aggregate_dataset(c, dataset_id, start, end, workflow, purpose))

    @app.get(API + "/analytics")
    def global_analytics(request: Request):
        p = principal(request)
        requested = set(request.query_params.getlist("dataset_id"))
        granted = {g.dataset_id for g in p.grants if g.role in READ_ROLES}
        if requested - granted:
            raise ApiError("FORBIDDEN")
        start, end = analytics_window(request)
        workflow, purpose = analytics_filters(request)
        datasets = [store.tx(lambda c, d=d: analytics.aggregate_dataset(c, d, start, end, workflow, purpose)) for d in sorted(requested or granted)]
        return {"window": {"start": canonical_timestamp(start), "end": canonical_timestamp(end)}, "datasets": datasets}

    @app.get(API + "/datasets/{dataset_id}/rule-history")
    def dataset_rule_history(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        start, end = analytics_window(request)
        metric_hash = request.query_params.get("metric_definition_hash")
        scope = request.query_params.get("scope")
        stage = request.query_params.get("stage")
        history_hash = request.query_params.get("history_identity_hash")
        if not all((metric_hash, scope, stage, history_hash)):
            raise ApiError("INVALID_CONFIG", "metric_definition_hash")
        limit = _limit(request.query_params.get("limit"))
        return store.tx(lambda c: analytics.rule_history(c, dataset_id, metric_hash, scope, stage, history_hash, start, end, limit))

    @app.get(API + "/executions/{execution_id}")
    def get_execution(execution_id: str, request: Request):
        exec_ctx(principal(request), execution_id, "read")
        return store.get_execution(execution_id)

    @app.post(API + "/executions/{execution_id}/attempts")
    async def create_attempt(execution_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "job")
        b = model(AttemptCreate, await body_json(request))
        return _jr(*store.create_attempt(p.name, execution_id, b, idem_key(request, required=False)))

    @app.post(API + "/executions/{execution_id}/attempts/{attempt_id}/fail")
    async def fail_attempt(execution_id: str, attempt_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "job_or_operator")
        b = model(AttemptFail, await body_json(request))
        return _jr(*store.fail_attempt(p.name, execution_id, attempt_id, b))

    @app.post(API + "/executions/{execution_id}/terminate")
    async def terminate(execution_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "job_or_operator")
        b = model(TerminateRequest, await body_json(request))
        return _jr(*store.terminate(p.name, execution_id, b, idem_key(request)))

    @app.post(API + "/executions/{execution_id}/attempts/{attempt_id}/stop-confirmation")
    async def stop_confirmation(execution_id: str, attempt_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "job_or_operator")
        b = model(StopConfirmation, await body_json(request))
        return _jr(*store.stop_confirmation(p.name, execution_id, attempt_id, b.reference))

    # ---------- reports ----------
    @app.post(API + "/executions/{execution_id}/reports")
    async def post_report(execution_id: str, request: Request):
        p = principal(request)
        e, _ = exec_ctx(p, execution_id, "job")
        raw = await body_json(request)
        cls = WriteReport if e["workflow"] == "WRITE_GATE" else AuditReport
        rep = model(cls, raw, "INVALID_REPORT")
        payload = rep.model_dump(mode="json")
        report_hash = content_hash(payload)
        stored = store.stored_report(execution_id)
        if stored is not None:
            if stored["report_hash"] == report_hash:
                return stored["accepted_response"]
            raise ApiError("CONTENT_CONFLICT")
        plan = e["plan"]
        _validate_report(plan, rep)
        assessment = evaluate(plan, rep)
        results = {(r.rule_id): r for r in rep.results}
        by_key = {}
        for rid, (sb, pr) in plan_rules(plan).items():
            r = results[rid]
            key = (pr["metric_definition_hash"], sb["scope"], sb["stage"])
            by_key[key] = {"measurement_status": r.measurement_status,
                           "metric_value": r.metric_value if r.measurement_status == "OK" else None}
        anomaly_obs = build_observations(plan["policy_revision"]["payload"]["anomaly"],
                                         plan["history_snapshot"]["contexts"], by_key, anomaly_client)
        accepted = {"resp": None, "alert": False}

        def validate(c, ex):
            if ex["state"] != "RUNNING" or str(ex["active_attempt_id"]) != rep.attempt_id:
                raise ApiError("ATTEMPT_CONFLICT")

        def build(c, ex):
            return _persist_report(c, ex, rep, payload, report_hash, assessment, anomaly_obs, accepted)

        status, resp = store.accept_report(execution_id, report_hash, payload, validate=validate, build=build)
        return _jr(status, resp)

    def _validate_report(plan: dict, rep) -> None:
        if (rep.dataset_id, rep.plan_hash, rep.suite_revision_id, rep.policy_revision_id) != (
                plan["dataset_id"], plan["plan_hash"], plan["suite_revision"]["revision_id"],
                plan["policy_revision"]["revision_id"]):
            raise ApiError("PLAN_MISMATCH")
        if (rep.engine, rep.interpreter_version) != (plan["engine"], plan["interpreter_version"]):
            raise ApiError("RUNTIME_MISMATCH")
        if rep.parameters != plan["parameters"]:
            raise ApiError("PLAN_MISMATCH", "parameters")
        expected = {k: v for k, v in plan["compat_manifest"]["payload"].items() if k != "format"}
        if rep.runtime_versions.model_dump(mode="json") != expected:
            raise ApiError("RUNTIME_MISMATCH", "runtime_versions")
        rules = plan_rules(plan)
        if {r.rule_id for r in rep.results} != set(rules):
            raise ApiError("INVALID_REPORT", "results")
        columns = {c["name"] for c in plan["registration_snapshot"]["business_schema"]}
        for r in rep.results:
            sb, pr = rules[r.rule_id]
            if (r.measurement_definition_hash, r.scope, r.stage, r.kind) != (
                    pr["measurement_definition_hash"], sb["scope"], sb["stage"], pr["kind"]):
                raise ApiError("PLAN_MISMATCH", "results")
            if r.measurement_status == "ERROR":
                check_client_error_refs(r.error, rule_ids=set(rules), columns=columns, field="results.error")
            if r.failed_rows is not None and pr["kind"] not in ("complete", "completeness", "unique", "pattern",
                                                                 "range", "in_set", "sql_predicate"):
                raise ApiError("INVALID_REPORT", "results.failed_rows")
        reg = plan["registration_snapshot"]
        if plan["workflow"] == "WRITE_GATE":
            loc = plan["candidate_locations"]["execution_root"]
            raw_uri = f"{loc}/{rep.attempt_id}/raw"
            if rep.raw_reference.uri != raw_uri:
                raise ApiError("PLAN_MISMATCH", "raw_reference")
            if rep.classified_reference is not None and rep.classified_reference.uri != f"{loc}/{rep.attempt_id}/classified":
                raise ApiError("PLAN_MISMATCH", "classified_reference")
            if rep.raw_reference.version != 0 or (rep.classified_reference and rep.classified_reference.version != 0):
                raise ApiError("PLAN_MISMATCH", "version")
            for err in rep.errors:
                check_client_error_refs(err, rule_ids=set(rules), columns=columns, field="errors")
        else:
            a = rep.audit_reference
            snap = plan["input_snapshot"]
            if (a.uri, a.table_id, a.version, a.schema_hash) != (snap["uri"], snap["table_id"], snap["version"],
                                                                  snap["schema_hash"]):
                raise ApiError("PLAN_MISMATCH", "audit_reference")
        _ = reg

    def _persist_report(c, e, rep, payload, report_hash, assessment, anomaly_obs, accepted) -> dict:
        plan = e["plan"]
        now = store.clock()
        eid = e["execution_id"]
        write = plan["workflow"] == "WRITE_GATE"
        classified = payload.get("classified_reference") if write else None
        fingerprint = content_hash({"plan_hash": plan["plan_hash"], "report_hash": report_hash,
                                    "classified_reference": classified})
        report_id = store.new_id()
        decision = None
        if write:
            decision = {"decision_id": str(store.new_id()), "decision": assessment.decision,
                        "plan_hash": plan["plan_hash"], "report_hash": report_hash,
                        "evaluation_fingerprint": fingerprint, "classified_reference": classified,
                        "reasons": assessment.reasons, "created_at": canonical_timestamp(now)}
        rules = plan_rules(plan)
        # one metric observation per numeric (metric hash, scope, stage); anomalies reference it for eligibility
        obs_ids: dict[tuple, Any] = {}
        for r in payload["results"]:
            sb, pr = rules[r["rule_id"]]
            key = (pr["metric_definition_hash"], sb["scope"], sb["stage"])
            if r["measurement_status"] == "OK" and r["schema_evidence"] is None and key not in obs_ids:
                obs_ids[key] = store.new_id()
        anomalies = []
        for o in anomaly_obs:
            oid = obs_ids.get((o["metric_definition_hash"], o["scope"], o["stage"]))
            anomalies.append({"anomaly_id": str(store.new_id()), "observation_id": str(oid) if oid else None, **o})
        if write:
            allow = assessment.decision == "ALLOW"
            q_na = (not allow) or rep.quarantine_rows == 0
            pub = "NOT_STARTED" if allow else "NOT_APPLICABLE"
            exec_state = "REPORTED" if allow else "COMPLETED"
        else:
            pub = "NOT_APPLICABLE"
            exec_state = "FAILED" if assessment.assessment == "ERROR" else "COMPLETED"
        resp = {"execution_id": str(eid), "report_id": str(report_id), "report_hash": report_hash,
                "evaluation_fingerprint": fingerprint, "accepted_at": canonical_timestamp(now), "decision": decision,
                "assessment": assessment.assessment, "anomaly_observations": anomalies, "publication_state": pub}
        c.execute(t.accepted_reports.insert().values(
            report_id=report_id, execution_id=eid, attempt_id=e["active_attempt_id"], report_hash=report_hash,
            evaluation_fingerprint=fingerprint, payload=payload, assessment=assessment.assessment, accepted_at=now,
            accepted_response=resp))
        reg = plan["registration_snapshot"]
        for r in payload["results"]:
            sb, pr = rules[r["rule_id"]]
            ok = r["measurement_status"] == "OK"
            numeric = ok and r["schema_evidence"] is None
            c.execute(t.rule_results.insert().values(
                execution_id=eid, rule_id=r["rule_id"], metric_definition_hash=pr["metric_definition_hash"],
                measurement_definition_hash=pr["measurement_definition_hash"], scope=sb["scope"], stage=sb["stage"],
                measurement_status=r["measurement_status"], metric_name=r["metric_name"] if numeric else None,
                metric_decimal=canonical_decimal(r["metric_value"]) if numeric else None,
                schema_evidence=r["schema_evidence"] if ok and not numeric else None,
                error_code=None if ok else r["error"]["code"], error_params=None if ok else r["error"]["params"],
                failed_rows=r["failed_rows"],
                payload={**r, "outcome": assessment.rule_outcomes.get(r["rule_id"]), "action": pr["action"],
                         "required": pr["required"]}))
        seen = set()
        for r in payload["results"]:
            sb, pr = rules[r["rule_id"]]
            key = (pr["metric_definition_hash"], sb["scope"], sb["stage"])
            if key not in obs_ids or key in seen:
                continue
            seen.add(key)
            h = history_identity(plan["dataset_id"], reg["silver"]["table_id"], reg["history_context"], sb["scope"],
                                 sb["stage"], pr["metric_definition_hash"], plan["engine"], plan["interpreter_version"])
            c.execute(t.metric_observations.insert().values(
                observation_id=obs_ids[key], execution_id=eid, metric_definition_hash=key[0], scope=key[1],
                stage=key[2], history_identity_hash=h,
                event_time=datetime.fromisoformat(plan["event_time"].replace("Z", "+00:00")),
                value=canonical_decimal(r["metric_value"]),
                payload={"rule_id": r["rule_id"], "metric_name": r["metric_name"], "value": str(r["metric_value"])}))
        for a in anomalies:
            c.execute(t.anomaly_observations.insert().values(
                anomaly_id=a["anomaly_id"], execution_id=eid, metric_definition_hash=a["metric_definition_hash"],
                scope=a["scope"], stage=a["stage"], status=a["status"], baseline_hash=a["baseline_hash"],
                detector_version=a.get("detector_version") or "unavailable", payload=a))
        if decision is not None:
            c.execute(t.gate_decisions.insert().values(
                decision_id=decision["decision_id"], execution_id=eid, decision=decision["decision"],
                plan_hash=plan["plan_hash"], report_hash=report_hash, evaluation_fingerprint=fingerprint,
                payload=decision, created_at=now))
            for dest in ("silver", "quarantine"):
                na = (not allow) or (dest == "quarantine" and q_na)
                c.execute(t.write_outcomes.insert().values(
                    execution_id=eid, destination=dest, state="NOT_APPLICABLE" if na else "NOT_STARTED",
                    outcome_version=0, payload={}, updated_at=now))
        c.execute(t.attempts.update().where(t.attempts.c.attempt_id == e["active_attempt_id"]).values(
            state="SUCCEEDED", retired_at=now))
        c.execute(t.executions.update().where(t.executions.c.execution_id == eid).values(
            state=exec_state, publication_state=pub, active_attempt_id=None, updated_at=now))
        if not write and exec_state == "COMPLETED":
            store._eligibility_on_success(c, store._execution(c, str(eid)), None, now)
        accepted["alert"] = (decision is not None and decision["decision"] == "DENY") or bool(assessment.reasons) or \
            assessment.assessment in ("VIOLATION", "ERROR")
        return resp

    @app.get(API + "/executions/{execution_id}/decision")
    def get_decision(execution_id: str, request: Request):
        e, _ = exec_ctx(principal(request), execution_id, "read")
        if e["workflow"] == "AUDIT_TABLE":
            raise ApiError("NOT_APPLICABLE")
        stored = store.stored_report(execution_id)
        if stored is None:
            return _jr(202, {"execution_id": execution_id, "state": e["state"]})
        return stored["accepted_response"]["decision"]

    @app.get(API + "/executions/{execution_id}/report")
    def get_report(execution_id: str, request: Request):
        e, _ = exec_ctx(principal(request), execution_id, "read")
        stored = store.stored_report(execution_id)
        if stored is None:
            return _jr(202, {"execution_id": execution_id, "state": e["state"]})
        return {"accepted": stored["accepted_response"], "report": stored["payload"],
                "links": {"rules": f"{API}/executions/{execution_id}/rules",
                          "metrics": f"{API}/executions/{execution_id}/metrics",
                          "anomalies": f"{API}/executions/{execution_id}/anomalies"}}

    def _paged(execution_id: str, request: Request, sql: str, keycol: str, view):
        e, _ = exec_ctx(principal(request), execution_id, "read")
        if store.stored_report(execution_id) is None:
            return _jr(202, {"execution_id": execution_id, "state": e["state"]})
        n = _limit(request.query_params.get("limit"))
        after = _cursor_decode(request.query_params.get("cursor"))
        if after is not None and not isinstance(after, str):
            raise ApiError("INVALID_CONFIG", "cursor")

        def run(c):
            return c.execute(sa.text(sql + f" AND {keycol} > :a ORDER BY {keycol} LIMIT :n"),
                             {"e": execution_id, "a": after or "", "n": n + 1}).mappings().all()
        rows = store.tx(run)
        items = [view(r) for r in rows[:n]]
        nxt = str(rows[n - 1][keycol.split(".")[-1].replace("::text", "")]) if len(rows) > n else None
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.get(API + "/executions/{execution_id}/rules")
    def get_rules(execution_id: str, request: Request):
        return _paged(execution_id, request, "SELECT * FROM rule_results WHERE execution_id=:e", "rule_id",
                      lambda r: r["payload"])

    @app.get(API + "/executions/{execution_id}/metrics")
    def get_metrics(execution_id: str, request: Request):
        return _paged(execution_id, request,
                      "SELECT *, observation_id::text AS oid FROM metric_observations WHERE execution_id=:e",
                      "observation_id::text",
                      lambda r: {"observation_id": sid(r["observation_id"]),
                                 "metric_definition_hash": r["metric_definition_hash"], "scope": r["scope"],
                                 "stage": r["stage"], "history_identity_hash": r["history_identity_hash"],
                                 "event_time": ts(r["event_time"]), "value": canonical_decimal(r["value"])})

    @app.get(API + "/executions/{execution_id}/anomalies")
    def get_anomalies(execution_id: str, request: Request):
        return _paged(execution_id, request,
                      "SELECT * FROM anomaly_observations WHERE execution_id=:e", "anomaly_id::text",
                      lambda r: r["payload"])

    @app.post(API + "/executions/{execution_id}/metrics/{observation_id}/eligibility")
    async def eligibility(execution_id: str, observation_id: str, request: Request):
        p = principal(request)
        e, _ = exec_ctx(p, execution_id, "read")
        require_dataset(p, e["dataset_id"], {"approver"})
        _uuid_or_404(observation_id)
        b = model(EligibilityRequest, await body_json(request))
        return _jr(*store.add_eligibility(p.name, execution_id, observation_id, b.model_dump(mode="json"),
                                         idem_key(request)))

    # ---------- outcomes ----------
    @app.put(API + "/executions/{execution_id}/write-outcomes/{destination}")
    async def put_outcome(execution_id: str, destination: str, request: Request):
        p = principal(request)
        if destination not in ("silver", "quarantine"):
            raise ApiError("NOT_FOUND")
        e, role = exec_ctx(p, execution_id, "job_or_operator")
        b = model(OutcomeRequest, await body_json(request))
        return _jr(*store.put_outcome(p.name, role, execution_id, destination, b))

    @app.post(API + "/executions/{execution_id}/publication/abandon")
    async def abandon(execution_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "job_or_operator")
        b = model(AbandonRequest, await body_json(request))
        return _jr(*store.abandon(p.name, execution_id, b, idem_key(request)))

    @app.post(API + "/executions/{execution_id}/publication/close-unresolved")
    async def close_unresolved(execution_id: str, request: Request):
        p = principal(request)
        exec_ctx(p, execution_id, "operator")
        b = model(PublicationClose, await body_json(request))
        return _jr(*store.close_unresolved(p.name, execution_id, b, idem_key(request)))

    @app.get(API + "/datasets/{dataset_id}/publications")
    def publications(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        dt = request.query_params.get("dt")
        if set(request.query_params) != {"dt"} or not dt:
            raise ApiError("INVALID_CONFIG", "dt")
        try:
            from models import PartitionValues
            PartitionValues(dt=dt)
        except ValidationError:
            raise ApiError("INVALID_CONFIG", "dt")
        return store.publications(dataset_id, dt)

    # ---------- console operations ----------
    @app.get(API + "/datasets/{dataset_id}/alert-settings")
    def get_alert_settings(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        return store.tx(lambda c: ops.get_settings(c, store, dataset_id))

    @app.put(API + "/datasets/{dataset_id}/alert-settings")
    async def put_alert_settings(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        key = idem_key(request)
        b = model(ops.AlertSettingsPut, await body_json(request))
        return _jr(*store.idempotent(p.name, f"PUT /datasets/{dataset_id}/alert-settings", dataset_id, key,
                                     b.model_dump(), lambda c: ops.put_settings(c, store, p.name, dataset_id, b)))

    @app.get(API + "/datasets/{dataset_id}/alerts")
    def list_alerts(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        cur, limit = collection_page(request, list)
        q = request.query_params
        items, nxt = store.tx(lambda c: ops.list_alerts(c, store, dataset_id, q.get("state"), q.get("kind"), limit, cur))
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.post(API + "/datasets/{dataset_id}/alerts/{alert_id}/acknowledgement")
    async def acknowledge_alert(dataset_id: str, alert_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        key = idem_key(request)
        b = model(ops.AlertAck, await body_json(request))
        return _jr(*store.idempotent(p.name, f"POST /datasets/{dataset_id}/alerts/{alert_id}/acknowledgement",
                                     dataset_id, key, b.model_dump(),
                                     lambda c: ops.acknowledge(c, store, p.name, dataset_id, alert_id, b)))

    @app.get(API + "/datasets/{dataset_id}/audit-events")
    def list_audit_events(dataset_id: str, request: Request):
        require_dataset(principal(request), dataset_id, READ_ROLES)
        cur, limit = collection_page(request, list)
        items, nxt = store.tx(lambda c: ops.list_audit_events(
            c, store, dataset_id, request.query_params.get("action"), limit, cur))
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    # ---------- restore ----------
    @app.get(API + "/datasets/{dataset_id}/restore")
    def get_restore(dataset_id: str, request: Request):
        require_read(principal(request), dataset_id)
        return store.restore_view(dataset_id)

    @app.post(API + "/datasets/{dataset_id}/restore/findings")
    async def restore_findings(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        b = model(RestoreFindings, await body_json(request))
        return _jr(*store.restore_findings(p.name, dataset_id, b, idem_key(request)))

    @app.post(API + "/datasets/{dataset_id}/restore/complete")
    async def restore_complete(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        b = model(RestoreComplete, await body_json(request))
        return _jr(*store.restore_complete(p.name, dataset_id, b, idem_key(request)))

    @app.post(API + "/datasets/{dataset_id}/unresolved/{marker_id}/clear")
    async def clear_marker(dataset_id: str, marker_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        _uuid_or_404(marker_id)
        b = model(ClearRequest, await body_json(request))
        return _jr(*store.clear_marker(p.name, dataset_id, marker_id, b))

    @app.get(API + "/datasets/{dataset_id}/retention")
    def retention(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        after = _cursor_decode(request.query_params.get("cursor"))
        if after is not None and not isinstance(after, str):
            raise ApiError("INVALID_CONFIG", "cursor")
        items, nxt = store.retention(dataset_id, _limit(request.query_params.get("limit")), after)
        return {"items": items, "next_cursor": _cursor_encode(nxt)}

    @app.post(API + "/datasets/{dataset_id}/cleanup-records")
    async def cleanup_record(dataset_id: str, request: Request):
        p = principal(request)
        require_dataset(p, dataset_id, {"operator"})
        from models import CleanupRecord
        b = model(CleanupRecord, await body_json(request))
        return _jr(*store.record_cleanup(p.name, dataset_id, b, idem_key(request)))

    app.add_middleware(BodyLimit)
    return app




def _ident(s: str) -> bool:
    import re
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", s))


def _uuid_or_404(s: str) -> None:
    import uuid
    try:
        uuid.UUID(s)
    except ValueError:
        raise ApiError("NOT_FOUND")


def app_from_env() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = os.environ.get("DATABASE_URL")
    if not url and os.environ.get("DATABASE_URL_FILE"):
        with open(os.environ["DATABASE_URL_FILE"], encoding="utf-8") as f:
            url = f.read().strip()
    if not url:
        raise RuntimeError("DATABASE_URL or DATABASE_URL_FILE required")
    reg_file = os.environ.get("DQ_TOKEN_REGISTRY_FILE")
    if not reg_file:
        raise RuntimeError("DQ_TOKEN_REGISTRY_FILE required")
    return create_app(Store(url), TokenRegistry.from_file(reg_file),
                      AnomalyClient(os.environ.get("DQ_ANOMALY_WORKER_URL", "http://127.0.0.1:8081")),
                      restore_mode=os.environ.get("DQ_RESTORE_MODE") == "1",
                      alert_webhook_url=os.environ.get("DQ_ALERT_WEBHOOK_URL", ""))
