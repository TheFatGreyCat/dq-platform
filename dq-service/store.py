"""
Persistence v2 (PostgreSQL 16 only). Schema comes from postgres/init.sql + migrations; startup only checks
`schema_versions` (no create_all, no auto-migrate). Every method runs in one short transaction with row locks;
no network or Spark work happens while a transaction is open.

Methods return `(http_status, body)` and raise `errors.ApiError` for client-visible failures.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError

import schema as t
from auth import TokenRegistry
from budget import activation_estimate, check_budget
from canonical import canonical_timestamp, content_hash
from errors import ApiError
from models import CompatManifest, Policy, Registration, Suite, check_policy_for_suite

SCHEMA_VERSION = 3
SQL_KINDS = frozenset({"sql_predicate", "sql_metric"})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def ts(v) -> str | None:
    return None if v is None else canonical_timestamp(v)


def sid(v) -> str | None:
    return None if v is None else str(v)


class Store:
    def __init__(self, url: str, *, clock: Callable[[], datetime] = _now,
                 new_id: Callable[[], uuid.UUID] = uuid.uuid4):
        self.engine = sa.create_engine(url, pool_pre_ping=True)
        self.clock = clock
        self.new_id = new_id

    def record_console_audit(self, c, *, dataset_id: str, actor: str, action: str, resource_type: str,
                             resource_id: str, details: dict) -> None:
        """Append a console audit event; call only inside the successful mutation's transaction."""
        c.execute(t.console_audit_events.insert().values(
            event_id=self.new_id(), dataset_id=dataset_id, actor=actor, action=action, resource_type=resource_type,
            resource_id=resource_id, occurred_at=self.clock(), details=details))

    # ---------- infra ----------
    def check_schema(self) -> None:
        with self.engine.connect() as c:
            ok = c.execute(sa.text("SELECT 1 FROM schema_versions WHERE version = :v"), {"v": SCHEMA_VERSION}).first()
        if not ok:
            raise RuntimeError("database schema version 3 missing; apply postgres/migrations/003_console_operations.sql first")

    def healthy(self) -> bool:
        try:
            with self.engine.connect() as c:
                c.execute(sa.text("SELECT 1"))
            return True
        except (OperationalError, DBAPIError):
            return False

    def tx(self, fn: Callable[[sa.Connection], Any], *, isolation: str | None = None) -> Any:
        """Run fn in one transaction. Unexpected DB errors -> 503 without detail; integrity races -> caller retry."""
        try:
            eng = self.engine.execution_options(isolation_level=isolation) if isolation else self.engine
            with eng.begin() as c:
                return fn(c)
        except ApiError:
            raise
        except IntegrityError:
            raise
        except (OperationalError, DBAPIError) as e:
            raise ApiError("SERVICE_UNAVAILABLE") from e

    def tx_retry(self, fn: Callable[[sa.Connection], Any], **kw) -> Any:
        """Unique-race safe: a concurrent winner makes the retry observe the stored resource/idempotency row."""
        for attempt in range(3):
            try:
                return self.tx(fn, **kw)
            except IntegrityError as e:
                if attempt == 2:
                    raise ApiError("SERVICE_UNAVAILABLE") from e
        raise AssertionError("unreachable")

    # ---------- idempotency ----------
    @staticmethod
    def lock(c, *parts) -> None:
        """Transaction advisory lock keyed by the JSON array of parts (unambiguous, no NUL in text)."""
        c.execute(sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
                  {"k": json.dumps(parts, separators=(",", ":"))})

    @staticmethod
    def idem_lock(c, principal: str, route: str, dataset_id: str | None, key: str) -> None:
        Store.lock(c, "idem", principal, route, dataset_id, key)

    def idem_get(self, c, principal: str, route: str, dataset_id: str | None, key: str | None,
                 request_hash: str) -> tuple[int, dict] | None:
        if key is None:
            return None
        self.idem_lock(c, principal, route, dataset_id, key)
        row = c.execute(sa.text(
            "SELECT request_hash, response_status, response FROM idempotency_records "
            "WHERE principal=:p AND route=:r AND dataset_id IS NOT DISTINCT FROM :d AND key=:k"),
            {"p": principal, "r": route, "d": dataset_id, "k": key}).mappings().first()
        if row is None:
            return None
        if row["request_hash"] != request_hash:
            raise ApiError("CONTENT_CONFLICT")
        return row["response_status"], row["response"]

    def idem_put(self, c, principal: str, route: str, dataset_id: str | None, key: str | None, request_hash: str,
                 resource_id: str, status: int, response: dict) -> None:
        if key is None:
            return
        c.execute(t.idempotency_records.insert().values(
            principal=principal, route=route, dataset_id=dataset_id, key=key, request_hash=request_hash,
            resource_id=uuid.UUID(resource_id), response_status=status, response=response, created_at=self.clock()))

    def idempotent(self, principal: str, route: str, dataset_id: str | None, key: str | None, body: Any,
                   fn: Callable[[sa.Connection], tuple[int, dict, str]], **kw) -> tuple[int, dict]:
        """fn(c) -> (status, response, resource_id); stored atomically with the resource."""
        rh = content_hash({"route": route, "body": body})

        def run(c):
            hit = self.idem_get(c, principal, route, dataset_id, key, rh)
            if hit is not None:
                return hit
            status, resp, rid = fn(c)
            self.idem_put(c, principal, route, dataset_id, key, rh, rid, status, resp)
            return status, resp
        return self.tx_retry(run, **kw)

    def list_datasets(self, dataset_ids: set[str], limit: int, after: str | None):
        if not dataset_ids:
            return [], None

        def run(c):
            rows = c.execute(sa.text(
                "SELECT dataset_id, registration, registration_hash FROM datasets "
                "WHERE dataset_id = ANY(:ids) AND dataset_id > :after ORDER BY dataset_id LIMIT :n"),
                {"ids": sorted(dataset_ids), "after": after or "", "n": limit + 1}).mappings().all()
            items = [{"dataset_id": r["dataset_id"], "owner": r["registration"]["owner"],
                      "history_context": r["registration"]["history_context"],
                      "registration_hash": r["registration_hash"]} for r in rows[:limit]]
            return items, (rows[limit - 1]["dataset_id"] if len(rows) > limit else None)
        return self.tx(run)

    def list_suites(self, dataset_id: str, limit: int, after: str | None):
        def run(c):
            self.require_dataset_row(c, dataset_id)
            rows = c.execute(sa.text("SELECT * FROM suites WHERE dataset_id=:d AND suite_id::text > :a "
                                     "ORDER BY suite_id::text LIMIT :n"),
                             {"d": dataset_id, "a": after or "", "n": limit + 1}).mappings().all()
            return [self.suite_view(r) for r in rows[:limit]], (sid(rows[limit - 1]["suite_id"]) if len(rows) > limit else None)
        return self.tx(run)

    def list_revisions(self, dataset_id: str, suite_id: str, kind: str, limit: int, after: int | None):
        table = "suite_revisions" if kind == "suite" else "policy_revisions"
        def run(c):
            self._suite(c, dataset_id, suite_id)
            rows = c.execute(sa.text(f"SELECT * FROM {table} WHERE suite_id=:s AND revision_no > :a "
                                     "ORDER BY revision_no LIMIT :n"),
                             {"s": suite_id, "a": after or 0, "n": limit + 1}).mappings().all()
            return [self.revision_view(r, kind) for r in rows[:limit]], (rows[limit - 1]["revision_no"] if len(rows) > limit else None)
        return self.tx(run)

    def list_purposes(self, dataset_id: str, limit: int, after: list[str] | None):
        workflow, purpose = after or ["", ""]
        def run(c):
            self.require_dataset_row(c, dataset_id)
            rows = c.execute(sa.text("SELECT * FROM purposes WHERE dataset_id=:d AND (workflow, purpose) > (:w, :p) "
                                     "ORDER BY workflow, purpose LIMIT :n"),
                             {"d": dataset_id, "w": workflow, "p": purpose, "n": limit + 1}).mappings().all()
            return [self.purpose_view(r) for r in rows[:limit]], (
                [rows[limit - 1]["workflow"], rows[limit - 1]["purpose"]] if len(rows) > limit else None)
        return self.tx(run)

    def list_accepted_manifests(self, dataset_id: str, limit: int, after: str | None):
        def run(c):
            self.require_dataset_row(c, dataset_id)
            rows = c.execute(sa.text("SELECT m.*, a.dataset_id AS accepted_dataset_id, a.manifest_id AS accepted_manifest_id, "
                                     "a.harness_reference, a.accepted_by, a.accepted_at FROM compat_acceptances a "
                                     "JOIN compat_manifests m USING (manifest_id) WHERE a.dataset_id=:d "
                                     "AND a.manifest_id::text > :a ORDER BY a.manifest_id::text LIMIT :n"),
                             {"d": dataset_id, "a": after or "", "n": limit + 1}).mappings().all()
            items = [{"manifest": self.manifest_view(r), "acceptance": {
                "dataset_id": r["accepted_dataset_id"], "manifest_id": sid(r["accepted_manifest_id"]),
                "harness_reference": r["harness_reference"], "accepted_by": r["accepted_by"],
                "accepted_at": ts(r["accepted_at"])}} for r in rows[:limit]]
            return items, (sid(rows[limit - 1]["accepted_manifest_id"]) if len(rows) > limit else None)
        return self.tx(run)

    # ---------- datasets ----------
    @staticmethod
    def registration_view(row) -> dict:
        return {"registration": row["registration"], "registration_hash": row["registration_hash"]}

    def get_dataset(self, c, dataset_id: str, *, lock: bool = False) -> dict | None:
        q = "SELECT * FROM datasets WHERE dataset_id=:d" + (" FOR UPDATE" if lock else "")
        return c.execute(sa.text(q), {"d": dataset_id}).mappings().first()

    def put_dataset(self, actor: str, dataset_id: str, reg: Registration, registry: TokenRegistry) -> tuple[int, dict]:
        body = reg.model_dump(mode="json")
        h = content_hash(body)

        def run(c):
            ids = sorted({reg.silver.table_id, reg.quarantine.table_id})
            for tid in ids:
                self.lock(c, "table", tid)
            clash = c.execute(sa.text(
                "SELECT 1 FROM datasets WHERE dataset_id <> :d AND ("
                "registration->'silver'->>'table_id' = ANY(:ids) OR registration->'quarantine'->>'table_id' = ANY(:ids))"),
                {"d": dataset_id, "ids": ids}).first()
            if clash:
                raise ApiError("INVALID_CONFIG", "silver.table_id")
            cur = self.get_dataset(c, dataset_id, lock=True)
            now = self.clock()
            if cur is None:
                c.execute(t.datasets.insert().values(dataset_id=dataset_id, registration=body, registration_hash=h,
                                                     updated_at=now, updated_by=actor))
            elif cur["registration_hash"] != h:
                c.execute(t.datasets.update().where(t.datasets.c.dataset_id == dataset_id).values(
                    registration=body, registration_hash=h, updated_at=now, updated_by=actor))
            if cur is None or cur["registration_hash"] != h:
                self.record_console_audit(c, dataset_id=dataset_id, actor=actor,
                                          action="dataset.register" if cur is None else "dataset.update",
                                          resource_type="dataset", resource_id=dataset_id,
                                          details={"hash_before": cur["registration_hash"] if cur else None,
                                                   "hash_after": h})
            self._sync_grants(c, dataset_id, registry)
            return 200, {"registration": body, "registration_hash": h}
        return self.tx_retry(run)

    @staticmethod
    def _sync_grants(c, dataset_id: str, registry: TokenRegistry) -> None:
        c.execute(sa.text("DELETE FROM principal_grants WHERE dataset_id=:d"), {"d": dataset_id})
        for name, g in registry.grants_for(dataset_id):
            c.execute(t.principal_grants.insert().values(principal=name, dataset_id=dataset_id, role=g.role,
                                                         workflow=g.workflow, purpose=g.purpose))

    def sync_all_grants(self, registry: TokenRegistry) -> None:
        def run(c):
            for (d,) in c.execute(sa.text("SELECT dataset_id FROM datasets ORDER BY dataset_id")).all():
                self._sync_grants(c, d, registry)
        self.tx(run)

    def require_dataset_row(self, c, dataset_id: str) -> dict:
        row = self.get_dataset(c, dataset_id)
        if row is None:
            raise ApiError("NOT_FOUND")
        return row

    # ---------- compat manifests ----------
    @staticmethod
    def manifest_view(row) -> dict:
        return {"manifest_id": sid(row["manifest_id"]), "format": row["format"], "content_hash": row["content_hash"],
                "payload": row["payload"], "created_at": ts(row["created_at"]), "created_by": row["created_by"]}

    def create_manifest(self, actor: str, m: CompatManifest, key: str) -> tuple[int, dict]:
        payload = m.model_dump(mode="json")
        h = content_hash(payload)

        def fn(c):
            self.lock(c, "manifest", h)
            row = c.execute(sa.text("SELECT * FROM compat_manifests WHERE content_hash=:h"), {"h": h}).mappings().first()
            if row is not None:
                return 200, self.manifest_view(row), sid(row["manifest_id"])
            mid = self.new_id()
            c.execute(t.compat_manifests.insert().values(manifest_id=mid, format=payload["format"], content_hash=h,
                                                         payload=payload, created_at=self.clock(), created_by=actor))
            row = c.execute(sa.text("SELECT * FROM compat_manifests WHERE manifest_id=:m"), {"m": mid}).mappings().one()
            return 201, self.manifest_view(row), str(mid)
        return self.idempotent(actor, "POST /compat-manifests", None, key, payload, fn)

    def get_manifest(self, manifest_id: str) -> dict:
        def run(c):
            row = c.execute(sa.text("SELECT * FROM compat_manifests WHERE manifest_id=:m"),
                            {"m": manifest_id}).mappings().first()
            if row is None:
                raise ApiError("NOT_FOUND")
            return self.manifest_view(row)
        return self.tx(run)

    def accept_manifest(self, actor: str, dataset_id: str, manifest_id: str, reference: str, key: str):
        route = f"PUT /datasets/{dataset_id}/compat-manifests/{manifest_id}/acceptance"

        def fn(c):
            self.require_dataset_row(c, dataset_id)
            if c.execute(sa.text("SELECT 1 FROM compat_manifests WHERE manifest_id=:m"), {"m": manifest_id}).first() is None:
                raise ApiError("NOT_FOUND")
            row = c.execute(sa.text("SELECT * FROM compat_acceptances WHERE dataset_id=:d AND manifest_id=:m"),
                            {"d": dataset_id, "m": manifest_id}).mappings().first()
            if row is None:
                c.execute(t.compat_acceptances.insert().values(dataset_id=dataset_id, manifest_id=uuid.UUID(manifest_id),
                                                               harness_reference=reference, accepted_by=actor,
                                                               accepted_at=self.clock()))
                row = c.execute(sa.text("SELECT * FROM compat_acceptances WHERE dataset_id=:d AND manifest_id=:m"),
                                {"d": dataset_id, "m": manifest_id}).mappings().one()
            elif row["harness_reference"] != reference:
                raise ApiError("CONTENT_CONFLICT")
            return 200, {"dataset_id": dataset_id, "manifest_id": manifest_id, "harness_reference": row["harness_reference"],
                         "accepted_by": row["accepted_by"], "accepted_at": ts(row["accepted_at"])}, manifest_id
        return self.idempotent(actor, route, dataset_id, key, {"harness_reference": reference}, fn)

    # ---------- suites / revisions ----------
    @staticmethod
    def suite_view(row) -> dict:
        return {"suite_id": sid(row["suite_id"]), "dataset_id": row["dataset_id"], "workflow": row["workflow"],
                "name": row["name"], "description": row["description"]}

    def _suite(self, c, dataset_id: str, suite_id: str, *, lock: bool = False) -> dict:
        try:
            uuid.UUID(suite_id)
        except ValueError:
            raise ApiError("NOT_FOUND")
        row = c.execute(sa.text("SELECT * FROM suites WHERE suite_id=:s AND dataset_id=:d" + (" FOR UPDATE" if lock else "")),
                        {"s": suite_id, "d": dataset_id}).mappings().first()
        if row is None:
            raise ApiError("NOT_FOUND")
        return row

    def create_suite(self, actor: str, dataset_id: str, body: dict, key: str):
        def fn(c):
            self.require_dataset_row(c, dataset_id)
            self.lock(c, "suite", dataset_id, body["name"])
            row = c.execute(sa.text("SELECT * FROM suites WHERE dataset_id=:d AND name=:n"),
                            {"d": dataset_id, "n": body["name"]}).mappings().first()
            if row is not None:
                if (row["workflow"], row["description"]) != (body["workflow"], body["description"]):
                    raise ApiError("CONTENT_CONFLICT")
                return 200, self.suite_view(row), sid(row["suite_id"])
            s = self.new_id()
            c.execute(t.suites.insert().values(suite_id=s, dataset_id=dataset_id, workflow=body["workflow"],
                                               name=body["name"], description=body["description"],
                                               created_at=self.clock(), created_by=actor))
            self.record_console_audit(c, dataset_id=dataset_id, actor=actor, action="suite.create",
                                      resource_type="suite", resource_id=str(s),
                                      details={"workflow": body["workflow"]})
            return 201, {"suite_id": str(s), "dataset_id": dataset_id, **body}, str(s)
        return self.idempotent(actor, f"POST /datasets/{dataset_id}/suites", dataset_id, key, body, fn)

    def get_suite(self, dataset_id: str, suite_id: str) -> dict:
        return self.tx(lambda c: self.suite_view(self._suite(c, dataset_id, suite_id)))

    @staticmethod
    def revision_view(row, kind: str) -> dict:
        v = {"revision_id": sid(row["revision_id"]), "suite_id": sid(row["suite_id"]), "revision_no": row["revision_no"],
             "content_hash": row["content_hash"], "payload": row["payload"], "created_by": row["created_by"],
             "created_at": ts(row["created_at"])}
        if kind == "suite":
            v["contains_sql"] = row["contains_sql"]
        return v

    def create_revision(self, actor: str, dataset_id: str, suite_id: str, kind: str, payload_model, key: str):
        payload = payload_model.model_dump(mode="json", by_alias=True)
        h = content_hash(payload)
        table = "suite_revisions" if kind == "suite" else "policy_revisions"
        route = f"POST /datasets/{dataset_id}/suites/{suite_id}/{'revisions' if kind == 'suite' else 'policy-revisions'}"

        def fn(c):
            suite = self._suite(c, dataset_id, suite_id, lock=True)
            if kind == "suite":
                s: Suite = payload_model
                if s.workflow is not None and s.workflow != suite["workflow"]:
                    raise ApiError("INVALID_CONFIG", "workflow")
                from models import WORKFLOW_STAGES
                if any(r.stage not in WORKFLOW_STAGES[suite["workflow"]] for r in s.rules):
                    raise ApiError("INVALID_CONFIG", "rules.stage")
                if suite["workflow"] == "AUDIT_TABLE" and any(r.check.kind == "sql_metric" and False for r in s.rules):
                    raise ApiError("INVALID_CONFIG")
            row = c.execute(sa.text(f"SELECT * FROM {table} WHERE suite_id=:s AND content_hash=:h"),
                            {"s": suite_id, "h": h}).mappings().first()
            if row is not None:
                return 200, self.revision_view(row, kind), sid(row["revision_id"])
            no = c.execute(sa.text(f"SELECT COALESCE(MAX(revision_no), 0) + 1 FROM {table} WHERE suite_id=:s"),
                           {"s": suite_id}).scalar_one()
            rid = self.new_id()
            values = dict(revision_id=rid, suite_id=uuid.UUID(suite_id), revision_no=no, content_hash=h, payload=payload,
                          created_at=self.clock(), created_by=actor)
            if kind == "suite":
                values["contains_sql"] = any(r["check"]["kind"] in SQL_KINDS for r in payload["rules"])
                c.execute(t.suite_revisions.insert().values(**values))
            else:
                c.execute(t.policy_revisions.insert().values(**values))
            self.record_console_audit(c, dataset_id=dataset_id, actor=actor,
                                      action=f"{kind}_revision.create", resource_type=f"{kind}_revision",
                                      resource_id=str(rid),
                                      details={"suite_id": suite_id, "revision_no": no, "content_hash": h})
            row = c.execute(sa.text(f"SELECT * FROM {table} WHERE revision_id=:r"), {"r": rid}).mappings().one()
            return 201, self.revision_view(row, kind), str(rid)
        return self.idempotent(actor, route, dataset_id, key, payload, fn)

    def get_revision(self, dataset_id: str, suite_id: str, revision_id: str, kind: str) -> dict:
        table = "suite_revisions" if kind == "suite" else "policy_revisions"

        def run(c):
            self._suite(c, dataset_id, suite_id)
            try:
                uuid.UUID(revision_id)
            except ValueError:
                raise ApiError("NOT_FOUND")
            row = c.execute(sa.text(f"SELECT * FROM {table} WHERE suite_id=:s AND revision_id=:r"),
                            {"s": suite_id, "r": revision_id}).mappings().first()
            if row is None:
                raise ApiError("NOT_FOUND")
            return self.revision_view(row, kind)
        return self.tx(run)

    # ---------- purposes / bindings ----------
    @staticmethod
    def purpose_view(row) -> dict:
        return {"dataset_id": row["dataset_id"], "workflow": row["workflow"], "purpose": row["purpose"],
                "description": row["description"], "state": row["state"], "binding_version": row["binding_version"],
                "suite_id": sid(row["suite_id"]), "suite_revision_id": sid(row["suite_revision_id"]),
                "policy_revision_id": sid(row["policy_revision_id"]), "updated_at": ts(row["updated_at"]),
                "updated_by": row["updated_by"]}

    def _purpose(self, c, dataset_id: str, workflow: str, purpose: str, *, lock: str = "") -> dict | None:
        return c.execute(sa.text("SELECT * FROM purposes WHERE dataset_id=:d AND workflow=:w AND purpose=:p " + lock),
                         {"d": dataset_id, "w": workflow, "p": purpose}).mappings().first()

    def _event(self, c, row, action: str, actor: str, reason: str | None, now) -> None:
        c.execute(t.binding_events.insert().values(
            event_id=self.new_id(), dataset_id=row["dataset_id"], workflow=row["workflow"], purpose=row["purpose"],
            binding_version=row["binding_version"], action=action, suite_id=row["suite_id"],
            suite_revision_id=row["suite_revision_id"], policy_revision_id=row["policy_revision_id"], actor=actor,
            reason=reason, created_at=now))

    def register_purpose(self, actor: str, dataset_id: str, workflow: str, purpose: str, description: str | None):
        def run(c):
            self.require_dataset_row(c, dataset_id)
            self.lock(c, "purpose", dataset_id)
            row = self._purpose(c, dataset_id, workflow, purpose)
            if row is not None:
                if row["description"] != description:
                    raise ApiError("CONTENT_CONFLICT")
                return 200, self.purpose_view(row)
            if workflow == "WRITE_GATE" and c.execute(sa.text(
                    "SELECT 1 FROM purposes WHERE dataset_id=:d AND workflow='WRITE_GATE'"), {"d": dataset_id}).first():
                raise ApiError("INVALID_CONFIG", "purpose")
            now = self.clock()
            c.execute(t.purposes.insert().values(dataset_id=dataset_id, workflow=workflow, purpose=purpose,
                                                 description=description, state="ACTIVE", binding_version=0,
                                                 updated_at=now, updated_by=actor))
            row = self._purpose(c, dataset_id, workflow, purpose)
            self._event(c, row, "REGISTER", actor, None, now)
            self.record_console_audit(c, dataset_id=dataset_id, actor=actor, action="purpose.register",
                                      resource_type="purpose", resource_id=f"{workflow}/{purpose}",
                                      details={"state_after": "ACTIVE"})
            return 201, self.purpose_view(row)
        return self.tx_retry(run)

    def get_purpose(self, dataset_id: str, workflow: str, purpose: str) -> dict:
        def run(c):
            row = self._purpose(c, dataset_id, workflow, purpose)
            if row is None:
                raise ApiError("NOT_FOUND")
            return self.purpose_view(row)
        return self.tx(run)

    def purpose_events(self, dataset_id: str, workflow: str, purpose: str, limit: int, after: int | None) -> dict:
        def run(c):
            if self._purpose(c, dataset_id, workflow, purpose) is None:
                raise ApiError("NOT_FOUND")
            rows = c.execute(sa.text(
                "SELECT * FROM binding_events WHERE dataset_id=:d AND workflow=:w AND purpose=:p AND binding_version > :a "
                "ORDER BY binding_version LIMIT :n"),
                {"d": dataset_id, "w": workflow, "p": purpose, "a": -1 if after is None else after,
                 "n": limit + 1}).mappings().all()
            items = [{"event_id": sid(r["event_id"]), "binding_version": r["binding_version"], "action": r["action"],
                      "suite_id": sid(r["suite_id"]), "suite_revision_id": sid(r["suite_revision_id"]),
                      "policy_revision_id": sid(r["policy_revision_id"]), "actor": r["actor"], "reason": r["reason"],
                      "created_at": ts(r["created_at"])} for r in rows[:limit]]
            return items, (rows[limit - 1]["binding_version"] if len(rows) > limit else None)
        return self.tx(run)

    def activate_binding(self, actor: str, dataset_id: str, workflow: str, purpose: str, body: dict, key: str):
        route = f"PUT /datasets/{dataset_id}/purposes/{workflow}/{purpose}/binding"

        def fn(c):
            ds = self.require_dataset_row(c, dataset_id)
            row = self._purpose(c, dataset_id, workflow, purpose, lock="FOR UPDATE")
            if row is None:
                raise ApiError("NOT_FOUND")
            if row["state"] == "RETIRED":
                raise ApiError("PURPOSE_RETIRED")
            if row["binding_version"] != body["expected_binding_version"]:
                raise ApiError("BINDING_CONFLICT")
            suite = self._suite(c, dataset_id, body["suite_id"])
            if suite["workflow"] != workflow:
                raise ApiError("INVALID_CONFIG", "suite_id")
            srev = c.execute(sa.text("SELECT * FROM suite_revisions WHERE suite_id=:s AND revision_id=:r"),
                             {"s": body["suite_id"], "r": body["suite_revision_id"]}).mappings().first()
            prev = c.execute(sa.text("SELECT * FROM policy_revisions WHERE suite_id=:s AND revision_id=:r"),
                             {"s": body["suite_id"], "r": body["policy_revision_id"]}).mappings().first()
            if srev is None or prev is None:
                raise ApiError("NOT_FOUND")
            if srev["contains_sql"] and srev["created_by"] == actor:
                raise ApiError("SELF_APPROVAL_FORBIDDEN")
            suite_m, policy_m = Suite.model_validate(srev["payload"]), Policy.model_validate(prev["payload"])
            try:
                check_policy_for_suite(suite_m, policy_m)
            except ValueError:
                raise ApiError("INVALID_CONFIG", "policy_revision_id")
            if workflow == "AUDIT_TABLE" and any(p.action == "QUARANTINE" for p in policy_m.rules):
                raise ApiError("INVALID_CONFIG", "policy_revision_id")
            if workflow == "WRITE_GATE" and not ds["registration"]["retention"].get("retry_window_seconds"):
                raise ApiError("INVALID_CONFIG", "retention.retry_window_seconds")
            check_budget(activation_estimate(Registration.model_validate(ds["registration"]), suite_m, policy_m))
            now = self.clock()
            c.execute(t.purposes.update().where(
                (t.purposes.c.dataset_id == dataset_id) & (t.purposes.c.workflow == workflow)
                & (t.purposes.c.purpose == purpose)).values(
                binding_version=row["binding_version"] + 1, suite_id=uuid.UUID(body["suite_id"]),
                suite_revision_id=uuid.UUID(body["suite_revision_id"]),
                policy_revision_id=uuid.UUID(body["policy_revision_id"]), updated_at=now, updated_by=actor))
            row = self._purpose(c, dataset_id, workflow, purpose)
            self._event(c, row, "ACTIVATE", actor, body["reason"], now)
            self.record_console_audit(c, dataset_id=dataset_id, actor=actor, action="binding.activate",
                                      resource_type="purpose", resource_id=f"{workflow}/{purpose}",
                                      details={"binding_version_before": row["binding_version"] - 1,
                                               "binding_version_after": row["binding_version"],
                                               "suite_id": body["suite_id"],
                                               "suite_revision_id": body["suite_revision_id"],
                                               "policy_revision_id": body["policy_revision_id"]})
            return 200, self.purpose_view(row), body["suite_revision_id"]
        return self.idempotent(actor, route, dataset_id, key, body, fn)

    def retire_purpose(self, actor: str, dataset_id: str, workflow: str, purpose: str, body: dict, key: str):
        route = f"POST /datasets/{dataset_id}/purposes/{workflow}/{purpose}/retire"

        def fn(c):
            row = self._purpose(c, dataset_id, workflow, purpose, lock="FOR UPDATE")
            if row is None:
                raise ApiError("NOT_FOUND")
            if workflow == "WRITE_GATE":
                raise ApiError("INVALID_CONFIG", "workflow")
            if row["state"] == "RETIRED":
                raise ApiError("PURPOSE_RETIRED")
            if row["binding_version"] != body["expected_binding_version"]:
                raise ApiError("BINDING_CONFLICT")
            now = self.clock()
            c.execute(t.purposes.update().where(
                (t.purposes.c.dataset_id == dataset_id) & (t.purposes.c.workflow == workflow)
                & (t.purposes.c.purpose == purpose)).values(
                state="RETIRED", binding_version=row["binding_version"] + 1, updated_at=now, updated_by=actor))
            row = self._purpose(c, dataset_id, workflow, purpose)
            self._event(c, row, "RETIRE", actor, body["reason"], now)
            return 200, self.purpose_view(row), str(self.new_id())
        return self.idempotent(actor, route, dataset_id, key, body, fn)

    # =====================================================================================
    # executions / attempts
    # =====================================================================================
    @staticmethod
    def attempt_view(a) -> dict:
        return {"attempt_id": sid(a["attempt_id"]), "ordinal": a["ordinal"], "state": a["state"],
                "runner_identity": a["runner_identity"], "image_digest": a["image_digest"],
                "started_at": ts(a["started_at"]), "retired_at": ts(a["retired_at"]),
                "process_stop": a["process_stop"], "stop_reference": a["stop_reference"],
                "stopped_at": ts(a["stopped_at"]), "error": a["error"]}

    @staticmethod
    def outcome_view(o) -> dict | None:
        if o is None:
            return None
        p = o["payload"] or {}
        return {"destination": o["destination"], "state": o["state"], "outcome_version": o["outcome_version"],
                "intent": p.get("intent"), "commit": p.get("commit"), "rows": p.get("rows"),
                "reason": p.get("reason"), "error": p.get("error"), "updated_at": ts(o["updated_at"])}

    def _execution(self, c, execution_id: str, *, lock: bool = False) -> dict:
        try:
            uuid.UUID(execution_id)
        except ValueError:
            raise ApiError("NOT_FOUND")
        row = c.execute(sa.text("SELECT * FROM executions WHERE execution_id=:e" + (" FOR UPDATE" if lock else "")),
                        {"e": execution_id}).mappings().first()
        if row is None:
            raise ApiError("NOT_FOUND")
        return row

    def execution_row(self, execution_id: str) -> dict:
        return self.tx(lambda c: dict(self._execution(c, execution_id)))

    def _unresolved(self, c, execution_id) -> bool:
        return c.execute(sa.text("SELECT 1 FROM partition_unresolved WHERE execution_id=:e AND cleared_at IS NULL"),
                         {"e": execution_id}).first() is not None

    def _execution_view(self, c, e) -> dict:
        attempts = c.execute(sa.text("SELECT * FROM attempts WHERE execution_id=:e ORDER BY ordinal"),
                             {"e": e["execution_id"]}).mappings().all()
        outs = {o["destination"]: o for o in c.execute(sa.text("SELECT * FROM write_outcomes WHERE execution_id=:e"),
                                                       {"e": e["execution_id"]}).mappings().all()}
        eid = sid(e["execution_id"])
        return {"execution_id": eid, "dataset_id": e["dataset_id"], "workflow": e["workflow"], "purpose": e["purpose"],
                "state": e["state"], "resolution": e["resolution"], "publication_state": e["publication_state"],
                "unresolved": self._unresolved(c, e["execution_id"]),
                "active_attempt_id": sid(e["active_attempt_id"]),
                "attempts": [self.attempt_view(a) for a in attempts], "plan": e["plan"],
                "error": None if e["error"] is None else {"category": e["error_category"], "error": e["error"]},
                "outcomes": {d: self.outcome_view(outs.get(d)) for d in ("silver", "quarantine")},
                "links": {"self": f"/api/v2/executions/{eid}", "report": f"/api/v2/executions/{eid}/report",
                          "decision": f"/api/v2/executions/{eid}/decision"}}

    def get_execution(self, execution_id: str) -> dict:
        return self.tx(lambda c: self._execution_view(c, self._execution(c, execution_id)))

    def open_restore(self, c, dataset_id: str):
        return c.execute(sa.text("SELECT * FROM restore_markers WHERE dataset_id=:d AND cleared_at IS NULL"),
                         {"d": dataset_id}).mappings().first()

    def create_execution(self, actor: str, req, key: str, *, compile_fn, history_fn) -> tuple[int, dict]:
        """compile_fn(c, ctx) -> plan dict (pure + budget); history_fn(c, ctx) -> history_snapshot."""
        body = req.model_dump(mode="json")

        def fn(c):
            ds = self.require_dataset_row(c, req.dataset_id)
            if self.open_restore(c, req.dataset_id):
                raise ApiError("RESTORE_RECONCILING")
            pur = self._purpose(c, req.dataset_id, req.workflow, req.purpose, lock="FOR SHARE")
            if pur is None:
                raise ApiError("NOT_FOUND")
            if pur["state"] == "RETIRED":
                raise ApiError("PURPOSE_RETIRED")
            if pur["suite_revision_id"] is None:
                raise ApiError("INVALID_CONFIG", "purpose")
            acc = c.execute(sa.text(
                "SELECT m.* FROM compat_acceptances a JOIN compat_manifests m USING (manifest_id) "
                "WHERE a.dataset_id=:d AND a.manifest_id=:m"),
                {"d": req.dataset_id, "m": req.compat_manifest_id}).mappings().first()
            if acc is None:
                raise ApiError("INVALID_CONFIG", "compat_manifest_id")
            suite = c.execute(sa.text("SELECT * FROM suites WHERE suite_id=:s"), {"s": pur["suite_id"]}).mappings().one()
            srev = c.execute(sa.text("SELECT * FROM suite_revisions WHERE revision_id=:r"),
                             {"r": pur["suite_revision_id"]}).mappings().one()
            prev = c.execute(sa.text("SELECT * FROM policy_revisions WHERE revision_id=:r"),
                             {"r": pur["policy_revision_id"]}).mappings().one()
            if req.previous_execution_id is not None:
                p = c.execute(sa.text("SELECT dataset_id, workflow, purpose FROM executions WHERE execution_id=:e"),
                              {"e": req.previous_execution_id}).mappings().first()
                if p is None or (p["dataset_id"], p["workflow"], p["purpose"]) != (req.dataset_id, req.workflow,
                                                                                   req.purpose):
                    raise ApiError("INVALID_CONFIG", "previous_execution_id")
            ctx = {"registration": ds["registration"], "purpose": pur, "suite": suite, "suite_rev": srev,
                   "policy_rev": prev, "manifest": acc, "request": body, "execution_id": str(self.new_id()),
                   "now": self.clock()}
            ctx["history_snapshot"] = history_fn(c, ctx)
            plan = compile_fn(c, ctx)
            eid = ctx["execution_id"]
            now = ctx["now"]
            c.execute(t.executions.insert().values(
                execution_id=uuid.UUID(eid), dataset_id=req.dataset_id, workflow=req.workflow, purpose=req.purpose,
                binding_version=pur["binding_version"], suite_id=pur["suite_id"],
                suite_revision_id=pur["suite_revision_id"], policy_revision_id=pur["policy_revision_id"],
                compat_manifest_id=uuid.UUID(req.compat_manifest_id), plan_hash=plan["plan_hash"], plan=plan,
                state="PLANNED", publication_state="NOT_STARTED" if req.workflow == "WRITE_GATE" else "NOT_APPLICABLE",
                event_time=req.event_time, created_at=now, updated_at=now,
                previous_execution_id=uuid.UUID(req.previous_execution_id) if req.previous_execution_id else None,
                actor=actor))
            resp = {"execution_id": eid, "state": "PLANNED", "plan": plan,
                    "links": {"self": f"/api/v2/executions/{eid}", "attempts": f"/api/v2/executions/{eid}/attempts"}}
            return 202, resp, eid
        return self.idempotent(actor, "POST /executions", req.dataset_id, key, body, fn, isolation="REPEATABLE READ")

    def list_executions(self, dataset_id: str, states: list[str], purposes: set[tuple[str, str]] | None,
                        limit: int, after: tuple[str, str] | None, *, workflow: str | None = None) -> tuple[list[dict], tuple | None]:
        def run(c):
            q = ["SELECT * FROM executions WHERE dataset_id=:d"]
            params: dict[str, Any] = {"d": dataset_id, "n": limit + 1}
            if states:
                q.append("AND state = ANY(:states)")
                params["states"] = states
            if workflow is not None:
                q.append("AND workflow = :workflow")
                params["workflow"] = workflow
            if purposes is not None:
                if not purposes:
                    return [], None
                q.append("AND (workflow || '/' || purpose) = ANY(:wp)")  # workflow enum has no '/'
                params["wp"] = [f"{w}/{p}" for w, p in purposes]
            if after is not None:
                q.append("AND (event_time, execution_id) > (:at, :ae)")
                params["at"], params["ae"] = after
            q.append("ORDER BY event_time, execution_id LIMIT :n")
            rows = c.execute(sa.text(" ".join(q)), params).mappings().all()
            items = [self._execution_view(c, r) for r in rows[:limit]]
            nxt = None
            if len(rows) > limit:
                last = rows[limit - 1]
                nxt = (last["event_time"].isoformat(), str(last["execution_id"]))
            return items, nxt
        return self.tx(run)

    def _attempt(self, c, execution_id: str, attempt_id: str, *, lock: bool = False):
        try:
            uuid.UUID(attempt_id)
        except ValueError:
            raise ApiError("NOT_FOUND")
        a = c.execute(sa.text("SELECT * FROM attempts WHERE execution_id=:e AND attempt_id=:a"
                              + (" FOR UPDATE" if lock else "")), {"e": execution_id, "a": attempt_id}).mappings().first()
        if a is None:
            raise ApiError("NOT_FOUND")
        return a

    @staticmethod
    def candidate_paths(e, attempt_id: str) -> dict | None:
        loc = e["plan"].get("candidate_locations")
        if not loc:
            return None
        root = loc["execution_root"]
        return {"raw": f"{root}/{attempt_id}/raw", "classified": f"{root}/{attempt_id}/classified"}

    def _retire(self, c, a, state: str, stop: dict | None, now, error_category=None, error=None) -> None:
        confirmed = bool(stop and stop["confirmed_stopped"])
        c.execute(t.attempts.update().where(t.attempts.c.attempt_id == a["attempt_id"]).values(
            state=state, retired_at=now, process_stop="CONFIRMED" if confirmed else "UNCONFIRMED",
            stop_reference=stop["reference"] if confirmed else None, stopped_at=now if confirmed else None,
            error_category=error_category, error=error))

    def create_attempt(self, actor: str, execution_id: str, body, key: str | None) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            e = self._execution(c, execution_id, lock=True)
            if self.open_restore(c, e["dataset_id"]):
                raise ApiError("RESTORE_RECONCILING")
            if e["state"] not in ("PLANNED", "RUNNING"):
                raise ApiError("STATE_CONFLICT")
            now = self.clock()
            if e["active_attempt_id"] is not None:
                act = self._attempt(c, execution_id, str(e["active_attempt_id"]), lock=True)
                if b["previous_attempt_id"] is None:
                    if act["runner_identity"] == b["runner_identity"] and act["image_digest"] == b["image_digest"]:
                        return 200, {"attempt": self.attempt_view(act),
                                     "candidate_paths": self.candidate_paths(e, str(act["attempt_id"]))}, \
                            str(act["attempt_id"])
                    raise ApiError("ATTEMPT_CONFLICT")
                if b["previous_attempt_id"] != str(act["attempt_id"]) or b["previous_stop"] is None:
                    raise ApiError("ATTEMPT_CONFLICT")
                self._retire(c, act, "ABANDONED", b["previous_stop"], now)
            ordinal = c.execute(sa.text("SELECT COALESCE(MAX(ordinal),0)+1 FROM attempts WHERE execution_id=:e"),
                                {"e": execution_id}).scalar_one()
            aid = self.new_id()
            c.execute(t.attempts.insert().values(attempt_id=aid, execution_id=e["execution_id"], ordinal=ordinal,
                                                 state="RUNNING", runner_identity=b["runner_identity"],
                                                 image_digest=b["image_digest"], started_at=now))
            c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(
                state="RUNNING", active_attempt_id=aid, updated_at=now))
            a = self._attempt(c, execution_id, str(aid))
            return 201, {"attempt": self.attempt_view(a), "candidate_paths": self.candidate_paths(e, str(aid))}, str(aid)
        if key is None:
            return self.tx_retry(lambda c: fn(c)[:2])
        return self.idempotent(actor, f"POST /executions/{execution_id}/attempts", None, key, b, fn)

    def fail_attempt(self, actor: str, execution_id: str, attempt_id: str, body) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def run(c):
            e = self._execution(c, execution_id, lock=True)
            a = self._attempt(c, execution_id, attempt_id, lock=True)
            if a["state"] == "FAILED" and a["error"] == b["error"] and a["error_category"] == b["error_category"]:
                return 200, {"execution": self._execution_view(c, e), "attempt": self.attempt_view(a)}
            if e["state"] != "RUNNING" or e["active_attempt_id"] != a["attempt_id"] or a["state"] != "RUNNING":
                raise ApiError("ATTEMPT_CONFLICT")
            now = self.clock()
            self._retire(c, a, "FAILED", b["process_stop"], now, b["error_category"], b["error"])
            c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(
                active_attempt_id=None, updated_at=now))
            self.record_console_audit(c, dataset_id=e["dataset_id"], actor=actor, action="attempt.fail",
                                      resource_type="attempt", resource_id=attempt_id,
                                      details={"execution_id": execution_id, "state_before": "RUNNING",
                                               "state_after": "FAILED", "error_category": b["error_category"]})
            e = self._execution(c, execution_id)
            return 200, {"execution": self._execution_view(c, e),
                         "attempt": self.attempt_view(self._attempt(c, execution_id, attempt_id))}
        return self.tx(run)

    def terminate(self, actor: str, execution_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            e = self._execution(c, execution_id, lock=True)
            if e["state"] not in ("PLANNED", "RUNNING"):
                raise ApiError("STATE_CONFLICT")
            now = self.clock()
            if e["active_attempt_id"] is not None:
                if b["process_stop"] is None:
                    raise ApiError("INVALID_CONFIG", "process_stop")
                a = self._attempt(c, execution_id, str(e["active_attempt_id"]), lock=True)
                self._retire(c, a, "FAILED" if b["state"] == "FAILED" else "ABANDONED", b["process_stop"], now,
                             "TERMINATED", b["error"])
            c.execute(t.write_outcomes.delete().where(t.write_outcomes.c.execution_id == e["execution_id"]))
            c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(
                state=b["state"], active_attempt_id=None, publication_state="NOT_APPLICABLE", updated_at=now,
                error_category="TERMINATED", error={"reason": b["reason"], "client_error": b["error"]}))
            self.record_console_audit(c, dataset_id=e["dataset_id"], actor=actor, action="execution.terminate",
                                      resource_type="execution", resource_id=execution_id,
                                      details={"state_before": e["state"], "state_after": b["state"],
                                               "attempt_id": sid(e["active_attempt_id"])})
            return 200, self._execution_view(c, self._execution(c, execution_id)), execution_id
        return self.idempotent(actor, f"POST /executions/{execution_id}/terminate", None, key, b, fn)

    def stop_confirmation(self, actor: str, execution_id: str, attempt_id: str, reference: str) -> tuple[int, dict]:
        def run(c):
            self._execution(c, execution_id)
            a = self._attempt(c, execution_id, attempt_id, lock=True)
            if a["state"] == "RUNNING":
                raise ApiError("STATE_CONFLICT")
            if a["process_stop"] == "CONFIRMED":
                if a["stop_reference"] != reference:
                    raise ApiError("CONTENT_CONFLICT")
                return 200, self.attempt_view(a)
            c.execute(t.attempts.update().where(t.attempts.c.attempt_id == a["attempt_id"]).values(
                process_stop="CONFIRMED", stop_reference=reference, stopped_at=self.clock()))
            return 200, self.attempt_view(self._attempt(c, execution_id, attempt_id))
        return self.tx(run)

    # =====================================================================================
    # reports
    # =====================================================================================
    def stored_report(self, execution_id: str):
        def run(c):
            self._execution(c, execution_id)
            return c.execute(sa.text("SELECT * FROM accepted_reports WHERE execution_id=:e"),
                             {"e": execution_id}).mappings().first()
        return self.tx(run)

    def accept_report(self, execution_id: str, report_hash: str, payload: dict, *, validate, build) -> tuple[int, dict]:
        """validate(c, e) re-checks active attempt/plan under lock; build(c, e) -> rows to insert.
        Concurrent duplicates: the loser observes the winner's row and returns it (identical) or 409."""
        def run(c):
            e = self._execution(c, execution_id, lock=True)
            stored = c.execute(sa.text("SELECT * FROM accepted_reports WHERE execution_id=:e"),
                               {"e": execution_id}).mappings().first()
            if stored is not None:
                if stored["report_hash"] == report_hash:
                    return 200, stored["accepted_response"]
                raise ApiError("CONTENT_CONFLICT")
            if self.open_restore(c, e["dataset_id"]):
                raise ApiError("RESTORE_RECONCILING")
            validate(c, e)
            out = build(c, e)
            self.record_console_audit(c, dataset_id=e["dataset_id"], actor="system", action="report.accept",
                                      resource_type="report", resource_id=execution_id,
                                      details={"execution_id": execution_id, "report_hash": report_hash})
            return 200, out
        return self.tx_retry(run)

    # =====================================================================================
    # outcomes
    # =====================================================================================
    @staticmethod
    def partition_key(pv: dict) -> str:
        from canonical import canonical_bytes
        return canonical_bytes(pv).decode("utf-8")

    @staticmethod
    def aggregate(outs: dict[str, dict]) -> str:
        states = [o["state"] for o in outs.values()]
        if all(s == "NOT_APPLICABLE" for s in states):
            return "NOT_APPLICABLE"
        if all(s in ("SUCCEEDED", "NOT_APPLICABLE") for s in states):
            return "SUCCEEDED"
        if any(s == "SUCCEEDED" for s in states):
            return "PARTIAL"
        for s in ("UNKNOWN", "IN_PROGRESS", "FAILED"):
            if s in states:
                return s
        return "NOT_STARTED"

    def _outcomes(self, c, execution_id, *, lock: bool = False) -> dict[str, dict]:
        rows = c.execute(sa.text("SELECT * FROM write_outcomes WHERE execution_id=:e ORDER BY destination"
                                 + (" FOR UPDATE" if lock else "")), {"e": execution_id}).mappings().all()
        return {r["destination"]: dict(r) for r in rows}

    def put_outcome(self, actor: str, role: str, execution_id: str, dest: str, req) -> tuple[int, dict]:
        b = req.model_dump(mode="json")

        def run(c):
            e = self._execution(c, execution_id, lock=True)
            if role == "job" and self.open_restore(c, e["dataset_id"]):
                raise ApiError("RESTORE_RECONCILING")
            rep = c.execute(sa.text("SELECT * FROM accepted_reports WHERE execution_id=:e"),
                            {"e": execution_id}).mappings().first()
            outs = self._outcomes(c, execution_id, lock=True)
            if rep is None or dest not in outs:
                raise ApiError("STATE_CONFLICT")
            o = outs[dest]
            if o["state"] == "NOT_APPLICABLE":
                raise ApiError("NOT_APPLICABLE")
            if e["resolution"] == "CLOSED_UNRESOLVED":
                raise ApiError("STATE_CONFLICT")
            stored = o["payload"] or {}
            # request intent null = keep stored intent (persisted that way below), so compare normalized
            desired = {"state": b["state"], "intent": b["intent"] if b["intent"] is not None else stored.get("intent"),
                       "commit": b["commit"], "rows": b["rows"]}
            current = {"state": o["state"], "intent": stored.get("intent"), "commit": stored.get("commit"),
                       "rows": stored.get("rows")}
            if o["state"] == "SUCCEEDED" and desired == current:
                return 200, self._outcome_response(c, e, dest)
            if e["state"] != "REPORTED":
                raise ApiError("STATE_CONFLICT")
            if o["state"] == "SUCCEEDED":
                raise ApiError("CONTENT_CONFLICT")
            if b["expected_outcome_version"] != o["outcome_version"]:
                if desired == current:
                    return 200, self._outcome_response(c, e, dest)
                raise ApiError("CONTENT_CONFLICT")
            self._check_transition(c, e, rep, outs, dest, role, b)
            now = self.clock()
            commit = b["commit"]
            intent = b["intent"] if b["intent"] is not None else stored.get("intent")
            payload = {"intent": intent, "commit": commit, "rows": b["rows"], "reason": b["reason"],
                       "error": b["error"], "recovery": b["recovery"], "actor": actor, "role": role}
            c.execute(t.write_outcomes.update().where(
                (t.write_outcomes.c.execution_id == e["execution_id"]) & (t.write_outcomes.c.destination == dest)).values(
                state=b["state"], outcome_version=o["outcome_version"] + 1, payload=payload, updated_at=now,
                table_id=commit["table_id"] if commit else None,
                commit_version=commit["version"] if commit else None,
                metadata_hash=commit["metadata_hash"] if commit else None,
                commit_evidence=commit["evidence"] if commit else None,
                txn_app_id=intent["txn_app_id"] if intent else None,
                txn_version=intent["txn_version"] if intent else None))
            self._after_outcome(c, e, rep, now)
            self.record_console_audit(c, dataset_id=e["dataset_id"], actor=actor, action="outcome.put",
                                      resource_type="write_outcome", resource_id=f"{execution_id}/{dest}",
                                      details={"execution_id": execution_id, "destination": dest, "role": role,
                                               "state_before": o["state"], "state_after": b["state"],
                                               "outcome_version": o["outcome_version"] + 1})
            return 200, self._outcome_response(c, self._execution(c, execution_id), dest)
        return self.tx(run)

    def _outcome_response(self, c, e, dest: str) -> dict:
        outs = self._outcomes(c, e["execution_id"])
        return {"outcome": self.outcome_view(outs[dest]), "publication_state": e["publication_state"],
                "unresolved": self._unresolved(c, e["execution_id"])}

    def _check_transition(self, c, e, rep, outs, dest: str, role: str, b: dict) -> None:
        plan = e["plan"]
        frm, to = outs[dest]["state"], b["state"]
        token = f"dq:{e['execution_id']}:{dest}"
        table_id = plan["registration_snapshot"][dest]["table_id"]
        base_version = plan["target_bases"][dest]["version"]
        rec = b["recovery"] or {}
        stop = rec.get("writer_stop") or {}
        stopped = bool(stop.get("confirmed_stopped"))
        commit = b["commit"]

        def need_commit_table():
            if commit["table_id"] != table_id:
                raise ApiError("PLAN_MISMATCH")
            if commit["evidence"] == "EXACT" and b["rows"] is None:
                raise ApiError("INVALID_CONFIG", "rows")

        def need_absence():
            ab = rec.get("absence")
            if not ab or ab["table_id"] != table_id or ab["checked_through_version"] < base_version:
                raise ApiError("INVALID_CONFIG", "recovery.absence")

        def intent_ok(intent):
            if intent is None or intent["txn_app_id"] != token or intent["txn_version"] != 1:
                raise ApiError("PLAN_MISMATCH")

        if to == "IN_PROGRESS":
            intent_ok(b["intent"])
            if role != "job":
                raise ApiError("FORBIDDEN")
            if dest == "silver" and outs["quarantine"]["state"] not in ("SUCCEEDED", "NOT_APPLICABLE"):
                raise ApiError("STATE_CONFLICT")
            if frm == "NOT_STARTED":
                return
            if frm not in ("FAILED", "UNKNOWN"):
                raise ApiError("STATE_CONFLICT")
            if frm == "UNKNOWN" and not stopped:
                raise ApiError("WRITER_UNCONFIRMED")
            if not rec:
                raise ApiError("INVALID_CONFIG", "recovery")
            need_absence()
            if rec["current_base_version"] != base_version:
                raise ApiError("BASE_CHANGED")
            window = plan["registration_snapshot"]["retention"].get("retry_window_seconds")
            if not window or (self.clock() - rep["accepted_at"]).total_seconds() > window:
                raise ApiError("RETRY_WINDOW_EXPIRED")
            return
        if b["intent"] is not None:
            intent_ok(b["intent"])
        if to == "SUCCEEDED":
            need_commit_table()
            if frm not in ("IN_PROGRESS", "UNKNOWN"):
                raise ApiError("STATE_CONFLICT")
            if commit["evidence"] == "TXN_ONLY":
                txn = rec.get("txn_evidence") or {}
                if role != "operator" or frm != "UNKNOWN":
                    raise ApiError("FORBIDDEN" if role != "operator" else "STATE_CONFLICT")
                if txn.get("txn_app_id") != token or txn.get("table_id") != table_id:
                    raise ApiError("PLAN_MISMATCH")
                if txn.get("txn_version") != 1:
                    raise ApiError("STATE_CONFLICT")
                return
            if role == "operator" and not stopped:
                raise ApiError("WRITER_UNCONFIRMED")
            return
        if to == "FAILED":
            if frm not in ("IN_PROGRESS", "UNKNOWN"):
                raise ApiError("STATE_CONFLICT")
            need_absence()
            if frm == "UNKNOWN" and not stopped:
                raise ApiError("WRITER_UNCONFIRMED")
            if frm == "IN_PROGRESS" and role != "job":
                raise ApiError("FORBIDDEN")
            return
        if to == "UNKNOWN":
            if frm != "IN_PROGRESS":
                raise ApiError("STATE_CONFLICT")
            if role == "operator" and not stopped:
                raise ApiError("WRITER_UNCONFIRMED")
            return
        raise ApiError("STATE_CONFLICT")

    def _after_outcome(self, c, e, rep, now) -> None:
        outs = self._outcomes(c, e["execution_id"])
        agg = self.aggregate(outs)
        values: dict[str, Any] = {"publication_state": agg, "updated_at": now}
        if agg == "SUCCEEDED":
            values["state"] = "COMPLETED"
        c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(**values))
        pv = e["plan"]["partition_values"]
        pkey = self.partition_key(pv)
        succeeded = [o for o in outs.values() if o["state"] == "SUCCEEDED"]
        for o in succeeded:
            if o["commit_evidence"] == "TXN_ONLY":
                c.execute(sa.text(
                    "INSERT INTO partition_unresolved (marker_id, dataset_id, scope, partition_key, partition_values, "
                    "execution_id, txn_app_id, destination, table_id, reason, recorded_at) VALUES "
                    "(:m, :d, 'PARTITION', :k, CAST(:pv AS jsonb), :e, :t, :dest, :tid, 'TXN_ONLY', :now) "
                    "ON CONFLICT DO NOTHING"),
                    {"m": self.new_id(), "d": e["dataset_id"], "k": pkey, "pv": _json(pv), "e": e["execution_id"],
                     "t": o["txn_app_id"], "dest": o["destination"], "tid": o["table_id"], "now": now})
        if agg != "SUCCEEDED" or any(o["commit_evidence"] == "TXN_ONLY" for o in succeeded):
            return
        silver, q = outs["silver"], outs["quarantine"]
        cur = c.execute(sa.text("SELECT * FROM partition_publications WHERE dataset_id=:d AND partition_key=:k FOR UPDATE"),
                        {"d": e["dataset_id"], "k": pkey}).mappings().first()
        rows_q = (q["payload"] or {}).get("rows") or 0 if q["state"] == "SUCCEEDED" else 0
        vals = dict(dataset_id=e["dataset_id"], partition_key=pkey, partition_values=pv, execution_id=e["execution_id"],
                    silver_table_id=silver["table_id"], silver_commit_version=silver["commit_version"],
                    quarantine_commit_version=q["commit_version"] if q["state"] == "SUCCEEDED" else None,
                    quarantine_rows=rows_q, published_at=now)
        if cur is None:
            c.execute(t.partition_publications.insert().values(**vals))
        elif silver["table_id"] == cur["silver_table_id"] and silver["commit_version"] > cur["silver_commit_version"]:
            c.execute(t.partition_publications.update().where(
                (t.partition_publications.c.dataset_id == e["dataset_id"])
                & (t.partition_publications.c.partition_key == pkey)).values(**vals))
        else:
            return  # older callback: never regress the pointer
        c.execute(sa.text(
            "UPDATE partition_unresolved SET cleared_at=:now, cleared_by=:by WHERE dataset_id=:d AND scope='PARTITION' "
            "AND partition_key=:k AND cleared_at IS NULL AND recorded_at < :created"),
            {"now": now, "by": f"publication:{e['execution_id']}", "d": e["dataset_id"], "k": pkey,
             "created": e["created_at"]})
        self._eligibility_on_success(c, e, rep, now)

    def _eligibility_on_success(self, c, e, rep, now) -> None:
        from anomaly import eligible_on_success
        policy_anomaly = e["plan"]["policy_revision"]["payload"]["anomaly"]
        statuses = {(a["metric_definition_hash"], a["scope"], a["stage"]): a["status"] for a in c.execute(sa.text(
            "SELECT metric_definition_hash, scope, stage, status FROM anomaly_observations WHERE execution_id=:e"),
            {"e": e["execution_id"]}).mappings()}
        for o in c.execute(sa.text("SELECT * FROM metric_observations WHERE execution_id=:e"),
                           {"e": e["execution_id"]}).mappings().all():
            st = statuses.get((o["metric_definition_hash"], o["scope"], o["stage"]))
            ok = eligible_on_success(policy_anomaly, st, e["plan"]["event_origin"])
            c.execute(t.metric_eligibility_events.insert().values(
                event_id=self.new_id(), observation_id=o["observation_id"], eligible=ok,
                reason="PUBLICATION_SUCCEEDED" if e["workflow"] == "WRITE_GATE" else "AUDIT_COMPLETED",
                actor="system", effective_at=now))

    def abandon(self, actor: str, execution_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            e = self._execution(c, execution_id, lock=True)
            if e["state"] != "REPORTED":
                raise ApiError("STATE_CONFLICT")
            outs = self._outcomes(c, execution_id, lock=True)
            if any(o["state"] in ("IN_PROGRESS", "UNKNOWN") for o in outs.values()):
                raise ApiError("OUTCOME_UNRESOLVED")
            pub = "PARTIAL" if any(o["state"] == "SUCCEEDED" for o in outs.values()) else "FAILED"
            c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(
                state="FAILED", publication_state=pub, updated_at=self.clock(), error_category="PUBLICATION_ABANDONED",
                error={"reason": b["reason"], "client_error": b["error"]}))
            return 200, self._execution_view(c, self._execution(c, execution_id)), execution_id
        return self.idempotent(actor, f"POST /executions/{execution_id}/publication/abandon", None, key, b, fn)

    def close_unresolved(self, actor: str, execution_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            e = self._execution(c, execution_id, lock=True)
            if e["state"] != "REPORTED" or e["resolution"] is not None:
                raise ApiError("STATE_CONFLICT")
            outs = self._outcomes(c, execution_id, lock=True)
            if any(o["state"] == "IN_PROGRESS" for o in outs.values()):
                raise ApiError("OUTCOME_UNRESOLVED")
            unknown = {d for d, o in outs.items() if o["state"] == "UNKNOWN"}
            if not unknown or unknown != {ev["destination"] for ev in b["evidence"]}:
                raise ApiError("INVALID_CONFIG", "evidence")
            reg = e["plan"]["registration_snapshot"]
            if any(ev["table_id"] != reg[ev["destination"]]["table_id"] for ev in b["evidence"]):
                raise ApiError("PLAN_MISMATCH")
            if c.execute(sa.text("SELECT 1 FROM attempts WHERE execution_id=:e AND process_stop IS DISTINCT FROM "
                                 "'CONFIRMED'"), {"e": execution_id}).first():
                raise ApiError("WRITER_UNCONFIRMED")
            if b["reason_code"] == "UNRESOLVABLE_LOG_EXPIRED" and any(ev["txn_present"] for ev in b["evidence"]):
                raise ApiError("COMMIT_EVIDENCE_PRESENT")
            now = self.clock()
            pv = e["plan"]["partition_values"]
            c.execute(t.executions.update().where(t.executions.c.execution_id == e["execution_id"]).values(
                state="FAILED", resolution="CLOSED_UNRESOLVED", publication_state=self.aggregate(outs),
                updated_at=now, error_category="CLOSED_UNRESOLVED",
                error={"reason_code": b["reason_code"], "reason": b["reason"], "evidence": b["evidence"],
                       "writer_stop": b["writer_stop"]}))
            for d in sorted(unknown):
                c.execute(sa.text(
                    "INSERT INTO partition_unresolved (marker_id, dataset_id, scope, partition_key, partition_values, "
                    "execution_id, txn_app_id, destination, table_id, reason, recorded_at) VALUES "
                    "(:m, :d, 'PARTITION', :k, CAST(:pv AS jsonb), :e, :t, :dest, :tid, 'CLOSED_UNRESOLVED', :now) "
                    "ON CONFLICT DO NOTHING"),
                    {"m": self.new_id(), "d": e["dataset_id"], "k": self.partition_key(pv), "pv": _json(pv),
                     "e": e["execution_id"], "t": f"dq:{e['execution_id']}:{d}", "dest": d,
                     "tid": reg[d]["table_id"], "now": now})
            return 200, self._execution_view(c, self._execution(c, execution_id)), execution_id
        return self.idempotent(actor, f"POST /executions/{execution_id}/publication/close-unresolved", None, key, b, fn)

    def publications(self, dataset_id: str, dt: str) -> dict:
        pv = {"dt": dt}
        pkey = self.partition_key(pv)

        def run(c):
            self.require_dataset_row(c, dataset_id)
            cur = c.execute(sa.text("SELECT * FROM partition_publications WHERE dataset_id=:d AND partition_key=:k"),
                            {"d": dataset_id, "k": pkey}).mappings().first()
            pending = c.execute(sa.text(
                "SELECT execution_id, publication_state FROM executions WHERE dataset_id=:d AND workflow='WRITE_GATE' "
                "AND state='REPORTED' AND plan->'partition_values'->>'dt' = :dt ORDER BY created_at DESC, execution_id "
                "DESC LIMIT 1"), {"d": dataset_id, "dt": dt}).mappings().first()
            unresolved = c.execute(sa.text(
                "SELECT execution_id FROM partition_unresolved WHERE dataset_id=:d AND scope='PARTITION' AND "
                "partition_key=:k AND execution_id IS NOT NULL AND cleared_at IS NULL ORDER BY recorded_at DESC, "
                "marker_id DESC LIMIT 1"), {"d": dataset_id, "k": pkey}).first()
            orphans = c.execute(sa.text(
                "SELECT marker_id, txn_app_id, scope, recorded_at FROM partition_unresolved WHERE dataset_id=:d AND "
                "reason='RESTORE_ORPHAN' AND cleared_at IS NULL AND (scope='DATASET' OR partition_key=:k) "
                "ORDER BY recorded_at, marker_id"), {"d": dataset_id, "k": pkey}).mappings().all()
            return {"dataset_id": dataset_id, "partition_values": pv,
                    "current": None if cur is None else {
                        "execution_id": sid(cur["execution_id"]),
                        "silver": {"table_id": cur["silver_table_id"], "commit_version": cur["silver_commit_version"]},
                        "quarantine_commit_version": cur["quarantine_commit_version"],
                        "quarantine_rows": cur["quarantine_rows"], "published_at": ts(cur["published_at"])},
                    "pending": None if pending is None else {"execution_id": sid(pending["execution_id"]),
                                                             "publication_state": pending["publication_state"]},
                    "unresolved_execution_id": None if unresolved is None else sid(unresolved[0]),
                    "restore_orphans": [{"marker_id": sid(o["marker_id"]), "txn_app_id": o["txn_app_id"],
                                         "scope": o["scope"], "recorded_at": ts(o["recorded_at"])} for o in orphans]}
        return self.tx(run)


    # =====================================================================================
    # restore reconciliation (Bước 9)
    # =====================================================================================
    def open_restore_all(self, actor: str) -> None:
        def run(c):
            for (d,) in c.execute(sa.text("SELECT dataset_id FROM datasets ORDER BY dataset_id FOR UPDATE")).all():
                if self.open_restore(c, d) is None:
                    c.execute(t.restore_markers.insert().values(restore_id=self.new_id(), dataset_id=d,
                                                                opened_at=self.clock(), opened_by=actor))
        self.tx(run)

    @staticmethod
    def _pending_reconcile(c, dataset_id: str) -> list[str]:
        return [sid(r[0]) for r in c.execute(sa.text(
            "SELECT DISTINCT o.execution_id FROM write_outcomes o JOIN executions e USING (execution_id) "
            "WHERE e.dataset_id=:d AND e.resolution IS NULL AND o.state IN ('IN_PROGRESS','UNKNOWN') "
            "ORDER BY 1"), {"d": dataset_id}).all()]

    def restore_view(self, dataset_id: str) -> dict:
        def run(c):
            self.require_dataset_row(c, dataset_id)
            rows = c.execute(sa.text("SELECT * FROM restore_markers WHERE dataset_id=:d ORDER BY opened_at, restore_id"),
                             {"d": dataset_id}).mappings().all()
            open_, hist = None, []
            for r in rows:
                if r["cleared_at"] is None:
                    n = c.execute(sa.text("SELECT count(*) FROM restore_findings WHERE restore_id=:r"),
                                  {"r": r["restore_id"]}).scalar_one()
                    open_ = {"restore_id": sid(r["restore_id"]), "opened_at": ts(r["opened_at"]),
                             "findings_submitted": n, "pending_reconcile": self._pending_reconcile(c, dataset_id)}
                else:
                    hist.append({"restore_id": sid(r["restore_id"]), "opened_at": ts(r["opened_at"]),
                                 "opened_by": r["opened_by"], "cleared_at": ts(r["cleared_at"]),
                                 "cleared_by": r["cleared_by"], "completion": r["completion"]})
            return {"open": open_, "history": hist}
        return self.tx(run)

    def restore_findings(self, actor: str, dataset_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            ds = self.require_dataset_row(c, dataset_id)
            m = c.execute(sa.text("SELECT * FROM restore_markers WHERE dataset_id=:d AND cleared_at IS NULL FOR UPDATE"),
                          {"d": dataset_id}).mappings().first()
            if m is None:
                raise ApiError("STATE_CONFLICT")
            rid = m["restore_id"]
            reg = ds["registration"]
            scans = {s["destination"]: s for s in b["scans"]}
            for d, s in scans.items():
                if s["table_id"] != reg[d]["table_id"]:
                    raise ApiError("PLAN_MISMATCH", f"scans.{d}")
            prev = c.execute(sa.text("SELECT * FROM restore_scans WHERE restore_id=:r AND scan_id=:s"),
                             {"r": rid, "s": b["scan_id"]}).mappings().all()
            n_pages = c.execute(sa.text("SELECT count(*) FROM restore_findings WHERE restore_id=:r AND scan_id=:s"),
                                {"r": rid, "s": b["scan_id"]}).scalar_one()
            if prev:
                if any(p["complete"] for p in prev):
                    raise ApiError("STATE_CONFLICT")
                if {p["destination"]: (p["table_id"], p["snapshot_version"]) for p in prev} != {
                        d: (s["table_id"], s["snapshot_version"]) for d, s in scans.items()}:
                    raise ApiError("CONTENT_CONFLICT")
            if b["page"] != n_pages:
                raise ApiError("STATE_CONFLICT", "page")
            now = self.clock()
            c.execute(t.restore_findings.insert().values(restore_id=rid, scan_id=uuid.UUID(b["scan_id"]),
                                                         page=b["page"], request_hash=content_hash(b), payload=b,
                                                         created_at=now, actor=actor))
            blocked = None
            for d, s in scans.items():
                mx = c.execute(sa.text(
                    "SELECT max(o.commit_version) FROM write_outcomes o JOIN executions e USING (execution_id) "
                    "WHERE e.dataset_id=:ds AND o.destination=:d AND o.commit_evidence='EXACT'"),
                    {"ds": dataset_id, "d": d}).scalar_one()
                if mx is not None and s["snapshot_version"] < mx:
                    blocked = "TARGET_NOT_INITIALIZED"
            if not prev:
                for d, s in scans.items():
                    c.execute(t.restore_scans.insert().values(restore_id=rid, scan_id=uuid.UUID(b["scan_id"]),
                                                              destination=d, table_id=s["table_id"],
                                                              snapshot_version=s["snapshot_version"], complete=False,
                                                              blocked_code=blocked))
            elif blocked:
                c.execute(sa.text("UPDATE restore_scans SET blocked_code=:b WHERE restore_id=:r AND scan_id=:s"),
                          {"b": blocked, "r": rid, "s": b["scan_id"]})
            known, orphans = [], []
            for tok in b["tokens"]:
                parts = tok["txn_app_id"].split(":")
                row = None
                if len(parts) == 3 and parts[0] == "dq" and parts[2] == tok["destination"]:
                    try:
                        uuid.UUID(parts[1])
                        row = c.execute(sa.text("SELECT 1 FROM executions WHERE execution_id=:e AND dataset_id=:d"),
                                        {"e": parts[1], "d": dataset_id}).first()
                    except ValueError:
                        row = None
                if row is not None:
                    known.append({"execution_id": parts[1], "destination": tok["destination"],
                                  "commit_version": tok["commit_version"]})
                    continue
                pv = tok["partition_values"]
                scope = "PARTITION" if pv else "DATASET"
                c.execute(sa.text(
                    "INSERT INTO partition_unresolved (marker_id, dataset_id, scope, partition_key, partition_values, "
                    "execution_id, txn_app_id, destination, table_id, reason, restore_id, recorded_at) VALUES "
                    "(:m, :d, :scope, :k, CAST(:pv AS jsonb), NULL, :t, :dest, :tid, 'RESTORE_ORPHAN', :r, :now) "
                    "ON CONFLICT DO NOTHING"),
                    {"m": self.new_id(), "d": dataset_id, "scope": scope,
                     "k": self.partition_key(pv) if pv else None, "pv": _json(pv) if pv else None,
                     "t": tok["txn_app_id"], "dest": tok["destination"], "tid": reg[tok["destination"]]["table_id"],
                     "r": rid, "now": now})
                orphans.append({"txn_app_id": tok["txn_app_id"], "destination": tok["destination"], "scope": scope,
                                "partition_values": pv})
            if b["final_page"]:
                c.execute(sa.text("UPDATE restore_scans SET complete=true WHERE restore_id=:r AND scan_id=:s"),
                          {"r": rid, "s": b["scan_id"]})
            return 200, {"restore_id": sid(rid), "scan_id": b["scan_id"], "page": b["page"],
                         "final_page": b["final_page"], "known": known, "orphans": orphans, "blocked": blocked}, \
                b["scan_id"]
        return self.idempotent(actor, f"POST /datasets/{dataset_id}/restore/findings", dataset_id, key, b, fn)

    def restore_complete(self, actor: str, dataset_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            self.require_dataset_row(c, dataset_id)
            m = c.execute(sa.text("SELECT * FROM restore_markers WHERE dataset_id=:d AND cleared_at IS NULL FOR UPDATE"),
                          {"d": dataset_id}).mappings().first()
            if m is None:
                raise ApiError("STATE_CONFLICT")
            scan = c.execute(sa.text(
                "SELECT s.scan_id, bool_and(s.complete) AS complete, max(s.blocked_code) AS blocked, count(*) AS n, "
                "(SELECT max(f.created_at) FROM restore_findings f WHERE f.restore_id=:r AND f.scan_id=s.scan_id) AS at "
                "FROM restore_scans s WHERE s.restore_id=:r GROUP BY s.scan_id ORDER BY at DESC LIMIT 1"),
                {"r": m["restore_id"]}).mappings().first()
            if scan is None or not scan["complete"] or scan["n"] != 2:
                raise ApiError("STATE_CONFLICT")
            if scan["blocked"]:
                raise ApiError("TARGET_NOT_INITIALIZED")
            if self._pending_reconcile(c, dataset_id):
                raise ApiError("OUTCOME_UNRESOLVED")
            now = self.clock()
            completion = {"reason": b["reason"], "reconcile_reference": b["reconcile_reference"],
                          "scan_id": sid(scan["scan_id"])}
            c.execute(t.restore_markers.update().where(t.restore_markers.c.restore_id == m["restore_id"]).values(
                cleared_at=now, cleared_by=actor, completion=completion))
            return 200, {"restore_id": sid(m["restore_id"]), "cleared_at": ts(now), "cleared_by": actor,
                         "completion": completion}, sid(m["restore_id"])
        return self.idempotent(actor, f"POST /datasets/{dataset_id}/restore/complete", dataset_id, key, b, fn)

    def clear_marker(self, actor: str, dataset_id: str, marker_id: str, body) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def run(c):
            m = c.execute(sa.text("SELECT * FROM partition_unresolved WHERE marker_id=:m AND dataset_id=:d FOR UPDATE"),
                          {"m": marker_id, "d": dataset_id}).mappings().first()
            if m is None:
                raise ApiError("NOT_FOUND")
            if m["scope"] != "DATASET":
                raise ApiError("INVALID_CONFIG", "scope")
            if m["cleared_at"] is None:
                c.execute(sa.text("UPDATE partition_unresolved SET cleared_at=:now, cleared_by=:by WHERE marker_id=:m"),
                          {"now": self.clock(), "by": actor, "m": marker_id})
            m = c.execute(sa.text("SELECT * FROM partition_unresolved WHERE marker_id=:m"),
                          {"m": marker_id}).mappings().one()
            return 200, {"marker_id": marker_id, "scope": m["scope"], "cleared_at": ts(m["cleared_at"]),
                         "cleared_by": m["cleared_by"], "reconcile_reference": b["reconcile_reference"]}
        return self.tx(run)

    # =====================================================================================
    # eligibility override (Bước 8)
    # =====================================================================================
    def add_eligibility(self, actor: str, execution_id: str, observation_id: str, body: dict, key: str):
        def fn(c):
            o = c.execute(sa.text("SELECT * FROM metric_observations WHERE observation_id=:o AND execution_id=:e"),
                          {"o": observation_id, "e": execution_id}).mappings().first()
            if o is None:
                raise ApiError("NOT_FOUND")
            ev, now = self.new_id(), self.clock()
            c.execute(t.metric_eligibility_events.insert().values(event_id=ev, observation_id=o["observation_id"],
                                                                  eligible=body["eligible"], reason=body["reason"],
                                                                  actor=actor, effective_at=now))
            return 201, {"event_id": str(ev), "observation_id": observation_id, "eligible": body["eligible"],
                         "reason": body["reason"], "actor": actor, "effective_at": ts(now)}, str(ev)
        return self.idempotent(actor, f"POST /executions/{execution_id}/metrics/{observation_id}/eligibility", None,
                               key, body, fn)

    # =====================================================================================
    # retention / cleanup (Bước 9.5)
    # =====================================================================================
    def _retention_rows(self, c, dataset_id: str, limit: int, after: str | None,
                        attempt_id: str | None = None) -> tuple[list[dict], str | None]:
        ds = self.require_dataset_row(c, dataset_id)
        ret = ds["registration"]["retention"]
        rows = c.execute(sa.text(
            "SELECT a.attempt_id, a.execution_id, a.process_stop, e.state, e.resolution, e.publication_state, "
            "e.plan->'retention'->>'retain_until' AS retain_until, "
            "e.plan->'candidate_locations'->>'execution_root' AS root, "
            "EXISTS (SELECT 1 FROM partition_publications p WHERE p.execution_id=e.execution_id) AS current_ref, "
            "EXISTS (SELECT 1 FROM partition_unresolved u WHERE u.execution_id=e.execution_id "
            "AND u.cleared_at IS NULL) AS unresolved "
            "FROM attempts a JOIN executions e USING (execution_id) "
            "WHERE e.dataset_id=:d AND e.workflow='WRITE_GATE' AND a.attempt_id::text > :a "
            "AND (CAST(:only AS text) IS NULL OR a.attempt_id::text = :only) "
            "ORDER BY a.attempt_id::text LIMIT :n"),
            {"d": dataset_id, "a": after or "", "n": limit + 1, "only": attempt_id}).mappings().all()
        removed = {(sid(r[0]), r[1]) for r in c.execute(sa.text(
            "SELECT attempt_id, uri FROM cleanup_records WHERE dataset_id=:d"), {"d": dataset_id}).all()}
        now = self.clock()
        items = []
        for r in rows[:limit]:
            until = r["retain_until"]
            if not ret.get("cleanup_enabled") or until is None:
                reason = "CLEANUP_DISABLED"
            elif r["state"] in ("PLANNED", "RUNNING", "REPORTED"):
                reason = "EXECUTION_ACTIVE"
            elif r["process_stop"] != "CONFIRMED":
                reason = "WRITER_UNCONFIRMED"
            elif r["current_ref"] or r["unresolved"]:
                reason = "REFERENCED"
            elif datetime.fromisoformat(until.replace("Z", "+00:00")) > now:
                reason = "NOT_EXPIRED"
            else:
                reason = None
            for kind in ("raw", "classified"):
                uri = f"{r['root']}/{r['attempt_id']}/{kind}"
                if (sid(r["attempt_id"]), uri) in removed:
                    continue
                items.append({"uri": uri, "execution_id": sid(r["execution_id"]), "attempt_id": sid(r["attempt_id"]),
                              "retain_until": until, "pinned": reason is not None, "reason": reason})
        return items, (sid(rows[limit - 1]["attempt_id"]) if len(rows) > limit else None)

    def retention(self, dataset_id: str, limit: int, after: str | None) -> tuple[list[dict], str | None]:
        return self.tx(lambda c: self._retention_rows(c, dataset_id, limit, after))

    def record_cleanup(self, actor: str, dataset_id: str, body, key: str) -> tuple[int, dict]:
        b = body.model_dump(mode="json")

        def fn(c):
            row = c.execute(sa.text(
                "SELECT e.execution_id FROM executions e JOIN attempts a USING (execution_id) WHERE e.dataset_id=:d "
                "AND e.execution_id=:e AND a.attempt_id=:a FOR UPDATE OF e"),
                {"d": dataset_id, "e": b["execution_id"], "a": b["attempt_id"]}).first()
            if row is None:
                raise ApiError("NOT_FOUND")
            items, _ = self._retention_rows(c, dataset_id, 1, None, attempt_id=b["attempt_id"])
            cand = {i["uri"]: i for i in items}.get(b["uri"])
            if cand is None:
                raise ApiError("INVALID_CONFIG", "uri")
            if cand["pinned"]:
                raise ApiError("STATE_CONFLICT")
            cid, now = self.new_id(), self.clock()
            c.execute(t.cleanup_records.insert().values(cleanup_id=cid, dataset_id=dataset_id,
                                                        execution_id=uuid.UUID(b["execution_id"]),
                                                        attempt_id=uuid.UUID(b["attempt_id"]), uri=b["uri"],
                                                        removed_at=now, actor=actor, stop_reference=b["stop_reference"]))
            return 201, {"cleanup_id": str(cid), **b, "removed_at": ts(now), "actor": actor}, str(cid)
        return self.idempotent(actor, f"POST /datasets/{dataset_id}/cleanup-records", dataset_id, key, b, fn)


def _json(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"))
