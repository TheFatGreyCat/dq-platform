"""Hành vi ràng buộc của schema v2 trên PostgreSQL thật (init.sql + migration 002)."""
import uuid
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError

import schema
from .conftest import POSTGRES_DIR, psql

H = "sha256:" + "a" * 64
NOW = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)


def uid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def conn(engine):
    """Mỗi test một transaction, rollback cuối; lỗi mong đợi chạy trong savepoint."""
    with engine.connect() as c:
        outer = c.begin()
        yield c
        outer.rollback()


def rejected(conn, stmt, exc=IntegrityError):
    with pytest.raises(exc):
        with conn.begin_nested():
            conn.execute(stmt)


def accepted(conn, stmt):
    with conn.begin_nested():
        conn.execute(stmt)


def dataset(conn, name=None) -> str:
    name = name or f"ds_{uid().hex[:8]}"
    conn.execute(sa.insert(schema.datasets).values(dataset_id=name, registration={}, registration_hash=H,
                                                   updated_at=NOW, updated_by="t"))
    return name


def suite(conn, ds, workflow="WRITE_GATE"):
    sid = uid()
    conn.execute(sa.insert(schema.suites).values(suite_id=sid, dataset_id=ds, workflow=workflow,
                                                 name=f"s_{sid.hex[:6]}", created_at=NOW, created_by="t"))
    return sid


def revisions(conn, sid, n=1, h=H):
    rev, pol = uid(), uid()
    conn.execute(sa.insert(schema.suite_revisions).values(revision_id=rev, suite_id=sid, revision_no=n,
                 content_hash=h, payload={}, contains_sql=False, created_at=NOW, created_by="t"))
    conn.execute(sa.insert(schema.policy_revisions).values(revision_id=pol, suite_id=sid, revision_no=n,
                 content_hash=h, payload={}, created_at=NOW, created_by="t"))
    return rev, pol


def purpose(conn, ds, workflow="AUDIT_TABLE", name="p", pointers=(None, None, None)):
    conn.execute(sa.insert(schema.purposes).values(
        dataset_id=ds, workflow=workflow, purpose=name, state="ACTIVE", binding_version=0,
        suite_id=pointers[0], suite_revision_id=pointers[1], policy_revision_id=pointers[2],
        updated_at=NOW, updated_by="t"))


def manifest(conn, ds):
    mid = uid()
    conn.execute(sa.insert(schema.compat_manifests).values(manifest_id=mid, format="dq-compat-v1",
                 content_hash="sha256:" + uid().hex * 2, payload={}, created_at=NOW, created_by="t"))
    conn.execute(sa.insert(schema.compat_acceptances).values(dataset_id=ds, manifest_id=mid,
                 harness_reference="ref", accepted_by="a", accepted_at=NOW))
    return mid


def bound(conn):
    """dataset + suite + revisions + purpose đã bind + manifest accepted."""
    ds = dataset(conn)
    sid = suite(conn, ds)
    rev, pol = revisions(conn, sid)
    purpose(conn, ds, "WRITE_GATE", "default", (sid, rev, pol))
    return ds, sid, rev, pol, manifest(conn, ds)


def execution(conn, ctx, **over):
    ds, sid, rev, pol, mid = ctx
    row = dict(execution_id=uid(), dataset_id=ds, workflow="WRITE_GATE", purpose="default", binding_version=1,
               suite_id=sid, suite_revision_id=rev, policy_revision_id=pol, compat_manifest_id=mid,
               plan_hash=H, plan={}, state="RUNNING", publication_state="NOT_STARTED", event_time=NOW,
               created_at=NOW, updated_at=NOW, actor="t")
    row.update(over)
    conn.execute(sa.insert(schema.executions).values(**row))
    return row["execution_id"]


def attempt(conn, eid, ordinal=1, **over):
    row = dict(attempt_id=uid(), execution_id=eid, ordinal=ordinal, state="RUNNING", runner_identity="r",
               started_at=NOW)
    row.update(over)
    conn.execute(sa.insert(schema.attempts).values(**row))
    return row["attempt_id"]


def test_cross_suite_revision_is_rejected(conn):
    ds = dataset(conn)
    s1, s2 = suite(conn, ds), suite(conn, ds)
    r1, p1 = revisions(conn, s1)
    r2, p2 = revisions(conn, s2)
    rejected(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="WRITE_GATE", purpose="x", state="ACTIVE", binding_version=1,
        suite_id=s1, suite_revision_id=r2, policy_revision_id=p1, updated_at=NOW, updated_by="t"))
    rejected(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="WRITE_GATE", purpose="x", state="ACTIVE", binding_version=1,
        suite_id=s1, suite_revision_id=r1, policy_revision_id=p2, updated_at=NOW, updated_by="t"))
    accepted(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="WRITE_GATE", purpose="x", state="ACTIVE", binding_version=1,
        suite_id=s1, suite_revision_id=r1, policy_revision_id=p1, updated_at=NOW, updated_by="t"))


def test_purpose_pointers_all_or_none_and_workflow_matches_suite(conn):
    ds = dataset(conn)
    sid = suite(conn, ds, "AUDIT_TABLE")
    rev, pol = revisions(conn, sid)
    base = dict(dataset_id=ds, purpose="p", state="ACTIVE", binding_version=1, updated_at=NOW, updated_by="t")
    rejected(conn, sa.insert(schema.purposes).values(workflow="AUDIT_TABLE", suite_id=sid, **base))
    rejected(conn, sa.insert(schema.purposes).values(
        workflow="WRITE_GATE", suite_id=sid, suite_revision_id=rev, policy_revision_id=pol, **base))


def test_only_one_write_gate_purpose_per_dataset(conn):
    ds = dataset(conn)
    purpose(conn, ds, "WRITE_GATE", "default")
    rejected(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="WRITE_GATE", purpose="second", state="ACTIVE", binding_version=0,
        updated_at=NOW, updated_by="t"))
    accepted(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="AUDIT_TABLE", purpose="a1", state="ACTIVE", binding_version=0,
        updated_at=NOW, updated_by="t"))
    accepted(conn, sa.insert(schema.purposes).values(
        dataset_id=ds, workflow="AUDIT_TABLE", purpose="a2", state="ACTIVE", binding_version=0,
        updated_at=NOW, updated_by="t"))


def test_dangling_references_rejected(conn):
    rejected(conn, sa.insert(schema.suites).values(suite_id=uid(), dataset_id="missing", workflow="WRITE_GATE",
                                                   name="n", created_at=NOW, created_by="t"))
    ctx = bound(conn)
    rejected(conn, sa.insert(schema.executions).values(
        execution_id=uid(), dataset_id=ctx[0], workflow="WRITE_GATE", purpose="nope", binding_version=1,
        suite_id=ctx[1], suite_revision_id=ctx[2], policy_revision_id=ctx[3], compat_manifest_id=ctx[4],
        plan_hash=H, plan={}, state="PLANNED", publication_state="NOT_STARTED", event_time=NOW,
        created_at=NOW, updated_at=NOW, actor="t"))


def test_execution_requires_accepted_manifest(conn):
    ctx = bound(conn)
    other_ds = dataset(conn)
    unaccepted = uid()
    conn.execute(sa.insert(schema.compat_manifests).values(manifest_id=unaccepted, format="dq-compat-v1",
                 content_hash="sha256:" + "c" * 64, payload={}, created_at=NOW, created_by="t"))
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            execution(conn, ctx, compat_manifest_id=unaccepted)
    assert other_ds
    execution(conn, ctx)


def test_hash_format_is_enforced(conn):
    rejected(conn, sa.insert(schema.datasets).values(dataset_id="bad", registration={}, registration_hash="md5:x",
                                                     updated_at=NOW, updated_by="t"))
    rejected(conn, sa.insert(schema.datasets).values(dataset_id="bad", registration={},
                                                     registration_hash="sha256:" + "A" * 64,
                                                     updated_at=NOW, updated_by="t"))


def test_revision_uniqueness(conn):
    ds = dataset(conn)
    sid = suite(conn, ds)
    revisions(conn, sid, 1)
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            revisions(conn, sid, 2)  # cùng content_hash trong suite
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            revisions(conn, sid, 1, h="sha256:" + "b" * 64)  # cùng revision_no


def test_attempt_stop_invariants(conn):
    eid = execution(conn, bound(conn))
    a = attempt(conn, eid, 1, state="SUCCEEDED")  # SUCCEEDED với process_stop NULL hợp lệ
    assert a
    rejected(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=2, state="RUNNING", runner_identity="r", started_at=NOW,
        process_stop="UNCONFIRMED"))
    rejected(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=2, state="FAILED", runner_identity="r", started_at=NOW))
    rejected(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=2, state="FAILED", runner_identity="r", started_at=NOW,
        process_stop="CONFIRMED"))
    accepted(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=2, state="FAILED", runner_identity="r", started_at=NOW,
        process_stop="CONFIRMED", stop_reference="local-process:k:1:exit=0", stopped_at=NOW))
    accepted(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=3, state="ABANDONED", runner_identity="r", started_at=NOW,
        process_stop="UNCONFIRMED"))


def test_only_one_running_attempt_per_execution(conn):
    eid = execution(conn, bound(conn))
    attempt(conn, eid, 1)
    rejected(conn, sa.insert(schema.attempts).values(
        attempt_id=uid(), execution_id=eid, ordinal=2, state="RUNNING", runner_identity="r", started_at=NOW))


def test_active_attempt_fk_is_deferrable_and_same_execution(conn):
    ctx = bound(conn)
    e1, e2 = execution(conn, ctx), execution(conn, ctx)
    a1 = attempt(conn, e1)
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            conn.execute(sa.update(schema.executions).where(schema.executions.c.execution_id == e2)
                         .values(active_attempt_id=a1))
            conn.execute(sa.text("SET CONSTRAINTS executions_active_attempt_fk IMMEDIATE"))
    # đảo thứ tự insert execution -> attempt trong một transaction nhờ deferred
    accepted(conn, sa.update(schema.executions).where(schema.executions.c.execution_id == e1)
             .values(active_attempt_id=a1))


def test_resolution_requires_failed(conn):
    ctx = bound(conn)
    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            execution(conn, ctx, state="REPORTED", resolution="CLOSED_UNRESOLVED")
    execution(conn, ctx, state="FAILED", resolution="CLOSED_UNRESOLVED")


def test_principal_grant_scope_and_uniqueness(conn):
    ds = dataset(conn)
    g = dict(principal="p", dataset_id=ds)
    rejected(conn, sa.insert(schema.principal_grants).values(role="job", **g))
    rejected(conn, sa.insert(schema.principal_grants).values(role="viewer", workflow="WRITE_GATE", purpose="x", **g))
    accepted(conn, sa.insert(schema.principal_grants).values(role="viewer", **g))
    rejected(conn, sa.insert(schema.principal_grants).values(role="viewer", **g))  # NULLS NOT DISTINCT
    accepted(conn, sa.insert(schema.principal_grants).values(role="job", workflow="WRITE_GATE", purpose="x", **g))


def test_idempotency_unique_with_null_dataset(conn):
    row = dict(principal="p", route="POST /compat-manifests", dataset_id=None, key="k", request_hash=H,
               resource_id=uid(), response_status=201, response={}, created_at=NOW)
    accepted(conn, sa.insert(schema.idempotency_records).values(**row))
    rejected(conn, sa.insert(schema.idempotency_records).values(**{**row, "resource_id": uid()}))


def test_write_outcome_evidence_shape(conn):
    eid = execution(conn, bound(conn))
    base = dict(execution_id=eid, outcome_version=0, payload={}, updated_at=NOW)
    rejected(conn, sa.insert(schema.write_outcomes).values(destination="silver", state="SUCCEEDED", **base))
    rejected(conn, sa.insert(schema.write_outcomes).values(
        destination="silver", state="IN_PROGRESS", commit_evidence="EXACT", table_id="t", commit_version=1,
        metadata_hash=H, **base))
    rejected(conn, sa.insert(schema.write_outcomes).values(  # EXACT thiếu metadata_hash
        destination="silver", state="SUCCEEDED", commit_evidence="EXACT", table_id="t", commit_version=1, **base))
    rejected(conn, sa.insert(schema.write_outcomes).values(  # TXN_ONLY có version
        destination="silver", state="SUCCEEDED", commit_evidence="TXN_ONLY", table_id="t", commit_version=1,
        txn_app_id="a", txn_version=1, **base))
    accepted(conn, sa.insert(schema.write_outcomes).values(
        destination="silver", state="SUCCEEDED", commit_evidence="EXACT", table_id="t", commit_version=1,
        metadata_hash=H, **base))
    accepted(conn, sa.insert(schema.write_outcomes).values(
        destination="quarantine", state="SUCCEEDED", commit_evidence="TXN_ONLY", table_id="t", txn_app_id="a",
        txn_version=1, **base))


def test_rule_result_shape(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    aid = attempt(conn, eid, state="SUCCEEDED")
    conn.execute(sa.insert(schema.accepted_reports).values(
        report_id=uid(), execution_id=eid, attempt_id=aid, report_hash=H, evaluation_fingerprint="f", payload={},
        accepted_at=NOW, accepted_response={}))
    base = dict(execution_id=eid, metric_definition_hash=H, measurement_definition_hash=H, scope="PARTITION",
                stage="CANDIDATE", payload={})
    rejected(conn, sa.insert(schema.rule_results).values(rule_id="a", measurement_status="OK", **base))
    rejected(conn, sa.insert(schema.rule_results).values(  # OK mang cả metric lẫn error
        rule_id="a", measurement_status="OK", metric_name="Size", metric_decimal=1, error_code="X", **base))
    rejected(conn, sa.insert(schema.rule_results).values(rule_id="a", measurement_status="ERROR", **base))
    accepted(conn, sa.insert(schema.rule_results).values(
        rule_id="a", measurement_status="OK", metric_name="Size", metric_decimal=1, **base))
    accepted(conn, sa.insert(schema.rule_results).values(
        rule_id="b", measurement_status="ERROR", error_code="SQL_ANALYSIS_ERROR", error_params={}, **base))


def test_partition_unresolved_shapes(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    base = dict(marker_id=uid(), dataset_id=ctx[0], recorded_at=NOW)
    pk = dict(partition_key='{"dt":"2026-10-20"}', partition_values={"dt": "2026-10-20"})
    rejected(conn, sa.insert(schema.partition_unresolved).values(
        **{**base, "marker_id": uid()}, scope="PARTITION", execution_id=eid, reason="TXN_ONLY"))
    rejected(conn, sa.insert(schema.partition_unresolved).values(
        **{**base, "marker_id": uid()}, scope="DATASET", execution_id=eid, reason="TXN_ONLY", **pk))
    rejected(conn, sa.insert(schema.partition_unresolved).values(  # orphan phải có restore_id, không execution
        **{**base, "marker_id": uid()}, scope="DATASET", reason="RESTORE_ORPHAN", txn_app_id="dq:x:silver"))
    first = dict(scope="PARTITION", execution_id=eid, reason="TXN_ONLY", txn_app_id="dq:e:silver",
                 destination="silver", **pk)
    accepted(conn, sa.insert(schema.partition_unresolved).values(**{**base, "marker_id": uid()}, **first))
    rejected(conn, sa.insert(schema.partition_unresolved).values(**{**base, "marker_id": uid()}, **first))


def test_single_open_restore_marker(conn):
    ds = dataset(conn)
    first = dict(dataset_id=ds, opened_at=NOW, opened_by="op")
    accepted(conn, sa.insert(schema.restore_markers).values(restore_id=uid(), **first))
    rejected(conn, sa.insert(schema.restore_markers).values(restore_id=uid(), **first))
    accepted(conn, sa.insert(schema.restore_markers).values(restore_id=uid(), cleared_at=NOW, cleared_by="op",
                                                            **first))


def test_publication_quarantine_null_requires_zero_rows(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    row = dict(dataset_id=ctx[0], partition_key="k", partition_values={}, execution_id=eid, silver_table_id="t",
               silver_commit_version=1, published_at=NOW)
    rejected(conn, sa.insert(schema.partition_publications).values(quarantine_rows=3, **row))
    accepted(conn, sa.insert(schema.partition_publications).values(quarantine_rows=0, **row))


def test_referenced_evidence_cannot_be_deleted(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    attempt(conn, eid)
    rejected(conn, sa.delete(schema.executions).where(schema.executions.c.execution_id == eid))
    rejected(conn, sa.delete(schema.datasets).where(schema.datasets.c.dataset_id == ctx[0]))
    rejected(conn, sa.delete(schema.suites).where(schema.suites.c.suite_id == ctx[1]))


def test_immutable_tables_reject_update_and_delete(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    aid = attempt(conn, eid, state="SUCCEEDED")
    conn.execute(sa.insert(schema.accepted_reports).values(
        report_id=uid(), execution_id=eid, attempt_id=aid, report_hash=H, evaluation_fingerprint="f", payload={},
        accepted_at=NOW, accepted_response={}))
    conn.execute(sa.insert(schema.gate_decisions).values(
        decision_id=uid(), execution_id=eid, decision="ALLOW", plan_hash=H, report_hash=H,
        evaluation_fingerprint="f", payload={}, created_at=NOW))
    for tbl, col, val in [(schema.suite_revisions, "created_by", "x"), (schema.policy_revisions, "created_by", "x"),
                          (schema.accepted_reports, "assessment", "PASS"), (schema.gate_decisions, "decision", "DENY"),
                          (schema.compat_acceptances, "accepted_by", "x"), (schema.compat_manifests, "created_by", "x")]:
        rejected(conn, sa.update(tbl).values({col: val}), DBAPIError)
        rejected(conn, sa.delete(tbl), DBAPIError)


def test_mutable_tables_still_update(conn):
    ctx = bound(conn)
    eid = execution(conn, ctx)
    accepted(conn, sa.update(schema.executions).where(schema.executions.c.execution_id == eid)
             .values(state="FAILED", updated_at=NOW))
    accepted(conn, sa.update(schema.purposes).where(schema.purposes.c.dataset_id == ctx[0])
             .values(binding_version=2))


def test_schema_py_columns_match_database(engine):
    live = sa.inspect(engine)
    tables = set(live.get_table_names())
    for t in schema.metadata.sorted_tables:
        assert t.name in tables, t.name
        assert {c["name"] for c in live.get_columns(t.name)} == {c.name for c in t.columns}, t.name
    assert {"dq_suites", "dq_runs", "dq_rule_results"} <= tables


def test_migration_rerun_is_noop(database_url, engine):
    for f in ("migrations/002_execution_v2.sql", str(POSTGRES_DIR / "init.sql")):
        done = psql(database_url, "-f", f)
        assert done.returncode == 0, done.stderr
    with engine.connect() as c:
        assert c.execute(sa.text("SELECT count(*) FROM schema_versions WHERE version = 2")).scalar() == 1


def test_migration_checksum_mismatch_fails(server_url):
    from .conftest import _url_with_db
    name = f"dqtest_{uid().hex[:12]}"
    admin = sa.create_engine(server_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(sa.text(f'CREATE DATABASE "{name}"'))
    try:
        url = _url_with_db(server_url, name)
        assert psql(url, "-f", "migrations/002_execution_v2.sql").returncode == 0
        tamper = psql(url, "-c", "ALTER TABLE schema_versions DISABLE TRIGGER USER; "
                                 "UPDATE schema_versions SET checksum = 'other' WHERE version = 2")
        assert tamper.returncode == 0, tamper.stderr
        done = psql(url, "-f", "migrations/002_execution_v2.sql")
        assert done.returncode != 0 and "checksum" in done.stderr
    finally:
        with admin.connect() as c:
            c.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()
