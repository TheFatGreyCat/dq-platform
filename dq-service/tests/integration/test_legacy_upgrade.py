"""Legacy (pre-v2) database -> apply 002 alone: legacy rows untouched and still readable; v2 starts empty."""
from __future__ import annotations

import uuid

import sqlalchemy as sa

from .conftest import POSTGRES_DIR, _url_with_db, psql

LEGACY_DDL = (POSTGRES_DIR / "init.sql").read_text(encoding="utf-8").split("\\ir migrations/002_execution_v2.sql")[0]


def test_legacy_schema_then_002_keeps_legacy_rows(server_url):
    name = f"dqlegacy_{uuid.uuid4().hex[:12]}"
    admin = sa.create_engine(server_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(sa.text(f'CREATE DATABASE "{name}"'))
    url = _url_with_db(server_url, name)
    eng = sa.create_engine(url)
    try:
        done = psql(url, "-c", LEGACY_DDL)
        assert done.returncode == 0, done.stderr
        with eng.begin() as c:
            c.execute(sa.text("INSERT INTO dq_suites (dataset, suite, policy) VALUES ('legacy.ds', "
                              "CAST(:s AS jsonb), CAST(:p AS jsonb))"),
                      {"s": '{"format":"dq-rules/v2"}', "p": '{"max_quarantine_ratio":0.05}'})
            c.execute(sa.text("INSERT INTO dq_runs (run_id, dataset, verdict, row_count) VALUES "
                              "('run-legacy-1', 'legacy.ds', 'WARN', 1000)"))
            c.execute(sa.text("INSERT INTO dq_rule_results (run_id, dataset, rule_id, status, metric_value) VALUES "
                              "('run-legacy-1', 'legacy.ds', 'r1', 'passed', 1.0)"))
        before = _legacy_snapshot(eng)
        for _ in range(2):  # idempotent re-apply
            done = psql(url, "-f", "migrations/002_execution_v2.sql")
            assert done.returncode == 0, done.stderr
        assert _legacy_snapshot(eng) == before
        with eng.connect() as c:
            assert c.execute(sa.text("SELECT version FROM schema_versions")).scalars().all() == [2]
            assert c.execute(sa.text("SELECT count(*) FROM datasets")).scalar_one() == 0
            assert c.execute(sa.text("SELECT count(*) FROM executions")).scalar_one() == 0
    finally:
        eng.dispose()
        with admin.connect() as c:
            c.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def _legacy_snapshot(eng):
    with eng.connect() as c:
        return tuple(tuple(map(tuple, c.execute(sa.text(q)).all())) for q in (
            "SELECT dataset, version, suite, policy FROM dq_suites ORDER BY dataset",
            "SELECT run_id, dataset, verdict, row_count FROM dq_runs ORDER BY run_id",
            "SELECT run_id, rule_id, status, metric_value FROM dq_rule_results ORDER BY id"))
