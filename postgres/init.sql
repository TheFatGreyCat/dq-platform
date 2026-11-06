-- Schema DQ platform (engine Deequ, rule spec v2). Chỉ chạy khi volume Postgres còn trống.
-- DB đã khởi tạo từ bản GX: chạy postgres/migrations/001_gx_to_deequ.sql.

-- Rule registry: mỗi dataset có một suite (rule spec v2, xem dq-service/models.py) + policy
CREATE TABLE IF NOT EXISTS dq_suites (
    dataset     TEXT PRIMARY KEY,           -- vd: silver.transactions
    version     INTEGER     NOT NULL DEFAULT 1,
    suite       JSONB       NOT NULL,       -- {format: dq-rules/v2, parameters, rules[]}
    policy      JSONB       NOT NULL,       -- max_quarantine_ratio, min_row_count, owner
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mỗi lần một job Spark validate một batch
CREATE TABLE IF NOT EXISTS dq_runs (
    run_id            TEXT PRIMARY KEY,
    dataset           TEXT        NOT NULL,
    partition_spec    TEXT,                 -- vd: dt=2026-10-17
    job_name          TEXT,
    client            TEXT,                 -- python | scala
    engine            TEXT,                 -- vd: deequ-2.0.21-spark-3.5
    suite_version     INTEGER,
    verdict           TEXT        NOT NULL, -- PASS | WARN | FAIL
    reasons           JSONB,
    row_count         BIGINT,
    quarantined_rows  BIGINT,
    success_percent   DOUBLE PRECISION,     -- % rule đạt
    raw_result        JSONB,                -- payload gốc job gửi lên
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_runs_dataset_time ON dq_runs (dataset, created_at DESC);

-- Kết quả từng rule -> trend, scorecard 6 dimension
CREATE TABLE IF NOT EXISTS dq_rule_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT REFERENCES dq_runs(run_id) ON DELETE CASCADE,
    dataset       TEXT,
    rule_id       TEXT,
    kind          TEXT,                     -- size, complete, pattern, sql_predicate...
    column_name   TEXT,
    dimension     TEXT,                     -- completeness | uniqueness | validity | accuracy | consistency | timeliness
    severity      TEXT,                     -- error | warn
    row_level     BOOLEAN,
    status        TEXT,                     -- passed | failed | error
    metric_name   TEXT,                     -- Completeness, PatternMatch, Size, Mean...
    metric_value  DOUBLE PRECISION,
    failed_rows   BIGINT,                   -- chỉ rule row-level
    message       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_rule_dim ON dq_rule_results (dataset, dimension, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_rule_id  ON dq_rule_results (dataset, rule_id, created_at DESC);

-- Schema v2 (execution/registry bất biến), cạnh các bảng legacy ở trên; không đụng dữ liệu legacy.
\ir migrations/002_execution_v2.sql

-- Schema v3 (console operations: alert settings/inbox/delivery, audit events).
\ir migrations/003_console_operations.sql
