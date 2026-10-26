-- Schema v2: registry bất biến, execution/attempt/report, write outcome, restore. SQL này là nguồn
-- chuẩn của schema; dq-service/schema.py chỉ phản chiếu để truy vấn. Bảng legacy dq_* giữ nguyên.
-- Chạy lại cùng version là no-op; checksum khác version 2 đã áp dụng -> lỗi.
--   psql -U dq -d dq -v ON_ERROR_STOP=1 -f postgres/migrations/002_execution_v2.sql
BEGIN;

CREATE TABLE IF NOT EXISTS schema_versions (
    version    INTEGER     PRIMARY KEY,
    checksum   TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM schema_versions WHERE version = 2 AND checksum <> 'dq-execution-v2/1') THEN
        RAISE EXCEPTION 'schema_versions: checksum của version 2 không khớp (dq-execution-v2/1)';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dq_hash') THEN
        CREATE DOMAIN dq_hash AS TEXT CHECK (VALUE ~ '^sha256:[0-9a-f]{64}$');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id        TEXT        PRIMARY KEY,
    registration      JSONB       NOT NULL,
    registration_hash dq_hash     NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL,
    updated_by        TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS compat_manifests (
    manifest_id  UUID        PRIMARY KEY,
    format       TEXT        NOT NULL CHECK (format = 'dq-compat-v1'),
    content_hash dq_hash     NOT NULL UNIQUE,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    created_by   TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS compat_acceptances (
    dataset_id        TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    manifest_id       UUID        NOT NULL REFERENCES compat_manifests(manifest_id) ON DELETE RESTRICT,
    harness_reference TEXT        NOT NULL CHECK (harness_reference <> ''),
    accepted_by       TEXT        NOT NULL,
    accepted_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataset_id, manifest_id)
);

CREATE TABLE IF NOT EXISTS suites (
    suite_id    UUID        PRIMARY KEY,
    dataset_id  TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    workflow    TEXT        NOT NULL CHECK (workflow IN ('WRITE_GATE', 'AUDIT_TABLE')),
    name        TEXT        NOT NULL CHECK (name <> ''),
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL,
    created_by  TEXT        NOT NULL,
    CONSTRAINT suites_dataset_name_key UNIQUE (dataset_id, name),
    CONSTRAINT suites_identity_key UNIQUE (dataset_id, workflow, suite_id)
);

CREATE TABLE IF NOT EXISTS suite_revisions (
    revision_id  UUID        PRIMARY KEY,
    suite_id     UUID        NOT NULL REFERENCES suites(suite_id) ON DELETE RESTRICT,
    revision_no  INTEGER     NOT NULL CHECK (revision_no > 0),
    content_hash dq_hash     NOT NULL,
    payload      JSONB       NOT NULL,
    contains_sql BOOLEAN     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    created_by   TEXT        NOT NULL,
    CONSTRAINT suite_revisions_no_key   UNIQUE (suite_id, revision_no),
    CONSTRAINT suite_revisions_hash_key UNIQUE (suite_id, content_hash),
    CONSTRAINT suite_revisions_id_key   UNIQUE (suite_id, revision_id)
);

CREATE TABLE IF NOT EXISTS policy_revisions (
    revision_id  UUID        PRIMARY KEY,
    suite_id     UUID        NOT NULL REFERENCES suites(suite_id) ON DELETE RESTRICT,
    revision_no  INTEGER     NOT NULL CHECK (revision_no > 0),
    content_hash dq_hash     NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    created_by   TEXT        NOT NULL,
    CONSTRAINT policy_revisions_no_key   UNIQUE (suite_id, revision_no),
    CONSTRAINT policy_revisions_hash_key UNIQUE (suite_id, content_hash),
    CONSTRAINT policy_revisions_id_key   UNIQUE (suite_id, revision_id)
);

CREATE TABLE IF NOT EXISTS purposes (
    dataset_id         TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    workflow           TEXT        NOT NULL CHECK (workflow IN ('WRITE_GATE', 'AUDIT_TABLE')),
    purpose            TEXT        NOT NULL CHECK (purpose <> ''),
    description        TEXT,
    state              TEXT        NOT NULL CHECK (state IN ('ACTIVE', 'RETIRED')),
    binding_version    BIGINT      NOT NULL CHECK (binding_version >= 0),
    suite_id           UUID,
    suite_revision_id  UUID,
    policy_revision_id UUID,
    updated_at         TIMESTAMPTZ NOT NULL,
    updated_by         TEXT        NOT NULL,
    PRIMARY KEY (dataset_id, workflow, purpose),
    CONSTRAINT purposes_pair_all_or_none CHECK (
        (suite_id IS NULL) = (suite_revision_id IS NULL) AND (suite_id IS NULL) = (policy_revision_id IS NULL)),
    CONSTRAINT purposes_suite_fk FOREIGN KEY (dataset_id, workflow, suite_id)
        REFERENCES suites (dataset_id, workflow, suite_id) ON DELETE RESTRICT,
    CONSTRAINT purposes_suite_revision_fk FOREIGN KEY (suite_id, suite_revision_id)
        REFERENCES suite_revisions (suite_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT purposes_policy_revision_fk FOREIGN KEY (suite_id, policy_revision_id)
        REFERENCES policy_revisions (suite_id, revision_id) ON DELETE RESTRICT
);
-- Mỗi dataset đúng một purpose WRITE_GATE (không retire được)
CREATE UNIQUE INDEX IF NOT EXISTS purposes_one_write_gate ON purposes (dataset_id) WHERE workflow = 'WRITE_GATE';

CREATE TABLE IF NOT EXISTS binding_events (
    event_id           UUID        PRIMARY KEY,
    dataset_id         TEXT        NOT NULL,
    workflow           TEXT        NOT NULL,
    purpose            TEXT        NOT NULL,
    binding_version    BIGINT      NOT NULL CHECK (binding_version >= 0),
    action             TEXT        NOT NULL CHECK (action IN ('REGISTER', 'ACTIVATE', 'RETIRE')),
    suite_id           UUID,
    suite_revision_id  UUID,
    policy_revision_id UUID,
    actor              TEXT        NOT NULL,
    reason             TEXT,
    created_at         TIMESTAMPTZ NOT NULL,
    CONSTRAINT binding_events_purpose_fk FOREIGN KEY (dataset_id, workflow, purpose)
        REFERENCES purposes (dataset_id, workflow, purpose) ON DELETE RESTRICT,
    CONSTRAINT binding_events_pair_all_or_none CHECK (
        (suite_id IS NULL) = (suite_revision_id IS NULL) AND (suite_id IS NULL) = (policy_revision_id IS NULL)),
    CONSTRAINT binding_events_suite_revision_fk FOREIGN KEY (suite_id, suite_revision_id)
        REFERENCES suite_revisions (suite_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT binding_events_policy_revision_fk FOREIGN KEY (suite_id, policy_revision_id)
        REFERENCES policy_revisions (suite_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT binding_events_version_key UNIQUE (dataset_id, workflow, purpose, binding_version)
);

CREATE TABLE IF NOT EXISTS principal_grants (
    principal  TEXT NOT NULL CHECK (principal <> ''),
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    role       TEXT NOT NULL CHECK (role IN ('viewer', 'editor', 'approver', 'operator', 'job')),
    workflow   TEXT CHECK (workflow IN ('WRITE_GATE', 'AUDIT_TABLE')),
    purpose    TEXT,
    CONSTRAINT principal_grants_scope CHECK (
        (role = 'job' AND workflow IS NOT NULL AND purpose IS NOT NULL)
        OR (role <> 'job' AND workflow IS NULL AND purpose IS NULL)),
    CONSTRAINT principal_grants_key UNIQUE NULLS NOT DISTINCT (principal, dataset_id, role, workflow, purpose)
);
CREATE INDEX IF NOT EXISTS principal_grants_dataset_role ON principal_grants (dataset_id, role);

CREATE TABLE IF NOT EXISTS executions (
    execution_id          UUID        PRIMARY KEY,
    dataset_id            TEXT        NOT NULL,
    workflow              TEXT        NOT NULL,
    purpose               TEXT        NOT NULL,
    binding_version       BIGINT      NOT NULL CHECK (binding_version >= 0),
    suite_id              UUID        NOT NULL,
    suite_revision_id     UUID        NOT NULL,
    policy_revision_id    UUID        NOT NULL,
    compat_manifest_id    UUID        NOT NULL,
    plan_hash             dq_hash     NOT NULL,
    plan                  JSONB       NOT NULL,
    state                 TEXT        NOT NULL CHECK (state IN
        ('PLANNED', 'RUNNING', 'REPORTED', 'COMPLETED', 'FAILED', 'CANCELLED')),
    publication_state     TEXT        NOT NULL CHECK (publication_state IN
        ('NOT_APPLICABLE', 'NOT_STARTED', 'IN_PROGRESS', 'SUCCEEDED', 'PARTIAL', 'UNKNOWN', 'FAILED')),
    event_time            TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    active_attempt_id     UUID,
    previous_execution_id UUID,
    actor                 TEXT        NOT NULL,
    error_category        TEXT,
    error                 JSONB,
    resolution            TEXT CHECK (resolution = 'CLOSED_UNRESOLVED'),
    CONSTRAINT executions_workflow_check CHECK (workflow IN ('WRITE_GATE', 'AUDIT_TABLE')),
    CONSTRAINT executions_resolution_failed CHECK (resolution IS NULL OR state = 'FAILED'),
    CONSTRAINT executions_dataset_key UNIQUE (execution_id, dataset_id),
    CONSTRAINT executions_purpose_fk FOREIGN KEY (dataset_id, workflow, purpose)
        REFERENCES purposes (dataset_id, workflow, purpose) ON DELETE RESTRICT,
    CONSTRAINT executions_suite_fk FOREIGN KEY (dataset_id, workflow, suite_id)
        REFERENCES suites (dataset_id, workflow, suite_id) ON DELETE RESTRICT,
    CONSTRAINT executions_suite_revision_fk FOREIGN KEY (suite_id, suite_revision_id)
        REFERENCES suite_revisions (suite_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT executions_policy_revision_fk FOREIGN KEY (suite_id, policy_revision_id)
        REFERENCES policy_revisions (suite_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT executions_manifest_fk FOREIGN KEY (dataset_id, compat_manifest_id)
        REFERENCES compat_acceptances (dataset_id, manifest_id) ON DELETE RESTRICT,
    CONSTRAINT executions_previous_fk FOREIGN KEY (previous_execution_id, dataset_id)
        REFERENCES executions (execution_id, dataset_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS executions_dataset_event ON executions (dataset_id, event_time, execution_id);
CREATE INDEX IF NOT EXISTS executions_state_updated ON executions (state, updated_at);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id      UUID        PRIMARY KEY,
    execution_id    UUID        NOT NULL REFERENCES executions(execution_id) ON DELETE RESTRICT,
    ordinal         INTEGER     NOT NULL CHECK (ordinal > 0),
    state           TEXT        NOT NULL CHECK (state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'ABANDONED')),
    runner_identity TEXT        NOT NULL,
    image_digest    TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    retired_at      TIMESTAMPTZ,
    process_stop    TEXT CHECK (process_stop IN ('CONFIRMED', 'UNCONFIRMED')),
    stop_reference  TEXT,
    stopped_at      TIMESTAMPTZ,
    error_category  TEXT,
    error           JSONB,
    CONSTRAINT attempts_ordinal_key UNIQUE (execution_id, ordinal),
    CONSTRAINT attempts_identity_key UNIQUE (execution_id, attempt_id),
    CONSTRAINT attempts_running_not_stopped CHECK (state <> 'RUNNING' OR process_stop IS NULL),
    CONSTRAINT attempts_failed_has_stop CHECK (state NOT IN ('FAILED', 'ABANDONED') OR process_stop IS NOT NULL),
    CONSTRAINT attempts_confirmed_evidence CHECK (
        (process_stop = 'CONFIRMED') = (stop_reference IS NOT NULL AND stopped_at IS NOT NULL)
        AND (process_stop = 'CONFIRMED' OR (stop_reference IS NULL AND stopped_at IS NULL)))
);
CREATE UNIQUE INDEX IF NOT EXISTS attempts_one_running ON attempts (execution_id) WHERE state = 'RUNNING';
CREATE INDEX IF NOT EXISTS attempts_unconfirmed_stop ON attempts (execution_id)
    WHERE process_stop IS DISTINCT FROM 'CONFIRMED';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'executions_active_attempt_fk') THEN
        ALTER TABLE executions ADD CONSTRAINT executions_active_attempt_fk
            FOREIGN KEY (execution_id, active_attempt_id) REFERENCES attempts (execution_id, attempt_id)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS accepted_reports (
    report_id              UUID        PRIMARY KEY,
    execution_id           UUID        NOT NULL UNIQUE REFERENCES executions(execution_id) ON DELETE RESTRICT,
    attempt_id             UUID        NOT NULL,
    report_hash            dq_hash     NOT NULL,
    evaluation_fingerprint TEXT        NOT NULL CHECK (evaluation_fingerprint <> ''),
    payload                JSONB       NOT NULL,
    assessment             TEXT CHECK (assessment IN ('PASS', 'VIOLATION', 'ERROR')),
    accepted_at            TIMESTAMPTZ NOT NULL,
    accepted_response      JSONB       NOT NULL,
    CONSTRAINT accepted_reports_attempt_fk FOREIGN KEY (execution_id, attempt_id)
        REFERENCES attempts (execution_id, attempt_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS rule_results (
    execution_id                UUID    NOT NULL REFERENCES accepted_reports(execution_id) ON DELETE RESTRICT,
    rule_id                     TEXT    NOT NULL,
    metric_definition_hash      dq_hash NOT NULL,
    measurement_definition_hash dq_hash NOT NULL,
    scope                       TEXT    NOT NULL,
    stage                       TEXT    NOT NULL,
    measurement_status          TEXT    NOT NULL CHECK (measurement_status IN ('OK', 'ERROR')),
    metric_name                 TEXT,
    metric_decimal              NUMERIC,
    schema_evidence             JSONB,
    error_code                  TEXT,
    error_params                JSONB,
    failed_rows                 BIGINT CHECK (failed_rows >= 0),
    payload                     JSONB   NOT NULL,
    PRIMARY KEY (execution_id, rule_id),
    CONSTRAINT rule_results_shape CHECK (
        (measurement_status = 'OK' AND error_code IS NULL AND error_params IS NULL AND (
            (metric_name IS NOT NULL AND metric_decimal IS NOT NULL AND schema_evidence IS NULL)
            OR (metric_name IS NULL AND metric_decimal IS NULL AND schema_evidence IS NOT NULL)))
        OR (measurement_status = 'ERROR' AND error_code IS NOT NULL AND error_params IS NOT NULL
            AND metric_name IS NULL AND metric_decimal IS NULL AND schema_evidence IS NULL))
);
CREATE INDEX IF NOT EXISTS rule_results_metric_hash ON rule_results (metric_definition_hash, scope, stage);
CREATE INDEX IF NOT EXISTS rule_results_measurement_hash ON rule_results (measurement_definition_hash, scope, stage);

CREATE TABLE IF NOT EXISTS metric_observations (
    observation_id         UUID        PRIMARY KEY,
    execution_id           UUID        NOT NULL REFERENCES accepted_reports(execution_id) ON DELETE RESTRICT,
    metric_definition_hash dq_hash     NOT NULL,
    scope                  TEXT        NOT NULL,
    stage                  TEXT        NOT NULL,
    history_identity_hash  dq_hash     NOT NULL,
    event_time             TIMESTAMPTZ NOT NULL,
    value                  NUMERIC     NOT NULL,
    payload                JSONB       NOT NULL,
    CONSTRAINT metric_observations_identity_key UNIQUE (execution_id, metric_definition_hash, scope, stage)
);
CREATE INDEX IF NOT EXISTS metric_observations_history
    ON metric_observations (history_identity_hash, event_time, execution_id);

CREATE TABLE IF NOT EXISTS anomaly_observations (
    anomaly_id             UUID    PRIMARY KEY,
    execution_id           UUID    NOT NULL REFERENCES accepted_reports(execution_id) ON DELETE RESTRICT,
    metric_definition_hash dq_hash NOT NULL,
    scope                  TEXT    NOT NULL,
    stage                  TEXT    NOT NULL,
    status                 TEXT    NOT NULL CHECK (status IN ('NORMAL', 'ANOMALY', 'INSUFFICIENT_HISTORY', 'ERROR')),
    baseline_hash          dq_hash NOT NULL,
    detector_version       TEXT    NOT NULL,
    payload                JSONB   NOT NULL,
    CONSTRAINT anomaly_observations_identity_key UNIQUE (execution_id, metric_definition_hash, scope, stage)
);
CREATE INDEX IF NOT EXISTS anomaly_observations_status ON anomaly_observations (status, execution_id);

CREATE TABLE IF NOT EXISTS metric_eligibility_events (
    event_id       UUID        PRIMARY KEY,
    observation_id UUID        NOT NULL REFERENCES metric_observations(observation_id) ON DELETE RESTRICT,
    eligible       BOOLEAN     NOT NULL,
    reason         TEXT        NOT NULL,
    actor          TEXT        NOT NULL,
    effective_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS metric_eligibility_events_obs ON metric_eligibility_events (observation_id, effective_at, event_id);

CREATE TABLE IF NOT EXISTS gate_decisions (
    decision_id            UUID        PRIMARY KEY,
    execution_id           UUID        NOT NULL UNIQUE REFERENCES accepted_reports(execution_id) ON DELETE RESTRICT,
    decision               TEXT        NOT NULL CHECK (decision IN ('ALLOW', 'DENY')),
    plan_hash              dq_hash     NOT NULL,
    report_hash            dq_hash     NOT NULL,
    evaluation_fingerprint TEXT        NOT NULL CHECK (evaluation_fingerprint <> ''),
    payload                JSONB       NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS write_outcomes (
    execution_id    UUID        NOT NULL REFERENCES executions(execution_id) ON DELETE RESTRICT,
    destination     TEXT        NOT NULL CHECK (destination IN ('silver', 'quarantine')),
    state           TEXT        NOT NULL CHECK (state IN
        ('NOT_APPLICABLE', 'NOT_STARTED', 'IN_PROGRESS', 'SUCCEEDED', 'FAILED', 'UNKNOWN')),
    outcome_version BIGINT      NOT NULL CHECK (outcome_version >= 0),
    table_id        TEXT,
    commit_version  BIGINT CHECK (commit_version >= 0),
    metadata_hash   dq_hash,
    commit_evidence TEXT CHECK (commit_evidence IN ('EXACT', 'TXN_ONLY')),
    txn_app_id      TEXT,
    txn_version     BIGINT CHECK (txn_version >= 0),
    payload         JSONB       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (execution_id, destination),
    CONSTRAINT write_outcomes_success_evidence CHECK ((state = 'SUCCEEDED') = (commit_evidence IS NOT NULL)),
    CONSTRAINT write_outcomes_exact CHECK (commit_evidence IS DISTINCT FROM 'EXACT'
        OR (table_id IS NOT NULL AND commit_version IS NOT NULL AND metadata_hash IS NOT NULL)),
    CONSTRAINT write_outcomes_txn_only CHECK (commit_evidence IS DISTINCT FROM 'TXN_ONLY'
        OR (table_id IS NOT NULL AND commit_version IS NULL AND metadata_hash IS NULL
            AND txn_app_id IS NOT NULL AND txn_version IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS write_outcomes_state ON write_outcomes (state, updated_at);

CREATE TABLE IF NOT EXISTS partition_publications (
    dataset_id                TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    partition_key             TEXT        NOT NULL,
    partition_values          JSONB       NOT NULL,
    execution_id              UUID        NOT NULL,
    silver_table_id           TEXT        NOT NULL,
    silver_commit_version     BIGINT      NOT NULL CHECK (silver_commit_version >= 0),
    quarantine_commit_version BIGINT CHECK (quarantine_commit_version >= 0),
    quarantine_rows           BIGINT      NOT NULL CHECK (quarantine_rows >= 0),
    published_at              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataset_id, partition_key),
    CONSTRAINT partition_publications_execution_fk FOREIGN KEY (execution_id, dataset_id)
        REFERENCES executions (execution_id, dataset_id) ON DELETE RESTRICT,
    CONSTRAINT partition_publications_quarantine CHECK (quarantine_commit_version IS NOT NULL OR quarantine_rows = 0)
);
CREATE INDEX IF NOT EXISTS partition_publications_execution ON partition_publications (execution_id);

CREATE TABLE IF NOT EXISTS restore_markers (
    restore_id UUID        PRIMARY KEY,
    dataset_id TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    opened_at  TIMESTAMPTZ NOT NULL,
    opened_by  TEXT        NOT NULL,
    cleared_at TIMESTAMPTZ,
    cleared_by TEXT,
    completion JSONB,
    CONSTRAINT restore_markers_cleared CHECK ((cleared_at IS NULL) = (cleared_by IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS restore_markers_one_open ON restore_markers (dataset_id) WHERE cleared_at IS NULL;

CREATE TABLE IF NOT EXISTS partition_unresolved (
    marker_id        UUID        PRIMARY KEY,
    dataset_id       TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    scope            TEXT        NOT NULL CHECK (scope IN ('PARTITION', 'DATASET')),
    partition_key    TEXT,
    partition_values JSONB,
    execution_id     UUID,
    txn_app_id       TEXT,
    destination      TEXT CHECK (destination IN ('silver', 'quarantine')),
    table_id         TEXT,
    reason           TEXT        NOT NULL CHECK (reason IN ('CLOSED_UNRESOLVED', 'TXN_ONLY', 'RESTORE_ORPHAN')),
    restore_id       UUID        REFERENCES restore_markers(restore_id) ON DELETE RESTRICT,
    recorded_at      TIMESTAMPTZ NOT NULL,
    cleared_at       TIMESTAMPTZ,
    cleared_by       TEXT,
    CONSTRAINT partition_unresolved_execution_fk FOREIGN KEY (execution_id, dataset_id)
        REFERENCES executions (execution_id, dataset_id) ON DELETE RESTRICT,
    CONSTRAINT partition_unresolved_scope_shape CHECK (
        (scope = 'PARTITION') = (partition_key IS NOT NULL AND partition_values IS NOT NULL)
        AND (scope = 'PARTITION' OR (partition_key IS NULL AND partition_values IS NULL))),
    CONSTRAINT partition_unresolved_reason_shape CHECK (
        (reason = 'RESTORE_ORPHAN' AND execution_id IS NULL AND txn_app_id IS NOT NULL AND restore_id IS NOT NULL)
        OR (reason <> 'RESTORE_ORPHAN' AND execution_id IS NOT NULL)),
    CONSTRAINT partition_unresolved_cleared CHECK ((cleared_at IS NULL) = (cleared_by IS NULL)),
    CONSTRAINT partition_unresolved_key UNIQUE NULLS NOT DISTINCT
        (dataset_id, partition_key, execution_id, txn_app_id, destination, restore_id)
);
CREATE INDEX IF NOT EXISTS partition_unresolved_open ON partition_unresolved (dataset_id, partition_key)
    WHERE cleared_at IS NULL;

CREATE TABLE IF NOT EXISTS idempotency_records (
    principal       TEXT        NOT NULL,
    route           TEXT        NOT NULL,
    dataset_id      TEXT        REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    key             TEXT        NOT NULL CHECK (key <> ''),
    request_hash    dq_hash     NOT NULL,
    resource_id     UUID        NOT NULL,
    response_status INTEGER     NOT NULL,
    response        JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    CONSTRAINT idempotency_records_key UNIQUE NULLS NOT DISTINCT (principal, route, dataset_id, key)
);

CREATE TABLE IF NOT EXISTS restore_findings (
    restore_id   UUID        NOT NULL REFERENCES restore_markers(restore_id) ON DELETE RESTRICT,
    scan_id      UUID        NOT NULL,
    page         INTEGER     NOT NULL CHECK (page >= 0),
    request_hash dq_hash     NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    actor        TEXT        NOT NULL,
    PRIMARY KEY (restore_id, scan_id, page)
);

CREATE TABLE IF NOT EXISTS restore_scans (
    restore_id       UUID    NOT NULL REFERENCES restore_markers(restore_id) ON DELETE RESTRICT,
    scan_id          UUID    NOT NULL,
    destination      TEXT    NOT NULL CHECK (destination IN ('silver', 'quarantine')),
    table_id         TEXT    NOT NULL,
    snapshot_version BIGINT  NOT NULL CHECK (snapshot_version >= 0),
    complete         BOOLEAN NOT NULL,
    blocked_code     TEXT,
    PRIMARY KEY (restore_id, scan_id, destination)
);

CREATE TABLE IF NOT EXISTS cleanup_records (
    cleanup_id     UUID        PRIMARY KEY,
    dataset_id     TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    execution_id   UUID        NOT NULL,
    attempt_id     UUID        NOT NULL,
    uri            TEXT        NOT NULL,
    removed_at     TIMESTAMPTZ NOT NULL,
    actor          TEXT        NOT NULL,
    stop_reference TEXT        NOT NULL,
    CONSTRAINT cleanup_records_attempt_fk FOREIGN KEY (execution_id, attempt_id)
        REFERENCES attempts (execution_id, attempt_id) ON DELETE RESTRICT,
    CONSTRAINT cleanup_records_execution_fk FOREIGN KEY (execution_id, dataset_id)
        REFERENCES executions (execution_id, dataset_id) ON DELETE RESTRICT,
    CONSTRAINT cleanup_records_attempt_uri_key UNIQUE (attempt_id, uri)
);

-- Bằng chứng bất bội: không UPDATE/DELETE (kể cả owner); chỉ INSERT.
CREATE OR REPLACE FUNCTION dq_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% là bảng append-only: % bị từ chối', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END $$;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['schema_versions', 'compat_manifests', 'compat_acceptances', 'suite_revisions',
        'policy_revisions', 'binding_events', 'accepted_reports', 'rule_results', 'metric_observations',
        'anomaly_observations', 'metric_eligibility_events', 'gate_decisions', 'restore_findings',
        'cleanup_records']
    LOOP
        EXECUTE format('CREATE OR REPLACE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
                       'FOR EACH ROW EXECUTE FUNCTION dq_reject_mutation()', t || '_append_only', t);
    END LOOP;
END $$;

INSERT INTO schema_versions (version, checksum) VALUES (2, 'dq-execution-v2/1') ON CONFLICT (version) DO NOTHING;

COMMIT;
