-- Schema v3: console operations (alert settings, alert inbox, webhook delivery, audit events).
-- dq-service/schema.py chỉ phản chiếu SQL này. Bảng v1/v2 và dữ liệu của chúng giữ nguyên.
-- Chạy lại cùng version là no-op; checksum khác version 3 đã áp dụng -> lỗi.
--   psql -U dq -d dq -v ON_ERROR_STOP=1 -f postgres/migrations/003_console_operations.sql
BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_versions WHERE version = 2) THEN
        RAISE EXCEPTION 'schema_versions: cần áp dụng 002_execution_v2.sql trước version 3';
    END IF;
    IF EXISTS (SELECT 1 FROM schema_versions WHERE version = 3 AND checksum <> 'dq-console-operations-v1') THEN
        RAISE EXCEPTION 'schema_versions: checksum của version 3 không khớp (dq-console-operations-v1)';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS console_alert_settings (
    dataset_id                 TEXT        PRIMARY KEY REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    version                    BIGINT      NOT NULL CHECK (version >= 1),
    enabled                    BOOLEAN     NOT NULL,
    quarantine_ratio_threshold NUMERIC     CHECK (quarantine_ratio_threshold BETWEEN 0 AND 1),
    no_report_after_seconds    BIGINT      CHECK (no_report_after_seconds BETWEEN 60 AND 604800),
    violation_rate_threshold   NUMERIC     CHECK (violation_rate_threshold BETWEEN 0 AND 1),
    violation_rate_min_reports BIGINT      NOT NULL CHECK (violation_rate_min_reports >= 1),
    window_seconds             BIGINT      NOT NULL CHECK (window_seconds BETWEEN 300 AND 604800),
    updated_at                 TIMESTAMPTZ NOT NULL,
    updated_by                 TEXT        NOT NULL
);

-- Alert là sự kiện bất biến, trừ lần acknowledge đầu tiên; không tự resolve/xóa.
CREATE TABLE IF NOT EXISTS console_alerts (
    alert_id             UUID        PRIMARY KEY,
    dataset_id           TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    alert_key            TEXT        NOT NULL UNIQUE,
    kind                 TEXT        NOT NULL
        CHECK (kind IN ('REPORT_ATTENTION', 'NO_REPORT', 'QUARANTINE_RATIO', 'VIOLATION_RATE')),
    execution_id         UUID        REFERENCES executions(execution_id) ON DELETE RESTRICT,
    rule_id              TEXT,
    window_start         TIMESTAMPTZ,
    window_end           TIMESTAMPTZ,
    owner_snapshot       TEXT        NOT NULL,
    details              JSONB       NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    acknowledged_at      TIMESTAMPTZ,
    acknowledged_by      TEXT,
    acknowledgement_note TEXT        CHECK (char_length(acknowledgement_note) <= 2048),
    version              BIGINT      NOT NULL CHECK (version >= 1),
    CONSTRAINT console_alerts_ack_shape CHECK (
        (acknowledged_at IS NULL AND acknowledged_by IS NULL AND acknowledgement_note IS NULL)
        OR (acknowledged_at IS NOT NULL AND acknowledged_by IS NOT NULL)),
    CONSTRAINT console_alerts_window_order CHECK (
        window_start IS NULL OR window_end IS NULL OR window_start <= window_end)
);
CREATE INDEX IF NOT EXISTS console_alerts_dataset_created ON console_alerts (dataset_id, created_at, alert_id);
CREATE INDEX IF NOT EXISTS console_alerts_unacked ON console_alerts (dataset_id, created_at, alert_id)
    WHERE acknowledged_at IS NULL;

-- Không lưu webhook URL hay response body.
CREATE TABLE IF NOT EXISTS console_alert_deliveries (
    alert_id        UUID        PRIMARY KEY REFERENCES console_alerts(alert_id) ON DELETE RESTRICT,
    state           TEXT        NOT NULL CHECK (state IN ('PENDING', 'DELIVERED', 'FAILED', 'DISABLED')),
    attempts        INTEGER     NOT NULL CHECK (attempts BETWEEN 0 AND 5),
    next_attempt_at TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    last_error_code TEXT        CHECK (last_error_code IN ('WEBHOOK_HTTP_ERROR', 'WEBHOOK_NETWORK_ERROR')),
    lease_until     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS console_alert_deliveries_due ON console_alert_deliveries (next_attempt_at)
    WHERE state = 'PENDING';

CREATE TABLE IF NOT EXISTS console_audit_events (
    event_id      UUID        PRIMARY KEY,
    dataset_id    TEXT        NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    actor         TEXT        NOT NULL,
    action        TEXT        NOT NULL,
    resource_type TEXT        NOT NULL,
    resource_id   TEXT        NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    details       JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS console_audit_events_dataset_time
    ON console_audit_events (dataset_id, occurred_at, event_id);

-- Audit events chỉ INSERT (dq_reject_mutation từ migration 002).
CREATE OR REPLACE TRIGGER console_audit_events_append_only BEFORE UPDATE OR DELETE ON console_audit_events
    FOR EACH ROW EXECUTE FUNCTION dq_reject_mutation();

INSERT INTO schema_versions (version, checksum) VALUES (3, 'dq-console-operations-v1') ON CONFLICT (version) DO NOTHING;

COMMIT;
