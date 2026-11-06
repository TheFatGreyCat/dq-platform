"""SQLAlchemy Core mirror của postgres/migrations/002_execution_v2.sql + 003_console_operations.sql (SQL là nguồn chuẩn).

Chỉ dùng để dựng truy vấn; KHÔNG gọi metadata.create_all. test_schema_constraints so khớp tên cột với DB thật.
"""
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, MetaData, Numeric, Table, Text
from sqlalchemy.dialects.postgresql import JSONB as _JSONB, UUID

# Python None -> SQL NULL (default JSONB writes JSON 'null', breaking IS NULL shape CHECKs)
JSONB = _JSONB(none_as_null=True)

metadata = MetaData()

_T = lambda: DateTime(timezone=True)  # noqa: E731
_U = lambda: UUID(as_uuid=True)  # noqa: E731


def _table(name: str, *cols: Column) -> Table:
    return Table(name, metadata, *cols)


schema_versions = _table("schema_versions", Column("version", Integer, primary_key=True),
                         Column("checksum", Text), Column("applied_at", _T()))
datasets = _table("datasets", Column("dataset_id", Text, primary_key=True), Column("registration", JSONB),
                  Column("registration_hash", Text), Column("updated_at", _T()), Column("updated_by", Text))
compat_manifests = _table("compat_manifests", Column("manifest_id", _U(), primary_key=True), Column("format", Text),
                          Column("content_hash", Text), Column("payload", JSONB), Column("created_at", _T()),
                          Column("created_by", Text))
compat_acceptances = _table("compat_acceptances", Column("dataset_id", Text, primary_key=True),
                            Column("manifest_id", _U(), primary_key=True), Column("harness_reference", Text),
                            Column("accepted_by", Text), Column("accepted_at", _T()))
suites = _table("suites", Column("suite_id", _U(), primary_key=True), Column("dataset_id", Text),
                Column("workflow", Text), Column("name", Text), Column("description", Text),
                Column("created_at", _T()), Column("created_by", Text))
suite_revisions = _table("suite_revisions", Column("revision_id", _U(), primary_key=True), Column("suite_id", _U()),
                         Column("revision_no", Integer), Column("content_hash", Text), Column("payload", JSONB),
                         Column("contains_sql", Boolean), Column("created_at", _T()), Column("created_by", Text))
policy_revisions = _table("policy_revisions", Column("revision_id", _U(), primary_key=True),
                          Column("suite_id", _U()), Column("revision_no", Integer), Column("content_hash", Text),
                          Column("payload", JSONB), Column("created_at", _T()), Column("created_by", Text))
purposes = _table("purposes", Column("dataset_id", Text, primary_key=True), Column("workflow", Text, primary_key=True),
                  Column("purpose", Text, primary_key=True), Column("description", Text), Column("state", Text),
                  Column("binding_version", BigInteger), Column("suite_id", _U()),
                  Column("suite_revision_id", _U()), Column("policy_revision_id", _U()),
                  Column("updated_at", _T()), Column("updated_by", Text))
binding_events = _table("binding_events", Column("event_id", _U(), primary_key=True), Column("dataset_id", Text),
                        Column("workflow", Text), Column("purpose", Text), Column("binding_version", BigInteger),
                        Column("action", Text), Column("suite_id", _U()), Column("suite_revision_id", _U()),
                        Column("policy_revision_id", _U()), Column("actor", Text), Column("reason", Text),
                        Column("created_at", _T()))
principal_grants = _table("principal_grants", Column("principal", Text), Column("dataset_id", Text),
                          Column("role", Text), Column("workflow", Text), Column("purpose", Text))
executions = _table("executions", Column("execution_id", _U(), primary_key=True), Column("dataset_id", Text),
                    Column("workflow", Text), Column("purpose", Text), Column("binding_version", BigInteger),
                    Column("suite_id", _U()), Column("suite_revision_id", _U()), Column("policy_revision_id", _U()),
                    Column("compat_manifest_id", _U()), Column("plan_hash", Text), Column("plan", JSONB),
                    Column("state", Text), Column("publication_state", Text), Column("event_time", _T()),
                    Column("created_at", _T()), Column("updated_at", _T()), Column("active_attempt_id", _U()),
                    Column("previous_execution_id", _U()), Column("actor", Text), Column("error_category", Text),
                    Column("error", JSONB), Column("resolution", Text))
attempts = _table("attempts", Column("attempt_id", _U(), primary_key=True), Column("execution_id", _U()),
                  Column("ordinal", Integer), Column("state", Text), Column("runner_identity", Text),
                  Column("image_digest", Text), Column("started_at", _T()), Column("retired_at", _T()),
                  Column("process_stop", Text), Column("stop_reference", Text), Column("stopped_at", _T()),
                  Column("error_category", Text), Column("error", JSONB))
accepted_reports = _table("accepted_reports", Column("report_id", _U(), primary_key=True),
                          Column("execution_id", _U()), Column("attempt_id", _U()), Column("report_hash", Text),
                          Column("evaluation_fingerprint", Text), Column("payload", JSONB),
                          Column("assessment", Text), Column("accepted_at", _T()),
                          Column("accepted_response", JSONB))
rule_results = _table("rule_results", Column("execution_id", _U(), primary_key=True),
                      Column("rule_id", Text, primary_key=True), Column("metric_definition_hash", Text),
                      Column("measurement_definition_hash", Text), Column("scope", Text), Column("stage", Text),
                      Column("measurement_status", Text), Column("metric_name", Text),
                      Column("metric_decimal", Numeric), Column("schema_evidence", JSONB),
                      Column("error_code", Text), Column("error_params", JSONB), Column("failed_rows", BigInteger),
                      Column("payload", JSONB))
metric_observations = _table("metric_observations", Column("observation_id", _U(), primary_key=True),
                             Column("execution_id", _U()), Column("metric_definition_hash", Text),
                             Column("scope", Text), Column("stage", Text), Column("history_identity_hash", Text),
                             Column("event_time", _T()), Column("value", Numeric), Column("payload", JSONB))
anomaly_observations = _table("anomaly_observations", Column("anomaly_id", _U(), primary_key=True),
                              Column("execution_id", _U()), Column("metric_definition_hash", Text),
                              Column("scope", Text), Column("stage", Text), Column("status", Text),
                              Column("baseline_hash", Text), Column("detector_version", Text),
                              Column("payload", JSONB))
metric_eligibility_events = _table("metric_eligibility_events", Column("event_id", _U(), primary_key=True),
                                   Column("observation_id", _U()), Column("eligible", Boolean),
                                   Column("reason", Text), Column("actor", Text), Column("effective_at", _T()))
gate_decisions = _table("gate_decisions", Column("decision_id", _U(), primary_key=True),
                        Column("execution_id", _U()), Column("decision", Text), Column("plan_hash", Text),
                        Column("report_hash", Text), Column("evaluation_fingerprint", Text),
                        Column("payload", JSONB), Column("created_at", _T()))
write_outcomes = _table("write_outcomes", Column("execution_id", _U(), primary_key=True),
                        Column("destination", Text, primary_key=True), Column("state", Text),
                        Column("outcome_version", BigInteger), Column("table_id", Text),
                        Column("commit_version", BigInteger), Column("metadata_hash", Text),
                        Column("commit_evidence", Text), Column("txn_app_id", Text),
                        Column("txn_version", BigInteger), Column("payload", JSONB), Column("updated_at", _T()))
partition_publications = _table("partition_publications", Column("dataset_id", Text, primary_key=True),
                                Column("partition_key", Text, primary_key=True), Column("partition_values", JSONB),
                                Column("execution_id", _U()), Column("silver_table_id", Text),
                                Column("silver_commit_version", BigInteger),
                                Column("quarantine_commit_version", BigInteger),
                                Column("quarantine_rows", BigInteger), Column("published_at", _T()))
restore_markers = _table("restore_markers", Column("restore_id", _U(), primary_key=True), Column("dataset_id", Text),
                         Column("opened_at", _T()), Column("opened_by", Text), Column("cleared_at", _T()),
                         Column("cleared_by", Text), Column("completion", JSONB))
partition_unresolved = _table("partition_unresolved", Column("marker_id", _U(), primary_key=True),
                              Column("dataset_id", Text), Column("scope", Text), Column("partition_key", Text),
                              Column("partition_values", JSONB), Column("execution_id", _U()),
                              Column("txn_app_id", Text), Column("destination", Text), Column("table_id", Text),
                              Column("reason", Text), Column("restore_id", _U()), Column("recorded_at", _T()),
                              Column("cleared_at", _T()), Column("cleared_by", Text))
idempotency_records = _table("idempotency_records", Column("principal", Text, primary_key=True),
                             Column("route", Text, primary_key=True), Column("dataset_id", Text, primary_key=True,
                                                                           nullable=True),
                             Column("key", Text, primary_key=True), Column("request_hash", Text),
                             Column("resource_id", _U()), Column("response_status", Integer),
                             Column("response", JSONB), Column("created_at", _T()))
restore_findings = _table("restore_findings", Column("restore_id", _U(), primary_key=True),
                          Column("scan_id", _U(), primary_key=True), Column("page", Integer, primary_key=True),
                          Column("request_hash", Text), Column("payload", JSONB), Column("created_at", _T()),
                          Column("actor", Text))
restore_scans = _table("restore_scans", Column("restore_id", _U(), primary_key=True),
                       Column("scan_id", _U(), primary_key=True), Column("destination", Text, primary_key=True),
                       Column("table_id", Text), Column("snapshot_version", BigInteger),
                       Column("complete", Boolean), Column("blocked_code", Text))
cleanup_records = _table("cleanup_records", Column("cleanup_id", _U(), primary_key=True), Column("dataset_id", Text),
                         Column("execution_id", _U()), Column("attempt_id", _U()), Column("uri", Text),
                         Column("removed_at", _T()), Column("actor", Text), Column("stop_reference", Text))

console_alert_settings = _table("console_alert_settings", Column("dataset_id", Text, primary_key=True),
                                Column("version", BigInteger), Column("enabled", Boolean),
                                Column("quarantine_ratio_threshold", Numeric),
                                Column("no_report_after_seconds", BigInteger),
                                Column("violation_rate_threshold", Numeric),
                                Column("violation_rate_min_reports", BigInteger),
                                Column("window_seconds", BigInteger), Column("updated_at", _T()),
                                Column("updated_by", Text))
console_alerts = _table("console_alerts", Column("alert_id", _U(), primary_key=True), Column("dataset_id", Text),
                        Column("alert_key", Text), Column("kind", Text), Column("execution_id", _U()),
                        Column("rule_id", Text), Column("window_start", _T()), Column("window_end", _T()),
                        Column("owner_snapshot", Text), Column("details", JSONB), Column("created_at", _T()),
                        Column("acknowledged_at", _T()), Column("acknowledged_by", Text),
                        Column("acknowledgement_note", Text), Column("version", BigInteger))
console_alert_deliveries = _table("console_alert_deliveries", Column("alert_id", _U(), primary_key=True),
                                  Column("state", Text), Column("attempts", Integer),
                                  Column("next_attempt_at", _T()), Column("delivered_at", _T()),
                                  Column("last_error_code", Text), Column("lease_until", _T()))
console_audit_events = _table("console_audit_events", Column("event_id", _U(), primary_key=True),
                              Column("dataset_id", Text), Column("actor", Text), Column("action", Text),
                              Column("resource_type", Text), Column("resource_id", Text),
                              Column("occurred_at", _T()), Column("details", JSONB))
