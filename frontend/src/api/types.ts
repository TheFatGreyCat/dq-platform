export type Page<T> = { items: T[]; next_cursor: string | null };
export type Grant = { dataset_id: string; role: 'viewer' | 'editor' | 'approver' | 'operator' | 'job'; workflow: string | null; purpose: string | null };
export type Identity = { principal: string; grants: Grant[] };
export type Dataset = { dataset_id: string; owner: string; history_context: string; registration_hash: string };
export type Workflow = 'WRITE_GATE' | 'AUDIT_TABLE';
export type ExecutionState = 'PLANNED' | 'RUNNING' | 'REPORTED' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'STARTED';
export type Execution = { execution_id: string; dataset_id: string; workflow: Workflow; purpose: string; state: ExecutionState; publication_state: string; unresolved: boolean; plan: { event_time?: string | null; plan_hash?: string; [key: string]: unknown }; attempts: Record<string, unknown>[]; outcomes: { silver?: Record<string, unknown> | null; quarantine?: Record<string, unknown> | null } };
export type ReportResponse = { accepted: { decision: { decision: 'ALLOW' | 'DENY'; reasons: unknown[] } | null; assessment: 'PASS' | 'VIOLATION' | 'ERROR' | null }; report: { candidate_rows?: number | null; valid_rows?: number | null; quarantine_rows?: number | null; measured_rows?: number | null; results?: unknown[]; [key: string]: unknown }; links: Record<string, string> };
export type ReportResult = { pending: true; payload: { execution_id: string; state: ExecutionState } } | { pending: false; payload: ReportResponse };
export type Revision = { revision_id: string; suite_id: string; revision_no: number; content_hash: string; payload: Record<string, unknown>; created_by: string; created_at: string; contains_sql?: boolean };

export type Dimension = 'completeness' | 'uniqueness' | 'validity' | 'accuracy' | 'consistency' | 'timeliness';
export type Scope = 'PARTITION' | 'TABLE';
export type Stage = 'RAW' | 'CANDIDATE' | 'CLEAN' | 'AUDIT_SNAPSHOT';
export type Action = 'BLOCK' | 'QUARANTINE' | 'OBSERVE';

export type ParamRef = { param: string };
export type Bound = string | number | ParamRef;
export type AssertionOp = '>=' | '>' | '<=' | '<' | '==' | 'between';

export type Assertion = {
  op: AssertionOp;
  value?: Bound | null;
  min?: Bound | null;
  max?: Bound | null;
};

export type SizeCheck = { kind: 'size'; assert: Assertion };
export type CompleteCheck = { kind: 'complete'; column: string; assert?: Assertion };
export type CompletenessCheck = { kind: 'completeness'; column: string; assert: Assertion };
export type UniqueCheck = { kind: 'unique'; columns: string[]; assert?: Assertion | null };
export type PatternCheck = { kind: 'pattern'; column: string; regex: string; assert?: Assertion | null };
export type RangeCheck = {
  kind: 'range';
  column: string;
  min: Bound;
  max: Bound;
  include_min?: boolean;
  include_max?: boolean;
  assert?: Assertion;
};
export type InSetCheck = { kind: 'in_set'; column: string; values: string[]; assert?: Assertion };
export type MeanCheck = { kind: 'mean'; column: string; assert: Assertion };
export type SqlPredicateCheck = { kind: 'sql_predicate'; predicate: string; columns: string[]; assert?: Assertion | null };
export type SqlMetricCheck = { kind: 'sql_metric'; sql: string; assert: Assertion };
export type SchemaColumnsCheck = { kind: 'schema_columns'; columns: string[]; exact?: boolean };
export type SchemaTypeCheck = {
  kind: 'schema_type';
  column: string;
  type: 'string' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'decimal' | 'date' | 'timestamp';
};

export type Check =
  | SizeCheck
  | CompleteCheck
  | CompletenessCheck
  | UniqueCheck
  | PatternCheck
  | RangeCheck
  | InSetCheck
  | MeanCheck
  | SqlPredicateCheck
  | SqlMetricCheck
  | SchemaColumnsCheck
  | SchemaTypeCheck;

export type Rule = {
  rule_id: string;
  dimension: Dimension;
  scope: Scope;
  stage: Stage;
  description?: string | null;
  check: Check;
};

export type Suite = {
  format: 'dq-rules/v3';
  workflow?: Workflow | null;
  description?: string | null;
  parameters?: Record<string, string>;
  rules: Rule[];
};

export type PolicyRule = {
  rule_id: string;
  action: Action;
  required: boolean;
};

export type AnomalyConfig =
  | { enabled: false }
  | {
      enabled: true;
      max_points: number;
      lower: string;
      upper: string;
      bootstrap: boolean;
    };

export type Policy = {
  rules: PolicyRule[];
  min_candidate_rows?: number;
  min_clean_rows?: number;
  max_quarantine_ratio: string;
  anomaly: AnomalyConfig;
};

export type RevisionValidationRequest = {
  suite: Suite;
  policy?: Policy | null;
};

export type RevisionValidationResponse = {
  valid: boolean;
  contains_sql: boolean;
};

export type AnalyticsWorkflow = {
  executions: { total: string; states: Record<string, string>; failed_ratio: string | null };
  decisions?: { ALLOW: string; DENY: string; deny_ratio: string | null };
  rules?: { total: string; OK: string; ERROR: string; error_ratio: string | null; failed_rows: string };
};
export type DatasetAnalytics = {
  dataset_id: string;
  window: { start: string; end: string };
  workflows: Partial<Record<Workflow, AnalyticsWorkflow>>;
};
export type RuleHistory = {
  truncated: boolean;
  points: { observation_id: string; execution_id: string; event_time: string; value: string }[];
};
