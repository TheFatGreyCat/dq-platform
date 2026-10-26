export type Page<T> = { items: T[]; next_cursor: string | null };
export type Grant = { dataset_id: string; role: 'viewer' | 'editor' | 'approver' | 'operator' | 'job'; workflow: string | null; purpose: string | null };
export type Identity = { principal: string; grants: Grant[] };
export type Dataset = { dataset_id: string; owner: string; history_context: string; registration_hash: string };
export type ExecutionState = 'PLANNED' | 'RUNNING' | 'REPORTED' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
export type Execution = { execution_id: string; dataset_id: string; workflow: string; purpose: string; event_time: string; state: ExecutionState; publication_state: string; unresolved: boolean; plan_hash: string; plan: Record<string, unknown>; attempts: Record<string, unknown>[]; outcomes: Record<string, unknown>[] };
export type ReportResponse = { accepted: Record<string, unknown>; report: Record<string, unknown>; links: Record<string, string> };
export type Revision = { revision_id: string; suite_id: string; revision_no: number; content_hash: string; payload: Record<string, unknown>; created_by: string; created_at: string; contains_sql?: boolean };
