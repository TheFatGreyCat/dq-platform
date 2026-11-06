import { apiRequest } from './client';
import type { Dataset, DatasetAnalytics, Execution, ExecutionState, Identity, Page, Policy, ReportResponse, ReportResult, Revision, RevisionValidationRequest, RevisionValidationResponse, RuleHistory, Suite, Workflow } from './types';

export type PublicationView = { current: Record<string, unknown> | null; pending: unknown; unresolved_execution_id: string | null; restore_orphans: Record<string, unknown>[] };
const base = '/api/v2';
const segment = (value: string) => encodeURIComponent(value);

export const endpoints = {
  me: (signal?: AbortSignal) => apiRequest<Identity>(`${base}/me`, { signal }),
  datasets: (cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<Dataset>>(
    `${base}/datasets${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  dataset: (id: string, signal?: AbortSignal) => apiRequest<Record<string, unknown>>(`${base}/datasets/${segment(id)}`, { signal }),
  executions: (id: string, filters: { states: ExecutionState[]; workflow: Workflow | null; cursor: string | null }, signal?: AbortSignal) => {
    const params = new URLSearchParams({ limit: '25' });
    for (const state of filters.states) params.append('state', state);
    if (filters.workflow) params.set('workflow', filters.workflow);
    if (filters.cursor) params.set('cursor', filters.cursor);
    return apiRequest<Page<Execution>>(`${base}/datasets/${segment(id)}/executions?${params}`, { signal });
  },
  execution: (id: string, signal?: AbortSignal) => apiRequest<Execution>(`${base}/executions/${segment(id)}`, { signal }),
  report: async (id: string, signal?: AbortSignal): Promise<ReportResult> => {
    const response = await apiRequest<ReportResponse | { execution_id: string; state: ExecutionState }>(`${base}/executions/${segment(id)}/report`, { signal });
    return response.status === 202 ? { pending: true, payload: response.data as { execution_id: string; state: ExecutionState } } :
      { pending: false, payload: response.data as ReportResponse };
  },
  publication: (id: string, dt: string, signal?: AbortSignal) => apiRequest<PublicationView>(`${base}/datasets/${segment(id)}/publications?dt=${segment(dt)}`, { signal }),
  restore: (id: string, signal?: AbortSignal) => apiRequest<{ open: { findings_submitted: number; pending_reconcile: string[] } | null; history: Record<string, unknown>[] }>(`${base}/datasets/${segment(id)}/restore`, { signal }),
  retention: (id: string, cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<Record<string, unknown>>>(
    `${base}/datasets/${segment(id)}/retention${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  purposeEvents: (id: string, workflow: string, purpose: string, cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<Record<string, unknown>>>(
    `${base}/datasets/${segment(id)}/purposes/${segment(workflow)}/${segment(purpose)}/events${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  collection: <T,>(id: string, name: string, cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<T>>(`${base}/datasets/${segment(id)}/${name}${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  suite: (id: string, suiteId: string, signal?: AbortSignal) => apiRequest<Record<string, unknown>>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}`, { signal }),
  revision: (id: string, suiteId: string, kind: 'revisions' | 'policy-revisions', revisionId: string, signal?: AbortSignal) => apiRequest<Revision>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}/${kind}/${segment(revisionId)}`, { signal }),
  validateSuite: (id: string, suiteId: string, body: RevisionValidationRequest, signal?: AbortSignal) => apiRequest<RevisionValidationResponse>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}/validate`, { method: 'POST', body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' }, signal }),
  createSuiteRevision: (id: string, suiteId: string, body: Suite, idempotencyKey: string, signal?: AbortSignal) => apiRequest<Revision>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}/revisions`, { method: 'POST', body: JSON.stringify(body), headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey }, signal }),
  createPolicyRevision: (id: string, suiteId: string, body: Policy, idempotencyKey: string, signal?: AbortSignal) => apiRequest<Revision>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}/policy-revisions`, { method: 'POST', body: JSON.stringify(body), headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey }, signal }),
  executionRules: (id: string, query: URLSearchParams, signal?: AbortSignal) => apiRequest<Page<Record<string, unknown>> | { execution_id: string; state: ExecutionState }>(
    `${base}/executions/${segment(id)}/rules?${query.toString()}`, { signal }),
  analytics: (id: string, query: URLSearchParams, signal?: AbortSignal) => apiRequest<DatasetAnalytics>(`${base}/datasets/${segment(id)}/analytics?${query.toString()}`, { signal }),
  globalAnalytics: (query: URLSearchParams, signal?: AbortSignal) => apiRequest<{ window: { start: string; end: string }; datasets: DatasetAnalytics[] }>(`${base}/analytics?${query.toString()}`, { signal }),
  ruleHistory: (id: string, query: URLSearchParams, signal?: AbortSignal) => apiRequest<RuleHistory>(`${base}/datasets/${segment(id)}/rule-history?${query.toString()}`, { signal }),
};
