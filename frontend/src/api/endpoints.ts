import { apiRequest } from './client';
import type { Dataset, Execution, Identity, Page, Revision } from './types';

export type PublicationView = { current: Record<string, unknown> | null; pending: unknown; unresolved_execution_id: string | null; restore_orphans: unknown };
const base = '/api/v2';
const segment = (value: string) => encodeURIComponent(value);

export const endpoints = {
  me: (signal?: AbortSignal) => apiRequest<Identity>(`${base}/me`, { signal }),
  datasets: (cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<Dataset>>(
    `${base}/datasets${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  dataset: (id: string, signal?: AbortSignal) => apiRequest<Record<string, unknown>>(`${base}/datasets/${segment(id)}`, { signal }),
  executions: (id: string, states: string[], cursor: string | null, signal?: AbortSignal) => {
    const params = new URLSearchParams({ limit: '25' });
    for (const state of states) params.append('state', state);
    if (cursor) params.set('cursor', cursor);
    return apiRequest<Page<Execution>>(`${base}/datasets/${segment(id)}/executions?${params}`, { signal });
  },
  execution: (id: string, signal?: AbortSignal) => apiRequest<Execution>(`${base}/executions/${segment(id)}`, { signal }),
  report: (id: string, signal?: AbortSignal) => apiRequest<ReportPayload>(`${base}/executions/${segment(id)}/report`, { signal }),
  publication: (id: string, dt: string, signal?: AbortSignal) => apiRequest<PublicationView>(`${base}/datasets/${segment(id)}/publications?dt=${segment(dt)}`, { signal }),
  collection: <T,>(id: string, name: string, cursor?: string | null, signal?: AbortSignal) => apiRequest<Page<T>>(`${base}/datasets/${segment(id)}/${name}${cursor ? `?cursor=${segment(cursor)}` : ''}`, { signal }),
  suite: (id: string, suiteId: string, signal?: AbortSignal) => apiRequest<Record<string, unknown>>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}`, { signal }),
  revision: (id: string, suiteId: string, kind: 'revisions' | 'policy-revisions', revisionId: string, signal?: AbortSignal) => apiRequest<Revision>(
    `${base}/datasets/${segment(id)}/suites/${segment(suiteId)}/${kind}/${segment(revisionId)}`, { signal }),
};

type ReportPayload = { accepted: Record<string, unknown>; report: Record<string, unknown>; links: Record<string, string> };
