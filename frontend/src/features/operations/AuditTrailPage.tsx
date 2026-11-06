import { useEffect, useState, type FormEvent } from 'react';
import { useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { apiRequest, ApiError } from '../../api/client';
import type { Page } from '../../api/types';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader } from '../../components/Primitives';

type AuditEvent = {
  event_id: string; actor: string; action: string; resource_type: string; resource_id: string;
  occurred_at: string; details: Record<string, unknown> | null;
};

export function AuditTrailPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [params, setParams] = useSearchParams();
  const action = params.get('action') ?? '';
  const [draft, setDraft] = useState(action);
  const [stack, setStack] = useState<(string | null)[]>([null]);
  const cursor = stack.at(-1) ?? null;
  useEffect(() => { setStack([null]); setDraft(action); }, [id, action]);
  const events = useQuery({
    queryKey: ['audit-events', id, action, cursor],
    queryFn: ({ signal }) => {
      const q = new URLSearchParams({ limit: '50' });
      if (action) q.set('action', action);
      if (cursor) q.set('cursor', cursor);
      return apiRequest<Page<AuditEvent>>(`/api/v2/datasets/${encodeURIComponent(id)}/audit-events?${q}`, { signal }).then(r => r.data);
    },
  });
  function applyFilter(event: FormEvent) {
    event.preventDefault();
    const next = new URLSearchParams(params);
    if (draft.trim()) next.set('action', draft.trim()); else next.delete('action');
    setParams(next);
  }
  return <div className="stack">
    <PageHeader title="Audit trail" description={`Read-only console audit events for ${id}.`} />
    <form className="row" onSubmit={applyFilter}>
      <label>Action<input value={draft} onChange={e => setDraft(e.target.value)} placeholder="e.g. ALERT_ACKNOWLEDGED" /></label>
      <button type="submit">Filter</button>
    </form>
    {events.isPending ? <LoadingState /> : events.isError ? <>{events.error instanceof ApiError && <p role="note">HTTP {events.error.status} · {events.error.code}</p>}<ApiErrorAlert error={events.error} retry={() => void events.refetch()} /></> :
      events.data.items.length === 0 ? <EmptyState title="No audit events" detail="No events match the current filter." /> :
        <div className="table-wrap"><table><thead><tr><th>Occurred</th><th>Actor</th><th>Action</th><th>Resource</th><th>Details</th></tr></thead>
          <tbody>{events.data.items.map(e => <tr key={e.event_id}><td>{e.occurred_at}</td><td>{e.actor}</td><td>{e.action}</td><td>{e.resource_type} / {e.resource_id}</td><td><code>{e.details ? JSON.stringify(e.details) : '—'}</code></td></tr>)}</tbody></table></div>}
    {!events.isPending && !events.isError && <CursorPager nextCursor={events.data.next_cursor} canBack={stack.length > 1} onBack={() => setStack(stack.slice(0, -1))} onNext={() => events.data.next_cursor && setStack([...stack, events.data.next_cursor])} />}
  </div>;
}
