import { useEffect, useState, type FormEvent } from 'react';
import { Link, useParams, useSearchParams } from 'react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiRequest, ApiError } from '../../api/client';
import type { Identity, Page } from '../../api/types';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';

const KINDS = ['REPORT_ATTENTION', 'NO_REPORT', 'QUARANTINE_RATIO', 'VIOLATION_RATE'];
const STATES = ['unacknowledged', 'acknowledged'];
const NOTE_MAX = 2048;
const base = '/api/v2';

type Alert = {
  alert_id: string; kind: string; execution_id: string | null; rule_id: string | null;
  window_start: string | null; window_end: string | null; owner_snapshot: string | null;
  details: Record<string, unknown> | null; created_at: string; acknowledged_at: string | null;
  acknowledged_by: string | null; acknowledgement_note: string | null; version: number;
  delivery_state: string | null; delivery_attempts: number | null; delivery_error_code: string | null;
};
type Settings = {
  version: number; enabled: boolean; quarantine_ratio_threshold: string | null; no_report_after_seconds: number | null;
  violation_rate_threshold: string | null; violation_rate_min_reports: number; window_seconds: number;
  updated_at: string | null; updated_by: string | null;
};

function ErrorWithStatus({ error, retry }: { error: unknown; retry?: () => void }) {
  return <>{error instanceof ApiError && <p role="note">HTTP {error.status} · {error.code}</p>}<ApiErrorAlert error={error} retry={retry} /></>;
}

export function AlertsPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [params, setParams] = useSearchParams();
  const state = params.get('state') ?? '';
  const kind = params.get('kind') ?? '';
  const [stack, setStack] = useState<(string | null)[]>([null]);
  const cursor = stack.at(-1) ?? null;
  useEffect(() => setStack([null]), [id, state, kind]);
  const identity = useQuery({ queryKey: ['me'], queryFn: ({ signal }) => apiRequest<Identity>(`${base}/me`, { signal }).then(r => r.data) });
  const isOperator = Boolean(identity.data?.grants.some(g => g.dataset_id === id && g.role === 'operator'));
  const alerts = useQuery({
    queryKey: ['alerts', id, state, kind, cursor],
    queryFn: ({ signal }) => {
      const q = new URLSearchParams({ limit: '25' });
      if (state) q.set('state', state);
      if (kind) q.set('kind', kind);
      if (cursor) q.set('cursor', cursor);
      return apiRequest<Page<Alert>>(`${base}/datasets/${encodeURIComponent(id)}/alerts?${q}`, { signal }).then(r => r.data);
    },
  });
  function setFilter(name: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(name, value); else next.delete(name);
    setParams(next);
  }
  return <div className="stack">
    <PageHeader title="Alerts" description={`Alert inbox for ${id}. Acknowledgement is the only action; alerts are never deleted.`} />
    <div className="row">
      <label>State<select value={state} onChange={e => setFilter('state', e.target.value)}><option value="">All</option>{STATES.map(s => <option key={s}>{s}</option>)}</select></label>
      <label>Kind<select value={kind} onChange={e => setFilter('kind', e.target.value)}><option value="">All</option>{KINDS.map(k => <option key={k}>{k}</option>)}</select></label>
    </div>
    {alerts.isPending ? <LoadingState /> : alerts.isError ? <ErrorWithStatus error={alerts.error} retry={() => void alerts.refetch()} /> :
      alerts.data.items.length === 0 ? <EmptyState title="No alerts" detail="No alerts match the current filters." /> :
        <div className="table-wrap"><table><thead><tr><th>Created</th><th>Kind</th><th>Execution</th><th>Rule</th><th>Window</th><th>Owner</th><th>Details</th><th>Delivery</th><th>Acknowledgement</th></tr></thead>
          <tbody>{alerts.data.items.map(a => <AlertRow key={a.alert_id} alert={a} datasetId={id} canAcknowledge={isOperator} />)}</tbody></table></div>}
    {!alerts.isPending && !alerts.isError && <CursorPager nextCursor={alerts.data.next_cursor} canBack={stack.length > 1} onBack={() => setStack(stack.slice(0, -1))} onNext={() => alerts.data.next_cursor && setStack([...stack, alerts.data.next_cursor])} />}
    <SettingsPanel datasetId={id} isOperator={isOperator} />
  </div>;
}

function AlertRow({ alert, datasetId, canAcknowledge }: { alert: Alert; datasetId: string; canAcknowledge: boolean }) {
  const [open, setOpen] = useState(false);
  const [note, setNote] = useState('');
  const client = useQueryClient();
  const ack = useMutation({
    mutationFn: () => apiRequest<Alert>(`${base}/datasets/${encodeURIComponent(datasetId)}/alerts/${encodeURIComponent(alert.alert_id)}/acknowledgement`, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
      body: JSON.stringify({ expected_version: alert.version, note: note.trim() || null }),
    }),
    onSuccess: () => { setOpen(false); setNote(''); void client.invalidateQueries({ queryKey: ['alerts', datasetId] }); },
  });
  function submit(event: FormEvent) { event.preventDefault(); ack.mutate(); }
  return <tr>
    <td>{alert.created_at}</td><td>{alert.kind}</td>
    <td>{alert.execution_id ? <Link to={`/executions/${encodeURIComponent(alert.execution_id)}`}>{alert.execution_id}</Link> : '—'}</td>
    <td>{alert.rule_id ?? '—'}</td>
    <td>{alert.window_start ? `${alert.window_start} → ${alert.window_end ?? '?'}` : '—'}</td>
    <td>{alert.owner_snapshot ?? '—'}</td>
    <td><code>{alert.details ? JSON.stringify(alert.details) : '—'}</code></td>
    <td><StatusBadge value={alert.delivery_state} /> attempts: {alert.delivery_attempts ?? 0}{alert.delivery_error_code && <> · {alert.delivery_error_code}</>}</td>
    <td>{alert.acknowledged_at
      ? <>Acknowledged by {alert.acknowledged_by} at {alert.acknowledged_at}{alert.acknowledgement_note && <p>{alert.acknowledgement_note}</p>}</>
      : !canAcknowledge ? 'Unacknowledged'
        : open
          ? <form className="stack" onSubmit={submit} aria-label={`Acknowledge alert ${alert.alert_id}`}>
            <label>Note (optional)<textarea value={note} maxLength={NOTE_MAX} onChange={e => setNote(e.target.value)} /></label>
            {ack.isError && <ErrorWithStatus error={ack.error} />}
            <div className="row"><button type="submit" disabled={ack.isPending}>Confirm acknowledgement</button><button type="button" onClick={() => { setOpen(false); ack.reset(); }}>Cancel</button></div>
          </form>
          : <button onClick={() => setOpen(true)}>Acknowledge</button>}</td>
  </tr>;
}

function SettingsPanel({ datasetId, isOperator }: { datasetId: string; isOperator: boolean }) {
  const client = useQueryClient();
  const settings = useQuery({ queryKey: ['alert-settings', datasetId], queryFn: ({ signal }) => apiRequest<Settings>(`${base}/datasets/${encodeURIComponent(datasetId)}/alert-settings`, { signal }).then(r => r.data) });
  const [form, setForm] = useState<Settings | null>(null);
  useEffect(() => setForm(settings.data ?? null), [settings.data]);
  const save = useMutation({
    mutationFn: (body: Settings) => apiRequest<Settings>(`${base}/datasets/${encodeURIComponent(datasetId)}/alert-settings`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
      body: JSON.stringify({
        expected_version: body.version, enabled: body.enabled,
        quarantine_ratio_threshold: body.quarantine_ratio_threshold || null, no_report_after_seconds: body.no_report_after_seconds,
        violation_rate_threshold: body.violation_rate_threshold || null, violation_rate_min_reports: body.violation_rate_min_reports,
        window_seconds: body.window_seconds,
      }),
    }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['alert-settings', datasetId] }),
  });
  return <section className="card stack"><h2>Alert settings</h2>
    {settings.isPending ? <LoadingState /> : settings.isError ? <ErrorWithStatus error={settings.error} retry={() => void settings.refetch()} /> : !form ? null :
      <form className="stack" onSubmit={(e: FormEvent) => { e.preventDefault(); save.mutate(form); }}>
        <fieldset disabled={!isOperator || save.isPending} className="stack">
          <label><input type="checkbox" checked={form.enabled} onChange={e => setForm({ ...form, enabled: e.target.checked })} /> Enabled</label>
          <label>Quarantine ratio threshold (0–1)<input value={form.quarantine_ratio_threshold ?? ''} onChange={e => setForm({ ...form, quarantine_ratio_threshold: e.target.value || null })} /></label>
          <label>No report after (seconds, 60–604800)<input type="number" min={60} max={604800} value={form.no_report_after_seconds ?? ''} onChange={e => setForm({ ...form, no_report_after_seconds: e.target.value === '' ? null : Number(e.target.value) })} /></label>
          <label>Violation rate threshold (0–1)<input value={form.violation_rate_threshold ?? ''} onChange={e => setForm({ ...form, violation_rate_threshold: e.target.value || null })} /></label>
          <label>Violation rate min reports<input type="number" min={1} value={form.violation_rate_min_reports} onChange={e => setForm({ ...form, violation_rate_min_reports: Number(e.target.value) })} /></label>
          <label>Window (seconds, 300–604800)<input type="number" min={300} max={604800} value={form.window_seconds} onChange={e => setForm({ ...form, window_seconds: Number(e.target.value) })} /></label>
        </fieldset>
        <p className="muted">Version {form.version}{form.updated_by && ` · updated by ${form.updated_by} at ${form.updated_at}`}</p>
        {save.isError && <ErrorWithStatus error={save.error} />}
        {isOperator ? <button type="submit" disabled={save.isPending}>Save settings</button> : <p className="muted">Only operators can change alert settings.</p>}
      </form>}
  </section>;
}
