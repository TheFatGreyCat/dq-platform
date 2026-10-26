import { useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { apiRequest } from '../../api/client';
import { endpoints } from '../../api/endpoints';
import type { Page, ExecutionState } from '../../api/types';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';

const states: ExecutionState[] = ['PLANNED', 'RUNNING', 'REPORTED', 'COMPLETED', 'FAILED', 'CANCELLED'];
type ReportEnvelope = { accepted: Record<string, unknown>; report: Record<string, unknown>; links: Record<string, string> };

export function ExecutionsPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [selected, setSelected] = useState<ExecutionState[]>([]);
  const [stack, setStack] = useState<(string | null)[]>([null]);
  const cursor = stack.at(-1) ?? null;
  const query = useQuery({ queryKey: ['executions', id, selected, cursor], queryFn: ({ signal }) =>
    endpoints.executions(id, selected, cursor, signal).then(r => r.data), enabled: Boolean(id) });
  function setStates(next: ExecutionState[]) { setSelected(next); setStack([null]); }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  return <div className="stack"><PageHeader title="Executions" description={`Execution theo API order cho ${id}.`} actions={<button onClick={() => void query.refetch()}>Refresh</button>} />
    <div className="row">{states.map(state => <label key={state} style={{ display: 'flex', alignItems: 'center' }}><input type="checkbox" style={{ width: 18, minHeight: 18 }} checked={selected.includes(state)} onChange={e => setStates(e.target.checked ? [...selected, state] : selected.filter(x => x !== state))} />{state}</label>)}</div>
    {query.data.items.length ? <div className="table-wrap"><table><thead><tr><th>Execution ID</th><th>Event time</th><th>Workflow</th><th>Purpose</th><th>State</th><th>Publication</th><th>Unresolved</th></tr></thead><tbody>{query.data.items.map(e => <tr key={e.execution_id}><td><Link className="mono" to={`/executions/${encodeURIComponent(e.execution_id)}`}>{e.execution_id}</Link></td><td>{typeof e.plan.event_time === 'string' ? e.plan.event_time : '—'}</td><td>{e.workflow}</td><td>{e.purpose}</td><td><StatusBadge value={e.state} /></td><td><StatusBadge value={e.publication_state} /></td><td>{e.unresolved ? 'Có' : 'Không'}</td></tr>)}</tbody></table></div> : <EmptyState title="Không có execution trên trang này" />}
    <CursorPager nextCursor={query.data.next_cursor} canBack={stack.length > 1} onBack={() => setStack(stack.slice(0, -1))} onNext={() => query.data.next_cursor && setStack([...stack, query.data.next_cursor])} />
  </div>;
}

export function ExecutionPage() {
  const { executionId = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const tab = params.get('tab') ?? 'overview';
  const execution = useQuery({ queryKey: ['execution', executionId], queryFn: ({ signal }) =>
    endpoints.execution(decodeURIComponent(executionId), signal).then(r => r.data), refetchInterval: query =>
      ['PLANNED', 'RUNNING', 'REPORTED'].includes(query.state.data?.state ?? '') && document.visibilityState === 'visible' ? 5000 : false });
  const report = useQuery({ queryKey: ['report', executionId], queryFn: async ({ signal }) => {
    const result = await endpoints.report(decodeURIComponent(executionId), signal);
    return result.status === 202 ? { pending: true as const, payload: result.data } :
      { pending: false as const, payload: result.data as ReportEnvelope };
  }, refetchInterval: query => query.state.data?.pending && document.visibilityState === 'visible' ? 5000 : false });
  if (execution.isPending || report.isPending) return <LoadingState />;
  if (execution.isError) return <ApiErrorAlert error={execution.error} retry={() => void execution.refetch()} />;
  if (report.isError) return <ApiErrorAlert error={report.error} retry={() => void report.refetch()} />;
  const e = execution.data;
  const pending = report.data.pending;
  const payload = pending ? undefined : report.data.payload;
  const reportBody = payload?.report;
  const gate = payload?.accepted.decision;
  const assessment = payload?.accepted.assessment;
  const isWrite = e.workflow === 'WRITE_GATE';
  return <div className="stack"><PageHeader title={`Execution ${decodeURIComponent(executionId)}`} description={`${e.workflow} · ${e.purpose} · plan ${String(e.plan.plan_hash ?? '')}`} actions={<StatusBadge value={e.state} />} />
    <div className="row"><div className="card"><h2>{isWrite ? 'Write gate decision' : 'Audit assessment'}</h2>{pending ? 'Chưa nhận report' : <pre>{JSON.stringify(isWrite ? gate ?? 'Không có decision' : assessment ?? 'Không có assessment', null, 2)}</pre>}</div><div className="card"><h2>Publication</h2><StatusBadge value={e.publication_state} /><pre>{JSON.stringify(e.outcomes, null, 2)}</pre>{e.unresolved && <strong>Marker unresolved — cần xử lý qua operator tools.</strong>}</div></div>
    <div className="row">{['overview', 'rules', 'metrics', 'anomalies', 'plan'].map(t => <button key={t} aria-pressed={tab === t} onClick={() => setParams({ tab: t })}>{t}</button>)}</div>
    {pending ? <EmptyState title={['COMPLETED', 'FAILED', 'CANCELLED'].includes(e.state) ? 'Kết thúc không có report' : 'Report chưa được gửi'} /> :
      <section className="card stack"><h2>{tab}</h2>{tab === 'overview' && <><p>Row counts: {JSON.stringify(reportBody?.row_counts ?? {})}</p><h3>Decision reasons</h3><pre>{JSON.stringify(isWrite ? getField(gate, 'reasons') : getField(assessment, 'reasons'), null, 2)}</pre><h3>Attempts & outcomes</h3><pre>{JSON.stringify({ attempts: e.attempts, outcomes: e.outcomes }, null, 2)}</pre></>}
        {tab === 'plan' && <pre>{JSON.stringify(e.plan, null, 2)}</pre>}
        {tab === 'rules' && <ReportCollection executionId={executionId} name="rules" />}
        {tab === 'metrics' && <ReportCollection executionId={executionId} name="metrics" />}
        {tab === 'anomalies' && <ReportCollection executionId={executionId} name="anomalies" />}</section>}
  </div>;
}

function getField(value: unknown, key: string): unknown {
  if (value && typeof value === 'object' && key in value) return Reflect.get(value, key);
  return undefined;
}

function ReportCollection({ executionId, name }: { executionId: string; name: 'rules' | 'metrics' | 'anomalies' }) {
  const query = useQuery({ queryKey: [name, executionId], queryFn: async ({ signal }) => {
    const { status, data } = await apiRequest<Page<unknown>>(`/api/v2/executions/${encodeURIComponent(executionId)}/${name}`, { signal });
    return status === 202 ? { pending: true as const, page: null } : { pending: false as const, page: data };
  } });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} />;
  return query.data.pending ? <EmptyState title="Chưa có report measurements" /> : <pre>{JSON.stringify(query.data.page.items, null, 2)}</pre>;
}
