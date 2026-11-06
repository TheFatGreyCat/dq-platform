import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams, useSearchParams } from 'react-router';
import { apiRequest } from '../../api/client';
import { endpoints } from '../../api/endpoints';
import type { Page, ExecutionState } from '../../api/types';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';
import { RuleResultExplorer } from './RuleResultExplorer';
import { parseExecutionFilters, withExecutionFilter } from './executionFilters';

const states: ExecutionState[] = ['PLANNED', 'RUNNING', 'REPORTED', 'COMPLETED', 'FAILED', 'CANCELLED'];

export function ExecutionsPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [params, setParams] = useSearchParams();
  const filters = parseExecutionFilters(params);
  const [stack, setStack] = useStateStack();
  const cursor = stack.at(-1) ?? null;
  const query = useQuery({ queryKey: ['executions', id, params.toString(), cursor], queryFn: ({ signal }) => endpoints.executions(id, { states: filters.states, workflow: filters.workflow, cursor }, signal).then(r => r.data), enabled: Boolean(id) });
  function setFilter(name: string, value: string) { setParams(withExecutionFilter(params, name, value)); setStack([null]); }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  return <div className="stack"><PageHeader title="Executions" description={`Execution theo API order cho ${id}.`} actions={<button onClick={() => void query.refetch()}>Refresh</button>} />
    <div className="row" style={{ flexWrap: 'wrap' }}>{states.map(state => <label key={state}><input type="checkbox" checked={filters.states.includes(state)} onChange={() => setFilter('state', state)} />{state}</label>)}<label>Workflow<select value={filters.workflow ?? ''} onChange={e => setFilter('workflow', e.target.value)}><option value="">Tất cả</option><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label><label>Purpose<input value={filters.purpose} onChange={e => setFilter('purpose', e.target.value)} /></label></div>
    {query.data.items.length ? <div className="table-wrap"><table><thead><tr><th>Execution ID</th><th>Event time</th><th>Workflow</th><th>Purpose</th><th>State</th><th>Publication</th><th>Unresolved</th></tr></thead><tbody>{query.data.items.map(e => <tr key={e.execution_id}><td><Link className="mono" to={`/executions/${encodeURIComponent(e.execution_id)}`}>{e.execution_id}</Link></td><td>{typeof e.plan.event_time === 'string' ? e.plan.event_time : 'Unknown'}</td><td>{e.workflow}</td><td>{e.purpose}</td><td><StatusBadge value={e.state} /></td><td><StatusBadge value={e.publication_state} /></td><td>{e.unresolved ? 'Có' : 'Không'}</td></tr>)}</tbody></table></div> : <EmptyState title="Không có execution trên trang này" />}
    <CursorPager nextCursor={query.data.next_cursor} canBack={stack.length > 1} onBack={() => setStack(stack.slice(0, -1))} onNext={() => query.data.next_cursor && setStack([...stack, query.data.next_cursor])} />
  </div>;
}

function useStateStack() {
  const [stack, setStack] = useState<(string | null)[]>([null]);
  return [stack, setStack] as const;
}

export function ExecutionPage() {
  const { executionId = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const tab = params.get('tab') ?? 'overview';
  const execution = useQuery({ queryKey: ['execution', executionId], queryFn: ({ signal }) => endpoints.execution(decodeURIComponent(executionId), signal).then(r => r.data), refetchInterval: query => ['PLANNED', 'RUNNING', 'REPORTED'].includes(query.state.data?.state ?? '') && document.visibilityState === 'visible' ? 5000 : false });
  const report = useQuery({ queryKey: ['report', executionId], queryFn: ({ signal }) => endpoints.report(decodeURIComponent(executionId), signal), refetchInterval: query => query.state.data?.pending && document.visibilityState === 'visible' ? 5000 : false });
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
  const counts = isWrite ? { candidate_rows: reportBody?.candidate_rows ?? 'Unknown', valid_rows: reportBody?.valid_rows ?? 'Unknown', quarantine_rows: reportBody?.quarantine_rows ?? 'Unknown' } : { measured_rows: reportBody?.measured_rows ?? 'Unknown' };
  return <div className="stack"><PageHeader title={`Execution ${decodeURIComponent(executionId)}`} description={`${e.workflow} · ${e.purpose} · plan ${String(e.plan.plan_hash ?? '')}`} actions={<StatusBadge value={e.state} />} />
    <div className="row"><div className="card"><h2>{isWrite ? 'Write gate decision' : 'Audit assessment'}</h2>{pending ? 'Chưa nhận report' : <pre>{JSON.stringify(isWrite ? gate ?? 'Không có decision' : assessment ?? 'Không có assessment', null, 2)}</pre>}</div><div className="card"><h2>Publication</h2><StatusBadge value={e.publication_state} /><pre>{JSON.stringify(e.outcomes, null, 2)}</pre>{e.unresolved && <strong>Marker unresolved — see Operations evidence.</strong>}</div></div>
    <div className="row">{['overview', 'rules', 'metrics', 'anomalies', 'plan'].map(t => <button key={t} aria-pressed={tab === t} onClick={() => setParams({ tab: t })}>{t}</button>)}</div>
    {pending ? <EmptyState title={['COMPLETED', 'FAILED', 'CANCELLED'].includes(e.state) ? 'Kết thúc không có report' : 'Report chưa được gửi'} /> : <section className="card stack"><h2>{tab}</h2>{tab === 'overview' && <><p>Row counts: {JSON.stringify(counts)}</p><h3>Decision reasons</h3>{isWrite ? <pre>{JSON.stringify(gate?.reasons ?? 'Unknown', null, 2)}</pre> : <p>Reason list not provided by report API.</p>}<h3>Attempts & outcomes</h3><pre>{JSON.stringify({ attempts: e.attempts, outcomes: e.outcomes }, null, 2)}</pre></>}{tab === 'plan' && <pre>{JSON.stringify(e.plan, null, 2)}</pre>}{tab === 'rules' && <RuleResultExplorer executionId={executionId} plan={e.plan} pending={pending} />}{tab === 'metrics' && <ReportCollection executionId={executionId} name="metrics" />}{tab === 'anomalies' && <ReportCollection executionId={executionId} name="anomalies" />}</section>}
  </div>;
}

function ReportCollection({ executionId, name }: { executionId: string; name: 'metrics' | 'anomalies' }) {
  const query = useQuery({ queryKey: [name, executionId], queryFn: async ({ signal }) => { const { status, data } = await apiRequest<Page<unknown>>(`/api/v2/executions/${encodeURIComponent(executionId)}/${name}`, { signal }); return status === 202 ? { pending: true as const, page: null } : { pending: false as const, page: data }; } });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} />;
  return query.data.pending ? <EmptyState title="Chưa có report measurements" /> : <pre>{JSON.stringify(query.data.page.items, null, 2)}</pre>;
}
