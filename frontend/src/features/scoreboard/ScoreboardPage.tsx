import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { ExecutionState, Workflow } from '../../api/types';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';
import { ExecutionReportCells } from './ExecutionReportCells';

const states: ExecutionState[] = ['PLANNED', 'RUNNING', 'REPORTED', 'COMPLETED', 'FAILED', 'CANCELLED'];

export function ScoreboardPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [workflow, setWorkflow] = useState<Workflow | null>(null);
  const [selectedStates, setSelectedStates] = useState<ExecutionState[]>([]);
  const [stack, setStack] = useState<(string | null)[]>([null]);
  useEffect(() => { setStack([null]); setWorkflow(null); setSelectedStates([]); }, [id]);
  const cursor = stack.at(-1) ?? null;
  const executions = useQuery({ queryKey: ['scoreboard', id, workflow, selectedStates, cursor], queryFn: ({ signal }) => endpoints.executions(id, { states: selectedStates, workflow, cursor }, signal).then(r => r.data) });
  function changeWorkflow(value: string) { setWorkflow(value ? value as Workflow : null); setStack([null]); }
  function changeState(state: ExecutionState, checked: boolean) { setSelectedStates(prev => checked ? [...prev, state] : prev.filter(s => s !== state)); setStack([null]); }
  const page = executions.data;
  return <div className="stack"><PageHeader title="Scoreboard" description={`Dataset-local execution verdicts and measurements for ${id}.`} />
    <label>Workflow<select aria-label="Workflow filter" value={workflow ?? ''} onChange={e => changeWorkflow(e.target.value)}><option value="">All workflows</option><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label>
    <div className="row">{states.map(state => <label key={state} style={{ display: 'flex', alignItems: 'center' }}><input aria-label={state} type="checkbox" style={{ width: 18, minHeight: 18 }} checked={selectedStates.includes(state)} onChange={e => changeState(state, e.target.checked)} />{state}</label>)}</div>
    {executions.isPending ? <LoadingState /> : executions.isError ? <ApiErrorAlert error={executions.error} retry={() => void executions.refetch()} /> : page?.items.length ? <div className="table-wrap"><table><thead><tr><th>Workflow</th><th>Purpose</th><th>Event time</th><th>Execution</th><th>Publication</th><th>Verdict</th><th>Measurements / row counts</th><th>Reasons</th></tr></thead><tbody>{page.items.map(e => <tr key={e.execution_id}><td>{e.workflow}</td><td>{e.purpose}</td><td>{typeof e.plan.event_time === 'string' ? e.plan.event_time : 'Unknown'}</td><td><Link to={`/executions/${encodeURIComponent(e.execution_id)}`}>{e.execution_id} <StatusBadge value={e.state} /></Link></td><td><StatusBadge value={e.publication_state} /></td><ExecutionReportCells execution={e} /></tr>)}</tbody></table></div> : !executions.isPending && !executions.isError ? <EmptyState title="No executions on this filtered API page" /> : null}
    {!executions.isPending && !executions.isError && page && <CursorPager nextCursor={page.next_cursor} canBack={stack.length > 1} onBack={() => setStack(stack.slice(0, -1))} onNext={() => page.next_cursor && setStack([...stack, page.next_cursor])} />}
  </div>;
}
