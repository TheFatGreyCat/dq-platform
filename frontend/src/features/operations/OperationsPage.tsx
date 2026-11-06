import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import { ApiErrorAlert, CursorPager, EmptyState, LoadingState, PageHeader } from '../../components/Primitives';

type PageStack = (string | null)[];
export function OperationsPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const restore = useQuery({ queryKey: ['restore', id], queryFn: ({ signal }) => endpoints.restore(id, signal).then(r => r.data) });
  const [retentionStack, setRetentionStack] = useState<PageStack>([null]);
  const retentionCursor = retentionStack.at(-1) ?? null;
  const retention = useQuery({ queryKey: ['retention', id, retentionCursor], queryFn: ({ signal }) => endpoints.retention(id, retentionCursor, signal).then(r => r.data) });
  const [workflow, setWorkflow] = useState('WRITE_GATE');
  const [purpose, setPurpose] = useState('');
  const [eventStack, setEventStack] = useState<PageStack>([null]);
  const eventCursor = eventStack.at(-1) ?? null;
  const events = useQuery({ queryKey: ['purpose-events', id, workflow, purpose, eventCursor], enabled: Boolean(purpose), queryFn: ({ signal }) => endpoints.purposeEvents(id, workflow, purpose, eventCursor, signal).then(r => r.data) });
  const [executionStack, setExecutionStack] = useState<PageStack>([null]);
  const executionCursor = executionStack.at(-1) ?? null;
  const executionPage = useQuery({ queryKey: ['operations-executions', id, executionCursor], queryFn: ({ signal }) => endpoints.executions(id, { states: [], workflow: null, cursor: executionCursor }, signal).then(r => r.data) });
  useEffect(() => { setRetentionStack([null]); setEventStack([null]); setExecutionStack([null]); setPurpose(''); }, [id]);
  return <div className="stack"><PageHeader title="Operations" description={`Read-only recovery, retention, and binding-event state for ${id}.`} />
    <section className="card stack"><h2>Restore state</h2>{restore.isPending ? <LoadingState /> : restore.isError ? <ApiErrorAlert error={restore.error} retry={() => void restore.refetch()} /> : <>
      {restore.data.open ? <div><h3>Open restore</h3><pre>{JSON.stringify(restore.data.open, null, 2)}</pre><p>Findings submitted: {restore.data.open.findings_submitted} · Pending reconciliation: {restore.data.open.pending_reconcile.length ? restore.data.open.pending_reconcile.join(', ') : 'none'}</p></div> : <EmptyState title="No open restore" />}
      <p>Restore evidence and completion guidance: <code>dq-service/README.md</code> publication/restore/retention sections; <code>scripts/dq_operator.py --help</code>. Viewer access includes restore status; retention access is operator-only (403 is expected for viewers). Recovery actions require external operator evidence and writer coordination.</p>
      {restore.data.history.length ? <div><h3>Restore history</h3><pre>{JSON.stringify(restore.data.history, null, 2)}</pre></div> : <p>Restore history: none returned.</p>}
    </>}</section>
    <section className="card stack"><h2>Retention candidates</h2>{retention.isPending ? <LoadingState /> : retention.isError ? <ApiErrorAlert error={retention.error} retry={() => void retention.refetch()} /> : retention.data.items.length ? <div className="table-wrap"><table><thead><tr><th>URI</th><th>Execution</th><th>Attempt</th><th>Retain until</th><th>Pinned</th><th>Reason</th></tr></thead><tbody>{retention.data.items.map((row, i) => <tr key={`${String(row.attempt_id)}-${i}`}><td>{String(row.uri ?? 'Unknown')}</td><td>{row.execution_id ? <Link to={`/executions/${String(row.execution_id)}`}>{String(row.execution_id)}</Link> : 'Unknown'}</td><td>{String(row.attempt_id ?? 'Unknown')}</td><td>{String(row.retain_until ?? 'Unknown')}</td><td>{row.pinned === true ? 'Yes' : row.pinned === false ? 'No' : 'Unknown'}</td><td>{String(row.reason ?? 'None')}</td></tr>)}</tbody></table></div> : <EmptyState title="No retention candidates on this page" />}
      {!retention.isPending && !retention.isError && <CursorPager nextCursor={retention.data.next_cursor} canBack={retentionStack.length > 1} onBack={() => setRetentionStack(retentionStack.slice(0, -1))} onNext={() => retention.data.next_cursor && setRetentionStack([...retentionStack, retention.data.next_cursor])} />}
      <p>Retention is read-only; cleanup actions are outside this UI.</p>
    </section>
    <section className="card stack"><h2>Purpose binding events</h2><div className="row"><label>Workflow<select value={workflow} onChange={e => { setWorkflow(e.target.value); setEventStack([null]); }}><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label><label>Purpose<input value={purpose} onChange={e => { setPurpose(e.target.value); setEventStack([null]); }} /></label></div>
      {!purpose ? <p>Enter a purpose to view its dataset-scoped binding events.</p> : events.isPending ? <LoadingState /> : events.isError ? <ApiErrorAlert error={events.error} retry={() => void events.refetch()} /> : events.data.items.length ? <div className="table-wrap"><table><thead><tr><th>Version</th><th>Action</th><th>Suite revision</th><th>Policy revision</th><th>Actor</th><th>Reason</th><th>Event time</th></tr></thead><tbody>{events.data.items.map((event, i) => <tr key={String(event.event_id ?? i)}><td>{String(event.binding_version ?? 'Unknown')}</td><td>{String(event.action ?? 'Unknown')}</td><td>{String(event.suite_revision_id ?? 'Unknown')}</td><td>{String(event.policy_revision_id ?? 'Unknown')}</td><td>{String(event.actor ?? 'Unknown')}</td><td>{String(event.reason ?? 'None')}</td><td>{String(event.created_at ?? 'Unknown')}</td></tr>)}</tbody></table></div> : <EmptyState title="No binding events on this page" />}
      {purpose && !events.isPending && !events.isError && <CursorPager nextCursor={events.data.next_cursor} canBack={eventStack.length > 1} onBack={() => setEventStack(eventStack.slice(0, -1))} onNext={() => events.data.next_cursor && setEventStack([...eventStack, events.data.next_cursor])} />}
    </section>
    <section className="card stack"><h2>Unresolved evidence on current API page</h2>{executionPage.isPending ? <LoadingState /> : executionPage.isError ? <ApiErrorAlert error={executionPage.error} retry={() => void executionPage.refetch()} /> : (() => { const items = executionPage.data.items.filter(e => e.unresolved || ['PARTIAL', 'UNKNOWN'].includes(e.publication_state)); return items.length ? <div className="table-wrap"><table><thead><tr><th>Execution</th><th>Workflow / purpose</th><th>State</th><th>Publication</th><th>Unresolved</th><th>Event time</th><th>Destination outcomes</th></tr></thead><tbody>{items.map(e => <tr key={e.execution_id}><td><Link to={`/executions/${encodeURIComponent(e.execution_id)}`}>{e.execution_id}</Link></td><td>{e.workflow} / {e.purpose}</td><td>{e.state}</td><td>{e.publication_state}</td><td>{e.unresolved ? 'Yes' : 'No'}</td><td>{typeof e.plan.event_time === 'string' ? e.plan.event_time : 'Unknown'}</td><td><pre>{JSON.stringify(e.outcomes)}</pre></td></tr>)}</tbody></table></div> : <p>No unresolved evidence on this current API page.</p>; })()}{!executionPage.isPending && !executionPage.isError && <CursorPager nextCursor={executionPage.data.next_cursor} canBack={executionStack.length > 1} onBack={() => setExecutionStack(executionStack.slice(0, -1))} onNext={() => executionPage.data.next_cursor && setExecutionStack([...executionStack, executionPage.data.next_cursor])} />}</section>
  </div>;
}
