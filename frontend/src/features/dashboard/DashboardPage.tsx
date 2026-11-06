import { Link, useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { DatasetAnalytics, Execution } from '../../api/types';
import { ApiErrorAlert, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';

export function DashboardPage() {
  const { datasetId } = useParams();
  return datasetId ? <DatasetDashboard id={decodeURIComponent(datasetId)} /> : <GlobalDashboard />;
}

function GlobalDashboard() {
  const identity = useQuery({ queryKey: ['me'], queryFn: ({ signal }) => endpoints.me(signal).then(r => r.data) });
  const datasets = useQuery({ queryKey: ['datasets'], queryFn: ({ signal }) => endpoints.datasets(null, signal).then(r => r.data) });
  if (identity.isPending || datasets.isPending) return <LoadingState />;
  if (identity.isError) return <ApiErrorAlert error={identity.error} retry={() => void identity.refetch()} />;
  if (datasets.isError) return <ApiErrorAlert error={datasets.error} retry={() => void datasets.refetch()} />;
  return <div className="stack"><PageHeader title="Dashboard" description="Identity, dataset grants, and registered datasets." />
    <section className="card"><h2>Identity & grants</h2><p>{identity.data.principal}</p>{identity.data.grants.length ? <ul>{identity.data.grants.map((grant, i) => <li key={`${grant.dataset_id}-${grant.role}-${i}`}>{grant.dataset_id} · {grant.role}</li>)}</ul> : <p>No grants returned.</p>}</section>
    <section className="card"><h2>Registered datasets</h2>{datasets.data.items.length ? <ul>{datasets.data.items.map(d => <li key={d.dataset_id}><Link to={`/datasets/${encodeURIComponent(d.dataset_id)}`}>{d.dataset_id}</Link> · {d.owner}</li>)}</ul> : <EmptyState title="No registered datasets" />}</section>
  </div>;
}

function DatasetDashboard({ id }: { id: string }) {
  const [params, setParams] = useSearchParams();
  const dataset = useQuery({ queryKey: ['dataset', id], queryFn: ({ signal }) => endpoints.dataset(id, signal).then(r => r.data) });
  const analytics = useQuery({
    queryKey: ['dataset-analytics', id, params.toString()],
    queryFn: ({ signal }) => endpoints.analytics(id, params, signal).then(r => r.data),
    refetchInterval: () => document.visibilityState === 'visible' ? 30_000 : false,
  });
  const executions = useQuery({
    queryKey: ['dashboard-executions', id],
    enabled: analytics.isError,
    queryFn: ({ signal }) => endpoints.executions(id, { states: [], workflow: null, cursor: null }, signal).then(r => r.data),
  });
  const setFilter = (name: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set(name, value); else next.delete(name);
    setParams(next);
  };
  if (dataset.isPending || analytics.isPending) return <LoadingState />;
  if (dataset.isError) return <ApiErrorAlert error={dataset.error} retry={() => void dataset.refetch()} />;
  if (analytics.isError) {
    if (executions.isPending) return <LoadingState />;
    if (executions.isError) return <ApiErrorAlert error={executions.error} retry={() => void executions.refetch()} />;
    return <LegacyDatasetDashboard id={id} data={dataset.data} executions={executions.data.items} />;
  }
  return <div className="stack"><PageHeader title={`Dashboard · ${id}`} description="KPI lấy từ toàn bộ window server-side; không cộng từ một trang executions." />
    <div className="card row" style={{ flexWrap: 'wrap' }}><label>Workflow<select value={params.get('workflow') ?? ''} onChange={e => setFilter('workflow', e.target.value)}><option value="">Tất cả</option><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label><label>Purpose<input value={params.get('purpose') ?? ''} onChange={e => setFilter('purpose', e.target.value)} /></label></div>
    <AnalyticsSummary data={analytics.data} /><nav className="row"><Link to={`/datasets/${encodeURIComponent(id)}/executions`}>Mở executions drill-down</Link><Link to={`/datasets/${encodeURIComponent(id)}/suites`}>Mở Suites</Link></nav>
  </div>;
}

function LegacyDatasetDashboard({ id, data, executions }: { id: string; data: Record<string, unknown>; executions: Execution[] }) {
  return <div className="stack"><PageHeader title={`Dashboard · ${id}`} description="Registered dataset overview." /><section className="card"><h2>Registration identity</h2><pre>{JSON.stringify(data.registration ?? data, null, 2)}</pre></section><section className="stack"><h2>Executions — recent/current API page</h2>{executions.length ? <div className="table-wrap"><table><thead><tr><th>Workflow</th><th>Purpose</th><th>Execution</th></tr></thead><tbody>{executions.map(e => <tr key={e.execution_id}><td>{e.workflow}</td><td>{e.purpose}</td><td><Link to={`/executions/${encodeURIComponent(e.execution_id)}`}><StatusBadge value={e.state} /></Link></td></tr>)}</tbody></table></div> : <EmptyState title="No executions on this API page" />}</section></div>;
}

function AnalyticsSummary({ data }: { data: DatasetAnalytics }) {
  return <section className="card stack"><div className="row" style={{ justifyContent: 'space-between' }}><h2>{data.dataset_id}</h2><small>Window UTC: {data.window.start} → {data.window.end}</small></div>{(['WRITE_GATE', 'AUDIT_TABLE'] as const).map(workflow => { const section = data.workflows[workflow]; if (!section) return null; return <div className="stack" key={workflow}><h3>{workflow}</h3><div className="row" style={{ flexWrap: 'wrap' }}><Kpi label="Executions" value={section.executions.total} /><Kpi label="Failed state" value={section.executions.states.FAILED ?? '0'} />{section.decisions && <><Kpi label="ALLOW" value={section.decisions.ALLOW} /><Kpi label="DENY" value={section.decisions.DENY} /></>}{section.rules && <><Kpi label="Rules OK" value={section.rules.OK} /><Kpi label="Rules ERROR" value={section.rules.ERROR} /><Kpi label="Failed rows" value={section.rules.failed_rows} /></>}</div></div>; })}</section>;
}

function Kpi({ label, value }: { label: string; value: string }) { return <div className="card" style={{ minWidth: 120 }}><small>{label}</small><strong className="mono">{value}</strong></div>; }
