import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { Dataset } from '../../api/types';
import { ApiErrorAlert, EmptyState, LoadingState, PageHeader, StatusBadge } from '../../components/Primitives';

export function DatasetsPage() {
  const [filter, setFilter] = useState('');
  const query = useQuery({ queryKey: ['datasets'], queryFn: ({ signal }) => endpoints.datasets(null, signal).then(r => r.data) });
  const identity = useQuery({ queryKey: ['me'], queryFn: ({ signal }) => endpoints.me(signal).then(r => r.data) });
  if (query.isPending || identity.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  if (identity.isError) return <ApiErrorAlert error={identity.error} retry={() => void identity.refetch()} />;
  const editorIds = identity.data.grants.filter(grant => grant.role === 'editor').map(grant => grant.dataset_id);
  const unregistered = editorIds.filter(datasetId => !query.data.items.some(dataset => dataset.dataset_id === datasetId));
  const visible = query.data.items.filter(dataset => dataset.dataset_id.toLowerCase().includes(filter.toLowerCase()));
  return <div className="stack"><PageHeader title="Datasets" description="Danh mục dataset theo quyền truy cập token." />
    <label>Tìm ID trên trang hiện tại<input value={filter} onChange={e => setFilter(e.target.value)} /></label>
    {query.data.items.length ? <div className="table-wrap"><table><thead><tr><th>Dataset ID</th><th>Owner</th><th>History context</th><th>Registration hash</th><th /></tr></thead><tbody>{visible.map((d: Dataset) => <tr key={d.dataset_id}><td className="mono">{d.dataset_id}</td><td>{d.owner}</td><td>{d.history_context}</td><td className="mono">{d.registration_hash}</td><td><Link to={`/datasets/${encodeURIComponent(d.dataset_id)}`}>Mở</Link></td></tr>)}</tbody></table></div> : <EmptyState title="Chưa có dataset đã đăng ký" detail="Catalog giới hạn theo human read grants." />}
    {unregistered.length > 0 && <div className="card"><h2>Được cấp quyền, chưa đăng ký</h2><ul>{unregistered.map(id => <li key={id}><Link to={`/datasets/${encodeURIComponent(id)}/registration`}>{id} — Đăng ký</Link></li>)}</ul></div>}
  </div>;
}

export function DatasetPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const query = useQuery({ queryKey: ['dataset', id], queryFn: ({ signal }) => endpoints.dataset(id, signal).then(r => r.data), enabled: Boolean(id) });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  const registration = query.data.registration as Record<string, unknown>;
  return <div className="stack"><PageHeader title={id} description="Registration snapshot hiện tại." actions={<StatusBadge value="Registered" />} />
    <div className="card stack"><h2>Registration</h2><pre>{JSON.stringify(registration, null, 2)}</pre><code>Hash: {String(query.data.registration_hash)}</code></div>
    <div className="row">{[['executions', 'Executions'], ['suites', 'Suites & Policies'], ['purposes', 'Purposes & Bindings'], ['compatibility', 'Compatibility'], ['publications', 'Publication'], ['registration', 'Chỉnh sửa registration']].map(([path, label]) => <Link key={path} to={`/datasets/${encodeURIComponent(id)}/${path}`}>{label}</Link>)}</div>
  </div>;
}
