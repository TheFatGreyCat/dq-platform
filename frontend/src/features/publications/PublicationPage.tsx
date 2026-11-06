import { useState } from 'react';
import { useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import { ApiErrorAlert, EmptyState, LoadingState, PageHeader } from '../../components/Primitives';

export function PublicationPage() {
  const { datasetId = '' } = useParams();
  const id = decodeURIComponent(datasetId);
  const [dt, setDt] = useState('');
  const query = useQuery({ queryKey: ['publications', id, dt], enabled: false,
    queryFn: ({ signal }) => endpoints.publication(id, dt, signal).then(r => r.data) });
  return <div className="stack"><PageHeader title="Publication" description="Đối soát trạng thái đã công bố theo partition date. Chỉ đọc." />
    <div className="card row"><label>Partition date (dt)<input type="date" value={dt} onChange={e => setDt(e.target.value)} required /></label>
      <button className="primary" disabled={!dt || query.isFetching} onClick={() => void query.refetch()}>Tải publication</button></div>
    {query.isFetching && <LoadingState />}{query.isError && <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />}
    {query.data && <div className="card"><pre>{JSON.stringify(query.data, null, 2)}</pre>{query.data.current === null && <EmptyState title="Chưa có publication hiện tại" />}
      {query.data.restore_orphans.length > 0 && <div role="alert">Có restore/orphan marker; cần xử lý qua operator tools.</div>}</div>}
  </div>;
}
