import { Link } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { Execution } from '../../api/types';
import { ApiErrorAlert, StatusBadge } from '../../components/Primitives';

export function ExecutionReportCells({ execution }: { execution: Execution }) {
  const query = useQuery({
    queryKey: ['report', execution.execution_id],
    queryFn: ({ signal }) => endpoints.report(execution.execution_id, signal),
    refetchInterval: result => result.state.data?.pending && document.visibilityState === 'visible' ? 5000 : false,
  });
  if (query.isPending) return <td colSpan={3}>Loading report…</td>;
  if (query.isError) return <td colSpan={3}><ApiErrorAlert error={query.error} retry={() => void query.refetch()} /></td>;
  const result = query.data;
  if (result.pending) return <><td>Report pending</td><td>—</td><td>—</td></>;
  const { accepted, report } = result.payload;
  const verdict = execution.workflow === 'WRITE_GATE' ? accepted.decision?.decision : accepted.assessment;
  const counts = execution.workflow === 'WRITE_GATE'
    ? { candidate_rows: report.candidate_rows ?? 'Unknown', valid_rows: report.valid_rows ?? 'Unknown', quarantine_rows: report.quarantine_rows ?? 'Unknown' }
    : { measured_rows: report.measured_rows ?? 'Unknown' };
  return <><td>{verdict ? <StatusBadge value={verdict} /> : 'Unknown'}</td><td><pre>{JSON.stringify(counts)}</pre></td><td>{execution.workflow === 'WRITE_GATE' ? JSON.stringify(accepted.decision?.reasons ?? 'Unknown') : <>Reason list is not provided by the report API (<Link to={`/executions/${encodeURIComponent(execution.execution_id)}?tab=rules`}>rules</Link>)</>}</td></>;
}
