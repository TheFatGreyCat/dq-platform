import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSearchParams } from 'react-router';
import { endpoints } from '../../api/endpoints';
import { ApiErrorAlert, EmptyState, LoadingState, StatusBadge } from '../../components/Primitives';

interface RuleResultExplorerProps {
  executionId: string;
  plan: Record<string, unknown>;
  pending: boolean;
}

type RuleResult = Record<string, unknown> & {
  rule_id?: string;
  outcome?: string;
  measurement_status?: string;
  metric_name?: string | null;
  metric_value?: string | number | null;
  failed_rows?: string | number | null;
  action?: string;
  required?: boolean;
};

export function RuleResultExplorer({ executionId, plan, pending }: RuleResultExplorerProps) {
  const [params, setParams] = useSearchParams();
  const search = params.get('ruleSearch')?.toLowerCase() ?? '';
  const outcomeFilter = params.get('outcome') ?? '';
  const kindFilter = params.get('kind') ?? '';
  const stageFilter = params.get('stage') ?? '';
  const actionFilter = params.get('action') ?? '';
  const selectedRule = params.get('rule') ?? '';

  const query = useQuery({
    queryKey: ['rule-results-explorer', executionId],
    queryFn: async ({ signal }) => {
      const items: RuleResult[] = [];
      const cursors = new Set<string>();
      let cursor: string | null = null;
      for (let page = 0; page < 500; page += 1) {
        const queryParams = new URLSearchParams({ limit: '500' });
        if (cursor) queryParams.set('cursor', cursor);
        const response = await endpoints.executionRules(executionId, queryParams, signal);
        if (response.status === 202) return { pending: true, items: [] as RuleResult[] };
        if (!('items' in response.data) || !Array.isArray(response.data.items)) {
          throw new Error('Rule response không hợp lệ.');
        }
        items.push(...response.data.items as RuleResult[]);
        const next = response.data.next_cursor;
        if (!next) return { pending: false, items };
        if (cursors.has(next) || items.length > 500) throw new Error('Rule results vượt giới hạn 500 hoặc cursor lặp.');
        cursors.add(next);
        cursor = next;
      }
      throw new Error('Rule results vượt giới hạn 500.');
    },
    enabled: !pending,
  });

  const stageBindings = Array.isArray(plan.stage_bindings) ? plan.stage_bindings : [];
  const pinnedRules = useMemo(() => extractPinnedRules(stageBindings), [stageBindings]);

  const filtered = useMemo(() => {
    if (!query.data || query.data.pending) return [];
    return query.data.items
      .filter(item => {
        const ruleId = String(item.rule_id ?? '').toLowerCase();
        const pinned = pinnedRules.get(String(item.rule_id ?? ''));
        const kind = String(pinned?.kind ?? item.kind ?? '');
        const stage = String(pinned?.stage ?? item.stage ?? '');
        const outcome = String(item.outcome ?? '');
        const action = String(item.action ?? '');
        return (!search || ruleId.includes(search)) &&
          (!outcomeFilter || outcome === outcomeFilter) &&
          (!kindFilter || kind === kindFilter) &&
          (!stageFilter || stage === stageFilter) &&
          (!actionFilter || action === actionFilter);
      })
      .sort((a, b) => String(a.rule_id ?? '').localeCompare(String(b.rule_id ?? '')));
  }, [query.data, search, outcomeFilter, kindFilter, stageFilter, actionFilter, pinnedRules]);

  function updateFilter(name: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(name, value);
    else next.delete(name);
    next.delete('cursor');
    setParams(next);
  }

  if (pending) return <EmptyState title="Report chưa được gửi; rules đang pending" />;
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  if (query.data.pending) return <EmptyState title="Report đang pending" />;
  if (!filtered.length) return <EmptyState title="Không có rule result phù hợp" />;

  const selected = filtered.find(item => String(item.rule_id) === selectedRule);
  const selectedPinned = selected ? pinnedRules.get(String(selected.rule_id ?? '')) : undefined;

  return (
    <div className="stack">
      <div className="row" style={{ flexWrap: 'wrap', gap: 8 }}>
        <label>Search ID <input value={params.get('ruleSearch') ?? ''} onChange={e => updateFilter('ruleSearch', e.target.value)} /></label>
        <label>Outcome <select value={outcomeFilter} onChange={e => updateFilter('outcome', e.target.value)}><option value="">Tất cả</option><option>PASS</option><option>VIOLATION</option><option>ERROR</option></select></label>
        <label>Kind <input value={kindFilter} onChange={e => updateFilter('kind', e.target.value)} /></label>
        <label>Stage <input value={stageFilter} onChange={e => updateFilter('stage', e.target.value)} /></label>
        <label>Action <input value={actionFilter} onChange={e => updateFilter('action', e.target.value)} /></label>
      </div>
      <div className="table-wrap">
        <table>
          <thead><tr><th>Rule ID</th><th>Kind</th><th>Scope / Stage</th><th>Outcome</th><th>Measurement</th><th>Actual</th><th>Expected</th><th>Failed rows</th><th>Action / Required</th></tr></thead>
          <tbody>{filtered.map(item => {
            const ruleId = String(item.rule_id ?? '');
            const pinned = pinnedRules.get(ruleId);
            return <tr key={ruleId} onClick={() => updateFilter('rule', ruleId)} style={{ cursor: 'pointer' }}>
              <td className="mono">{ruleId}</td>
              <td>{String(pinned?.kind ?? item.kind ?? '—')}</td>
              <td>{String(pinned?.scope ?? item.scope ?? '—')} / {String(pinned?.stage ?? item.stage ?? '—')}</td>
              <td><StatusBadge value={String(item.outcome ?? 'ERROR')} /></td>
              <td>{String(item.measurement_status ?? '—')}</td>
              <td>{item.metric_value == null ? 'Không có measurement' : String(item.metric_value)}</td>
              <td>{pinned?.assertion ? JSON.stringify(pinned.assertion) : '—'}</td>
              <td>{item.failed_rows == null ? 'Không có measurement' : String(item.failed_rows)}</td>
              <td>{String(item.action ?? '—')} / {item.required === true ? 'true' : item.required === false ? 'false' : '—'}</td>
            </tr>;
          })}</tbody>
        </table>
      </div>
      {selected && <div className="card stack"><h3>Rule detail: {String(selected.rule_id)}</h3><p>Pinned resolved check từ execution plan; không dùng current registry revision.</p><pre>{JSON.stringify({ result: selected, pinned: selectedPinned ?? null }, null, 2)}</pre></div>}
    </div>
  );
}

function extractPinnedRules(bindings: unknown[]): Map<string, Record<string, unknown>> {
  const result = new Map<string, Record<string, unknown>>();
  for (const binding of bindings) {
    if (!binding || typeof binding !== 'object' || !('rules' in binding) || !Array.isArray(binding.rules)) continue;
    for (const rule of binding.rules) {
      if (!rule || typeof rule !== 'object' || !('rule_id' in rule) || typeof rule.rule_id !== 'string') continue;
      result.set(rule.rule_id, rule as Record<string, unknown>);
    }
  }
  return result;
}
