import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { Execution, ReportResult } from '../../api/types';
import { ApiErrorAlert, LoadingState, StatusBadge } from '../../components/Primitives';

export type RuleRow = { ruleId: string; scope: string; stage: string; kind: string; metricHash: string | null; status: string | null; value: string | null };
export type RuleDiffStatus = 'same' | 'changed' | 'definition-changed' | 'unavailable' | 'missing-left' | 'missing-right';
export type RuleDiff = { ruleId: string; scope: string; stage: string; kind: string; left: RuleRow | null; right: RuleRow | null; status: RuleDiffStatus; delta: string | null };
export type Side = { execution: Execution; report: ReportResult; rules: RuleRow[] | null };

const DECIMAL = /^([+-]?)(\d+)(?:\.(\d+))?$/;
const MAX_RULE_PAGES = 100;
const text = (value: unknown): string | null => (typeof value === 'string' || typeof value === 'number' ? String(value) : null);
const ruleKey = (r: { ruleId: string; scope: string; stage: string; kind: string }) => JSON.stringify([r.ruleId, r.scope, r.stage, r.kind]);

function parseDecimal(value: string): { units: bigint; scale: number } | null {
  const match = DECIMAL.exec(value.trim());
  if (!match) return null;
  const fraction = match[3] ?? '';
  const units = BigInt(match[2]! + fraction);
  return { units: match[1] === '-' ? -units : units, scale: fraction.length };
}

/** Exact right − left on decimal strings; null when either side is not a plain decimal. */
export function decimalDelta(left: string, right: string): string | null {
  const a = parseDecimal(left);
  const b = parseDecimal(right);
  if (!a || !b) return null;
  const scale = Math.max(a.scale, b.scale);
  const diff = b.units * 10n ** BigInt(scale - b.scale) - a.units * 10n ** BigInt(scale - a.scale);
  const digits = (diff < 0n ? -diff : diff).toString().padStart(scale + 1, '0');
  const whole = digits.slice(0, digits.length - scale);
  const fraction = digits.slice(digits.length - scale).replace(/0+$/, '');
  return `${diff < 0n ? '-' : ''}${whole}${fraction ? `.${fraction}` : ''}`;
}

type PlanBinding = { scope?: unknown; stage?: unknown; rules?: { rule_id?: unknown; kind?: unknown; metric_definition_hash?: unknown }[] };

/** Rule rows from rule-result payloads; scope/stage/kind/metric hash fall back to the pinned plan binding. */
export function toRuleRows(items: Record<string, unknown>[], plan: Record<string, unknown>): RuleRow[] {
  const pinned = new Map<string, { scope: string | null; stage: string | null; kind: string | null; metricHash: string | null }>();
  const bindings = (Array.isArray(plan.stage_bindings) ? plan.stage_bindings : []) as PlanBinding[];
  for (const binding of bindings) {
    for (const rule of Array.isArray(binding?.rules) ? binding.rules : []) {
      if (typeof rule?.rule_id === 'string') {
        pinned.set(rule.rule_id, { scope: text(binding.scope), stage: text(binding.stage), kind: text(rule.kind), metricHash: text(rule.metric_definition_hash) });
      }
    }
  }
  return items.flatMap(item => {
    const ruleId = text(item.rule_id);
    if (!ruleId) return [];
    const pin = pinned.get(ruleId);
    return [{
      ruleId, scope: text(item.scope) ?? pin?.scope ?? '', stage: text(item.stage) ?? pin?.stage ?? '', kind: text(item.kind) ?? pin?.kind ?? '',
      metricHash: text(item.metric_definition_hash) ?? pin?.metricHash ?? null,
      status: text(item.measurement_status), value: text(item.metric_value),
    }];
  });
}

const isAvailable = (row: RuleRow) => row.status === 'OK' && row.value !== null;

function diffRule(left: RuleRow | null, right: RuleRow | null): Pick<RuleDiff, 'status' | 'delta'> {
  if (!left) return { status: 'missing-left', delta: null };
  if (!right) return { status: 'missing-right', delta: null };
  if (!isAvailable(left) || !isAvailable(right)) return { status: 'unavailable', delta: null };
  if (left.metricHash && right.metricHash && left.metricHash !== right.metricHash) return { status: 'definition-changed', delta: null };
  const delta = decimalDelta(left.value!, right.value!);
  if (delta === null) return { status: left.value === right.value ? 'same' : 'changed', delta: null };
  return { status: delta === '0' ? 'same' : 'changed', delta };
}

/** Match by rule_id/scope/stage/kind. A null side (report pending) is unavailable, never zero. */
export function compareRules(left: RuleRow[] | null, right: RuleRow[] | null): RuleDiff[] {
  const index = (rows: RuleRow[] | null) => new Map((rows ?? []).map(row => [ruleKey(row), row]));
  const l = index(left);
  const r = index(right);
  return [...new Set([...l.keys(), ...r.keys()])].sort().map(key => {
    const a = l.get(key) ?? null;
    const b = r.get(key) ?? null;
    const id = (a ?? b)!;
    const outcome = left === null || right === null ? { status: 'unavailable' as const, delta: null } : diffRule(a, b);
    return { ruleId: id.ruleId, scope: id.scope, stage: id.stage, kind: id.kind, left: a, right: b, ...outcome };
  });
}

/** Banner reasons that make a numeric comparison invalid. */
export function incompatibilities(left: Execution, right: Execution): string[] {
  const reasons: string[] = [];
  if (left.dataset_id !== right.dataset_id) reasons.push(`Khác dataset: ${left.dataset_id} ≠ ${right.dataset_id}.`);
  if (left.workflow !== right.workflow) reasons.push(`Workflow không tương thích: ${left.workflow} ≠ ${right.workflow}.`);
  return reasons;
}

export const decisionOf = (report: ReportResult): string | null =>
  report.pending ? null : report.payload.accepted.decision?.decision ?? report.payload.accepted.assessment;

async function loadRules(id: string, execution: Execution, signal: AbortSignal): Promise<RuleRow[] | null> {
  const items: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  let cursor: string | null = null;
  for (let page = 0; page < MAX_RULE_PAGES; page += 1) {
    const query = new URLSearchParams({ limit: '500' });
    if (cursor) query.set('cursor', cursor);
    const response = await endpoints.executionRules(id, query, signal);
    if (response.status === 202) return null;
    if (!('items' in response.data) || !Array.isArray(response.data.items)) throw new Error('Rule response không hợp lệ.');
    items.push(...response.data.items);
    cursor = response.data.next_cursor;
    if (!cursor) return toRuleRows(items, execution.plan);
    if (seen.has(cursor)) throw new Error('Rule cursor lặp.');
    seen.add(cursor);
  }
  throw new Error('Rule results vượt giới hạn trang.');
}

async function loadSide(id: string, signal: AbortSignal): Promise<Side> {
  const [executionResponse, report] = await Promise.all([endpoints.execution(id, signal), endpoints.report(id, signal)]);
  const execution = executionResponse.data;
  return { execution, report, rules: report.pending ? null : await loadRules(id, execution, signal) };
}

const UNAVAILABLE = 'Unavailable';
const STATUS_LABEL: Record<RuleDiffStatus, string> = {
  same: 'Same', changed: 'Changed', 'definition-changed': 'Definition changed', unavailable: UNAVAILABLE,
  'missing-left': 'Missing in left', 'missing-right': 'Missing in right',
};
const cell = (row: RuleRow | null) => (row && isAvailable(row) ? row.value : UNAVAILABLE);

function MetaRow({ label, left, right }: { label: string; left: string | null | undefined; right: string | null | undefined }) {
  return <tr><th scope="row">{label}</th><td className="mono">{left ?? UNAVAILABLE}</td><td className="mono">{right ?? UNAVAILABLE}</td></tr>;
}

function DecisionCell({ side }: { side: Side }) {
  return side.report.pending ? <>{UNAVAILABLE} (report {side.report.payload.state})</> : <StatusBadge value={decisionOf(side.report)} />;
}

export function ExecutionCompare({ leftId, rightId }: { leftId: string; rightId: string }) {
  const query = useQuery({
    queryKey: ['execution-compare', leftId, rightId],
    queryFn: async ({ signal }) => {
      const [left, right] = await Promise.all([loadSide(leftId, signal), loadSide(rightId, signal)]);
      return { left, right };
    },
  });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;

  const { left, right } = query.data;
  const problems = incompatibilities(left.execution, right.execution);
  const diffs = problems.length ? [] : compareRules(left.rules, right.rules);
  const l = left.execution;
  const r = right.execution;
  return <div className="stack">
    {problems.length > 0 && <div className="card" role="alert">{problems.map(problem => <div key={problem}>{problem}</div>)}<div>Không so sánh số liệu rule.</div></div>}
    <section className="card stack"><h2>Metadata</h2><div className="table-wrap"><table><thead><tr><th /><th>Left</th><th>Right</th></tr></thead><tbody>
      <MetaRow label="Execution" left={l.execution_id} right={r.execution_id} />
      <MetaRow label="Dataset" left={l.dataset_id} right={r.dataset_id} />
      <MetaRow label="Workflow" left={l.workflow} right={r.workflow} />
      <MetaRow label="Purpose" left={l.purpose} right={r.purpose} />
      <MetaRow label="State" left={l.state} right={r.state} />
      <MetaRow label="Event time" left={text(l.plan.event_time)} right={text(r.plan.event_time)} />
      <MetaRow label="Plan hash" left={l.plan.plan_hash} right={r.plan.plan_hash} />
    </tbody></table></div></section>
    <section className="card stack"><h2>Gate decision</h2><div className="row"><DecisionCell side={left} /><DecisionCell side={right} /></div></section>
    <section className="card stack"><h2>Publication</h2><div className="table-wrap"><table><tbody>
      <MetaRow label="Publication state" left={l.publication_state} right={r.publication_state} />
      <MetaRow label="Unresolved" left={String(l.unresolved)} right={String(r.unresolved)} />
    </tbody></table></div></section>
    {!problems.length && <section className="card stack"><h2>Rules</h2>{diffs.length ? <div className="table-wrap"><table>
      <thead><tr><th>Rule</th><th>Scope</th><th>Stage</th><th>Kind</th><th>Left</th><th>Right</th><th>Delta</th><th>Result</th></tr></thead>
      <tbody>{diffs.map(d => <tr key={ruleKey(d)}>
        <td className="mono">{d.ruleId}</td><td>{d.scope}</td><td>{d.stage}</td><td>{d.kind}</td>
        <td className="mono">{cell(d.left)}</td><td className="mono">{cell(d.right)}</td>
        <td className="mono">{d.delta ?? UNAVAILABLE}</td><td>{STATUS_LABEL[d.status]}</td>
      </tr>)}</tbody></table></div> : <div>{UNAVAILABLE}: không có rule result.</div>}</section>}
  </div>;
}
