import { describe, expect, it } from 'vitest';
import type { Execution } from '../../api/types';
import { compareRules, decimalDelta, decisionOf, incompatibilities, toRuleRows, type RuleRow } from './ExecutionCompare';

const row = (over: Partial<RuleRow> = {}): RuleRow => ({
  ruleId: 'r1', scope: 'PARTITION', stage: 'CANDIDATE', kind: 'size', metricHash: 'h1', status: 'OK', value: '100', ...over,
});
const execution = (over: Partial<Execution> = {}): Execution => ({
  execution_id: 'e', dataset_id: 'ds', workflow: 'WRITE_GATE', purpose: 'p', state: 'COMPLETED', publication_state: 'NONE',
  unresolved: false, plan: {}, attempts: [], outcomes: {}, ...over,
});

describe('decimalDelta', () => {
  it('is exact beyond float precision', () => {
    expect(decimalDelta('0.1', '0.3')).toBe('0.2');
    expect(decimalDelta('9007199254740993', '9007199254740994')).toBe('1');
    expect(decimalDelta('1.50', '1.5')).toBe('0');
    expect(decimalDelta('1', '0.25')).toBe('-0.75');
  });
  it('is null for non-decimal input', () => {
    expect(decimalDelta('1e3', '2')).toBeNull();
  });
});

describe('compareRules', () => {
  it('reports exact numeric delta for changed values', () => {
    const [diff] = compareRules([row({ value: '0.9744' })], [row({ value: '0.9801' })]);
    expect(diff).toMatchObject({ status: 'changed', delta: '0.0057' });
  });

  it('treats equal decimals as same', () => {
    expect(compareRules([row({ value: '1.0' })], [row({ value: '1' })])[0]!.status).toBe('same');
  });

  it('flags metric definition hash change instead of a numeric delta', () => {
    const [diff] = compareRules([row()], [row({ metricHash: 'h2', value: '999' })]);
    expect(diff).toMatchObject({ status: 'definition-changed', delta: null });
  });

  it('keeps pending/missing/errored measurements unavailable, never zero', () => {
    expect(compareRules(null, [row()])[0]).toMatchObject({ status: 'unavailable', delta: null });
    expect(compareRules([row({ status: 'ERROR', value: null })], [row({ value: '0' })])[0]).toMatchObject({ status: 'unavailable', delta: null });
    expect(compareRules([row()], [])[0]!.status).toBe('missing-right');
    expect(compareRules([], [row()])[0]!.status).toBe('missing-left');
  });

  it('matches by rule_id, scope, stage and kind', () => {
    const diffs = compareRules([row()], [row({ stage: 'CLEAN' })]);
    expect(diffs.map(d => d.status).sort()).toEqual(['missing-left', 'missing-right']);
  });
});

describe('toRuleRows', () => {
  it('falls back to pinned plan binding and keeps metric values as strings', () => {
    const plan = { stage_bindings: [{ scope: 'TABLE', stage: 'CLEAN', rules: [{ rule_id: 'r1', kind: 'mean', metric_definition_hash: 'sha256:m' }] }] };
    expect(toRuleRows([{ rule_id: 'r1', measurement_status: 'OK', metric_value: '0.10' }, { nope: 1 }], plan)).toEqual([
      { ruleId: 'r1', scope: 'TABLE', stage: 'CLEAN', kind: 'mean', metricHash: 'sha256:m', status: 'OK', value: '0.10' },
    ]);
  });
});

describe('incompatibilities and decisions', () => {
  it('flags workflow and dataset mismatch', () => {
    expect(incompatibilities(execution(), execution())).toEqual([]);
    expect(incompatibilities(execution(), execution({ workflow: 'AUDIT_TABLE' }))).toHaveLength(1);
    expect(incompatibilities(execution(), execution({ dataset_id: 'other', workflow: 'AUDIT_TABLE' }))).toHaveLength(2);
  });

  it('decision is null while the report is pending', () => {
    expect(decisionOf({ pending: true, payload: { execution_id: 'e', state: 'RUNNING' } })).toBeNull();
    const accepted = { decision: { decision: 'DENY' as const, reasons: [] }, assessment: null };
    expect(decisionOf({ pending: false, payload: { accepted, report: {}, links: {} } })).toBe('DENY');
  });
});
