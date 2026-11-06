import type { ExecutionState, Workflow } from '../../api/types';

export type ExecutionFilters = {
  states: ExecutionState[];
  workflow: Workflow | null;
  purpose: string;
  ruleId: string;
  outcome: string;
  cursor: string | null;
};

const STATES: readonly ExecutionState[] = ['PLANNED', 'RUNNING', 'REPORTED', 'COMPLETED', 'FAILED', 'CANCELLED'];

export function parseExecutionFilters(params: URLSearchParams): ExecutionFilters {
  const states = params.getAll('state').filter((value): value is ExecutionState => STATES.includes(value as ExecutionState));
  const workflowValue = params.get('workflow');
  const workflow = workflowValue === 'WRITE_GATE' || workflowValue === 'AUDIT_TABLE' ? workflowValue : null;
  return {
    states,
    workflow,
    purpose: params.get('purpose') ?? '',
    ruleId: params.get('rule_id') ?? '',
    outcome: params.get('outcome') ?? '',
    cursor: params.get('cursor'),
  };
}

export function serializeExecutionFilters(filters: ExecutionFilters): URLSearchParams {
  const params = new URLSearchParams();
  for (const state of filters.states) params.append('state', state);
  if (filters.workflow) params.set('workflow', filters.workflow);
  if (filters.purpose) params.set('purpose', filters.purpose);
  if (filters.ruleId) params.set('rule_id', filters.ruleId);
  if (filters.outcome) params.set('outcome', filters.outcome);
  if (filters.cursor) params.set('cursor', filters.cursor);
  return params;
}

export function withExecutionFilter(params: URLSearchParams, name: string, value: string): URLSearchParams {
  const next = new URLSearchParams(params);
  if (name === 'state') {
    const states = new Set(next.getAll('state'));
    if (states.has(value)) states.delete(value); else states.add(value);
    next.delete('state');
    for (const state of states) next.append('state', state);
  } else if (value) {
    next.set(name, value);
  } else {
    next.delete(name);
  }
  next.delete('cursor');
  return next;
}
