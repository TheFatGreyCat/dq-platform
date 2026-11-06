import type { Policy, PolicyRule, Rule, Suite } from '../../api/types';

export interface FieldChange {
  path: string;
  left: unknown;
  right: unknown;
}

export type RuleDiffItem<T> =
  | { rule_id: string; changeType: 'added'; right: T }
  | { rule_id: string; changeType: 'removed'; left: T }
  | { rule_id: string; changeType: 'changed'; left: T; right: T; changes: FieldChange[] }
  | { rule_id: string; changeType: 'unchanged'; rule: T };

export interface SuiteDiffResult {
  suiteLevelChanges: { field: string; left: unknown; right: unknown }[];
  ruleChanges: RuleDiffItem<Rule>[];
}

export interface PolicyDiffResult {
  globalChanges: { field: string; left: unknown; right: unknown }[];
  ruleChanges: RuleDiffItem<PolicyRule>[];
}

function areValuesEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a === b;
  if (typeof a !== 'object' || typeof b !== 'object') return false;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!areValuesEqual(a[i], b[i])) return false;
    }
    return true;
  }

  if (Array.isArray(a) || Array.isArray(b)) return false;

  const aObj = a as Record<string, unknown>;
  const bObj = b as Record<string, unknown>;
  const aKeys = Object.keys(aObj);
  const bKeys = Object.keys(bObj);

  const allKeys = new Set([...aKeys, ...bKeys]);
  for (const k of allKeys) {
    if (!areValuesEqual(aObj[k], bObj[k])) return false;
  }
  return true;
}

function compareObjects(
  prefix: string,
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  outChanges: FieldChange[]
) {
  const allKeys = new Set([...Object.keys(left), ...Object.keys(right)]);
  for (const k of allKeys) {
    const lVal = left[k];
    const rVal = right[k];
    const path = prefix ? `${prefix}.${k}` : k;

    if (!areValuesEqual(lVal, rVal)) {
      if (
        lVal &&
        rVal &&
        typeof lVal === 'object' &&
        typeof rVal === 'object' &&
        !Array.isArray(lVal) &&
        !Array.isArray(rVal)
      ) {
        compareObjects(path, lVal as Record<string, unknown>, rVal as Record<string, unknown>, outChanges);
      } else {
        outChanges.push({ path, left: lVal, right: rVal });
      }
    }
  }
}

export function diffSuitePayloads(left: Suite, right: Suite): SuiteDiffResult {
  const suiteLevelChanges: { field: string; left: unknown; right: unknown }[] = [];

  if (left.description !== right.description) {
    suiteLevelChanges.push({
      field: 'description',
      left: left.description ?? null,
      right: right.description ?? null,
    });
  }

  if (!areValuesEqual(left.parameters ?? {}, right.parameters ?? {})) {
    suiteLevelChanges.push({
      field: 'parameters',
      left: left.parameters ?? {},
      right: right.parameters ?? {},
    });
  }

  const leftRules = new Map<string, Rule>();
  for (const r of left.rules) {
    leftRules.set(r.rule_id, r);
  }

  const rightRules = new Map<string, Rule>();
  for (const r of right.rules) {
    rightRules.set(r.rule_id, r);
  }

  const ruleChanges: RuleDiffItem<Rule>[] = [];
  const processedIds = new Set<string>();

  // Check left rules
  for (const [id, lRule] of leftRules) {
    processedIds.add(id);
    const rRule = rightRules.get(id);
    if (!rRule) {
      ruleChanges.push({ rule_id: id, changeType: 'removed', left: lRule });
    } else {
      const fieldChanges: FieldChange[] = [];
      compareObjects('', lRule as unknown as Record<string, unknown>, rRule as unknown as Record<string, unknown>, fieldChanges);
      if (fieldChanges.length > 0) {
        ruleChanges.push({ rule_id: id, changeType: 'changed', left: lRule, right: rRule, changes: fieldChanges });
      } else {
        ruleChanges.push({ rule_id: id, changeType: 'unchanged', rule: lRule });
      }
    }
  }

  // Check added rules in right
  for (const [id, rRule] of rightRules) {
    if (!processedIds.has(id)) {
      ruleChanges.push({ rule_id: id, changeType: 'added', right: rRule });
    }
  }

  return { suiteLevelChanges, ruleChanges };
}

export function diffPolicyPayloads(left: Policy, right: Policy): PolicyDiffResult {
  const globalChanges: { field: string; left: unknown; right: unknown }[] = [];

  if (left.min_candidate_rows !== right.min_candidate_rows) {
    globalChanges.push({
      field: 'min_candidate_rows',
      left: left.min_candidate_rows ?? null,
      right: right.min_candidate_rows ?? null,
    });
  }

  if (left.min_clean_rows !== right.min_clean_rows) {
    globalChanges.push({
      field: 'min_clean_rows',
      left: left.min_clean_rows ?? null,
      right: right.min_clean_rows ?? null,
    });
  }

  if (left.max_quarantine_ratio !== right.max_quarantine_ratio) {
    globalChanges.push({
      field: 'max_quarantine_ratio',
      left: left.max_quarantine_ratio,
      right: right.max_quarantine_ratio,
    });
  }

  const leftAnomaly = left.anomaly;
  const rightAnomaly = right.anomaly;
  if (!areValuesEqual(leftAnomaly, rightAnomaly)) {
    if (leftAnomaly.enabled !== rightAnomaly.enabled) {
      globalChanges.push({
        field: 'anomaly.enabled',
        left: leftAnomaly.enabled,
        right: rightAnomaly.enabled,
      });
    }
    if (leftAnomaly.enabled && rightAnomaly.enabled) {
      if (leftAnomaly.max_points !== rightAnomaly.max_points) {
        globalChanges.push({ field: 'anomaly.max_points', left: leftAnomaly.max_points, right: rightAnomaly.max_points });
      }
      if (leftAnomaly.lower !== rightAnomaly.lower) {
        globalChanges.push({ field: 'anomaly.lower', left: leftAnomaly.lower, right: rightAnomaly.lower });
      }
      if (leftAnomaly.upper !== rightAnomaly.upper) {
        globalChanges.push({ field: 'anomaly.upper', left: leftAnomaly.upper, right: rightAnomaly.upper });
      }
      if (leftAnomaly.bootstrap !== rightAnomaly.bootstrap) {
        globalChanges.push({ field: 'anomaly.bootstrap', left: leftAnomaly.bootstrap, right: rightAnomaly.bootstrap });
      }
    }
  }

  const leftRules = new Map<string, PolicyRule>();
  for (const r of left.rules) {
    leftRules.set(r.rule_id, r);
  }

  const rightRules = new Map<string, PolicyRule>();
  for (const r of right.rules) {
    rightRules.set(r.rule_id, r);
  }

  const ruleChanges: RuleDiffItem<PolicyRule>[] = [];
  const processedIds = new Set<string>();

  for (const [id, lRule] of leftRules) {
    processedIds.add(id);
    const rRule = rightRules.get(id);
    if (!rRule) {
      ruleChanges.push({ rule_id: id, changeType: 'removed', left: lRule });
    } else {
      const fieldChanges: FieldChange[] = [];
      if (lRule.action !== rRule.action) {
        fieldChanges.push({ path: 'action', left: lRule.action, right: rRule.action });
      }
      if (lRule.required !== rRule.required) {
        fieldChanges.push({ path: 'required', left: lRule.required, right: rRule.required });
      }
      if (fieldChanges.length > 0) {
        ruleChanges.push({ rule_id: id, changeType: 'changed', left: lRule, right: rRule, changes: fieldChanges });
      } else {
        ruleChanges.push({ rule_id: id, changeType: 'unchanged', rule: lRule });
      }
    }
  }

  for (const [id, rRule] of rightRules) {
    if (!processedIds.has(id)) {
      ruleChanges.push({ rule_id: id, changeType: 'added', right: rRule });
    }
  }

  return { globalChanges, ruleChanges };
}
