import type {
  Action,
  AnomalyConfig,
  Assertion,
  Bound,
  Check,
  Dimension,
  Policy,
  Rule,
  Scope,
  Stage,
  Suite,
  Workflow,
} from '../../api/types';

export const ROW_LEVEL_KINDS = [
  'complete',
  'completeness',
  'unique',
  'pattern',
  'range',
  'in_set',
  'sql_predicate',
] as const;

export type RowLevelKind = (typeof ROW_LEVEL_KINDS)[number];

export const WORKFLOW_STAGES: Record<Workflow, readonly Stage[]> = {
  WRITE_GATE: ['RAW', 'CANDIDATE', 'CLEAN'],
  AUDIT_TABLE: ['AUDIT_SNAPSHOT'],
};

export const CHECK_KINDS = [
  'size',
  'complete',
  'completeness',
  'unique',
  'pattern',
  'range',
  'in_set',
  'mean',
  'sql_predicate',
  'sql_metric',
  'schema_columns',
  'schema_type',
] as const;

export type CheckKind = (typeof CHECK_KINDS)[number];

export const DIMENSIONS: readonly Dimension[] = [
  'completeness',
  'uniqueness',
  'validity',
  'accuracy',
  'consistency',
  'timeliness',
];

export interface RuleDraft {
  editorKey: string;
  rule_id: string;
  dimension: Dimension;
  scope: Scope;
  stage: Stage;
  description: string;
  check: Check;
}

export interface SuiteDraft {
  workflow: Workflow;
  description: string;
  parameters: Record<string, string>;
  rules: RuleDraft[];
}

export interface PolicyRuleDraft {
  rule_id: string;
  action: Action;
  required: boolean;
}

export interface PolicyDraft {
  rules: PolicyRuleDraft[];
  min_candidate_rows: string;
  min_clean_rows: string;
  max_quarantine_ratio: string;
  anomaly: {
    enabled: boolean;
    max_points: string;
    lower: string;
    upper: string;
    bootstrap: boolean;
  };
}

export interface ValidationError {
  field: string;
  message: string;
}

export function isQuarantineEligible(kind: string, stage: Stage, scope: Scope): boolean {
  return (
    ROW_LEVEL_KINDS.includes(kind as RowLevelKind) &&
    stage === 'CANDIDATE' &&
    scope === 'PARTITION'
  );
}

export function canonicalDecimalOrNull(raw: string): string | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  // Disallow scientific notation, multiple dots, whitespace
  if (/[eE]/.test(trimmed) || /\s/.test(trimmed)) return null;
  // Match standard decimal: optional negative sign, digits, optional dot with digits
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(trimmed)) return null;
  if (trimmed === '-0') return null;
  if (trimmed.includes('.')) {
    // Trailing zeros after decimal point are not canonical (e.g. 1.50)
    if (trimmed.endsWith('0')) return null;
    if (trimmed.endsWith('.')) return null;
  }
  return trimmed;
}

export function isValidInt64String(raw: string): boolean {
  const trimmed = raw.trim();
  if (!/^-?(?:0|[1-9]\d*)$/.test(trimmed)) return false;
  if (trimmed === '-0') return false;
  try {
    const val = BigInt(trimmed);
    const min = -9223372036854775808n;
    const max = 9223372036854775807n;
    return val >= min && val <= max;
  } catch {
    return false;
  }
}

export function createEmptyRuleDraft(rule_id: string, workflow: Workflow): RuleDraft {
  const defaultStage = workflow === 'WRITE_GATE' ? 'CANDIDATE' : 'AUDIT_SNAPSHOT';
  const defaultScope = workflow === 'WRITE_GATE' ? 'PARTITION' : 'TABLE';
  return {
    editorKey: `k_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    rule_id,
    dimension: 'validity',
    scope: defaultScope,
    stage: defaultStage,
    description: '',
    check: {
      kind: 'complete',
      column: '',
      assert: { op: '==', value: '1' },
    },
  };
}

export function createDefaultPolicyDraft(rules: readonly (Rule | RuleDraft)[]): PolicyDraft {
  return {
    rules: rules.map(r => ({
      rule_id: r.rule_id,
      action: 'OBSERVE',
      required: true,
    })),
    min_candidate_rows: '1',
    min_clean_rows: '1',
    max_quarantine_ratio: '0',
    anomaly: {
      enabled: false,
      max_points: '30',
      lower: '0',
      upper: '1',
      bootstrap: false,
    },
  };
}

function extractParamRefsFromBound(bound: Bound | null | undefined): string[] {
  if (!bound) return [];
  if (typeof bound === 'object' && 'param' in bound && typeof bound.param === 'string') {
    return [bound.param];
  }
  return [];
}

function extractParamRefsFromAssertion(assert?: Assertion | null): string[] {
  if (!assert) return [];
  return [
    ...extractParamRefsFromBound(assert.value),
    ...extractParamRefsFromBound(assert.min),
    ...extractParamRefsFromBound(assert.max),
  ];
}

function extractParamRefsFromCheck(check: Check): string[] {
  const refs: string[] = [];
  if ('assert' in check) {
    refs.push(...extractParamRefsFromAssertion(check.assert));
  }
  if ('min' in check && check.min) {
    refs.push(...extractParamRefsFromBound(check.min));
  }
  if ('max' in check && check.max) {
    refs.push(...extractParamRefsFromBound(check.max));
  }
  if ('predicate' in check && typeof check.predicate === 'string') {
    const matches = check.predicate.match(/\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}/g);
    if (matches) {
      for (const m of matches) {
        refs.push(m.slice(2, -1));
      }
    }
  }
  if ('sql' in check && typeof check.sql === 'string') {
    const matches = check.sql.match(/\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}/g);
    if (matches) {
      for (const m of matches) {
        refs.push(m.slice(2, -1));
      }
    }
  }
  return refs;
}

export function validateDraftSuite(draft: SuiteDraft): ValidationError[] {
  const errors: ValidationError[] = [];
  if (draft.rules.length === 0) {
    errors.push({ field: 'rules', message: 'Suite phải chứa ít nhất 1 rule (tối đa 500).' });
  }
  if (draft.rules.length > 500) {
    errors.push({ field: 'rules', message: 'Suite vượt quá giới hạn 500 rules.' });
  }

  const ids = draft.rules.map(r => r.rule_id.trim());
  const seenIds = new Set<string>();
  const duplicateIds = new Set<string>();
  for (const id of ids) {
    if (!id) continue;
    if (seenIds.has(id)) duplicateIds.add(id);
    seenIds.add(id);
  }

  const declaredParams = new Set(Object.keys(draft.parameters));
  if (declaredParams.has('batch')) {
    errors.push({ field: 'parameters.batch', message: "Tham số 'batch' dành riêng cho hệ thống." });
  }

  const allowedStages = WORKFLOW_STAGES[draft.workflow];

  draft.rules.forEach((rule, idx) => {
    const prefix = `rules.${idx}`;
    const trimmedId = rule.rule_id.trim();
    if (!trimmedId) {
      errors.push({ field: `${prefix}.rule_id`, message: 'Rule ID không được để trống.' });
    } else if (!/^[a-zA-Z0-9_.-]{1,128}$/.test(trimmedId)) {
      errors.push({ field: `${prefix}.rule_id`, message: 'Rule ID chỉ chứa a-z, A-Z, 0-9, _, ., - và tối đa 128 ký tự.' });
    } else if (duplicateIds.has(trimmedId)) {
      errors.push({ field: `${prefix}.rule_id`, message: `Rule ID '${trimmedId}' bị trùng.` });
    }

    if (!allowedStages.includes(rule.stage)) {
      errors.push({
        field: `${prefix}.stage`,
        message: `Stage '${rule.stage}' không hợp lệ cho workflow ${draft.workflow} (cho phép: ${allowedStages.join(', ')}).`,
      });
    }

    // Check specific validation
    const refs = extractParamRefsFromCheck(rule.check);
    for (const ref of refs) {
      if (ref === 'batch') continue;
      if (!declaredParams.has(ref)) {
        errors.push({
          field: `${prefix}.check`,
          message: `Rule '${rule.rule_id}' tham chiếu tham số '${ref}' chưa khai báo trong suite parameters.`,
        });
      }
    }

    if ('assert' in rule.check && rule.check.assert) {
      const a = rule.check.assert;
      if (a.op === 'between') {
        const minStr = a.min ? String(typeof a.min === 'object' ? a.min.param : a.min) : '';
        const maxStr = a.max ? String(typeof a.max === 'object' ? a.max.param : a.max) : '';
        const minDec = canonicalDecimalOrNull(minStr);
        const maxDec = canonicalDecimalOrNull(maxStr);
        if (minDec !== null && maxDec !== null && Number(minDec) > Number(maxDec)) {
          errors.push({
            field: `${prefix}.check.assert`,
            message: `Assertion between: min > max (${minDec} > ${maxDec}).`,
          });
        }
      }
    }
  });

  return errors;
}

export function validateDraftPolicy(draft: PolicyDraft, suiteRules: readonly (Rule | RuleDraft)[]): ValidationError[] {
  const errors: ValidationError[] = [];

  const ratio = canonicalDecimalOrNull(draft.max_quarantine_ratio);
  if (ratio === null || Number(ratio) < 0 || Number(ratio) > 1) {
    errors.push({
      field: 'max_quarantine_ratio',
      message: 'max_quarantine_ratio phải là số thập phân chuẩn trong khoảng [0, 1].',
    });
  }

  const suiteRuleMap = new Map<string, Rule | RuleDraft>();
  for (const r of suiteRules) {
    suiteRuleMap.set(r.rule_id.trim(), r);
  }

  draft.rules.forEach((pr, idx) => {
    const prefix = `rules.${idx}`;
    const rule = suiteRuleMap.get(pr.rule_id);
    if (!rule) {
      errors.push({
        field: `${prefix}.rule_id`,
        message: `Policy khai báo rule_id '${pr.rule_id}' không tồn tại trong suite.`,
      });
      return;
    }

    if (pr.action === 'QUARANTINE') {
      const eligible = isQuarantineEligible(rule.check.kind, rule.stage, rule.scope);
      if (!eligible) {
        errors.push({
          field: `${prefix}.action`,
          message: `Rule '${pr.rule_id}': action QUARANTINE chỉ hợp lệ cho row-level check ở stage CANDIDATE và scope PARTITION.`,
        });
      }
    }
  });

  if (draft.anomaly.enabled) {
    const lower = canonicalDecimalOrNull(draft.anomaly.lower);
    const upper = canonicalDecimalOrNull(draft.anomaly.upper);
    if (lower === null || upper === null) {
      errors.push({ field: 'anomaly', message: 'Lower và Upper của anomaly phải là số thập phân chuẩn.' });
    } else if (Number(lower) < 0 || Number(lower) > Number(upper)) {
      errors.push({ field: 'anomaly', message: 'Anomaly yêu cầu 0 <= lower <= upper.' });
    }
  }

  return errors;
}

export function serializeSuitePayload(draft: SuiteDraft): string {
  const payload: Suite = {
    format: 'dq-rules/v3',
    workflow: draft.workflow,
    description: draft.description.trim() || null,
    parameters: draft.parameters,
    rules: draft.rules.map(r => ({
      rule_id: r.rule_id.trim(),
      dimension: r.dimension,
      scope: r.scope,
      stage: r.stage,
      description: r.description.trim() || null,
      check: r.check,
    })),
  };

  return JSON.stringify(payload, (_key, value) => {
    if (typeof value === 'bigint') {
      return value.toString();
    }
    return value;
  }, 2);
}

export function parseSuitePayload(rawJson: string, defaultWorkflow: Workflow): { draft: SuiteDraft | null; error: string | null } {
  try {
    const parsed: unknown = JSON.parse(rawJson);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { draft: null, error: 'JSON payload phải là một object.' };
    }
    const obj = parsed as Record<string, unknown>;
    if (obj.format !== 'dq-rules/v3') {
      return { draft: null, error: "Trường 'format' phải là 'dq-rules/v3'." };
    }
    if (!Array.isArray(obj.rules)) {
      return { draft: null, error: "Trường 'rules' phải là danh sách các rule." };
    }

    const workflow = (typeof obj.workflow === 'string' && (obj.workflow === 'WRITE_GATE' || obj.workflow === 'AUDIT_TABLE'))
      ? (obj.workflow as Workflow)
      : defaultWorkflow;

    const draft: SuiteDraft = {
      workflow,
      description: typeof obj.description === 'string' ? obj.description : '',
      parameters: (obj.parameters && typeof obj.parameters === 'object' && !Array.isArray(obj.parameters))
        ? (obj.parameters as Record<string, string>)
        : {},
      rules: (obj.rules as Record<string, unknown>[]).map((r, i) => ({
        editorKey: `k_import_${i}_${Date.now()}`,
        rule_id: typeof r.rule_id === 'string' ? r.rule_id : `rule_${i + 1}`,
        dimension: (r.dimension as Dimension) ?? 'validity',
        scope: (r.scope as Scope) ?? (workflow === 'WRITE_GATE' ? 'PARTITION' : 'TABLE'),
        stage: (r.stage as Stage) ?? (workflow === 'WRITE_GATE' ? 'CANDIDATE' : 'AUDIT_SNAPSHOT'),
        description: typeof r.description === 'string' ? r.description : '',
        check: (r.check as Check) ?? { kind: 'complete', column: '', assert: { op: '==', value: '1' } },
      })),
    };

    return { draft, error: null };
  } catch (err) {
    return { draft: null, error: err instanceof Error ? err.message : 'Lỗi parse JSON.' };
  }
}

export function serializePolicyPayload(draft: PolicyDraft): string {
  const anomaly: AnomalyConfig = draft.anomaly.enabled
    ? {
        enabled: true,
        max_points: Math.max(1, parseInt(draft.anomaly.max_points, 10) || 30),
        lower: draft.anomaly.lower,
        upper: draft.anomaly.upper,
        bootstrap: draft.anomaly.bootstrap,
      }
    : { enabled: false };

  const payload: Policy = {
    rules: draft.rules.map(r => ({
      rule_id: r.rule_id,
      action: r.action,
      required: r.required,
    })),
    min_candidate_rows: Math.max(1, parseInt(draft.min_candidate_rows, 10) || 1),
    min_clean_rows: Math.max(1, parseInt(draft.min_clean_rows, 10) || 1),
    max_quarantine_ratio: draft.max_quarantine_ratio,
    anomaly,
  };

  return JSON.stringify(payload, null, 2);
}

export function parsePolicyPayload(rawJson: string): { draft: PolicyDraft | null; error: string | null } {
  try {
    const parsed: unknown = JSON.parse(rawJson);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { draft: null, error: 'JSON payload phải là một object.' };
    }
    const obj = parsed as Record<string, unknown>;
    if (!Array.isArray(obj.rules)) {
      return { draft: null, error: "Trường 'rules' phải là danh sách." };
    }

    const rules: PolicyRuleDraft[] = (obj.rules as Record<string, unknown>[]).map(r => ({
      rule_id: typeof r.rule_id === 'string' ? r.rule_id : '',
      action: (typeof r.action === 'string' && ['BLOCK', 'QUARANTINE', 'OBSERVE'].includes(r.action))
        ? (r.action as Action)
        : 'OBSERVE',
      required: Boolean(r.required),
    }));

    const anomalyObj = (obj.anomaly && typeof obj.anomaly === 'object') ? (obj.anomaly as Record<string, unknown>) : null;
    const anomalyEnabled = Boolean(anomalyObj?.enabled);

    const draft: PolicyDraft = {
      rules,
      min_candidate_rows: String(obj.min_candidate_rows ?? '1'),
      min_clean_rows: String(obj.min_clean_rows ?? '1'),
      max_quarantine_ratio: String(obj.max_quarantine_ratio ?? '0'),
      anomaly: {
        enabled: anomalyEnabled,
        max_points: String(anomalyObj?.max_points ?? '30'),
        lower: String(anomalyObj?.lower ?? '0'),
        upper: String(anomalyObj?.upper ?? '1'),
        bootstrap: Boolean(anomalyObj?.bootstrap),
      },
    };

    return { draft, error: null };
  } catch (err) {
    return { draft: null, error: err instanceof Error ? err.message : 'Lỗi parse JSON.' };
  }
}

export function generateIdempotencyKey(): string {
  return `console_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}
