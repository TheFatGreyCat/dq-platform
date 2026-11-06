import { useEffect, useId, useState } from 'react';
import { Link } from 'react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type {
  Action,
  Assertion,
  AssertionOp,
  Check,
  Dimension,
  Scope,
  Stage,
  Workflow,
} from '../../api/types';
import { ApiErrorAlert } from '../../components/Primitives';
import { PolicyBuilder } from './PolicyBuilder';
import {
  CHECK_KINDS,
  type CheckKind,
  createDefaultPolicyDraft,
  createEmptyRuleDraft,
  DIMENSIONS,
  generateIdempotencyKey,
  parsePolicyPayload,
  parseSuitePayload,
  type PolicyDraft,
  type RuleDraft,
  serializePolicyPayload,
  serializeSuitePayload,
  type SuiteDraft,
  validateDraftPolicy,
  validateDraftSuite,
  type ValidationError,
  WORKFLOW_STAGES,
} from './revisionDraft';

interface SuiteBuilderProps {
  dataset: string;
  suiteId: string;
  workflow: Workflow;
  clonedPayload?: Record<string, unknown> | null;
  onSavedRevision?: (kind: 'revisions' | 'policy-revisions', id: string) => void;
}

type TabMode = 'form' | 'json' | 'preview';
type FormStep = 'meta' | 'rules' | 'policy' | 'save';

export function SuiteBuilder({
  dataset,
  suiteId,
  workflow,
  clonedPayload,
  onSavedRevision,
}: SuiteBuilderProps) {
  const cache = useQueryClient();
  const datalistId = useId();

  // Tab & Step states
  const [activeTab, setActiveTab] = useState<TabMode>('form');
  const [formStep, setFormStep] = useState<FormStep>('meta');

  // Draft states
  const [suiteDraft, setSuiteDraft] = useState<SuiteDraft>(() => ({
    workflow,
    description: '',
    parameters: {},
    rules: [createEmptyRuleDraft('rule_1', workflow)],
  }));

  const [policyDraft, setPolicyDraft] = useState<PolicyDraft>(() =>
    createDefaultPolicyDraft(suiteDraft.rules)
  );

  // Advanced JSON editors
  const [suiteJson, setSuiteJson] = useState(() => serializeSuitePayload(suiteDraft));
  const [policyJson, setPolicyJson] = useState(() => serializePolicyPayload(policyDraft));
  const [jsonError, setJsonError] = useState<string | null>(null);

  // Unsaved changes tracking
  const [isDirty, setIsDirty] = useState(false);

  // Save / Validate states
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([]);
  const [serverValidation, setServerValidation] = useState<{
    valid: boolean;
    contains_sql: boolean;
  } | null>(null);
  const [savedSuiteId, setSavedSuiteId] = useState<string | null>(null);
  const [savedPolicyId, setSavedPolicyId] = useState<string | null>(null);
  const [suiteIdempotencyKey, setSuiteIdempotencyKey] = useState(generateIdempotencyKey);
  const [policyIdempotencyKey, setPolicyIdempotencyKey] = useState(generateIdempotencyKey);

  // Fetch business schema for column suggestions
  const datasetQuery = useQuery({
    queryKey: ['dataset', dataset],
    queryFn: ({ signal }) => endpoints.dataset(dataset, signal).then(r => r.data),
  });

  const rawRegistration = datasetQuery.data && typeof datasetQuery.data === 'object' && 'registration' in datasetQuery.data
    ? datasetQuery.data.registration
    : null;

  const rawBusinessSchema = rawRegistration && typeof rawRegistration === 'object' && 'business_schema' in rawRegistration
    ? rawRegistration.business_schema
    : null;

  const knownColumns: string[] = [];
  if (rawBusinessSchema && typeof rawBusinessSchema === 'object' && 'columns' in rawBusinessSchema && Array.isArray(rawBusinessSchema.columns)) {
    for (const c of rawBusinessSchema.columns) {
      if (c && typeof c === 'object' && 'name' in c && typeof c.name === 'string') {
        knownColumns.push(c.name);
      }
    }
  }

  // Handle clone initial load
  useEffect(() => {
    if (!clonedPayload) return;
    const jsonStr = JSON.stringify(clonedPayload);
    const parsed = parseSuitePayload(jsonStr, workflow);
    if (parsed.draft) {
      setSuiteDraft(parsed.draft);
      setSuiteJson(jsonStr);
      const newPolicy = createDefaultPolicyDraft(parsed.draft.rules);
      setPolicyDraft(newPolicy);
      setPolicyJson(serializePolicyPayload(newPolicy));
      setIsDirty(true);
    }
  }, [clonedPayload, workflow]);

  // Warn on page unload when dirty
  useEffect(() => {
    function handleBeforeUnload(e: BeforeUnloadEvent) {
      if (isDirty) {
        e.preventDefault();
      }
    }
    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => window.removeEventListener('beforeunload', handleBeforeUnload);
  }, [isDirty]);

  // Keep policy rules in sync with suite rules when editing in Form
  function syncPolicyWithSuiteRules(nextRules: RuleDraft[]) {
    const existingPolicyMap = new Map(policyDraft.rules.map(pr => [pr.rule_id, pr]));
    const nextPolicyRules = nextRules.map(r => {
      const existing = existingPolicyMap.get(r.rule_id);
      if (existing) return existing;
      return {
        rule_id: r.rule_id,
        action: 'OBSERVE' as Action,
        required: true,
      };
    });
    const nextPolicy = { ...policyDraft, rules: nextPolicyRules };
    setPolicyDraft(nextPolicy);
    setPolicyJson(serializePolicyPayload(nextPolicy));
  }

  // Handle Tab Switch
  function handleTabChange(targetTab: TabMode) {
    if (targetTab === activeTab) return;

    if (activeTab === 'json') {
      // Validate and sync JSON to Form drafts
      const parsedSuite = parseSuitePayload(suiteJson, workflow);
      if (parsedSuite.error) {
        setJsonError(`Lỗi Suite JSON: ${parsedSuite.error}`);
        return;
      }
      const parsedPolicy = parsePolicyPayload(policyJson);
      if (parsedPolicy.error) {
        setJsonError(`Lỗi Policy JSON: ${parsedPolicy.error}`);
        return;
      }
      if (parsedSuite.draft && parsedPolicy.draft) {
        setSuiteDraft(parsedSuite.draft);
        setPolicyDraft(parsedPolicy.draft);
        setJsonError(null);
      }
    } else {
      // Sync Form draft to JSON
      setSuiteJson(serializeSuitePayload(suiteDraft));
      setPolicyJson(serializePolicyPayload(policyDraft));
      setJsonError(null);
    }

    setActiveTab(targetTab);
  }

  // Server Validation Mutation
  const validateMutation = useMutation({
    mutationFn: async () => {
      // Local validation first
      const suiteErrors = validateDraftSuite(suiteDraft);
      const policyErrors = validateDraftPolicy(policyDraft, suiteDraft.rules);
      const combined = [...suiteErrors, ...policyErrors];
      setValidationErrors(combined);
      if (combined.length > 0) {
        throw new Error('Vui lòng sửa các lỗi cấu hình trước khi gửi kiểm tra máy chủ.');
      }

      const suitePayload = JSON.parse(serializeSuitePayload(suiteDraft));
      const policyPayload = JSON.parse(serializePolicyPayload(policyDraft));

      const res = await endpoints.validateSuite(dataset, suiteId, {
        suite: suitePayload,
        policy: policyPayload,
      });
      return res.data;
    },
    onSuccess: data => {
      setServerValidation(data);
    },
  });

  // Save Suite Revision Mutation
  const saveSuiteMutation = useMutation({
    mutationFn: async () => {
      const suitePayload = JSON.parse(serializeSuitePayload(suiteDraft));
      const res = await endpoints.createSuiteRevision(
        dataset,
        suiteId,
        suitePayload,
        suiteIdempotencyKey
      );
      return res.data;
    },
    onSuccess: async res => {
      setSavedSuiteId(res.revision_id);
      onSavedRevision?.('revisions', res.revision_id);
      await cache.invalidateQueries({ queryKey: ['revisions', dataset, suiteId, 'revisions'] });
      // Now save policy
      savePolicyMutation.mutate(res.revision_id);
    },
  });

  // Save Policy Revision Mutation
  const savePolicyMutation = useMutation({
    mutationFn: async (_suiteRevId: string) => {
      const policyPayload = JSON.parse(serializePolicyPayload(policyDraft));
      const res = await endpoints.createPolicyRevision(
        dataset,
        suiteId,
        policyPayload,
        policyIdempotencyKey
      );
      return res.data;
    },
    onSuccess: async res => {
      setSavedPolicyId(res.revision_id);
      setIsDirty(false);
      onSavedRevision?.('policy-revisions', res.revision_id);
      await cache.invalidateQueries({ queryKey: ['revisions', dataset, suiteId, 'policy-revisions'] });
    },
  });

  function startSave() {
    saveSuiteMutation.mutate();
  }

  function retryPolicy() {
    if (savedSuiteId) {
      savePolicyMutation.mutate(savedSuiteId);
    }
  }

  // Form Rule modifications
  function addRule() {
    const nextIdx = suiteDraft.rules.length + 1;
    const newRule = createEmptyRuleDraft(`rule_${nextIdx}`, workflow);
    const nextRules = [...suiteDraft.rules, newRule];
    setSuiteDraft({ ...suiteDraft, rules: nextRules });
    syncPolicyWithSuiteRules(nextRules);
    setIsDirty(true);
    setServerValidation(null);
  }

  function removeRule(index: number) {
    if (suiteDraft.rules.length <= 1) return;
    const nextRules = suiteDraft.rules.filter((_, i) => i !== index);
    setSuiteDraft({ ...suiteDraft, rules: nextRules });
    syncPolicyWithSuiteRules(nextRules);
    setIsDirty(true);
    setServerValidation(null);
  }

  function updateRuleDraft(index: number, patch: Partial<RuleDraft>) {
    const nextRules = [...suiteDraft.rules];
    const target = nextRules[index];
    if (!target) return;
    nextRules[index] = { ...target, ...patch };
    setSuiteDraft({ ...suiteDraft, rules: nextRules });
    if (patch.rule_id) {
      syncPolicyWithSuiteRules(nextRules);
    }
    setIsDirty(true);
    setServerValidation(null);
  }

  return (
    <div className="card stack" style={{ gap: 20 }}>
      {/* Tab Navigation */}
      <div className="row" style={{ justifyContent: 'space-between', borderBottom: '1px solid var(--border)', paddingBottom: 12 }}>
        <div className="row" style={{ gap: 8 }}>
          <button
            type="button"
            className={activeTab === 'form' ? 'primary' : ''}
            onClick={() => handleTabChange('form')}
          >
            Form Authoring
          </button>
          <button
            type="button"
            className={activeTab === 'json' ? 'primary' : ''}
            onClick={() => handleTabChange('json')}
          >
            JSON nâng cao
          </button>
          <button
            type="button"
            className={activeTab === 'preview' ? 'primary' : ''}
            onClick={() => handleTabChange('preview')}
          >
            Preview Payload
          </button>
        </div>
        {isDirty && (
          <span style={{ fontSize: 13, color: 'var(--brand, #d97706)', fontWeight: 500 }}>
            ● Có thay đổi chưa lưu
          </span>
        )}
      </div>

      {/* Datalist for Column Suggestions */}
      <datalist id={datalistId}>
        {knownColumns.map(col => (
          <option key={col} value={col} />
        ))}
      </datalist>

      {/* TAB 1: FORM VIEW */}
      {activeTab === 'form' && (
        <div className="stack" style={{ gap: 20 }}>
          {/* Step indicator */}
          <div className="row" style={{ gap: 12, borderBottom: '1px solid var(--border)', paddingBottom: 8 }}>
            {[
              { id: 'meta', label: '1. Metadata & Params' },
              { id: 'rules', label: `2. Rules (${suiteDraft.rules.length})` },
              { id: 'policy', label: '3. Policy Companion' },
              { id: 'save', label: '4. Kiểm tra & Lưu' },
            ].map(step => (
              <button
                key={step.id}
                type="button"
                style={{
                  background: formStep === step.id ? 'var(--surface-active, #e2e8f0)' : 'transparent',
                  fontWeight: formStep === step.id ? 600 : 400,
                  padding: '6px 12px',
                  borderRadius: 6,
                }}
                onClick={() => setFormStep(step.id as FormStep)}
              >
                {step.label}
              </button>
            ))}
          </div>

          {/* Step 1: Metadata */}
          {formStep === 'meta' && (
            <div className="stack" style={{ gap: 16 }}>
              <div className="row" style={{ gap: 20 }}>
                <label style={{ display: 'grid', gap: 4 }}>
                  <span>Workflow (readonly từ suite)</span>
                  <input type="text" value={workflow} disabled style={{ width: 200 }} />
                </label>
                <label style={{ display: 'grid', gap: 4, flex: 1 }}>
                  <span>Description (mô tả suite revision)</span>
                  <input
                    type="text"
                    value={suiteDraft.description}
                    placeholder="VD: Kiểm tra tính toàn vẹn và duy nhất của customer_id"
                    onChange={e => {
                      setSuiteDraft({ ...suiteDraft, description: e.target.value });
                      setIsDirty(true);
                      setServerValidation(null);
                    }}
                  />
                </label>
              </div>

              <div className="card stack" style={{ background: 'var(--surface-muted, #f8fafc)', padding: 14 }}>
                <div className="row" style={{ justifyContent: 'space-between' }}>
                  <b>Tham số Suite (Parameters)</b>
                  <button
                    type="button"
                    onClick={() => {
                      const paramName = prompt('Nhập tên tham số mới (chữ cái, số, _):');
                      if (paramName && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(paramName)) {
                        if (paramName === 'batch') {
                          alert("'batch' là tham số dành riêng của hệ thống.");
                          return;
                        }
                        setSuiteDraft({
                          ...suiteDraft,
                          parameters: { ...suiteDraft.parameters, [paramName]: '' },
                        });
                        setIsDirty(true);
                        setServerValidation(null);
                      }
                    }}
                  >
                    + Thêm tham số
                  </button>
                </div>
                {Object.keys(suiteDraft.parameters).length === 0 ? (
                  <p style={{ margin: 0, fontSize: 13, color: 'var(--muted)' }}>
                    Chưa khai báo tham số nào.
                  </p>
                ) : (
                  <div className="stack" style={{ gap: 8 }}>
                    {Object.entries(suiteDraft.parameters).map(([key, val]) => (
                      <div key={key} className="row" style={{ gap: 12 }}>
                        <span className="mono" style={{ minWidth: 140 }}>
                          ${'{' + key + '}'}
                        </span>
                        <input
                          type="text"
                          value={val}
                          placeholder="Mô tả / default value"
                          onChange={e => {
                            setSuiteDraft({
                              ...suiteDraft,
                              parameters: { ...suiteDraft.parameters, [key]: e.target.value },
                            });
                            setIsDirty(true);
                            setServerValidation(null);
                          }}
                          style={{ flex: 1 }}
                        />
                        <button
                          type="button"
                          onClick={() => {
                            const nextParams = { ...suiteDraft.parameters };
                            delete nextParams[key];
                            setSuiteDraft({ ...suiteDraft, parameters: nextParams });
                            setIsDirty(true);
                            setServerValidation(null);
                          }}
                        >
                          Xóa
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="row" style={{ justifyContent: 'flex-end', marginTop: 12 }}>
                <button type="button" className="primary" onClick={() => setFormStep('rules')}>
                  Tiếp theo: Soạn Rules →
                </button>
              </div>
            </div>
          )}

          {/* Step 2: Rules */}
          {formStep === 'rules' && (
            <div className="stack" style={{ gap: 16 }}>
              <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                <p style={{ margin: 0, color: 'var(--muted)' }}>
                  Định nghĩa các rule đo lường (measurement). Tối đa 500 rules.
                </p>
                <button type="button" className="primary" onClick={addRule}>
                  + Thêm Rule
                </button>
              </div>

              <div className="stack" style={{ gap: 16 }}>
                {suiteDraft.rules.map((rule, idx) => (
                  <RuleEditorCard
                    key={rule.editorKey}
                    rule={rule}
                    index={idx}
                    workflow={workflow}
                    knownColumns={knownColumns}
                    datalistId={datalistId}
                    canRemove={suiteDraft.rules.length > 1}
                    onUpdate={patch => updateRuleDraft(idx, patch)}
                    onRemove={() => removeRule(idx)}
                  />
                ))}
              </div>

              <div className="row" style={{ justifyContent: 'space-between', marginTop: 12 }}>
                <button type="button" onClick={() => setFormStep('meta')}>
                  ← Quay lại: Metadata
                </button>
                <button type="button" className="primary" onClick={() => setFormStep('policy')}>
                  Tiếp theo: Cấu hình Policy →
                </button>
              </div>
            </div>
          )}

          {/* Step 3: Policy Companion */}
          {formStep === 'policy' && (
            <div className="stack" style={{ gap: 16 }}>
              <PolicyBuilder
                draft={policyDraft}
                suiteRules={suiteDraft.rules}
                errors={validationErrors.filter(e => e.field.startsWith('policy') || e.field.startsWith('rules.') && e.field.includes('action'))}
                onChange={next => {
                  setPolicyDraft(next);
                  setIsDirty(true);
                  setServerValidation(null);
                }}
              />

              <div className="row" style={{ justifyContent: 'space-between', marginTop: 12 }}>
                <button type="button" onClick={() => setFormStep('rules')}>
                  ← Quay lại: Soạn Rules
                </button>
                <button type="button" className="primary" onClick={() => setFormStep('save')}>
                  Tiếp theo: Kiểm tra & Lưu →
                </button>
              </div>
            </div>
          )}

          {/* Step 4: Preview & Save */}
          {formStep === 'save' && (
            <div className="stack" style={{ gap: 20 }}>
              <div className="card stack">
                <h3>Xác thực cấu hình trước khi lưu</h3>
                <p style={{ color: 'var(--muted)', fontSize: 14 }}>
                  Hệ thống yêu cầu kiểm tra hình dạng Suite và Policy qua endpoint validate trước khi tạo immutable revisions.
                </p>

                {validationErrors.length > 0 && (
                  <div role="alert" style={{ color: 'var(--error, #e53e3e)', background: 'var(--surface-error, #fff5f5)', padding: 12, borderRadius: 6 }}>
                    <b>Phát hiện lỗi cục bộ:</b>
                    <ul style={{ margin: '6px 0 0 18px', padding: 0 }}>
                      {validationErrors.map((err, i) => (
                        <li key={i}>{err.message}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {validateMutation.isError && (
                  <ApiErrorAlert error={validateMutation.error} />
                )}

                {serverValidation && (
                  <div style={{ background: 'var(--surface-success, #f0fdf4)', color: 'var(--success, #16a34a)', padding: 12, borderRadius: 6 }}>
                    <b>✓ Cấu hình hợp lệ trên máy chủ.</b>
                    {serverValidation.contains_sql && (
                      <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--brand, #d97706)' }}>
                        ⚠ Suite có chứa SQL check. Khi bind purpose sẽ cần người duyệt độc lập (khác tác giả) phê duyệt!
                      </p>
                    )}
                  </div>
                )}

                <div className="row" style={{ gap: 12, marginTop: 8 }}>
                  <button
                    type="button"
                    disabled={validateMutation.isPending}
                    onClick={() => validateMutation.mutate()}
                  >
                    {validateMutation.isPending ? 'Đang kiểm tra...' : 'Kiểm tra máy chủ (Validate)'}
                  </button>

                  <button
                    type="button"
                    className="primary"
                    disabled={
                      !serverValidation?.valid ||
                      validateMutation.isPending ||
                      saveSuiteMutation.isPending ||
                      savePolicyMutation.isPending ||
                      Boolean(savedPolicyId)
                    }
                    onClick={startSave}
                  >
                    {saveSuiteMutation.isPending
                      ? 'Đang lưu Suite...'
                      : savePolicyMutation.isPending
                      ? 'Đang lưu Policy...'
                      : 'Lưu Revisions Mới'}
                  </button>
                </div>
              </div>

              {saveSuiteMutation.isError && (
                <ApiErrorAlert error={saveSuiteMutation.error} />
              )}

              {savedSuiteId && (
                <div className="card stack" style={{ background: 'var(--surface-success, #f0fdf4)', border: '1px solid var(--success, #16a34a)' }}>
                  <h4>✓ Đã tạo Suite Revision thành công!</h4>
                  <p className="mono">Suite Revision ID: {savedSuiteId}</p>

                  {savePolicyMutation.isError && (
                    <div className="stack" style={{ gap: 8 }}>
                      <div role="alert" style={{ color: 'var(--error, #e53e3e)' }}>
                        Lỗi lưu Policy Revision: {savePolicyMutation.error.message}
                      </div>
                      <p style={{ fontSize: 13 }}>
                        Suite revision đã được lưu an toàn. Nhấn thử lại để gửi lại policy mà không tạo mới suite.
                      </p>
                      <button type="button" className="primary" onClick={retryPolicy}>
                        Thử lại lưu Policy
                      </button>
                    </div>
                  )}

                  {savedPolicyId && (
                    <div className="stack" style={{ gap: 8 }}>
                      <h4>✓ Đã tạo Policy Revision thành công!</h4>
                      <p className="mono">Policy Revision ID: {savedPolicyId}</p>
                      <div className="row" style={{ marginTop: 12 }}>
                        <Link
                          to={`/datasets/${encodeURIComponent(dataset)}/purposes`}
                          className="button primary"
                          style={{ textDecoration: 'none', padding: '8px 16px', borderRadius: 6 }}
                        >
                          Đến trang Bindings để kích hoạt revision →
                        </Link>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* TAB 2: ADVANCED JSON */}
      {activeTab === 'json' && (
        <div className="stack" style={{ gap: 20 }}>
          {jsonError && (
            <div role="alert" style={{ color: 'var(--error, #e53e3e)', background: 'var(--surface-error, #fff5f5)', padding: 12, borderRadius: 6 }}>
              {jsonError}
            </div>
          )}
          <div className="card stack">
            <h3>Suite JSON (dq-rules/v3)</h3>
            <textarea
              rows={16}
              className="mono"
              value={suiteJson}
              onChange={e => {
                setSuiteJson(e.target.value);
                setIsDirty(true);
                setSuiteIdempotencyKey(generateIdempotencyKey());
                setServerValidation(null);
              }}
            />
          </div>
          <div className="card stack">
            <h3>Policy JSON companion</h3>
            <textarea
              rows={12}
              className="mono"
              value={policyJson}
              onChange={e => {
                setPolicyJson(e.target.value);
                setIsDirty(true);
                setPolicyIdempotencyKey(generateIdempotencyKey());
                setServerValidation(null);
              }}
            />
          </div>
        </div>
      )}

      {/* TAB 3: PREVIEW PAYLOAD */}
      {activeTab === 'preview' && (
        <div className="stack" style={{ gap: 16 }}>
          <div className="card stack">
            <h3>Preview Suite Revision Payload</h3>
            <pre className="mono" style={{ maxHeight: 300, overflow: 'auto', background: 'var(--surface-muted, #f8fafc)', padding: 12 }}>
              {serializeSuitePayload(suiteDraft)}
            </pre>
          </div>
          <div className="card stack">
            <h3>Preview Policy Revision Payload</h3>
            <pre className="mono" style={{ maxHeight: 300, overflow: 'auto', background: 'var(--surface-muted, #f8fafc)', padding: 12 }}>
              {serializePolicyPayload(policyDraft)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

// ----------------------------------------------------------------------
// Sub-component: RuleEditorCard
// ----------------------------------------------------------------------

interface RuleEditorCardProps {
  rule: RuleDraft;
  index: number;
  workflow: Workflow;
  knownColumns: string[];
  datalistId: string;
  canRemove: boolean;
  onUpdate: (patch: Partial<RuleDraft>) => void;
  onRemove: () => void;
}

function RuleEditorCard({
  rule,
  index,
  workflow,
  knownColumns,
  datalistId,
  canRemove,
  onUpdate,
  onRemove,
}: RuleEditorCardProps) {
  const allowedStages = WORKFLOW_STAGES[workflow];

  function handleKindChange(nextKind: CheckKind) {
    if (nextKind === rule.check.kind) return;
    const defaultCheck = getDefaultCheckForKind(nextKind);
    onUpdate({ check: defaultCheck });
  }

  function handleCheckUpdate(patch: Partial<Check>) {
    onUpdate({ check: { ...rule.check, ...patch } as Check });
  }

  function handleAssertionUpdate(assertPatch: Partial<Assertion> | null) {
    if (!('assert' in rule.check)) return;
    if (assertPatch === null) {
      handleCheckUpdate({ assert: null });
      return;
    }
    const current = rule.check.assert ?? { op: '==', value: '1' };
    handleCheckUpdate({ assert: { ...current, ...assertPatch } });
  }

  const isUnknownCol = 'column' in rule.check && typeof rule.check.column === 'string' && rule.check.column.trim() !== '' && knownColumns.length > 0 && !knownColumns.includes(rule.check.column.trim());

  return (
    <div className="card stack" style={{ border: '1px solid var(--border)', padding: 16, gap: 12 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <div className="row" style={{ gap: 8, alignItems: 'center' }}>
          <span style={{ fontWeight: 600, color: 'var(--muted)' }}>#{index + 1}</span>
          <input
            type="text"
            className="mono"
            style={{ fontWeight: 600, width: 180 }}
            value={rule.rule_id}
            placeholder="rule_id"
            onChange={e => onUpdate({ rule_id: e.target.value })}
          />
        </div>
        {canRemove && (
          <button type="button" onClick={onRemove} style={{ color: 'var(--error, #e53e3e)' }}>
            Xóa Rule
          </button>
        )}
      </div>

      <div className="row" style={{ flexWrap: 'wrap', gap: 12 }}>
        <label style={{ display: 'grid', gap: 4 }}>
          <span>Check Kind</span>
          <select
            value={rule.check.kind}
            onChange={e => handleKindChange(e.target.value as CheckKind)}
          >
            {CHECK_KINDS.map(k => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 4 }}>
          <span>Dimension</span>
          <select
            value={rule.dimension}
            onChange={e => onUpdate({ dimension: e.target.value as Dimension })}
          >
            {DIMENSIONS.map(d => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 4 }}>
          <span>Stage</span>
          <select
            value={rule.stage}
            onChange={e => onUpdate({ stage: e.target.value as Stage })}
          >
            {allowedStages.map(st => (
              <option key={st} value={st}>
                {st}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 4 }}>
          <span>Scope</span>
          <select
            value={rule.scope}
            onChange={e => onUpdate({ scope: e.target.value as Scope })}
          >
            <option value="PARTITION">PARTITION</option>
            <option value="TABLE">TABLE</option>
          </select>
        </label>
      </div>

      {/* Kind-specific inputs */}
      <div className="stack" style={{ background: 'var(--surface-muted, #f8fafc)', padding: 12, borderRadius: 6, gap: 10 }}>
        {'column' in rule.check && (
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Cột kiểm tra (Column)</span>
            <input
              type="text"
              list={datalistId}
              value={rule.check.column}
              placeholder="Chọn hoặc nhập tên cột"
              onChange={e => handleCheckUpdate({ column: e.target.value } as unknown as Partial<Check>)}
            />
            {isUnknownCol && (
              <span style={{ fontSize: 12, color: 'var(--brand, #d97706)' }}>
                ⚠ Cột này không có trong business_schema hiện tại (vẫn hợp lệ cho RAW stage).
              </span>
            )}
          </label>
        )}

        {rule.check.kind === 'unique' && (
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Các cột duy nhất (cách nhau bởi dấu phẩy)</span>
            <input
              type="text"
              value={rule.check.columns.join(', ')}
              placeholder="col_a, col_b"
              onChange={e =>
                handleCheckUpdate({
                  columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean),
                } as unknown as Partial<Check>)
              }
            />
          </label>
        )}

        {rule.check.kind === 'pattern' && (
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Regex pattern (Java syntax, e.g. ^[0-9]{'{'}10{'}'}$)</span>
            <input
              type="text"
              className="mono"
              value={rule.check.regex}
              placeholder="^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$"
              onChange={e => handleCheckUpdate({ regex: e.target.value } as unknown as Partial<Check>)}
            />
          </label>
        )}

        {rule.check.kind === 'range' && (
          <div className="row" style={{ flexWrap: 'wrap', gap: 12 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Min bound</span>
              <input
                type="text"
                value={String(typeof rule.check.min === 'object' ? rule.check.min.param : rule.check.min)}
                placeholder="0"
                onChange={e => handleCheckUpdate({ min: e.target.value } as unknown as Partial<Check>)}
              />
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Max bound</span>
              <input
                type="text"
                value={String(typeof rule.check.max === 'object' ? rule.check.max.param : rule.check.max)}
                placeholder="100"
                onChange={e => handleCheckUpdate({ max: e.target.value } as unknown as Partial<Check>)}
              />
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 22 }}>
              <input
                type="checkbox"
                checked={rule.check.include_min ?? true}
                onChange={e => handleCheckUpdate({ include_min: e.target.checked } as unknown as Partial<Check>)}
              />
              <span>Bao gồm Min</span>
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 22 }}>
              <input
                type="checkbox"
                checked={rule.check.include_max ?? true}
                onChange={e => handleCheckUpdate({ include_max: e.target.checked } as unknown as Partial<Check>)}
              />
              <span>Bao gồm Max</span>
            </label>
          </div>
        )}

        {rule.check.kind === 'in_set' && (
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Tập giá trị hợp lệ (cách nhau bởi dấu phẩy)</span>
            <input
              type="text"
              value={rule.check.values.join(', ')}
              placeholder="ACTIVE, PENDING, CLOSED"
              onChange={e =>
                handleCheckUpdate({
                  values: e.target.value.split(',').map(s => s.trim()).filter(Boolean),
                } as unknown as Partial<Check>)
              }
            />
          </label>
        )}

        {rule.check.kind === 'sql_predicate' && (
          <div className="stack" style={{ gap: 8 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>SQL Predicate (VD: age &gt;= 18 AND status = 'ACTIVE')</span>
              <input
                type="text"
                className="mono"
                value={rule.check.predicate}
                placeholder="amount > 0"
                onChange={e => handleCheckUpdate({ predicate: e.target.value } as unknown as Partial<Check>)}
              />
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Các cột phụ thuộc (columns)</span>
              <input
                type="text"
                value={rule.check.columns.join(', ')}
                placeholder="age, status"
                onChange={e =>
                  handleCheckUpdate({
                    columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean),
                  } as unknown as Partial<Check>)
                }
              />
            </label>
            <span style={{ fontSize: 12, color: 'var(--brand, #d97706)' }}>
              ⚠ SQL predicate cần independent approver khi kích hoạt binding!
            </span>
          </div>
        )}

        {rule.check.kind === 'sql_metric' && (
          <div className="stack" style={{ gap: 8 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>SQL Query (bắt buộc đọc từ {'${batch}'})</span>
              <textarea
                rows={3}
                className="mono"
                value={rule.check.sql}
                placeholder="SELECT count(*) FROM ${batch} WHERE status = 'ERROR'"
                onChange={e => handleCheckUpdate({ sql: e.target.value } as unknown as Partial<Check>)}
              />
            </label>
            <span style={{ fontSize: 12, color: 'var(--brand, #d97706)' }}>
              ⚠ SQL metric cần independent approver khi kích hoạt binding!
            </span>
          </div>
        )}

        {rule.check.kind === 'schema_columns' && (
          <div className="stack" style={{ gap: 8 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Danh sách cột mong đợi (cách nhau bởi dấu phẩy)</span>
              <input
                type="text"
                value={rule.check.columns.join(', ')}
                placeholder="id, name, created_at"
                onChange={e =>
                  handleCheckUpdate({
                    columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean),
                  } as unknown as Partial<Check>)
                }
              />
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <input
                type="checkbox"
                checked={rule.check.exact ?? false}
                onChange={e => handleCheckUpdate({ exact: e.target.checked } as unknown as Partial<Check>)}
              />
              <span>Exact matching (chính xác tuyệt đối các cột)</span>
            </label>
          </div>
        )}

        {rule.check.kind === 'schema_type' && (
          <div className="row" style={{ gap: 12 }}>
            <label style={{ display: 'grid', gap: 4, flex: 1 }}>
              <span>Cột (Column)</span>
              <input
                type="text"
                list={datalistId}
                value={rule.check.column}
                onChange={e => handleCheckUpdate({ column: e.target.value } as unknown as Partial<Check>)}
              />
            </label>
            <label style={{ display: 'grid', gap: 4, width: 180 }}>
              <span>Kiểu dữ liệu (Type)</span>
              <select
                value={rule.check.type}
                onChange={e =>
                  handleCheckUpdate({
                    type: e.target.value as 'string' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'decimal' | 'date' | 'timestamp',
                  } as unknown as Partial<Check>)
                }
              >
                {['string', 'boolean', 'int', 'long', 'float', 'double', 'decimal', 'date', 'timestamp'].map(t => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </label>
          </div>
        )}

        {/* Assertion editor (if kind supports assertion) */}
        {'assert' in rule.check && (() => {
          const assertion = rule.check.assert;
          return (
            <div className="stack" style={{ marginTop: 8, borderTop: '1px solid var(--border)', paddingTop: 8 }}>
              <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                <b>Assertion (điều kiện kỳ vọng)</b>
                {assertion == null ? (
                  <button
                    type="button"
                    onClick={() => handleAssertionUpdate({ op: '==', value: '1' })}
                  >
                    + Thêm Assertion
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={() => handleAssertionUpdate(null)}
                    style={{ color: 'var(--muted)' }}
                  >
                    Xóa Assertion (chỉ đo lường)
                  </button>
                )}
              </div>

              {assertion == null ? (
                <span style={{ fontSize: 13, color: 'var(--muted)' }}>
                  Chỉ đo lường (không đánh giá pass/fail trên assertion).
                </span>
              ) : (
                <div className="row" style={{ flexWrap: 'wrap', gap: 12, alignItems: 'center' }}>
                  <label style={{ display: 'grid', gap: 4 }}>
                    <span>Toán tử (Operator)</span>
                    <select
                      value={assertion.op}
                      onChange={e => handleAssertionUpdate({ op: e.target.value as AssertionOp })}
                    >
                      {['==', '>=', '>', '<=', '<', 'between'].map(op => (
                        <option key={op} value={op}>
                          {op}
                        </option>
                      ))}
                    </select>
                  </label>

                  {assertion.op === 'between' ? (
                    <>
                      <label style={{ display: 'grid', gap: 4 }}>
                        <span>Min bound</span>
                        <input
                          type="text"
                          value={String(assertion.min && typeof assertion.min === 'object' ? assertion.min.param : assertion.min ?? '')}
                          placeholder="0"
                          onChange={e => handleAssertionUpdate({ min: e.target.value, value: null })}
                          style={{ width: 120 }}
                        />
                      </label>
                      <label style={{ display: 'grid', gap: 4 }}>
                        <span>Max bound</span>
                        <input
                          type="text"
                          value={String(assertion.max && typeof assertion.max === 'object' ? assertion.max.param : assertion.max ?? '')}
                          placeholder="1"
                          onChange={e => handleAssertionUpdate({ max: e.target.value, value: null })}
                          style={{ width: 120 }}
                        />
                      </label>
                    </>
                  ) : (
                    <label style={{ display: 'grid', gap: 4 }}>
                      <span>Giá trị kỳ vọng (Value)</span>
                      <input
                        type="text"
                        value={String(assertion.value && typeof assertion.value === 'object' ? assertion.value.param : assertion.value ?? '')}
                        placeholder="1"
                        onChange={e => handleAssertionUpdate({ value: e.target.value, min: null, max: null })}
                        style={{ width: 160 }}
                      />
                    </label>
                  )}
                </div>
              )}
            </div>
          );
        })()}
      </div>
    </div>
  );
}

function getDefaultCheckForKind(kind: CheckKind): Check {
  switch (kind) {
    case 'size':
      return { kind: 'size', assert: { op: '>=', value: '1' } };
    case 'complete':
      return { kind: 'complete', column: '', assert: { op: '==', value: '1' } };
    case 'completeness':
      return { kind: 'completeness', column: '', assert: { op: '>=', value: '0.99' } };
    case 'unique':
      return { kind: 'unique', columns: [], assert: { op: '==', value: '1' } };
    case 'pattern':
      return { kind: 'pattern', column: '', regex: '', assert: { op: '==', value: '1' } };
    case 'range':
      return { kind: 'range', column: '', min: '0', max: '100', include_min: true, include_max: true, assert: { op: '==', value: '1' } };
    case 'in_set':
      return { kind: 'in_set', column: '', values: [], assert: { op: '==', value: '1' } };
    case 'mean':
      return { kind: 'mean', column: '', assert: { op: '>=', value: '0' } };
    case 'sql_predicate':
      return { kind: 'sql_predicate', predicate: '', columns: [], assert: { op: '==', value: '1' } };
    case 'sql_metric':
      return { kind: 'sql_metric', sql: 'SELECT count(*) FROM ${batch}', assert: { op: '==', value: '0' } };
    case 'schema_columns':
      return { kind: 'schema_columns', columns: [], exact: false };
    case 'schema_type':
      return { kind: 'schema_type', column: '', type: 'string' };
  }
}
