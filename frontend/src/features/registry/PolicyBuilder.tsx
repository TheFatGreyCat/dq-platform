import type { ChangeEvent } from 'react';
import type { Action, Rule } from '../../api/types';
import {
  isQuarantineEligible,
  type PolicyDraft,
  type PolicyRuleDraft,
  type RuleDraft,
  type ValidationError,
} from './revisionDraft';

interface PolicyBuilderProps {
  draft: PolicyDraft;
  onChange: (next: PolicyDraft) => void;
  suiteRules: readonly (Rule | RuleDraft)[];
  errors: ValidationError[];
  disabled?: boolean;
}

export function PolicyBuilder({
  draft,
  onChange,
  suiteRules,
  errors,
  disabled = false,
}: PolicyBuilderProps) {
  function updateRule(index: number, patch: Partial<PolicyRuleDraft>) {
    const nextRules = [...draft.rules];
    const target = nextRules[index];
    if (!target) return;
    nextRules[index] = { ...target, ...patch };
    onChange({ ...draft, rules: nextRules });
  }

  function handleRatioChange(e: ChangeEvent<HTMLInputElement>) {
    onChange({ ...draft, max_quarantine_ratio: e.target.value });
  }

  function handleAnomalyToggle(enabled: boolean) {
    onChange({
      ...draft,
      anomaly: {
        ...draft.anomaly,
        enabled,
      },
    });
  }

  // Ensure policy has entries for all suite rules
  const ruleMap = new Map<string, Rule | RuleDraft>();
  for (const r of suiteRules) {
    ruleMap.set(r.rule_id, r);
  }

  return (
    <div className="stack" style={{ gap: 20 }}>
      <div className="card stack">
        <h3>Quy tắc xử lý (Actions & Enforcement)</h3>
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>
          Mỗi rule trong suite tương ứng một hành động. Mặc định draft mới là <b>OBSERVE</b> và <b>required = true</b>.
        </p>

        {errors.length > 0 && (
          <div role="alert" style={{ color: 'var(--error, #e53e3e)', background: 'var(--surface-error, #fff5f5)', padding: 12, borderRadius: 6 }}>
            <b>Lỗi chính sách:</b>
            <ul style={{ margin: '6px 0 0 18px', padding: 0 }}>
              {errors.map((err, i) => (
                <li key={i}>{err.message}</li>
              ))}
            </ul>
          </div>
        )}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rule ID</th>
                <th>Check kind</th>
                <th>Stage / Scope</th>
                <th>Action</th>
                <th>Required</th>
                <th>Ghi chú</th>
              </tr>
            </thead>
            <tbody>
              {draft.rules.map((pr, idx) => {
                const companionRule = ruleMap.get(pr.rule_id);
                const eligible = companionRule
                  ? isQuarantineEligible(companionRule.check.kind, companionRule.stage, companionRule.scope)
                  : false;

                return (
                  <tr key={pr.rule_id}>
                    <td><b className="mono">{pr.rule_id}</b></td>
                    <td>{companionRule?.check.kind ?? '—'}</td>
                    <td>{companionRule ? `${companionRule.stage} / ${companionRule.scope}` : '—'}</td>
                    <td>
                      <select
                        value={pr.action}
                        disabled={disabled}
                        aria-label={`Action cho ${pr.rule_id}`}
                        onChange={e => updateRule(idx, { action: e.target.value as Action })}
                      >
                        <option value="BLOCK">BLOCK</option>
                        <option value="QUARANTINE" disabled={!eligible}>
                          QUARANTINE {!eligible ? '(không khả dụng)' : ''}
                        </option>
                        <option value="OBSERVE">OBSERVE</option>
                      </select>
                    </td>
                    <td style={{ textAlign: 'center' }}>
                      <input
                        type="checkbox"
                        checked={pr.required}
                        disabled={disabled}
                        aria-label={`Required cho ${pr.rule_id}`}
                        onChange={e => updateRule(idx, { required: e.target.checked })}
                      />
                    </td>
                    <td style={{ fontSize: 13, color: 'var(--muted)' }}>
                      {!eligible && pr.action !== 'QUARANTINE' && (
                        <span>QUARANTINE chỉ cho row-level ở CANDIDATE/PARTITION.</span>
                      )}
                      {!eligible && pr.action === 'QUARANTINE' && (
                        <span style={{ color: 'var(--error, #e53e3e)' }}>Không hợp lệ cho QUARANTINE!</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card stack">
        <h3>Ngưỡng toàn cục (Global Thresholds)</h3>
        <div className="row" style={{ flexWrap: 'wrap', gap: 16 }}>
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Min candidate rows</span>
            <input
              type="text"
              value={draft.min_candidate_rows}
              disabled={disabled}
              onChange={e => onChange({ ...draft, min_candidate_rows: e.target.value })}
              style={{ width: 140 }}
            />
          </label>
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Min clean rows</span>
            <input
              type="text"
              value={draft.min_clean_rows}
              disabled={disabled}
              onChange={e => onChange({ ...draft, min_clean_rows: e.target.value })}
              style={{ width: 140 }}
            />
          </label>
          <label style={{ display: 'grid', gap: 4 }}>
            <span>Max quarantine ratio (0 - 1)</span>
            <input
              type="text"
              value={draft.max_quarantine_ratio}
              disabled={disabled}
              onChange={handleRatioChange}
              placeholder="0.05"
              style={{ width: 160 }}
            />
          </label>
        </div>
      </div>

      <div className="card stack">
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <div>
            <h3 style={{ margin: 0 }}>Anomaly Advisory Detection</h3>
            <p style={{ margin: '4px 0 0', fontSize: 14, color: 'var(--muted)' }}>
              Phát hiện dị thường metric dựa trên lịch sử (luôn là <b>advisory</b>, không chặn gate).
            </p>
          </div>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              checked={draft.anomaly.enabled}
              disabled={disabled}
              onChange={e => handleAnomalyToggle(e.target.checked)}
            />
            <b>Bật Anomaly</b>
          </label>
        </div>

        {draft.anomaly.enabled && (
          <div className="row" style={{ flexWrap: 'wrap', gap: 16, marginTop: 12 }}>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Max history points (1-200)</span>
              <input
                type="text"
                value={draft.anomaly.max_points}
                disabled={disabled}
                onChange={e =>
                  onChange({
                    ...draft,
                    anomaly: { ...draft.anomaly, max_points: e.target.value },
                  })
                }
                style={{ width: 140 }}
              />
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Lower bound (canonical decimal)</span>
              <input
                type="text"
                value={draft.anomaly.lower}
                disabled={disabled}
                onChange={e =>
                  onChange({
                    ...draft,
                    anomaly: { ...draft.anomaly, lower: e.target.value },
                  })
                }
                style={{ width: 140 }}
              />
            </label>
            <label style={{ display: 'grid', gap: 4 }}>
              <span>Upper bound (canonical decimal)</span>
              <input
                type="text"
                value={draft.anomaly.upper}
                disabled={disabled}
                onChange={e =>
                  onChange({
                    ...draft,
                    anomaly: { ...draft.anomaly, upper: e.target.value },
                  })
                }
                style={{ width: 140 }}
              />
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 24 }}>
              <input
                type="checkbox"
                checked={draft.anomaly.bootstrap}
                disabled={disabled}
                onChange={e =>
                  onChange({
                    ...draft,
                    anomaly: { ...draft.anomaly, bootstrap: e.target.checked },
                  })
                }
              />
              <span>Bootstrap (khởi tạo khi thiếu điểm)</span>
            </label>
          </div>
        )}
      </div>
    </div>
  );
}
