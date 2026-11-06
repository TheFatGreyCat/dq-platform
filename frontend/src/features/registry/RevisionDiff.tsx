import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../../api/endpoints';
import type { Policy, Revision, Suite } from '../../api/types';
import { ApiErrorAlert, CursorPager, LoadingState } from '../../components/Primitives';
import { diffPolicyPayloads, diffSuitePayloads } from './revisionDiffUtils';

interface RevisionDiffProps {
  dataset: string;
  suiteId: string;
}

export function RevisionDiff({ dataset, suiteId }: RevisionDiffProps) {
  const [params, setParams] = useSearchParams();

  const kind = (params.get('kind') === 'policy-revisions' ? 'policy-revisions' : 'revisions') as 'revisions' | 'policy-revisions';
  const leftId = params.get('left') ?? '';
  const rightId = params.get('right') ?? '';

  // Pagination for revisions list
  const [cursorStack, setCursorStack] = useState<(string | null)[]>([null]);
  const currentCursor = cursorStack.at(-1) ?? null;

  const revisionsQuery = useQuery({
    queryKey: ['revisions-diff-list', dataset, suiteId, kind, currentCursor],
    queryFn: ({ signal }) =>
      endpoints.collection<Revision>(
        dataset,
        `suites/${encodeURIComponent(suiteId)}/${kind}`,
        currentCursor,
        signal
      ).then(r => r.data),
  });

  // Query left revision
  const leftQuery = useQuery({
    queryKey: ['revision-detail', dataset, suiteId, kind, leftId],
    enabled: Boolean(leftId),
    queryFn: ({ signal }) =>
      endpoints.revision(dataset, suiteId, kind, leftId, signal).then(r => r.data),
  });

  // Query right revision
  const rightQuery = useQuery({
    queryKey: ['revision-detail', dataset, suiteId, kind, rightId],
    enabled: Boolean(rightId),
    queryFn: ({ signal }) =>
      endpoints.revision(dataset, suiteId, kind, rightId, signal).then(r => r.data),
  });

  function setKind(nextKind: 'revisions' | 'policy-revisions') {
    setCursorStack([null]);
    setParams({ kind: nextKind, left: '', right: '' });
  }

  function setLeft(id: string) {
    const next = new URLSearchParams(params);
    if (id) next.set('left', id);
    else next.delete('left');
    setParams(next);
  }

  function setRight(id: string) {
    const next = new URLSearchParams(params);
    if (id) next.set('right', id);
    else next.delete('right');
    setParams(next);
  }

  const items = revisionsQuery.data?.items ?? [];

  return (
    <div className="card stack" style={{ gap: 20 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h3 style={{ margin: 0 }}>So sánh Revisions (Diff)</h3>
          <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--muted)' }}>
            So sánh thay đổi giữa 2 revision cụ thể cùng loại. Match rule theo <code>rule_id</code>.
          </p>
        </div>
        <div className="row" style={{ gap: 8 }}>
          <button
            type="button"
            className={kind === 'revisions' ? 'primary' : ''}
            onClick={() => setKind('revisions')}
          >
            Suite Revisions
          </button>
          <button
            type="button"
            className={kind === 'policy-revisions' ? 'primary' : ''}
            onClick={() => setKind('policy-revisions')}
          >
            Policy Revisions
          </button>
        </div>
      </div>

      {revisionsQuery.isPending && <LoadingState />}
      {revisionsQuery.isError && (
        <ApiErrorAlert error={revisionsQuery.error} retry={() => void revisionsQuery.refetch()} />
      )}

      {/* Selectors */}
      <div className="row" style={{ flexWrap: 'wrap', gap: 20, alignItems: 'flex-start' }}>
        <label style={{ display: 'grid', gap: 4, flex: 1, minWidth: 260 }}>
          <span><b>Revision Gốc (Left / Baseline)</b></span>
          <select
            value={leftId}
            aria-label="Chọn revision gốc"
            onChange={e => setLeft(e.target.value)}
          >
            <option value="">-- Chọn revision --</option>
            {items.map(r => (
              <option key={r.revision_id} value={r.revision_id}>
                r{r.revision_no} ({r.revision_id.slice(0, 8)}) · {r.created_at.slice(0, 16)}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 4, flex: 1, minWidth: 260 }}>
          <span><b>Revision So Sánh (Right / Target)</b></span>
          <select
            value={rightId}
            aria-label="Chọn revision so sánh"
            onChange={e => setRight(e.target.value)}
          >
            <option value="">-- Chọn revision --</option>
            {items.map(r => (
              <option key={r.revision_id} value={r.revision_id}>
                r{r.revision_no} ({r.revision_id.slice(0, 8)}) · {r.created_at.slice(0, 16)}
              </option>
            ))}
          </select>
        </label>
      </div>

      {revisionsQuery.data && (
        <CursorPager
          nextCursor={revisionsQuery.data.next_cursor}
          canBack={cursorStack.length > 1}
          onBack={() => setCursorStack(cursorStack.slice(0, -1))}
          onNext={() =>
            revisionsQuery.data?.next_cursor &&
            setCursorStack([...cursorStack, revisionsQuery.data.next_cursor])
          }
        />
      )}

      {/* Error displays for individual revisions */}
      {leftQuery.isError && (
        <div className="stack">
          <b>Lỗi tải revision gốc:</b>
          <ApiErrorAlert error={leftQuery.error} retry={() => void leftQuery.refetch()} />
        </div>
      )}
      {rightQuery.isError && (
        <div className="stack">
          <b>Lỗi tải revision so sánh:</b>
          <ApiErrorAlert error={rightQuery.error} retry={() => void rightQuery.refetch()} />
        </div>
      )}

      {/* Diff Output */}
      {leftQuery.isPending || rightQuery.isPending ? (
        <LoadingState />
      ) : leftId && rightId && leftQuery.data && rightQuery.data ? (
        <DiffViewer
          kind={kind}
          left={leftQuery.data}
          right={rightQuery.data}
        />
      ) : (
        <div style={{ padding: 24, textAlign: 'center', color: 'var(--muted)', background: 'var(--surface-muted, #f8fafc)', borderRadius: 8 }}>
          Vui lòng chọn cả 2 revision từ danh sách trên để xem chi tiết khác biệt.
        </div>
      )}
    </div>
  );
}

// ----------------------------------------------------------------------
// Sub-component: DiffViewer
// ----------------------------------------------------------------------

interface DiffViewerProps {
  kind: 'revisions' | 'policy-revisions';
  left: Revision;
  right: Revision;
}

function DiffViewer({ kind, left, right }: DiffViewerProps) {
  if (kind === 'revisions') {
    const leftSuite = left.payload as unknown as Suite;
    const rightSuite = right.payload as unknown as Suite;
    const diff = diffSuitePayloads(leftSuite, rightSuite);

    const added = diff.ruleChanges.filter(r => r.changeType === 'added').length;
    const removed = diff.ruleChanges.filter(r => r.changeType === 'removed').length;
    const changed = diff.ruleChanges.filter(r => r.changeType === 'changed').length;
    const unchanged = diff.ruleChanges.filter(r => r.changeType === 'unchanged').length;

    return (
      <div className="stack" style={{ gap: 16 }}>
        <div className="row" style={{ flexWrap: 'wrap', gap: 12, padding: '10px 14px', background: 'var(--surface-muted, #f8fafc)', borderRadius: 6 }}>
          <span><b>Tổng quan:</b></span>
          <span style={{ color: 'var(--success, #16a34a)' }}>+{added} thêm mới</span>
          <span style={{ color: 'var(--error, #e53e3e)' }}>-{removed} đã xóa</span>
          <span style={{ color: 'var(--brand, #d97706)' }}>~{changed} đã sửa</span>
          <span style={{ color: 'var(--muted)' }}>={unchanged} không đổi</span>
        </div>

        {diff.suiteLevelChanges.length > 0 && (
          <div className="card stack" style={{ border: '1px solid var(--border)' }}>
            <b>Thay đổi cấp Suite:</b>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Trường</th>
                    <th>Revision r{left.revision_no}</th>
                    <th>Revision r{right.revision_no}</th>
                  </tr>
                </thead>
                <tbody>
                  {diff.suiteLevelChanges.map(c => (
                    <tr key={c.field}>
                      <td><b className="mono">{c.field}</b></td>
                      <td style={{ color: 'var(--error, #e53e3e)' }}><pre style={{ margin: 0 }}>{JSON.stringify(c.left, null, 2)}</pre></td>
                      <td style={{ color: 'var(--success, #16a34a)' }}><pre style={{ margin: 0 }}>{JSON.stringify(c.right, null, 2)}</pre></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <div className="stack" style={{ gap: 12 }}>
          <b>Chi tiết Rules:</b>
          {diff.ruleChanges.map(r => (
            <div
              key={r.rule_id}
              className="card stack"
              style={{
                border: '1px solid var(--border)',
                borderLeft: `4px solid ${
                  r.changeType === 'added'
                    ? 'var(--success, #16a34a)'
                    : r.changeType === 'removed'
                    ? 'var(--error, #e53e3e)'
                    : r.changeType === 'changed'
                    ? 'var(--brand, #d97706)'
                    : 'var(--border)'
                }`,
                padding: 12,
                gap: 8,
              }}
            >
              <div className="row" style={{ justifyContent: 'space-between' }}>
                <span className="mono" style={{ fontWeight: 600 }}>{r.rule_id}</span>
                <span
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    textTransform: 'uppercase',
                    color:
                      r.changeType === 'added'
                        ? 'var(--success, #16a34a)'
                        : r.changeType === 'removed'
                        ? 'var(--error, #e53e3e)'
                        : r.changeType === 'changed'
                        ? 'var(--brand, #d97706)'
                        : 'var(--muted)',
                  }}
                >
                  {r.changeType === 'added' ? 'Thêm mới (+)' : r.changeType === 'removed' ? 'Đã xóa (-)' : r.changeType === 'changed' ? 'Đã thay đổi (~)' : 'Không đổi'}
                </span>
              </div>

              {r.changeType === 'changed' && (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Đường dẫn thuộc tính</th>
                        <th>Trước (r{left.revision_no})</th>
                        <th>Sau (r{right.revision_no})</th>
                      </tr>
                    </thead>
                    <tbody>
                      {r.changes.map(ch => (
                        <tr key={ch.path}>
                          <td><b className="mono">{ch.path}</b></td>
                          <td style={{ color: 'var(--error, #e53e3e)' }}>{String(typeof ch.left === 'object' ? JSON.stringify(ch.left) : ch.left ?? '—')}</td>
                          <td style={{ color: 'var(--success, #16a34a)' }}>{String(typeof ch.right === 'object' ? JSON.stringify(ch.right) : ch.right ?? '—')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {r.changeType === 'added' && (
                <pre className="mono" style={{ margin: 0, fontSize: 13, maxHeight: 160, overflow: 'auto' }}>
                  {JSON.stringify(r.right, null, 2)}
                </pre>
              )}

              {r.changeType === 'removed' && (
                <pre className="mono" style={{ margin: 0, fontSize: 13, maxHeight: 160, overflow: 'auto' }}>
                  {JSON.stringify(r.left, null, 2)}
                </pre>
              )}
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Policy diff
  const leftPolicy = left.payload as unknown as Policy;
  const rightPolicy = right.payload as unknown as Policy;
  const diff = diffPolicyPayloads(leftPolicy, rightPolicy);

  const added = diff.ruleChanges.filter(r => r.changeType === 'added').length;
  const removed = diff.ruleChanges.filter(r => r.changeType === 'removed').length;
  const changed = diff.ruleChanges.filter(r => r.changeType === 'changed').length;
  const unchanged = diff.ruleChanges.filter(r => r.changeType === 'unchanged').length;

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="row" style={{ flexWrap: 'wrap', gap: 12, padding: '10px 14px', background: 'var(--surface-muted, #f8fafc)', borderRadius: 6 }}>
        <span><b>Tổng quan Policy:</b></span>
        <span style={{ color: 'var(--success, #16a34a)' }}>+{added} thêm mới</span>
        <span style={{ color: 'var(--error, #e53e3e)' }}>-{removed} đã xóa</span>
        <span style={{ color: 'var(--brand, #d97706)' }}>~{changed} đã sửa</span>
        <span style={{ color: 'var(--muted)' }}>={unchanged} không đổi</span>
      </div>

      {diff.globalChanges.length > 0 && (
        <div className="card stack" style={{ border: '1px solid var(--border)' }}>
          <b>Thay đổi Ngưỡng Toàn Cục:</b>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Ngưỡng</th>
                  <th>Revision r{left.revision_no}</th>
                  <th>Revision r{right.revision_no}</th>
                </tr>
              </thead>
              <tbody>
                {diff.globalChanges.map(c => (
                  <tr key={c.field}>
                    <td><b className="mono">{c.field}</b></td>
                    <td style={{ color: 'var(--error, #e53e3e)' }}>{String(typeof c.left === 'object' ? JSON.stringify(c.left) : c.left ?? '—')}</td>
                    <td style={{ color: 'var(--success, #16a34a)' }}>{String(typeof c.right === 'object' ? JSON.stringify(c.right) : c.right ?? '—')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="stack" style={{ gap: 12 }}>
        <b>Chi tiết Policy Rules:</b>
        {diff.ruleChanges.map(r => (
          <div
            key={r.rule_id}
            className="card stack"
            style={{
              border: '1px solid var(--border)',
              borderLeft: `4px solid ${
                r.changeType === 'added'
                  ? 'var(--success, #16a34a)'
                  : r.changeType === 'removed'
                  ? 'var(--error, #e53e3e)'
                  : r.changeType === 'changed'
                  ? 'var(--brand, #d97706)'
                  : 'var(--border)'
              }`,
              padding: 12,
              gap: 8,
            }}
          >
            <div className="row" style={{ justifyContent: 'space-between' }}>
              <span className="mono" style={{ fontWeight: 600 }}>{r.rule_id}</span>
              <span
                style={{
                  fontSize: 12,
                  fontWeight: 600,
                  textTransform: 'uppercase',
                  color:
                    r.changeType === 'added'
                      ? 'var(--success, #16a34a)'
                      : r.changeType === 'removed'
                      ? 'var(--error, #e53e3e)'
                      : r.changeType === 'changed'
                      ? 'var(--brand, #d97706)'
                      : 'var(--muted)',
                }}
              >
                {r.changeType === 'added' ? 'Thêm mới (+)' : r.changeType === 'removed' ? 'Đã xóa (-)' : r.changeType === 'changed' ? 'Đã thay đổi (~)' : 'Không đổi'}
              </span>
            </div>

            {r.changeType === 'changed' && (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Thuộc tính</th>
                      <th>Trước (r{left.revision_no})</th>
                      <th>Sau (r{right.revision_no})</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.changes.map(ch => (
                      <tr key={ch.path}>
                        <td><b className="mono">{ch.path}</b></td>
                        <td style={{ color: 'var(--error, #e53e3e)' }}>{String(ch.left)}</td>
                        <td style={{ color: 'var(--success, #16a34a)' }}>{String(ch.right)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
