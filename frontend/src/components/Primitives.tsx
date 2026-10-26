import { useId, useState, type ReactNode } from 'react';
import { AlertTriangle, Check, Copy } from 'lucide-react';

export function PageHeader({ title, description, actions }: { title: string; description?: string; actions?: ReactNode }) {
  return <header className="row" style={{ justifyContent: 'space-between', alignItems: 'start' }}><div><h1 style={{ margin: '0 0 4px', fontSize: 24 }}>{title}</h1>{description && <div className="muted">{description}</div>}</div>{actions}</header>;
}
export function StatusBadge({ value }: { value: string | null | undefined }) {
  const status = value ?? 'UNKNOWN';
  return <span className={`status ${status.toLowerCase()}`}>{status}</span>;
}
export function Field({ label, children, hint }: { label: string; children: (id: string) => ReactNode; hint?: string }) {
  const id = useId();
  return <label htmlFor={id}>{label}{children(id)}{hint && <small className="muted">{hint}</small>}</label>;
}
export function ApiErrorAlert({ error, retry }: { error: unknown; retry?: () => void }) {
  const message = error instanceof Error ? error.message : 'Không thể tải dữ liệu.';
  return <div className="card" role="alert" style={{ borderColor: 'var(--fail-fg)' }}><div className="row"><AlertTriangle size={18} /><span>{message}</span>{retry && <button onClick={retry}>Thử lại</button>}</div></div>;
}
export function EmptyState({ title, detail, action }: { title: string; detail?: string; action?: ReactNode }) {
  return <div className="card" style={{ textAlign: 'center', padding: 32 }}><h2 style={{ marginTop: 0 }}>{title}</h2>{detail && <p className="muted">{detail}</p>}{action}</div>;
}
export function LoadingState() { return <div className="card" role="status">Đang tải…</div>; }
export function CopyId({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  }
  return <button aria-label="Sao chép ID" onClick={copy} title={value} style={{ padding: 4, minHeight: 32 }}>
    {copied ? <Check size={16} /> : <Copy size={16} />}
  </button>;
}
export function CursorPager({ nextCursor, onNext, onBack, canBack }: { nextCursor: string | null; onNext: () => void; onBack: () => void; canBack: boolean }) {
  return <div className="row" style={{ justifyContent: 'end' }}><button onClick={onBack} disabled={!canBack}>Trước</button><button onClick={onNext} disabled={!nextCursor}>Tiếp</button></div>;
}
