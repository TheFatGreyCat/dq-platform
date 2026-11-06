import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiRequest } from '../../api/client';
import { endpoints } from '../../api/endpoints';
import type { Revision, Workflow } from '../../api/types';
import { SuiteBuilder } from './SuiteBuilder';
import { RevisionDiff } from './RevisionDiff';
import { ApiErrorAlert, EmptyState, Field, LoadingState, PageHeader } from '../../components/Primitives';

export function RegistryPage() {
  const { datasetId = '', '*': route = '' } = useParams();
  const dataset = decodeURIComponent(datasetId);
  if (route === 'registration') return <RegistrationPage dataset={dataset} />;
  if (route === 'purposes') return <PurposesPage dataset={dataset} />;
  if (route.startsWith('purposes/')) return <PurposeDetail dataset={dataset} route={route} />;
  if (route === 'compatibility') return <CompatibilityPage dataset={dataset} />;
  if (route === 'suites') return <SuitesPage dataset={dataset} />;
  if (route.startsWith('suites/') && route.split('/').length === 2 && route.split('/')[1]) return <SuitePage dataset={dataset} suite={route.slice('suites/'.length)} />;
  return <div className="card stack" role="status"><h1>Page not found</h1><p>Unknown dataset route: {route}</p><Link to="/datasets">Datasets</Link></div>;
}

function SuitesPage({ dataset }: { dataset: string }) {
  const cache = useQueryClient();
  const [name, setName] = useState('');
  const [workflow, setWorkflow] = useState('WRITE_GATE');
  const list = useQuery({ queryKey: ['suites', dataset], queryFn: ({ signal }) => endpoints.collection<Record<string, unknown>>(dataset, 'suites', null, signal).then(r => r.data) });
  const create = useMutation({ mutationFn: () => apiRequest(`/api/v2/datasets/${encodeURIComponent(dataset)}/suites`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
    body: JSON.stringify({ name, workflow, description: null }),
  }), onSuccess: () => { setName(''); void cache.invalidateQueries({ queryKey: ['suites', dataset] }); } });
  if (list.isPending) return <LoadingState />;
  if (list.isError) return <ApiErrorAlert error={list.error} retry={() => void list.refetch()} />;
  return <div className="stack"><PageHeader title="Suites & Policies" description="Revision đã tạo là immutable." />
    <div className="card row"><label>Tên suite<input value={name} onChange={e => setName(e.target.value)} /></label><label>Workflow<select value={workflow} onChange={e => setWorkflow(e.target.value)}><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label><button className="primary" disabled={!name || create.isPending} onClick={() => create.mutate()}>Tạo suite</button></div>
    {create.isError && <ApiErrorAlert error={create.error} />}{list.data.items.length ? <div className="table-wrap"><table><thead><tr><th>Name</th><th>Workflow</th><th>ID</th><th /></tr></thead><tbody>{list.data.items.map((suite, index) => <tr key={String(suite.suite_id ?? index)}><td>{String(suite.name)}</td><td>{String(suite.workflow)}</td><td className="mono">{String(suite.suite_id)}</td><td><Link to={`/datasets/${encodeURIComponent(dataset)}/suites/${String(suite.suite_id)}`}>Mở</Link></td></tr>)}</tbody></table></div> : <EmptyState title="Chưa có suite" />}</div>;
}

function SuitePage({ dataset, suite }: { dataset: string; suite: string }) {
  const [kind, setKind] = useState<'revisions' | 'policy-revisions'>('revisions');
  const [selected, setSelected] = useState('');
  const [activeTab, setActiveTab] = useState<'builder' | 'diff'>('builder');
  const [clonedPayload, setClonedPayload] = useState<Record<string, unknown> | null>(null);
  const suiteMeta = useQuery({ queryKey: ['suite-meta', dataset, suite], queryFn: ({ signal }) => endpoints.suite(dataset, suite, signal).then(r => r.data) });
  const list = useQuery({ queryKey: ['revisions', dataset, suite, kind], queryFn: ({ signal }) => endpoints.collection<Revision>(dataset, `suites/${encodeURIComponent(suite)}/${kind}`, null, signal).then(r => r.data) });
  const detail = useQuery({ queryKey: ['revision', dataset, suite, kind, selected], enabled: Boolean(selected), queryFn: ({ signal }) => endpoints.revision(dataset, suite, kind, selected, signal).then(r => r.data) });

  if (suiteMeta.isPending || list.isPending) return <LoadingState />;
  if (suiteMeta.isError) return <ApiErrorAlert error={suiteMeta.error} retry={() => void suiteMeta.refetch()} />;
  if (list.isError) return <ApiErrorAlert error={list.error} retry={() => void list.refetch()} />;

  const workflow = ((suiteMeta.data?.workflow as Workflow) ?? 'WRITE_GATE');

  return <div className="stack">
    <PageHeader title={`Suite: ${String(suiteMeta.data?.name ?? suite)}`} description={`Workflow: ${workflow} · ID: ${suite}`} />
    <div className="row" style={{ gap: 8, borderBottom: '1px solid var(--border)', paddingBottom: 12 }}>
      <button type="button" className={activeTab === 'builder' ? 'primary' : ''} onClick={() => setActiveTab('builder')}>Soạn thảo & Revisions</button>
      <button type="button" className={activeTab === 'diff' ? 'primary' : ''} onClick={() => setActiveTab('diff')}>So sánh Revisions (Diff)</button>
    </div>
    {activeTab === 'diff' ? <RevisionDiff dataset={dataset} suiteId={suite} /> : <>
    <div className="card stack">
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <div className="row" style={{ gap: 8 }}>
          <button className={kind === 'revisions' ? 'primary' : ''} onClick={() => { setKind('revisions'); setSelected(''); }}>Suite revisions</button>
          <button className={kind === 'policy-revisions' ? 'primary' : ''} onClick={() => { setKind('policy-revisions'); setSelected(''); }}>Policy revisions</button>
        </div>
        <Link to={`/datasets/${encodeURIComponent(dataset)}/purposes`} style={{ fontSize: 14 }}>Xem mục Purposes & Bindings →</Link>
      </div>
      <div className="row" style={{ flexWrap: 'wrap', gap: 8 }}>
        {list.data.items.length === 0 ? <span style={{ color: 'var(--muted)', fontSize: 13 }}>Chưa có revision nào.</span> :
          list.data.items.map(r => <button key={r.revision_id} className={selected === r.revision_id ? 'primary' : ''} onClick={() => setSelected(r.revision_id)}>r{r.revision_no} · {r.content_hash.slice(0, 12)}</button>)}
      </div>
      {detail.data && <div className="card stack" style={{ background: 'var(--surface-muted, #f8fafc)', border: '1px solid var(--border)' }}>
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <b>r{detail.data.revision_no} · {detail.data.created_by} · {detail.data.content_hash} {detail.data.contains_sql ? '· contains SQL' : ''}</b>
          <button type="button" onClick={() => setClonedPayload(detail.data.payload)}>Clone vào form soạn thảo</button>
        </div>
        <pre className="mono" style={{ maxHeight: 200, overflow: 'auto' }}>{JSON.stringify(detail.data.payload, null, 2)}</pre>
      </div>}
    </div>

    <SuiteBuilder
      dataset={dataset}
      suiteId={suite}
      workflow={workflow}
      clonedPayload={clonedPayload}
      onSavedRevision={(savedKind, id) => {
        setKind(savedKind);
        setSelected(id);
        void list.refetch();
      }}
    />
    </>}
  </div>;
}

function RegistrationPage({ dataset }: { dataset: string }) {
  const cache = useQueryClient();
  const [payload, setPayload] = useState('');
  const [confirm, setConfirm] = useState(false);
  const query = useQuery({ queryKey: ['dataset', dataset], queryFn: async ({ signal }) => {
    try { return (await endpoints.dataset(dataset, signal)).data; }
    catch (error) { if (error instanceof Error && 'status' in error && Reflect.get(error, 'status') === 404) return null; throw error; }
  } });
  const save = useMutation({ mutationFn: () => {
    if (new TextEncoder().encode(payload).length > 1_048_576) throw new Error('Registration vượt 1 MiB.');
    const parsed: unknown = JSON.parse(payload);
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error('Registration phải là JSON object.');
    return apiRequest<Record<string, unknown>>(`/api/v2/datasets/${encodeURIComponent(dataset)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: payload });
  }, onSuccess: async () => { setConfirm(false); await cache.invalidateQueries({ queryKey: ['dataset', dataset] }); await cache.invalidateQueries({ queryKey: ['datasets'] }); } });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ApiErrorAlert error={query.error} retry={() => void query.refetch()} />;
  const registration = query.data?.registration;
  return <div className="stack"><PageHeader title="Registration editor" description={registration ? "Save thay registration hiện tại; execution cũ tiếp tục dùng snapshot." : "Dataset grant cho phép đăng ký; nhập exact bootstrap Registration JSON."} /><div className="card stack"><Field label="Registration JSON (object, ≤1 MiB)">{id => <textarea id={id} rows={20} value={payload} onChange={e => setPayload(e.target.value)} placeholder={JSON.stringify(registration ?? {}, null, 2)} />}</Field>{registration != null ? <button onClick={() => setPayload(JSON.stringify(registration, null, 2))}>Nạp registration hiện tại</button> : null}<button className="primary" disabled={!payload.trim()} onClick={() => setConfirm(true)}>Xem trước</button>{confirm && <div role="alertdialog" aria-modal="true" className="card"><p>{registration ? "Thay registration hiện tại. Execution cũ vẫn dùng snapshot." : "Tạo registration cho dataset được cấp grant."}</p><button onClick={() => setConfirm(false)}>Hủy</button><button className="primary" disabled={save.isPending} onClick={() => save.mutate()}>Xác nhận</button></div>}{save.isError && <ApiErrorAlert error={save.error} />}{save.data && <p role="status">Registration hash: {String(save.data.data.registration_hash)}</p>}</div></div>;
}

function PurposesPage({ dataset }: { dataset: string }) {
  const [workflow, setWorkflow] = useState('WRITE_GATE'); const [purpose, setPurpose] = useState(''); const [description, setDescription] = useState('');
  const list = useQuery({ queryKey: ['purposes', dataset], queryFn: ({ signal }) => endpoints.collection<Record<string, unknown>>(dataset, 'purposes', null, signal).then(r => r.data) });
  const create = useMutation({ mutationFn: () => apiRequest(`/api/v2/datasets/${encodeURIComponent(dataset)}/purposes/${workflow}/${encodeURIComponent(purpose)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ description: description || null }) }), onSuccess: () => void list.refetch() });
  if (list.isPending) return <LoadingState />;
  if (list.isError) return <ApiErrorAlert error={list.error} retry={() => void list.refetch()} />;
  return <div className="stack"><PageHeader title="Purposes & Bindings" description="Binding revisions are pinned by explicit IDs." /><div className="card row"><label>Workflow<select value={workflow} onChange={e => setWorkflow(e.target.value)}><option>WRITE_GATE</option><option>AUDIT_TABLE</option></select></label><label>Purpose<input value={purpose} onChange={e => setPurpose(e.target.value)} /></label><label>Description<input value={description} onChange={e => setDescription(e.target.value)} /></label><button className="primary" disabled={!purpose || create.isPending} onClick={() => create.mutate()}>Đăng ký purpose</button></div>{create.isError && <ApiErrorAlert error={create.error} />}<div className="table-wrap"><table><thead><tr><th>Workflow</th><th>Purpose</th><th>State</th><th>Version</th><th>Binding</th></tr></thead><tbody>{list.data.items.map((p, i) => <tr key={`${String(p.workflow)}-${String(p.purpose)}-${i}`}><td>{String(p.workflow)}</td><td><Link to={`/datasets/${encodeURIComponent(dataset)}/purposes/${p.workflow}/${p.purpose}`}>{String(p.purpose)}</Link></td><td>{String(p.state)}</td><td>{String(p.binding_version)}</td><td>{String(p.suite_revision_id)} / {String(p.policy_revision_id)}</td></tr>)}</tbody></table></div></div>;
}

function PurposeDetail({ dataset, route }: { dataset: string; route: string }) {
  const [, workflow = '', purpose = ''] = route.split('/');
  const cache = useQueryClient();
  const key = ['purpose', dataset, workflow, purpose];
  const detail = useQuery({ queryKey: key, queryFn: ({ signal }) => apiRequest<Record<string, unknown>>(`/api/v2/datasets/${encodeURIComponent(dataset)}/purposes/${workflow}/${encodeURIComponent(purpose)}`, { signal }).then(r => r.data) });
  const suites = useQuery({ queryKey: ['suites', dataset], queryFn: ({ signal }) => endpoints.collection<Record<string, unknown>>(dataset, 'suites', null, signal).then(r => r.data) });
  const events = useQuery({ queryKey: ['purpose-events', dataset, workflow, purpose], queryFn: ({ signal }) => apiRequest<{ items: unknown[] }>(`/api/v2/datasets/${encodeURIComponent(dataset)}/purposes/${workflow}/${encodeURIComponent(purpose)}/events`, { signal }).then(r => r.data) });
  const [suiteId, setSuiteId] = useState(''); const [suiteRevision, setSuiteRevision] = useState(''); const [policyRevision, setPolicyRevision] = useState(''); const [reason, setReason] = useState(''); const [confirmed, setConfirmed] = useState(false);
  const suiteRevs = useQuery({ queryKey: ['revision-list', dataset, suiteId, 'revisions'], enabled: Boolean(suiteId), queryFn: ({ signal }) => endpoints.collection<Revision>(dataset, `suites/${encodeURIComponent(suiteId)}/revisions`, null, signal).then(r => r.data) });
  const policyRevs = useQuery({ queryKey: ['revision-list', dataset, suiteId, 'policy-revisions'], enabled: Boolean(suiteId), queryFn: ({ signal }) => endpoints.collection<Revision>(dataset, `suites/${encodeURIComponent(suiteId)}/policy-revisions`, null, signal).then(r => r.data) });
  const bind = useMutation({ mutationFn: () => {
    const current = detail.data;
    if (!current) throw new Error('Purpose binding chưa được tải.');
    return apiRequest(`/api/v2/datasets/${encodeURIComponent(dataset)}/purposes/${workflow}/${encodeURIComponent(purpose)}/binding`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_binding_version: current.binding_version, suite_id: suiteId, suite_revision_id: suiteRevision, policy_revision_id: policyRevision, reason }) });
  }, onSuccess: () => { setConfirmed(false); void cache.invalidateQueries({ queryKey: key }); void events.refetch(); }, onError: error => { if (error instanceof Error && 'code' in error && Reflect.get(error, 'code') === 'BINDING_CONFLICT') { void cache.invalidateQueries({ queryKey: key }); void events.refetch(); } } });
  if (detail.isPending || suites.isPending) return <LoadingState />;
  if (detail.isError) return <ApiErrorAlert error={detail.error} retry={() => void detail.refetch()} />;
  if (suites.isError) return <ApiErrorAlert error={suites.error} retry={() => void suites.refetch()} />;
  return <div className="stack"><PageHeader title={`${workflow} / ${purpose}`} description={`Canonical binding version ${String(detail.data.binding_version)}`} /><div className="card"><pre>{JSON.stringify(detail.data, null, 2)}</pre></div><div className="card stack"><h2>Bind revision</h2><label>Suite<select value={suiteId} onChange={e => { setSuiteId(e.target.value); setSuiteRevision(''); setPolicyRevision(''); }}><option value="">Chọn suite</option>{suites.data.items.filter(s => s.workflow === workflow).map(s => <option key={String(s.suite_id)} value={String(s.suite_id)}>{String(s.name)} · {String(s.suite_id)}</option>)}</select></label><label>Suite revision<select value={suiteRevision} onChange={e => setSuiteRevision(e.target.value)}><option value="">Chọn revision</option>{suiteRevs.data?.items.map(r => <option key={r.revision_id} value={r.revision_id}>r{r.revision_no} · {r.content_hash}</option>)}</select></label><label>Policy revision<select value={policyRevision} onChange={e => setPolicyRevision(e.target.value)}><option value="">Chọn policy</option>{policyRevs.data?.items.map(r => <option key={r.revision_id} value={r.revision_id}>r{r.revision_no} · {r.content_hash}</option>)}</select></label><label>Reason<input value={reason} onChange={e => setReason(e.target.value)} /></label><button className="primary" disabled={!suiteId || !suiteRevision || !policyRevision || !reason || bind.isPending} onClick={() => setConfirmed(true)}>Preview binding</button>{confirmed && <div role="alertdialog" aria-modal="true" className="card"><p>Bind exact selected revisions against version {String(detail.data.binding_version)}. Confirm only after reviewing the IDs.</p><button onClick={() => setConfirmed(false)}>Hủy</button><button className="primary" onClick={() => bind.mutate()}>Xác nhận binding</button></div>}{bind.isError && <ApiErrorAlert error={bind.error} retry={() => void detail.refetch()} />}</div><div className="card"><h2>Binding events</h2>{events.isError ? <ApiErrorAlert error={events.error} /> : <pre>{JSON.stringify(events.data?.items ?? [], null, 2)}</pre>}</div></div>;
}

function CompatibilityPage({ dataset }: { dataset: string }) {
  const [manifestId, setManifestId] = useState(''); const [manifest, setManifest] = useState<Record<string, unknown> | null>(null); const [reference, setReference] = useState('');
  const list = useQuery({ queryKey: ['compat', dataset], queryFn: ({ signal }) => endpoints.collection<Record<string, unknown>>(dataset, 'compat-manifests', null, signal).then(r => r.data) });
  const lookup = useMutation({ mutationFn: () => apiRequest<Record<string, unknown>>(`/api/v2/compat-manifests/${encodeURIComponent(manifestId)}`), onSuccess: result => setManifest(result.data) });
  const accept = useMutation({ mutationFn: () => apiRequest(`/api/v2/datasets/${encodeURIComponent(dataset)}/compat-manifests/${encodeURIComponent(manifestId)}/acceptance`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ harness_reference: reference }) }), onSuccess: () => void list.refetch() });
  if (list.isPending) return <LoadingState />;
  if (list.isError) return <ApiErrorAlert error={list.error} retry={() => void list.refetch()} />;
  return <div className="stack"><PageHeader title="Compatibility manifests" description="Acceptance ghi nhận bằng chứng ngoài; hệ thống không chạy lại probe." /><div className="card stack"><label>Manifest UUID<input value={manifestId} onChange={e => setManifestId(e.target.value)} /></label><button disabled={!manifestId || lookup.isPending} onClick={() => lookup.mutate()}>Tra cứu</button><Field label="Import manifest JSON, tối đa 1 MiB">{id => <textarea id={id} rows={8} onChange={e => { try { const parsed: unknown = JSON.parse(e.target.value); setManifest(parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null); } catch { setManifest(null); } }} />}</Field>{manifest && <pre>{JSON.stringify(manifest, null, 2)}</pre>}<label>Harness reference<input value={reference} onChange={e => setReference(e.target.value)} /></label><button className="primary" disabled={!manifest || !manifestId || !reference || accept.isPending} onClick={() => { if (window.confirm('Xác nhận bằng chứng bên ngoài; hệ thống không chạy lại probe.')) accept.mutate(); }}>Xác nhận acceptance</button>{lookup.isError && <ApiErrorAlert error={lookup.error} />}{accept.isError && <ApiErrorAlert error={accept.error} />}</div><div className="stack">{list.data.items.map((item, i) => <div className="card" key={i}><pre>{JSON.stringify(item, null, 2)}</pre></div>)}</div></div>;
}
