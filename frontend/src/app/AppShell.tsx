import { useEffect, useRef, useState } from 'react';
import { Link, NavLink, Outlet, useLocation, useNavigate, useParams } from 'react-router';
import { Menu, Moon, Sun, Unplug, PanelLeftClose, PanelLeftOpen, X } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { endpoints } from '../api/endpoints';
import { queryClient } from './queryClient';
import { applyTheme, type Theme } from './preferences';
import { session } from '../features/auth/session';
import { ConnectPage } from '../features/auth/ConnectPage';

export function ProtectedApp() {
  const [connected, setConnected] = useState(Boolean(session.token()));
  const navigate = useNavigate();
  useEffect(() => {
    const update = (event: Event) => {
      const isConnected = Reflect.get(event, 'detail') === true;
      if (isConnected) navigate('/datasets', { replace: true });
      else { void queryClient.cancelQueries(); queryClient.clear(); }
      setConnected(isConnected);
    };
    window.addEventListener('dq-session-state', update);
    session.onUnauthorized(() => session.disconnect());
    return () => { window.removeEventListener('dq-session-state', update); session.onUnauthorized(() => {}); };
  }, [navigate]);
  if (!connected) return <ConnectPage />;
  return <AppShell />;
}

function AppShell() {
  const { data: identity } = useQuery({ queryKey: ['me'], queryFn: ({ signal }) => endpoints.me(signal).then(result => result.data) });
  const { data: datasets } = useQuery({ queryKey: ['datasets'], queryFn: ({ signal }) => endpoints.datasets(null, signal).then(result => result.data) });
  const location = useLocation();
  const navigate = useNavigate();
  const { executionId } = useParams();
  const execution = useQuery({ queryKey: ['execution', executionId], enabled: Boolean(executionId), queryFn: ({ signal }) => endpoints.execution(decodeURIComponent(executionId ?? ''), signal).then(r => r.data) });
  const [theme, setTheme] = useState<Theme>('light');
  const [collapsed, setCollapsed] = useState(false);
  const [drawer, setDrawer] = useState(false);
  const [selected, setSelected] = useState('');
  const menuOpener = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    const desktop = window.matchMedia('(min-width: 1024px)');
    const closeOnDesktop = (event: MediaQueryListEvent) => { if (event.matches) setDrawer(false); };
    desktop.addEventListener('change', closeOnDesktop);
    return () => desktop.removeEventListener('change', closeOnDesktop);
  }, []);
  function closeDrawer() { setDrawer(false); if (window.matchMedia('(max-width: 1023px)').matches) menuOpener.current?.focus(); }
  useEffect(() => {
    if (!drawer) return;
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === 'Escape') closeDrawer(); };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [drawer]);
  useEffect(() => {
    const match = location.pathname.match(/^\/datasets\/([^/]+)/);
    setSelected(match?.[1] ? decodeURIComponent(match[1]) : '');
  }, [location.pathname]);
  useEffect(() => { if (executionId && execution.data) setSelected(execution.data.dataset_id); }, [executionId, execution.data]);
  useEffect(() => { applyTheme(theme); }, [theme]);
  function disconnect() { session.disconnect(); }
  const navItems: { href: string; label: string; dataset: boolean }[] = [
    { href: '/', label: 'Dashboard', dataset: false }, { href: '/datasets', label: 'Datasets', dataset: false },
    ...(selected ? [
      { href: `/datasets/${encodeURIComponent(selected)}/scoreboard`, label: 'Scoreboard', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/executions`, label: 'Executions', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/suites`, label: 'Suites & Policies', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/purposes`, label: 'Purposes & Bindings', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/compatibility`, label: 'Compatibility', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/publications`, label: 'Publication', dataset: true },
      { href: `/datasets/${encodeURIComponent(selected)}/operations`, label: 'Operations', dataset: true },
{ href: `/datasets/${encodeURIComponent(selected)}/alerts`, label: 'Alerts', dataset: true },
{ href: `/datasets/${encodeURIComponent(selected)}/audit`, label: 'Audit trail', dataset: true },
    ] : []),
  ];
  const nav = <nav className="stack" aria-label="Điều hướng chính">{navItems.map(({ href, label, dataset }) => href.endsWith('/executions') && location.pathname.startsWith('/executions/') ? <Link key={href} to={href} aria-current="page" onClick={closeDrawer} className="navigation-link">{label}</Link> : <NavLink key={href} to={href} end={!dataset} onClick={closeDrawer} className="navigation-link">{label}</NavLink>)}</nav>;
  return <div style={{ minHeight: '100vh', borderTop: '4px solid var(--brand)' }}>
    <header className="row app-header" style={{ minHeight: 64, background: 'var(--nav)', color: 'white', padding: '0 16px', justifyContent: 'space-between' }}>
      <div className="row"><button ref={menuOpener} className="mobile-menu" aria-label="Mở điều hướng" aria-expanded={drawer} onClick={() => setDrawer(true)}><Menu size={20} /></button><b><span style={{ color: 'var(--brand)' }}>DQ</span> Platform</b><select aria-label="Dataset hiện tại" value={selected} onChange={event => { const id = event.target.value; setSelected(id); navigate(id ? `/datasets/${encodeURIComponent(id)}` : '/datasets'); }} style={{ width: 220 }}><option value="">Chọn dataset</option>{selected && !datasets?.items.some(d => d.dataset_id === selected) && <option value={selected}>{selected}</option>}{datasets?.items.map(dataset => <option key={dataset.dataset_id} value={dataset.dataset_id}>{dataset.dataset_id}</option>)}</select></div>
      <div className="row"><span className="principal-label" title={identity?.principal}>{identity?.principal}</span><button aria-label="Đổi giao diện" onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}>{theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}</button><button aria-label="Ngắt kết nối" onClick={disconnect}><Unplug size={16} /> <span className="logout-label">Ngắt kết nối</span></button></div>
    </header>
    <div className="layout"><aside className="sidebar" style={{ width: collapsed ? 64 : 240 }}><button aria-label={collapsed ? 'Mở rộng điều hướng' : 'Thu gọn điều hướng'} onClick={() => setCollapsed(!collapsed)}>{collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}</button>{!collapsed && nav}</aside><main className="content"><Outlet /></main></div>
    {drawer && <div className="drawer-backdrop" onClick={closeDrawer}><section role="dialog" aria-modal="true" aria-label="Điều hướng" onClick={event => event.stopPropagation()}><button autoFocus aria-label="Đóng điều hướng" onClick={closeDrawer}><X size={20} /> Đóng</button>{nav}</section></div>}
  </div>;
}
export { AppShell };
