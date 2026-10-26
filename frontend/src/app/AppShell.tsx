import { useEffect, useState } from 'react';
import { Link, Outlet, useLocation, useNavigate } from 'react-router';
import { Moon, Sun, Unplug, PanelLeftClose, PanelLeftOpen } from 'lucide-react';
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
    return () => {
      window.removeEventListener('dq-session-state', update);
      session.onUnauthorized(() => {});
    };
  }, [navigate]);
  if (!connected) return <ConnectPage />;
  return <AppShell />;
}

function AppShell() {
  const { data: identity } = useQuery({ queryKey: ['me'], queryFn: ({ signal }) => endpoints.me(signal).then(result => result.data) });
  const { data: datasets } = useQuery({ queryKey: ['datasets'], queryFn: ({ signal }) => endpoints.datasets(null, signal).then(result => result.data) });
  const location = useLocation();
  const navigate = useNavigate();
  const [theme, setTheme] = useState<Theme>('light');
  const [collapsed, setCollapsed] = useState(false);
  const [drawer, setDrawer] = useState(false);
  const [selected, setSelected] = useState('');
  useEffect(() => {
    const match = location.pathname.match(/^\/datasets\/([^/]+)/);
    setSelected(match?.[1] ? decodeURIComponent(match[1]) : '');
  }, [location.pathname]);
  useEffect(() => { applyTheme(theme); }, [theme]);
  function disconnect() { session.disconnect(); }
  const links = [{ href: '/datasets', label: 'Datasets' }, ...(selected ? [
    { href: `/datasets/${encodeURIComponent(selected)}/executions`, label: 'Executions' },
    { href: `/datasets/${encodeURIComponent(selected)}/suites`, label: 'Suites & Policies' },
    { href: `/datasets/${encodeURIComponent(selected)}/purposes`, label: 'Purposes & Bindings' },
    { href: `/datasets/${encodeURIComponent(selected)}/compatibility`, label: 'Compatibility' },
    { href: `/datasets/${encodeURIComponent(selected)}/publications`, label: 'Publication' },
  ] : [])];
  const nav = <nav className="stack" aria-label="Điều hướng chính">{links.map(({ href, label }) => <Link key={href} to={href} onClick={() => setDrawer(false)} style={{ padding: '10px 12px', textDecoration: 'none', borderRadius: 8, background: location.pathname === href ? 'var(--surface)' : 'transparent' }}>{label}</Link>)}</nav>;
  return <div style={{ minHeight: '100vh', borderTop: '4px solid var(--brand)' }}>
    <header className="row" style={{ height: 64, background: 'var(--nav)', color: 'white', padding: '0 16px', justifyContent: 'space-between' }}>
      <div className="row"><button className="mobile-menu" aria-label="Mở điều hướng" onClick={() => setDrawer(true)}>☰</button><b><span style={{ color: 'var(--brand)' }}>DQ</span> Platform</b><select aria-label="Dataset hiện tại" value={selected} onChange={event => { setSelected(event.target.value); if (event.target.value) navigate(`/datasets/${encodeURIComponent(event.target.value)}`); }} style={{ width: 220 }}><option value="">Chọn dataset</option>{datasets?.items.map(dataset => <option key={dataset.dataset_id} value={dataset.dataset_id}>{dataset.dataset_id}</option>)}</select></div>
      <div className="row"><span>{identity?.principal}</span><button aria-label="Đổi giao diện" onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}>{theme === 'light' ? <Moon size={18} /> : <Sun size={18} />}</button><button onClick={disconnect}><Unplug size={16} /> <span className="logout-label">Ngắt kết nối</span></button></div>
    </header>
    <div className="layout"><aside className="sidebar" style={{ width: collapsed ? 64 : 240 }}><button aria-label={collapsed ? 'Mở rộng điều hướng' : 'Thu gọn điều hướng'} onClick={() => setCollapsed(!collapsed)}>{collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}</button>{!collapsed && nav}</aside><main className="content"><Outlet /></main></div>
    {drawer && <div className="drawer-backdrop" onClick={() => setDrawer(false)}><section role="dialog" aria-modal="true" aria-label="Điều hướng" onClick={event => event.stopPropagation()}><button autoFocus aria-label="Đóng điều hướng" onClick={() => setDrawer(false)}>Đóng</button>{nav}</section></div>}
  </div>;
}
