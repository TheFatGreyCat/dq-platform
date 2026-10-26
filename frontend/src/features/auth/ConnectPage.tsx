import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router';
import { ApiError } from '../../api/client';
import { endpoints } from '../../api/endpoints';
import { queryClient } from '../../app/queryClient';
import { session } from './session';

export function ConnectPage() {
  const [token, setToken] = useState('');
  const [error, setError] = useState('');
  const [pending, setPending] = useState(false);
  const navigate = useNavigate();
  async function connect(event: FormEvent) {
    event.preventDefault();
    setError(''); setPending(true);
    session.connect(token.trim());
    try {
      const { data } = await endpoints.me();
      await queryClient.cancelQueries(); queryClient.clear();
      queryClient.setQueryData(['me'], data);
      setToken('');
      session.activate();
      navigate('/datasets', { replace: true });
    } catch (failure) {
      session.disconnect();
      setError(failure instanceof ApiError && failure.status === 401 ? 'Token không hợp lệ.' :
        failure instanceof Error ? failure.message : 'Không thể xác thực token.');
    } finally { setPending(false); }
  }
  return <main style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 16 }}>
    <form className="card stack" onSubmit={connect} style={{ width: 'min(440px, 100%)' }}>
      <div className="row"><span style={{ color: 'var(--brand)', fontWeight: 700 }}>DQ</span><h1 style={{ fontSize: 24 }}>Kết nối DQ Platform</h1></div>
      <p className="muted">Dùng bearer token được cấp. Token chỉ tồn tại trong bộ nhớ trang này.</p>
      <label htmlFor="bearer-token">Bearer token<input id="bearer-token" type="password" autoComplete="off" value={token}
        onChange={event => setToken(event.target.value)} required /></label>
      {error && <div role="alert" style={{ color: 'var(--fail-fg)' }}>{error}</div>}
      <button className="primary" disabled={pending || !token.trim()}>{pending ? 'Đang kết nối…' : 'Kết nối'}</button>
    </form>
  </main>;
}
