import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiRequest, ApiError } from './client';
import { session } from '../features/auth/session';

afterEach(() => { vi.unstubAllGlobals(); session.disconnect(); session.onUnauthorized(() => {}); });

describe('apiRequest', () => {
  it('preserves HTTP 202 as a successful pending response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"state":"RUNNING"}', { status: 202 })));
    const result = await apiRequest<{ state: string }>('/api/v2/executions/test/report');
    expect(result.status).toBe(202);
    expect(result.data.state).toBe('RUNNING');
  });

  it('expires the session on 401 and retains structured API errors', async () => {
    const expire = vi.fn();
    session.connect('synthetic-token');
    session.onUnauthorized(expire);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"error":{"code":"UNAUTHENTICATED","message":"invalid"}}', { status: 401 })));
    await expect(apiRequest('/api/v2/me')).rejects.toMatchObject({ status: 401, code: 'UNAUTHENTICATED' } satisfies Partial<ApiError>);
    expect(expire).toHaveBeenCalledOnce();
  });
});
