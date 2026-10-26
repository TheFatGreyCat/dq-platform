import { session } from '../features/auth/session';

export class ApiError extends Error {
  constructor(readonly status: number, readonly code: string, message: string, readonly field?: string) {
    super(message);
    this.name = 'ApiError';
  }
}

export async function apiRequest<T>(path: string, init: RequestInit = {}): Promise<{ status: number; data: T }> {
  if (!path.startsWith('/api/v2/')) throw new Error('Chỉ hỗ trợ API tương đối /api/v2/.');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new DOMException('Request timed out', 'TimeoutError')), 15_000);
  const abort = () => controller.abort(init.signal?.reason);
  init.signal?.addEventListener('abort', abort, { once: true });
  try {
    const headers = new Headers(init.headers);
    const token = session.token();
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const response = await fetch(path, { ...init, headers, signal: controller.signal });
    if (response.status === 401) session.expire();
    const data = await response.json().catch(() => null) as T | { error?: { code?: string; message?: string; field?: string } } | null;
    if (!response.ok) {
      const error = data && typeof data === 'object' && 'error' in data ? data.error : undefined;
      throw new ApiError(response.status, error?.code ?? 'HTTP_ERROR', error?.message ??
        (response.status === 413 ? 'Yêu cầu vượt quá giới hạn 1 MiB.' : `Yêu cầu thất bại (${response.status}).`), error?.field);
    }
    return { status: response.status, data: data as T };
  } catch (error) {
    if (error instanceof ApiError) throw error;
    if (controller.signal.aborted) throw new Error(controller.signal.reason?.name === 'TimeoutError' ?
      'Hết thời gian chờ API.' : 'Yêu cầu đã bị hủy.');
    throw new Error('Không kết nối được API. Kiểm tra kết nối rồi thử lại.');
  } finally {
    clearTimeout(timeout);
    init.signal?.removeEventListener('abort', abort);
  }
}
