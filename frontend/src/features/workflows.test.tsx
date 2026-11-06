import { QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { queryClient } from '../app/queryClient';
import { appRoutes } from '../app/router';
import { session } from './auth/session';
import type { PublicationView } from '../api/endpoints';
import type {
  Dataset,
  Execution,
  Identity,
  Page,
  ReportResponse,
} from '../api/types';

if (typeof window !== 'undefined' && (!window.matchMedia || !window.matchMedia('min-width').addEventListener)) {
  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
}

type RecordedRequest = {
  url: string;
  path: string;
  pathname: string;
  method: string;
  headers: Headers;
  urlObj: URL;
};

type FetchHandler = (
  url: URL,
  req: { method: string; headers: Headers; signal?: AbortSignal },
) => Promise<Response | null> | Response | null;

let recordedRequests: RecordedRequest[] = [];
let customHandlers: FetchHandler[] = [];

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function errorResponse(status: number, code: string, message: string, field?: string): Response {
  return new Response(
    JSON.stringify({
      error: { code, message, ...(field ? { field } : {}) },
    }),
    {
      status,
      headers: { 'Content-Type': 'application/json' },
    },
  );
}

function createExecution(overrides: Partial<Execution> = {}): Execution {
  return {
    execution_id: 'exec-1',
    dataset_id: 'ds-a',
    workflow: 'WRITE_GATE',
    purpose: 'daily',
    state: 'COMPLETED',
    publication_state: 'SUCCEEDED',
    unresolved: false,
    plan: {
      event_time: '2026-10-20T00:00:00Z',
      plan_hash: 'hash-exec-1',
    },
    attempts: [{ attempt_id: 'att-1', status: 'SUCCEEDED' }],
    outcomes: {
      silver: { status: 'SUCCEEDED' },
      quarantine: null,
    },
    ...overrides,
  } satisfies Execution;
}

function createReportResponse(overrides: Partial<ReportResponse> = {}): ReportResponse {
  return {
    accepted: {
      decision: { decision: 'ALLOW', reasons: [{ code: 'OK' }] },
      assessment: null,
    },
    report: {
      candidate_rows: 100,
      valid_rows: 97,
      quarantine_rows: 3,
      measured_rows: null,
      results: [],
    },
    links: {},
    ...overrides,
  } satisfies ReportResponse;
}

const defaultIdentity: Identity = {
  principal: 'viewer@dq.local',
  grants: [
    { dataset_id: 'ds-a', role: 'viewer', workflow: null, purpose: null },
    { dataset_id: 'ds-b', role: 'operator', workflow: null, purpose: null },
  ],
} satisfies Identity;

const defaultDatasets: Page<Dataset> = {
  items: [
    { dataset_id: 'ds-a', owner: 'data-eng', history_context: 'daily', registration_hash: 'hash-a' },
    { dataset_id: 'ds-b', owner: 'analytics', history_context: 'hourly', registration_hash: 'hash-b' },
  ],
  next_cursor: null,
} satisfies Page<Dataset>;

function setupDefaultFetch() {
  vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const rawUrl = typeof input === 'string' ? input : input instanceof Request ? input.url : String(input);
    const urlObj = new URL(rawUrl, 'http://localhost');
    const method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
    const headers = new Headers(init?.headers);

    recordedRequests.push({
      url: urlObj.toString(),
      path: urlObj.pathname + urlObj.search,
      pathname: urlObj.pathname,
      method,
      headers,
      urlObj,
    });

    for (const handler of customHandlers) {
      const customRes = await handler(urlObj, { method, headers, signal: init?.signal ?? undefined });
      if (customRes) return customRes;
    }

    const pathname = urlObj.pathname;

    if (pathname === '/api/v2/me') {
      return jsonResponse(defaultIdentity);
    }
    if (pathname === '/api/v2/datasets') {
      return jsonResponse(defaultDatasets);
    }
    const datasetExecMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/executions$/);
    if (datasetExecMatch) {
      const datasetId = decodeURIComponent(datasetExecMatch[1]!);
      return jsonResponse({
        items: [createExecution({ dataset_id: datasetId })],
        next_cursor: null,
      } satisfies Page<Execution>);
    }
    const reportMatch = pathname.match(/^\/api\/v2\/executions\/([^/]+)\/report$/);
    if (reportMatch) {
      return jsonResponse(createReportResponse());
    }
    const execDetailMatch = pathname.match(/^\/api\/v2\/executions\/([^/]+)$/);
    if (execDetailMatch) {
      const executionId = decodeURIComponent(execDetailMatch[1]!);
      return jsonResponse(createExecution({ execution_id: executionId }));
    }
    const datasetDetailMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)$/);
    if (datasetDetailMatch) {
      const datasetId = decodeURIComponent(datasetDetailMatch[1]!);
      return jsonResponse({
        dataset_id: datasetId,
        owner: 'data-eng',
        history_context: 'daily',
        registration: { suite_id: 'suite-1' },
        registration_hash: 'hash-1',
      });
    }
    const pubMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/publications$/);
    if (pubMatch) {
      return jsonResponse({
        current: null,
        pending: null,
        unresolved_execution_id: null,
        restore_orphans: [],
      } satisfies PublicationView);
    }
    const restoreMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/restore$/);
    if (restoreMatch) {
      return jsonResponse({ open: null, history: [] });
    }
    const retentionMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/retention$/);
    if (retentionMatch) {
      return jsonResponse({ items: [], next_cursor: null } satisfies Page<Record<string, unknown>>);
    }
    const purposeMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/purposes\/([^/]+)\/([^/]+)\/events$/);
    if (purposeMatch) {
      return jsonResponse({ items: [], next_cursor: null } satisfies Page<Record<string, unknown>>);
    }
    const suitesMatch = pathname.match(/^\/api\/v2\/datasets\/([^/]+)\/suites$/);
    if (suitesMatch) {
      return jsonResponse({ items: [], next_cursor: null } satisfies Page<Record<string, unknown>>);
    }

    return errorResponse(404, 'NOT_FOUND', `Unmocked API route: ${pathname}`);
  });
}

function renderApp(initialEntry = '/') {
  const router = createMemoryRouter(appRoutes, { initialEntries: [initialEntry] });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return { ...view, router };
}

beforeEach(() => {
  queryClient.clear();
  session.connect('test-bearer-token');
  recordedRequests = [];
  customHandlers = [];
  setupDefaultFetch();
});

afterEach(() => {
  cleanup();
  queryClient.clear();
  session.disconnect();
  session.onUnauthorized(() => {});
  vi.unstubAllGlobals();
});

describe('workflow filtering and keyset pagination', () => {
  it('filters executions by workflow at API boundary, respects equal event times keyset cursors, and paginates forward/backward', async () => {
    const gateExec1 = createExecution({
      execution_id: 'gate-1',
      workflow: 'WRITE_GATE',
      plan: { event_time: '2026-10-20T00:00:00Z', plan_hash: 'h1' },
    });
    const gateExec2 = createExecution({
      execution_id: 'gate-2',
      workflow: 'WRITE_GATE',
      plan: { event_time: '2026-10-20T00:01:00Z', plan_hash: 'h2' },
    });
    const auditExec1 = createExecution({
      execution_id: 'audit-1',
      workflow: 'AUDIT_TABLE',
      plan: { event_time: '2026-10-20T00:00:00Z', plan_hash: 'ha1' },
    });
    const auditExec2 = createExecution({
      execution_id: 'audit-2',
      workflow: 'AUDIT_TABLE',
      plan: { event_time: '2026-10-20T00:00:00Z', plan_hash: 'ha2' },
    });
    const auditExec3 = createExecution({
      execution_id: 'audit-3',
      workflow: 'AUDIT_TABLE',
      plan: { event_time: '2026-10-20T00:00:00Z', plan_hash: 'ha3' },
    });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        const wf = url.searchParams.get('workflow');
        const cursor = url.searchParams.get('cursor');
        if (!wf && !cursor) {
          return jsonResponse({
            items: [gateExec1, gateExec2],
            next_cursor: 'cursor-gate-2',
          } satisfies Page<Execution>);
        }
        if (wf === 'AUDIT_TABLE' && !cursor) {
          return jsonResponse({
            items: [auditExec1, auditExec2],
            next_cursor: 'cursor-audit-page-2',
          } satisfies Page<Execution>);
        }
        if (wf === 'AUDIT_TABLE' && cursor === 'cursor-audit-page-2') {
          return jsonResponse({
            items: [auditExec3],
            next_cursor: null,
          } satisfies Page<Execution>);
        }
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('gate-1')).toBeInTheDocument();
    expect(screen.getByText('gate-2')).toBeInTheDocument();

    const select = screen.getByLabelText('Workflow filter');
    fireEvent.change(select, { target: { value: 'AUDIT_TABLE' } });

    expect(await screen.findByText('audit-1')).toBeInTheDocument();
    expect(screen.getByText('audit-2')).toBeInTheDocument();
    expect(screen.queryByText('gate-1')).not.toBeInTheDocument();

    const nextBtn = screen.getByRole('button', { name: 'Tiếp' });
    expect(nextBtn).toBeEnabled();
    fireEvent.click(nextBtn);

    expect(await screen.findByText('audit-3')).toBeInTheDocument();
    expect(screen.queryByText('audit-1')).not.toBeInTheDocument();

    const prevBtn = screen.getByRole('button', { name: 'Trước' });
    expect(prevBtn).toBeEnabled();
    fireEvent.click(prevBtn);

    expect(await screen.findByText('audit-1')).toBeInTheDocument();
    expect(screen.getByText('audit-2')).toBeInTheDocument();

    const execRequests = recordedRequests.filter(r => r.pathname === '/api/v2/datasets/ds-a/executions');
    expect(execRequests).toHaveLength(3);
    expect(execRequests[0]!.urlObj.searchParams.get('workflow')).toBeNull();
    expect(execRequests[1]!.urlObj.searchParams.get('workflow')).toBe('AUDIT_TABLE');
    expect(execRequests[1]!.urlObj.searchParams.get('cursor')).toBeNull();
    expect(execRequests[2]!.urlObj.searchParams.get('workflow')).toBe('AUDIT_TABLE');
    expect(execRequests[2]!.urlObj.searchParams.get('cursor')).toBe('cursor-audit-page-2');
  });

  it('resets cursor stack when state filter is toggled', async () => {
    const auditExec1 = createExecution({ execution_id: 'audit-1', workflow: 'AUDIT_TABLE' });
    const auditExec2 = createExecution({ execution_id: 'audit-2', workflow: 'AUDIT_TABLE' });
    const auditFailed = createExecution({ execution_id: 'audit-failed', workflow: 'AUDIT_TABLE', state: 'FAILED' });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        const wf = url.searchParams.get('workflow');
        const cursor = url.searchParams.get('cursor');
        const states = url.searchParams.getAll('state');
        if (states.includes('FAILED')) {
          return jsonResponse({ items: [auditFailed], next_cursor: null } satisfies Page<Execution>);
        }
        if (wf === 'AUDIT_TABLE' && !cursor) {
          return jsonResponse({ items: [auditExec1], next_cursor: 'cur-2' } satisfies Page<Execution>);
        }
        if (wf === 'AUDIT_TABLE' && cursor === 'cur-2') {
          return jsonResponse({ items: [auditExec2], next_cursor: null } satisfies Page<Execution>);
        }
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    fireEvent.change(screen.getByLabelText('Workflow filter'), { target: { value: 'AUDIT_TABLE' } });
    expect(await screen.findByText('audit-1')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Tiếp' }));
    expect(await screen.findByText('audit-2')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('FAILED'));
    expect(await screen.findByText('audit-failed')).toBeInTheDocument();

    const lastReq = recordedRequests.filter(r => r.pathname === '/api/v2/datasets/ds-a/executions').at(-1)!;
    expect(lastReq.urlObj.searchParams.getAll('state')).toContain('FAILED');
    expect(lastReq.urlObj.searchParams.get('cursor')).toBeNull();
  });

  it('renders ApiErrorAlert when API returns 422 INVALID_CONFIG for invalid workflow', async () => {
    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return errorResponse(422, 'INVALID_CONFIG', 'Workflow must be WRITE_GATE or AUDIT_TABLE', 'workflow');
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(screen.getByText('Workflow must be WRITE_GATE or AUDIT_TABLE')).toBeInTheDocument();
  });
});

describe('report verdicts and counts semantics', () => {
  it('preserves distinct WRITE_GATE ALLOW/DENY and AUDIT_TABLE PASS/VIOLATION/ERROR with actual count semantics', async () => {
    const gateAllow = createExecution({ execution_id: 'gate-allow', workflow: 'WRITE_GATE' });
    const gateDeny = createExecution({ execution_id: 'gate-deny', workflow: 'WRITE_GATE' });
    const auditPass = createExecution({ execution_id: 'audit-pass', workflow: 'AUDIT_TABLE' });
    const auditViolation = createExecution({ execution_id: 'audit-violation', workflow: 'AUDIT_TABLE' });
    const auditError = createExecution({ execution_id: 'audit-error', workflow: 'AUDIT_TABLE' });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({
          items: [gateAllow, gateDeny, auditPass, auditViolation, auditError],
          next_cursor: null,
        } satisfies Page<Execution>);
      }
      if (url.pathname === '/api/v2/executions/gate-allow/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'ALLOW', reasons: [{ code: 'OK' }] }, assessment: null },
          report: { candidate_rows: 100, valid_rows: 97, quarantine_rows: 3, measured_rows: null },
        }));
      }
      if (url.pathname === '/api/v2/executions/gate-deny/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'DENY', reasons: ['RULE_FAIL'] }, assessment: null },
          report: { candidate_rows: 100, valid_rows: 94, quarantine_rows: 6, measured_rows: null },
        }));
      }
      if (url.pathname === '/api/v2/executions/audit-pass/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: null, assessment: 'PASS' },
          report: { candidate_rows: null, valid_rows: null, quarantine_rows: null, measured_rows: 5 },
        }));
      }
      if (url.pathname === '/api/v2/executions/audit-violation/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: null, assessment: 'VIOLATION' },
          report: { candidate_rows: null, valid_rows: null, quarantine_rows: null, measured_rows: 0 },
        }));
      }
      if (url.pathname === '/api/v2/executions/audit-error/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: null, assessment: 'ERROR' },
          report: { candidate_rows: null, valid_rows: null, quarantine_rows: null, measured_rows: null },
        }));
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('ALLOW')).toBeInTheDocument();
    expect(screen.getByText('DENY')).toBeInTheDocument();
    expect(screen.getByText('PASS')).toBeInTheDocument();
    expect(screen.getByText('VIOLATION')).toBeInTheDocument();
    expect(screen.getByText('ERROR')).toBeInTheDocument();

    expect(screen.getByText(JSON.stringify({ candidate_rows: 100, valid_rows: 97, quarantine_rows: 3 }))).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify({ candidate_rows: 100, valid_rows: 94, quarantine_rows: 6 }))).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify({ measured_rows: 5 }))).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify({ measured_rows: 0 }))).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify({ measured_rows: 'Unknown' }))).toBeInTheDocument();

    expect(screen.getAllByText(/Reason list is not provided by the report API/)).toHaveLength(3);
    expect(screen.getByText(JSON.stringify([{ code: 'OK' }]))).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify(['RULE_FAIL']))).toBeInTheDocument();
  });

  it('labels 202 reports pending without displaying passed or allowed badge', async () => {
    const pendingExec = createExecution({ execution_id: 'exec-pending', workflow: 'WRITE_GATE', state: 'RUNNING' });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({ items: [pendingExec], next_cursor: null } satisfies Page<Execution>);
      }
      if (url.pathname === '/api/v2/executions/exec-pending/report') {
        return new Response(JSON.stringify({ execution_id: 'exec-pending', state: 'RUNNING' }), {
          status: 202,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('Report pending')).toBeInTheDocument();
    expect(screen.queryByText('ALLOW')).not.toBeInTheDocument();
    expect(screen.queryByText('PASS')).not.toBeInTheDocument();
  });

  it('renders fast report badge immediately while a deferred slow report is in flight', async () => {
    const fastExec = createExecution({ execution_id: 'exec-fast', workflow: 'WRITE_GATE' });
    const slowExec = createExecution({ execution_id: 'exec-slow', workflow: 'WRITE_GATE' });

    let resolveSlow: (res: Response) => void;
    const slowPromise = new Promise<Response>((resolve) => {
      resolveSlow = resolve;
    });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({ items: [fastExec, slowExec], next_cursor: null } satisfies Page<Execution>);
      }
      if (url.pathname === '/api/v2/executions/exec-fast/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'ALLOW', reasons: [] }, assessment: null },
        }));
      }
      if (url.pathname === '/api/v2/executions/exec-slow/report') {
        return slowPromise;
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('ALLOW')).toBeInTheDocument();
    expect(screen.getByText('Loading report…')).toBeInTheDocument();

    resolveSlow!(jsonResponse(createReportResponse({
      accepted: { decision: { decision: 'DENY', reasons: [] }, assessment: null },
    })));

    expect(await screen.findByText('DENY')).toBeInTheDocument();
    expect(screen.queryByText('Loading report…')).not.toBeInTheDocument();
  });

  it('isolates row report network failure with retry button without affecting other rows', async () => {
    const okExec = createExecution({ execution_id: 'exec-ok', workflow: 'WRITE_GATE' });
    const failExec = createExecution({ execution_id: 'exec-fail', workflow: 'WRITE_GATE' });

    let shouldFail = true;

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({ items: [okExec, failExec], next_cursor: null } satisfies Page<Execution>);
      }
      if (url.pathname === '/api/v2/executions/exec-ok/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'ALLOW', reasons: [] }, assessment: null },
        }));
      }
      if (url.pathname === '/api/v2/executions/exec-fail/report') {
        if (shouldFail) {
          return errorResponse(500, 'REPORT_FAILED', 'Failed to retrieve report');
        }
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'DENY', reasons: [] }, assessment: null },
        }));
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('ALLOW')).toBeInTheDocument();
    expect(await screen.findByText('Failed to retrieve report')).toBeInTheDocument();

    const retryBtn = screen.getByRole('button', { name: 'Thử lại' });
    expect(retryBtn).toBeInTheDocument();

    const okReportCalls = recordedRequests.filter(r => r.pathname === '/api/v2/executions/exec-ok/report');
    expect(okReportCalls).toHaveLength(1);

    shouldFail = false;
    fireEvent.click(retryBtn);

    expect(await screen.findByText('DENY')).toBeInTheDocument();
    expect(recordedRequests.filter(r => r.pathname === '/api/v2/executions/exec-ok/report')).toHaveLength(1);
    expect(recordedRequests.filter(r => r.pathname === '/api/v2/executions/exec-fail/report')).toHaveLength(2);
  });

  it('bounds report requests to exactly the number of displayed executions on the page', async () => {
    const items = Array.from({ length: 25 }, (_, i) =>
      createExecution({ execution_id: `exec-bound-${i}`, workflow: 'WRITE_GATE' }),
    );

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({ items, next_cursor: 'next-page' } satisfies Page<Execution>);
      }
      return null;
    });

    renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('exec-bound-0')).toBeInTheDocument();
    expect(screen.getByText('exec-bound-24')).toBeInTheDocument();

    await waitFor(() => {
      const reportCalls = recordedRequests.filter(r => r.pathname.includes('/report'));
      expect(reportCalls).toHaveLength(25);
    });
  });
});

describe('caching, navigation, and stale response isolation', () => {
  it('prevents stale dataset responses from overwriting the current dataset page', async () => {
    let resolveDatasetA: (res: Response) => void;
    const delayedResponseA = new Promise<Response>((resolve) => {
      resolveDatasetA = resolve;
    });

    const execA = createExecution({ execution_id: 'exec-from-a', dataset_id: 'ds-a' });
    const execB = createExecution({ execution_id: 'exec-from-b', dataset_id: 'ds-b' });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return delayedResponseA;
      }
      if (url.pathname === '/api/v2/datasets/ds-b/executions') {
        return jsonResponse({ items: [execB], next_cursor: null } satisfies Page<Execution>);
      }
      return null;
    });

    const { router } = renderApp('/datasets/ds-a/scoreboard');

    await router.navigate('/datasets/ds-b/scoreboard');

    expect(await screen.findByText('exec-from-b')).toBeInTheDocument();

    resolveDatasetA!(jsonResponse({ items: [execA], next_cursor: null } satisfies Page<Execution>));

    await waitFor(() => {
      expect(screen.getByText('exec-from-b')).toBeInTheDocument();
      expect(screen.queryByText('exec-from-a')).not.toBeInTheDocument();
    });
  });

  it('shares report cache between scoreboard and execution detail page without duplicate fetch', async () => {
    const exec1 = createExecution({ execution_id: 'exec-shared-1', workflow: 'WRITE_GATE' });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({ items: [exec1], next_cursor: null } satisfies Page<Execution>);
      }
      if (url.pathname === '/api/v2/executions/exec-shared-1') {
        return jsonResponse(exec1);
      }
      if (url.pathname === '/api/v2/executions/exec-shared-1/report') {
        return jsonResponse(createReportResponse({
          accepted: { decision: { decision: 'ALLOW', reasons: [{ code: 'SHARED_CACHE_HIT' }] }, assessment: null },
        }));
      }
      return null;
    });

    const { router } = renderApp('/datasets/ds-a/scoreboard');

    expect(await screen.findByText('ALLOW')).toBeInTheDocument();
    expect(screen.getByText(JSON.stringify([{ code: 'SHARED_CACHE_HIT' }]))).toBeInTheDocument();

    const reportCallsBefore = recordedRequests.filter(r => r.pathname === '/api/v2/executions/exec-shared-1/report');
    expect(reportCallsBefore).toHaveLength(1);

    await router.navigate('/executions/exec-shared-1');

    expect(await screen.findByRole('heading', { name: 'Execution exec-shared-1' })).toBeInTheDocument();
    expect(screen.getAllByText(/SHARED_CACHE_HIT/).length).toBeGreaterThan(0);

    const reportCallsAfter = recordedRequests.filter(r => r.pathname === '/api/v2/executions/exec-shared-1/report');
    expect(reportCallsAfter).toHaveLength(1);
  });

  it('clears session token and query cache when disconnecting, transitioning to ConnectPage', async () => {
    renderApp('/');

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
    expect(session.token()).toBe('test-bearer-token');
    expect(queryClient.getQueryData(['me'])).toBeDefined();

    const disconnectBtn = screen.getByRole('button', { name: 'Ngắt kết nối' });
    fireEvent.click(disconnectBtn);

    expect(await screen.findByRole('heading', { name: 'Kết nối DQ Platform' })).toBeInTheDocument();
    expect(session.token()).toBeNull();
    expect(queryClient.getQueryData(['me'])).toBeUndefined();
  });
});

describe('operations evidence and publication read-only contracts', () => {
  it('renders operations with independent 403 retention failure, restore summary, purpose events, and unresolved evidence', async () => {
    const unresolvedExec = createExecution({
      execution_id: 'exec-unres',
      unresolved: true,
      publication_state: 'SUCCEEDED',
      outcomes: { silver: { status: 'SUCCEEDED' }, quarantine: null },
    });
    const partialExec = createExecution({
      execution_id: 'exec-part',
      unresolved: false,
      publication_state: 'PARTIAL',
      outcomes: { silver: { status: 'PARTIAL' }, quarantine: null },
    });
    const cleanExec = createExecution({
      execution_id: 'exec-clean',
      unresolved: false,
      publication_state: 'SUCCEEDED',
      outcomes: { silver: { status: 'SUCCEEDED' }, quarantine: null },
    });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/restore') {
        return jsonResponse({
          open: { findings_submitted: 4, pending_reconcile: ['marker-1', 'marker-2'] },
          history: [{ id: 'hist-1', completed_at: '2026-10-20T01:00:00Z' }],
        });
      }
      if (url.pathname === '/api/v2/datasets/ds-a/retention') {
        return errorResponse(403, 'FORBIDDEN', 'Retention candidates require operator grant');
      }
      if (url.pathname.startsWith('/api/v2/datasets/ds-a/purposes/')) {
        return jsonResponse({
          items: [
            {
              event_id: 'evt-101',
              binding_version: 1,
              action: 'BIND',
              suite_revision_id: 'rev-1',
              policy_revision_id: 'pol-1',
              actor: 'operator-1',
              reason: 'seed',
              event_time: '2026-10-20T00:00:00Z',
            },
          ],
          next_cursor: 'cur-evt-2',
        } satisfies Page<Record<string, unknown>>);
      }
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({
          items: [unresolvedExec, partialExec, cleanExec],
          next_cursor: null,
        } satisfies Page<Execution>);
      }
      return null;
    });

    renderApp('/datasets/ds-a/operations');

    expect(await screen.findByRole('heading', { name: 'Open restore' })).toBeInTheDocument();
    expect(screen.getByText(/Findings submitted: 4/)).toBeInTheDocument();
    expect(screen.getByText(/dq-service\/README\.md/)).toBeInTheDocument();
    expect(screen.getByText(/scripts\/dq_operator\.py --help/)).toBeInTheDocument();

    expect(await screen.findByText('Retention candidates require operator grant')).toBeInTheDocument();

    const purposeInput = screen.getByLabelText('Purpose');
    fireEvent.change(purposeInput, { target: { value: 'daily' } });
    expect(await screen.findByText('BIND')).toBeInTheDocument();
    expect(screen.getByText('operator-1')).toBeInTheDocument();

    expect(await screen.findByText('exec-unres')).toBeInTheDocument();
    expect(screen.getByText('exec-part')).toBeInTheDocument();
    expect(screen.queryByText('exec-clean')).not.toBeInTheDocument();
  });

  it('renders empty states for restore and source-page-limited unresolved executions', async () => {
    const cleanExec = createExecution({
      execution_id: 'exec-clean-only',
      unresolved: false,
      publication_state: 'SUCCEEDED',
    });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/restore') {
        return jsonResponse({ open: null, history: [] });
      }
      if (url.pathname === '/api/v2/datasets/ds-a/executions') {
        return jsonResponse({
          items: [cleanExec],
          next_cursor: 'cur-next-clean',
        } satisfies Page<Execution>);
      }
      return null;
    });

    renderApp('/datasets/ds-a/operations');

    expect(await screen.findByText('No open restore')).toBeInTheDocument();
    expect(screen.getByText('Restore history: none returned.')).toBeInTheDocument();
    expect(await screen.findByText('No unresolved evidence on this current API page.')).toBeInTheDocument();
  });

  it('differentiates empty restore_orphans from nonempty array with warning alert', async () => {
    let orphans: Record<string, unknown>[] = [];

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-a/publications') {
        return jsonResponse({
          current: { snapshot_version: 1 },
          pending: null,
          unresolved_execution_id: null,
          restore_orphans: orphans,
        } satisfies PublicationView);
      }
      return null;
    });

    renderApp('/datasets/ds-a/publications');

    const dateInput = screen.getByLabelText(/Partition date/);
    fireEvent.change(dateInput, { target: { value: '2026-10-20' } });
    const loadBtn = screen.getByRole('button', { name: 'Tải publication' });
    fireEvent.click(loadBtn);

    await waitFor(() => {
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    });

    orphans = [{ marker_id: 'orphan-marker-1' }];
    fireEvent.click(loadBtn);

    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(screen.getByText(/Có restore\/orphan marker; cần xử lý qua operator tools\./)).toBeInTheDocument();
  });

  it('enforces GET-only requests across dashboard, scoreboard, operations, and publications navigation', async () => {
    const { router } = renderApp('/');

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();

    await router.navigate('/datasets/ds-a');
    expect(await screen.findByRole('heading', { name: 'Dashboard · ds-a' })).toBeInTheDocument();

    await router.navigate('/datasets/ds-a/scoreboard');
    expect(await screen.findByRole('heading', { name: 'Scoreboard' })).toBeInTheDocument();

    await router.navigate('/datasets/ds-a/operations');
    expect(await screen.findByRole('heading', { name: 'Operations' })).toBeInTheDocument();

    await router.navigate('/datasets/ds-a/publications');
    expect(await screen.findByRole('heading', { name: 'Publication' })).toBeInTheDocument();

    expect(recordedRequests.length).toBeGreaterThan(0);
    const nonGetRequests = recordedRequests.filter(r => r.method !== 'GET');
    expect(nonGetRequests).toHaveLength(0);
  });
});

describe('authentication transitions and direct routes', () => {
  it('handles unauthenticated state, 401 session expiry, and 403 error retaining session', async () => {
    session.disconnect();
    const unauth = renderApp('/');
    expect(await screen.findByRole('heading', { name: 'Kết nối DQ Platform' })).toBeInTheDocument();
    unauth.unmount();

    session.connect('expired-token');
    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/me') {
        return errorResponse(401, 'UNAUTHENTICATED', 'Session expired');
      }
      return null;
    });

    const authExpired = renderApp('/');
    expect(await screen.findByRole('heading', { name: 'Kết nối DQ Platform' })).toBeInTheDocument();
    expect(session.token()).toBeNull();
    authExpired.unmount();

    queryClient.clear();
    customHandlers = [];
    session.connect('viewer-token');
    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/datasets/ds-forbidden') {
        return errorResponse(403, 'FORBIDDEN', 'Access denied to dataset ds-forbidden');
      }
      return null;
    });

    renderApp('/datasets/ds-forbidden');
    expect(await screen.findByText('Access denied to dataset ds-forbidden')).toBeInTheDocument();
    expect(session.token()).toBe('viewer-token');
  });

  it('mounts direct routes via production appRoutes including executions, registry, and 404', async () => {
    const targetExec = createExecution({
      execution_id: 'exec-detail-1',
      dataset_id: 'ds-a',
      workflow: 'WRITE_GATE',
      purpose: 'daily',
    });

    customHandlers.unshift((url) => {
      if (url.pathname === '/api/v2/executions/exec-detail-1') {
        return jsonResponse(targetExec);
      }
      return null;
    });

    const { unmount: unmount1 } = renderApp('/executions/exec-detail-1');
    expect(await screen.findByRole('heading', { name: 'Execution exec-detail-1' })).toBeInTheDocument();

    const executionsLink = screen.getByRole('link', { name: 'Executions' });
    expect(executionsLink).toHaveAttribute('aria-current', 'page');
    unmount1();

    const { unmount: unmount2 } = renderApp('/datasets/ds-a/suites');
    expect(await screen.findByRole('heading', { name: 'Suites & Policies' })).toBeInTheDocument();
    unmount2();

    const { unmount: unmount3 } = renderApp('/datasets/ds-a/unknown-subroute');
    expect(await screen.findByText('Unknown dataset route: unknown-subroute')).toBeInTheDocument();
    expect(screen.getByText('Suites & Policies')).toBeInTheDocument();
    unmount3();

    const { unmount: unmount4 } = renderApp('/completely-unknown-path');
    expect(await screen.findByRole('heading', { name: 'Page not found' })).toBeInTheDocument();
    unmount4();
  });

  it('retains dataset in dataset selector even when absent from registered datasets catalog', async () => {
    renderApp('/datasets/unregistered-dataset');

    const select = await screen.findByLabelText('Dataset hiện tại');
    expect(select).toHaveValue('unregistered-dataset');
  });
});

describe('responsive dataset navigation', () => {
  it('closes drawer after navigation and on Escape', async () => {
    renderApp('/datasets/ds-a/scoreboard');

    const openBtn = screen.getByRole('button', { name: 'Mở điều hướng' });
    fireEvent.click(openBtn);
    expect(screen.getByRole('dialog', { name: 'Điều hướng' })).toBeInTheDocument();

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(screen.queryByRole('dialog', { name: 'Điều hướng' })).not.toBeInTheDocument();

    fireEvent.click(openBtn);
    const operationsLinks = screen.getAllByRole('link', { name: 'Operations' });
    fireEvent.click(operationsLinks.at(-1)!);

    expect(await screen.findByRole('heading', { name: 'Operations' })).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Điều hướng' })).not.toBeInTheDocument();
  });
});
