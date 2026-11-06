import { Link, type RouteObject } from 'react-router';
import { ProtectedApp } from './AppShell';
import { DatasetsPage } from '../features/datasets/Pages';
import { ExecutionCompare } from '../features/executions/ExecutionCompare';
import { ExecutionPage } from '../features/executions/Pages';
import { PublicationPage } from '../features/publications/PublicationPage';
import { RegistryPage } from '../features/registry/RegistryPage';
import { DashboardPage } from '../features/dashboard/DashboardPage';
import { ScoreboardPage } from '../features/scoreboard/ScoreboardPage';
import { OperationsPage } from '../features/operations/OperationsPage';
import { AlertsPage } from '../features/alerts/AlertsPage';
import { AuditTrailPage } from '../features/operations/AuditTrailPage';
export const appRoutes: RouteObject[] = [
  { path: '/connect', element: <ProtectedApp /> },
  { path: '/', element: <ProtectedApp />, children: [
    { index: true, element: <DashboardPage /> },
    { path: 'datasets', element: <DatasetsPage /> },
    { path: 'datasets/:datasetId', element: <DashboardPage /> },
    { path: 'datasets/:datasetId/scoreboard', element: <ScoreboardPage /> },
    { path: 'datasets/:datasetId/operations', element: <OperationsPage /> },
{ path: 'datasets/:datasetId/alerts', element: <AlertsPage /> },
{ path: 'datasets/:datasetId/audit', element: <AuditTrailPage /> },
    { path: 'datasets/:datasetId/compare', element: <ExecutionCompare leftId={new URLSearchParams(window.location.search).get('left') ?? ''} rightId={new URLSearchParams(window.location.search).get('right') ?? ''} /> },
    { path: 'executions/:executionId', element: <ExecutionPage /> },
    { path: 'datasets/:datasetId/publications', element: <PublicationPage /> },
    { path: 'datasets/:datasetId/*', element: <RegistryPage /> },
    { path: '*', element: <NotFoundPage /> },
  ] },
];

function NotFoundPage() {
  return <main className="card stack" style={{ margin: 32 }}><h1>Page not found</h1><p>The requested route is not available.</p><div className="row"><Link to="/">Dashboard</Link><Link to="/datasets">Datasets</Link></div></main>;
}
