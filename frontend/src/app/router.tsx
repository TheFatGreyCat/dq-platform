import { createBrowserRouter, Navigate } from 'react-router';
import { ProtectedApp } from './AppShell';
import { DatasetsPage, DatasetPage } from '../features/datasets/Pages';
import { ExecutionsPage, ExecutionPage } from '../features/executions/Pages';
import { PublicationPage } from '../features/publications/PublicationPage';
import { RegistryPage } from '../features/registry/RegistryPage';

export const router = createBrowserRouter([
  { path: '/connect', element: <ProtectedApp /> },
  { path: '/', element: <ProtectedApp />, children: [
    { index: true, element: <Navigate to="/datasets" replace /> },
    { path: 'datasets', element: <DatasetsPage /> },
    { path: 'datasets/:datasetId', element: <DatasetPage /> },
    { path: 'datasets/:datasetId/executions', element: <ExecutionsPage /> },
    { path: 'executions/:executionId', element: <ExecutionPage /> },
    { path: 'datasets/:datasetId/publications', element: <PublicationPage /> },
    { path: 'datasets/:datasetId/*', element: <RegistryPage /> },
  ] },
]);
