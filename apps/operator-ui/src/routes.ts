import type { Routes } from '@angular/router';
export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'overview' },
  {
    path: 'overview',
    title: 'Overview · Kill It Twice',
    loadComponent: () => import('./pages/overview').then((m) => m.OverviewPage),
  },
  {
    path: 'backfill',
    title: 'Backfill · Kill It Twice',
    loadComponent: () => import('./pages/backfill').then((m) => m.BackfillPage),
  },
  {
    path: 'records',
    title: 'Records · Kill It Twice',
    loadComponent: () => import('./pages/records').then((m) => m.RecordsPage),
  },
  {
    path: 'failures',
    title: 'Failures · Kill It Twice',
    loadComponent: () => import('./pages/failures').then((m) => m.FailuresPage),
  },
  {
    path: 'simulations',
    title: 'Simulations · Kill It Twice',
    loadComponent: () =>
      import('./pages/simulations').then((m) => m.SimulationsPage),
  },
  {
    path: 'configuration',
    title: 'Configuration · Kill It Twice',
    loadComponent: () =>
      import('./pages/configuration').then((m) => m.ConfigurationPage),
  },
  { path: '**', redirectTo: 'overview' },
];
