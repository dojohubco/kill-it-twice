import { dialogKeyboard } from './shared/dialog-keyboard';
import {
  Component,
  DestroyRef,
  ElementRef,
  inject,
  signal,
  viewChild,
} from '@angular/core';
import {
  Router,
  RouterLink,
  RouterLinkActive,
  RouterOutlet,
  NavigationEnd,
} from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { filter } from 'rxjs';
import { Workspace } from './core/workspace';
import { Api } from './core/api';
import { timestamp } from './core/value';
import { Icon } from './shared/icon';
import { ActionDialog } from './shared/action-dialog';
@Component({
  selector: 'kit-root',
  standalone: true,
  imports: [RouterOutlet, RouterLink, RouterLinkActive, Icon, ActionDialog],
  templateUrl: './app.html',
})
export class AppComponent {
  readonly dialogKeyboard = dialogKeyboard;
  readonly workspace = inject(Workspace);
  readonly api = inject(Api);
  readonly router = inject(Router);
  readonly destroy = inject(DestroyRef);
  readonly menu = signal(false);
  readonly accessError = signal('');
  readonly announcement = signal('');
  readonly timestamp = timestamp;
  readonly accessDialog =
    viewChild<ElementRef<HTMLDialogElement>>('accessDialog');
  readonly links = [
    { path: '/overview', title: 'Overview', icon: 'overview' },
    { path: '/backfill', title: 'Backfill', icon: 'backfill' },
    { path: '/records', title: 'Records', icon: 'records' },
    { path: '/failures', title: 'Failures', icon: 'failures' },
    { path: '/simulations', title: 'Simulations', icon: 'simulations' },
    { path: '/configuration', title: 'Configuration', icon: 'configuration' },
  ];
  constructor() {
    this.workspace.start();
    this.destroy.onDestroy(() => this.workspace.stop());
    this.router.events
      .pipe(
        filter((e) => e instanceof NavigationEnd),
        takeUntilDestroyed(this.destroy),
      )
      .subscribe((event) => {
        this.menu.set(false);
        if (event.id > 1)
          requestAnimationFrame(() =>
            document.querySelector<HTMLElement>('main h1')?.focus(),
          );
      });
  }
  toggleMenu(): void {
    this.menu.update((v) => !v);
    if (this.menu())
      requestAnimationFrame(() =>
        document.querySelector<HTMLElement>('.sidebar nav a')?.focus(),
      );
  }
  access(): void {
    if (this.api.operator()) {
      this.api.lock();
      this.announcement.set(
        'Operator access disconnected. Read-only mode is active.',
      );
    } else {
      this.accessError.set('');
      this.accessDialog()?.nativeElement.showModal();
    }
  }
  closeAccess(): void {
    const el = this.accessDialog()?.nativeElement;
    el?.querySelector('form')?.reset();
    el?.close();
    this.accessError.set('');
  }
  submitAccess(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const token = new FormData(form).get('token');
    try {
      this.api.unlock(typeof token === 'string' ? token : '');
      form.reset();
      this.closeAccess();
      this.announcement.set(
        'Operator token set for this page session. The server validates each action.',
      );
    } catch (e) {
      this.accessError.set(e instanceof Error ? e.message : 'Check the token.');
    }
  }
}
