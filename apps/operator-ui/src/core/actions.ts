import { Injectable, inject, signal } from '@angular/core';
import { Api, ApiFailure } from './api';
import { Workspace } from './workspace';
import type { Data } from './value';
export interface ActionRequest {
  title: string;
  description: string;
  label: string;
  path: string;
  method: 'POST' | 'PUT';
  body: Data;
  danger?: boolean;
  key?: string;
}
export interface PendingAction extends ActionRequest {
  requestKey: string;
}
@Injectable({ providedIn: 'root' })
export class Actions {
  readonly api = inject(Api);
  readonly workspace = inject(Workspace);
  readonly current = signal<PendingAction | null>(null);
  readonly busy = signal(false);
  readonly error = signal<ApiFailure | null>(null);
  readonly accepted = signal<{
    requestId: string | null;
    outcome: string;
  } | null>(null);
  #trigger: HTMLElement | null = null;
  open(request: ActionRequest): void {
    if (this.current()) return;
    this.#trigger =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    this.error.set(null);
    this.accepted.set(null);
    this.current.set({
      ...request,
      body: structuredClone(request.body),
      requestKey: request.key ?? crypto.randomUUID(),
    });
  }
  close(): void {
    if (this.busy()) return;
    this.current.set(null);
    this.error.set(null);
    this.accepted.set(null);
    if (this.#trigger?.isConnected) this.#trigger.focus();
  }
  async confirm(): Promise<void> {
    const request = this.current();
    if (!request || this.busy() || this.accepted()) return;
    this.busy.set(true);
    this.error.set(null);
    try {
      const result = await this.api.mutate(
        request.path,
        request.method,
        request.body,
        request.requestKey,
      );
      this.accepted.set({
        requestId: result.requestId,
        outcome: result.outcome,
      });
    } catch (e) {
      this.error.set(
        e instanceof ApiFailure ? e : new ApiFailure(0, 'unavailable', null),
      );
    } finally {
      this.busy.set(false);
    }
    if (this.accepted()) void this.workspace.refresh();
  }
}
