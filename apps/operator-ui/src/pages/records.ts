import { Component, DestroyRef, effect, inject, signal } from '@angular/core';
import { Api, ApiFailure } from '../core/api';
import { Workspace } from '../core/workspace';
import {
  formText,
  record,
  records,
  value,
  label,
  timestamp,
  jsonText,
  type Data,
} from '../core/value';
import { Badge } from '../shared/badge';
import { Icon } from '../shared/icon';
@Component({
  standalone: true,
  imports: [Badge, Icon],
  templateUrl: './records.html',
})
export class RecordsPage {
  readonly api = inject(Api);
  readonly workspace = inject(Workspace);
  readonly destroy = inject(DestroyRef);
  readonly result = signal<Data | null>(null);
  readonly loading = signal(false);
  readonly error = signal('');
  readonly detail = signal<Data | null>(null);
  readonly detailError = signal('');
  readonly detailLoading = signal(false);
  readonly pageNo = signal(1);
  readonly record = record;
  readonly records = records;
  readonly value = value;
  readonly label = label;
  readonly timestamp = timestamp;
  readonly jsonText = jsonText;
  #query = '';
  #deleted = false;
  #cursor = '';
  #previous: string[] = [];
  #listRequest: AbortController | undefined;
  #detailRequest: AbortController | undefined;
  #detailPath = '';
  #timer: ReturnType<typeof setTimeout> | undefined;
  #disposed = false;
  readonly #visibility = () => {
    clearTimeout(this.#timer);
    if (!document.hidden && this.workspace.automatic()) void this.refresh(true);
  };
  constructor() {
    document.addEventListener('visibilitychange', this.#visibility);
    this.destroy.onDestroy(() => {
      this.#disposed = true;
      clearTimeout(this.#timer);
      this.#listRequest?.abort();
      this.#detailRequest?.abort();
      document.removeEventListener('visibilitychange', this.#visibility);
    });
    effect(() => this.schedule());
    void this.load();
  }
  private schedule(): void {
    clearTimeout(this.#timer);
    if (this.workspace.automatic() && !this.#disposed && !document.hidden)
      this.#timer = setTimeout(() => void this.refresh(true), 5000);
  }
  async refresh(background = false): Promise<void> {
    if (this.#disposed) return;
    if (background && (!this.workspace.automatic() || document.hidden)) {
      this.schedule();
      return;
    }
    await Promise.all([this.load(background), this.loadDetail(background)]);
    this.schedule();
  }
  async load(background = false): Promise<void> {
    if (this.#disposed || (background && this.#listRequest)) return;
    this.#listRequest?.abort();
    const request = new AbortController();
    this.#listRequest = request;
    this.loading.set(!background);
    const q = new URLSearchParams({
      limit: '25',
      include_deleted: String(this.#deleted),
    });
    if (this.#query) q.set('q', this.#query);
    if (this.#cursor) q.set('cursor', this.#cursor);
    try {
      const result = await this.api.read(
        '/api/v1/entities?' + q.toString(),
        request.signal,
      );
      if (request.signal.aborted || this.#disposed) return;
      this.result.set(result);
      this.error.set('');
    } catch (e) {
      if (request.signal.aborted || this.#disposed) return;
      this.error.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to load records.',
      );
    } finally {
      if (this.#listRequest === request) {
        this.#listRequest = undefined;
        this.loading.set(false);
      }
    }
  }
  search(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const data = new FormData(form);
    this.#query = formText(data, 'q').trim().slice(0, 128);
    this.#deleted = data.get('deleted') === 'on';
    this.#cursor = '';
    this.#previous = [];
    this.pageNo.set(1);
    this.result.set(null);
    this.closeDetail();
    void this.load();
  }
  next(): void {
    const next = this.result()?.['next_cursor'];
    if (
      typeof next !== 'string' ||
      this.loading() ||
      this.#previous.length >= 100
    )
      return;
    this.#previous.push(this.#cursor);
    this.#cursor = next;
    this.pageNo.update((n) => n + 1);
    this.result.set(null);
    void this.load();
  }
  previous(): void {
    if (!this.#previous.length || this.loading()) return;
    this.#cursor = this.#previous.pop() ?? '';
    this.pageNo.update((n) => n - 1);
    this.result.set(null);
    void this.load();
  }
  async inspect(item: Data): Promise<void> {
    this.closeDetail();
    this.#detailPath = `/api/v1/entities/${encodeURIComponent(value(item['source_epoch']))}/${encodeURIComponent(value(item['entity_id']))}`;
    await this.loadDetail(false, true);
  }
  closeDetail(): void {
    this.#detailRequest?.abort();
    this.#detailRequest = undefined;
    this.#detailPath = '';
    this.detailLoading.set(false);
    this.detailError.set('');
    this.detail.set(null);
  }
  private async loadDetail(background = false, focus = false): Promise<void> {
    if (
      !this.#detailPath ||
      this.#disposed ||
      (background && this.#detailRequest)
    )
      return;
    this.#detailRequest?.abort();
    const request = new AbortController();
    this.#detailRequest = request;
    this.detailLoading.set(!background);
    try {
      const detail = await this.api.read(this.#detailPath, request.signal);
      if (request.signal.aborted || this.#disposed) return;
      this.detail.set(detail);
      this.detailError.set('');
      if (focus)
        requestAnimationFrame(() => {
          if (
            !request.signal.aborted &&
            !this.#disposed &&
            this.detail() === detail
          )
            document.getElementById('record-detail-title')?.focus();
        });
    } catch (e) {
      if (request.signal.aborted || this.#disposed) return;
      this.detailError.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to read this record.',
      );
    } finally {
      if (this.#detailRequest === request) {
        this.#detailRequest = undefined;
        this.detailLoading.set(false);
      }
    }
  }
}
