import { Component, DestroyRef, inject, signal } from '@angular/core';
import { Api, ApiFailure } from '../core/api';
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
  #cancel = new AbortController();
  constructor() {
    this.destroy.onDestroy(() => this.#cancel.abort());
    void this.load();
  }
  async load(): Promise<void> {
    if (this.loading()) return;
    this.loading.set(true);
    this.error.set('');
    const q = new URLSearchParams({
      limit: '25',
      include_deleted: String(this.#deleted),
    });
    if (this.#query) q.set('q', this.#query);
    if (this.#cursor) q.set('cursor', this.#cursor);
    try {
      this.result.set(
        await this.api.read(
          '/api/v1/entities?' + q.toString(),
          this.#cancel.signal,
        ),
      );
    } catch (e) {
      this.error.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to load records.',
      );
    } finally {
      this.loading.set(false);
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
    this.detail.set(null);
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
    void this.load();
  }
  previous(): void {
    if (!this.#previous.length || this.loading()) return;
    this.#cursor = this.#previous.pop() ?? '';
    this.pageNo.update((n) => n - 1);
    void this.load();
  }
  async inspect(item: Data): Promise<void> {
    if (this.detailLoading()) return;
    this.detailLoading.set(true);
    this.detailError.set('');
    this.detail.set(null);
    try {
      this.detail.set(
        await this.api.read(
          `/api/v1/entities/${encodeURIComponent(value(item['source_epoch']))}/${encodeURIComponent(value(item['entity_id']))}`,
          this.#cancel.signal,
        ),
      );
      requestAnimationFrame(() =>
        document.getElementById('record-detail-title')?.focus(),
      );
    } catch (e) {
      this.detailError.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to read this record.',
      );
    } finally {
      this.detailLoading.set(false);
    }
  }
}
