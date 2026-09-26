import { Component, DestroyRef, inject, signal } from '@angular/core';
import { Api, ApiFailure } from '../core/api';
import { Actions } from '../core/actions';
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
  templateUrl: './failures.html',
})
export class FailuresPage {
  readonly api = inject(Api);
  readonly actions = inject(Actions);
  readonly destroy = inject(DestroyRef);
  readonly result = signal<Data | null>(null);
  readonly loading = signal(false);
  readonly error = signal('');
  readonly selected = signal<Data | null>(null);
  readonly evidence = signal<Data | null>(null);
  readonly evidenceError = signal('');
  readonly pageNo = signal(1);
  readonly record = record;
  readonly records = records;
  readonly value = value;
  readonly label = label;
  readonly timestamp = timestamp;
  readonly jsonText = jsonText;
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
    try {
      const q = new URLSearchParams({ limit: '25' });
      if (this.#cursor) q.set('cursor', this.#cursor);
      this.result.set(
        await this.api.read(
          '/api/v1/failures?' + q.toString(),
          this.#cancel.signal,
        ),
      );
    } catch (e) {
      this.error.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to load failures.',
      );
    } finally {
      this.loading.set(false);
    }
  }
  next(): void {
    const c = this.result()?.['next_cursor'];
    if (typeof c !== 'string' || this.loading() || this.pageNo() >= 101) return;
    this.#previous.push(this.#cursor);
    this.#cursor = c;
    this.pageNo.update((n) => n + 1);
    void this.load();
  }
  previous(): void {
    if (!this.#previous.length || this.loading()) return;
    this.#cursor = this.#previous.pop() ?? '';
    this.pageNo.update((n) => n - 1);
    void this.load();
  }
  inspect(item: Data): void {
    this.selected.set(item);
    this.evidence.set(null);
    this.evidenceError.set('');
    requestAnimationFrame(() =>
      document.getElementById('failure-detail-title')?.focus(),
    );
  }
  async details(): Promise<void> {
    const id = this.selected()?.['event_id'];
    if (typeof id !== 'string') return;
    try {
      this.evidence.set(
        await this.api.read(
          '/api/v1/events/' + encodeURIComponent(id),
          this.#cancel.signal,
        ),
      );
    } catch (e) {
      this.evidenceError.set(
        e instanceof ApiFailure
          ? e.instruction
          : 'Unable to read event evidence.',
      );
    }
  }
  replay(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget,
      item = this.selected();
    if (
      !(form instanceof HTMLFormElement) ||
      !item ||
      item['replayable'] !== true
    )
      return;
    const reason = formText(new FormData(form), 'reason').trim();
    if (!reason || reason.length > 128) return;
    this.actions.open({
      title: 'Replay this Elasticsearch failure?',
      description:
        'Retry this Elasticsearch delivery. If the cause is unchanged, it may fail again. RabbitMQ and consumer results are preserved.',
      label: 'Schedule replay',
      path:
        '/api/v1/failures/elasticsearch/' +
        encodeURIComponent(value(item['event_id'])) +
        '/replay',
      method: 'POST',
      body: {
        attempt_id: item['attempt_id'],
        destination_id: item['destination_id'],
        generation: item['generation'],
        reason,
      },
    });
  }
}
