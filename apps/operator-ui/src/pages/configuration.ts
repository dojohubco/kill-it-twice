import { Component, effect, inject, signal, untracked } from '@angular/core';
import { Actions } from '../core/actions';
import { ApiFailure } from '../core/api';
import { Workspace } from '../core/workspace';
import {
  count,
  record,
  records,
  value,
  label,
  jsonText,
  timestamp,
  type Data,
} from '../core/value';
import { Badge } from '../shared/badge';
import { Icon } from '../shared/icon';
@Component({
  standalone: true,
  imports: [Badge, Icon],
  templateUrl: './configuration.html',
  host: { '(window:beforeunload)': 'beforeUnload($event)' },
})
export class ConfigurationPage {
  readonly ws = inject(Workspace);
  readonly actions = inject(Actions);
  readonly polling = signal<Data | null>(null);
  readonly pollingLoading = signal(false);
  readonly pollingError = signal('');
  readonly captureMs = signal('1000');
  readonly backfillMs = signal('1000');
  readonly dirty = signal(false);
  readonly records = records;
  readonly timestamp = timestamp;
  constructor() {
    void this.loadPolling();
    effect(() => {
      if (
        this.actions.accepted() &&
        this.actions.current()?.path === '/api/v1/config/polling'
      )
        untracked(() => {
          void this.loadPolling();
        });
    });
  }
  async loadPolling(): Promise<void> {
    if (this.pollingLoading()) return;
    this.pollingLoading.set(true);
    this.pollingError.set('');
    try {
      const current = await this.ws.api.read('/api/v1/config/polling');
      this.polling.set(current);
      this.captureMs.set(String(current['capture_poll_ms']));
      this.backfillMs.set(String(current['backfill_idle_ms']));
      this.dirty.set(false);
    } catch (error) {
      this.polling.set(null);
      this.pollingError.set(
        error instanceof ApiFailure
          ? error.instruction
          : 'Unable to load polling settings. Reload to try again.',
      );
    } finally {
      this.pollingLoading.set(false);
    }
  }
  edit(event: Event, key: 'capture' | 'backfill'): void {
    const input = event.target;
    if (!(input instanceof HTMLInputElement)) return;
    (key === 'capture' ? this.captureMs : this.backfillMs).set(input.value);
    this.dirty.set(true);
  }
  canLeave(): boolean {
    return !this.dirty() || window.confirm('Discard unsaved polling changes?');
  }
  beforeUnload(event: BeforeUnloadEvent): void {
    if (this.dirty()) event.preventDefault();
  }
  save(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    const current = this.polling();
    if (
      !(form instanceof HTMLFormElement) ||
      !form.reportValidity() ||
      !current
    )
      return;
    const capture = Number(this.captureMs()),
      backfill = Number(this.backfillMs());
    if (
      ![capture, backfill].every(
        (n) => Number.isInteger(n) && n >= 50 && n <= 30000,
      )
    )
      return;
    this.actions.open({
      title: 'Save polling intervals?',
      description: `Incremental capture: ${capture} ms. Idle backfill: ${backfill} ms. Workers read saved settings between iterations; an existing wait or in-flight work can finish first.`,
      label: 'Save intervals',
      path: '/api/v1/config/polling',
      method: 'PUT',
      body: {
        expected_revision: current['revision'],
        capture_poll_ms: capture,
        backfill_idle_ms: backfill,
      },
    });
  }
  readonly count = count;
  readonly record = record;
  readonly value = value;
  readonly label = label;
  readonly jsonText = jsonText;
  readonly limits = [
    { key: 'page', name: 'Maximum list page' },
    { key: 'attempt_history', name: 'Recent attempts per event' },
    { key: 'body_bytes', name: 'HTTP request body bytes' },
    { key: 'ranges', name: 'Maximum backfill ranges' },
    { key: 'fixtures', name: 'Named simulation fixtures' },
    { key: 'freshness_seconds', name: 'Observation freshness seconds' },
  ];
}
