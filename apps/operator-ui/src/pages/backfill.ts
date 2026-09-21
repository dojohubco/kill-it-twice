import { Component, computed, inject, signal } from '@angular/core';
import { Workspace } from '../core/workspace';
import { Api, ApiFailure } from '../core/api';
import { Actions } from '../core/actions';
import {
  record,
  records,
  value,
  count,
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
  templateUrl: './backfill.html',
})
export class BackfillPage {
  readonly ws = inject(Workspace);
  readonly api = inject(Api);
  readonly actions = inject(Actions);
  readonly selected = signal<Data | null>(null);
  readonly loading = signal(false);
  readonly error = signal('');
  readonly run = computed(
    () =>
      this.selected() ??
      (this.ws.snapshot()?.['backfill']
        ? record(this.ws.snapshot()?.['backfill'])
        : null),
  );
  readonly record = record;
  readonly records = records;
  readonly value = value;
  readonly count = count;
  readonly label = label;
  readonly timestamp = timestamp;
  readonly jsonText = jsonText;
  start(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const ranges = Number(new FormData(form).get('ranges'));
    if (!Number.isInteger(ranges) || ranges < 1 || ranges > 16) return;
    const key = crypto.randomUUID();
    this.actions.open({
      title: 'Start a backfill run?',
      description:
        'Create a finite current-state scan. Incremental capture remains independent. Receiver and consumer work must drain before completion.',
      label: 'Start backfill',
      path: '/api/v1/backfills',
      method: 'POST',
      key,
      body: { run_id: key, ranges },
    });
  }
  pause(paused: boolean): void {
    const r = this.run();
    if (!r) return;
    this.actions.open({
      title: paused ? 'Pause new page admission?' : 'Resume this backfill?',
      description: paused
        ? 'An in-flight bounded page may still commit. Pausing does not roll back completed work or stop independent capture.'
        : 'Continue from the retained checkpoints. Existing events and receiver outcomes are preserved.',
      label: paused ? 'Request pause' : 'Resume backfill',
      path: `/api/v1/backfills/${encodeURIComponent(value(r['run_id']))}/${paused ? 'pause' : 'resume'}`,
      method: 'POST',
      body: {},
    });
  }
  async select(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const id = new FormData(form).get('run');
    if (typeof id !== 'string' || !/^[0-9a-f-]{36}$/i.test(id)) {
      this.error.set('Enter a valid run UUID.');
      return;
    }
    this.loading.set(true);
    this.error.set('');
    try {
      this.selected.set(
        await this.api.read(`/api/v1/backfills/${encodeURIComponent(id)}`),
      );
    } catch (e) {
      this.error.set(
        e instanceof ApiFailure ? e.instruction : 'Unable to read this run.',
      );
    } finally {
      this.loading.set(false);
    }
  }
  latest(): void {
    this.selected.set(null);
    this.error.set('');
    void this.ws.refresh();
  }
}
