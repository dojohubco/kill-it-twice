import { Component, DestroyRef, inject, signal } from '@angular/core';
import { Api, ApiFailure } from '../core/api';
import { Actions } from '../core/actions';
import {
  formText,
  record,
  records,
  value,
  timestamp,
  type Data,
} from '../core/value';
import { Badge } from '../shared/badge';
import { Icon } from '../shared/icon';
@Component({
  standalone: true,
  imports: [Badge, Icon],
  templateUrl: './simulations.html',
})
export class SimulationsPage {
  readonly api = inject(Api);
  readonly actions = inject(Actions);
  readonly destroy = inject(DestroyRef);
  readonly result = signal<Data | null>(null);
  readonly error = signal('');
  readonly loading = signal(false);
  readonly chosen = signal('fixture-01');
  readonly record = record;
  readonly records = records;
  readonly value = value;
  readonly timestamp = timestamp;
  readonly fixtures = Array.from(
    { length: 16 },
    (_, i) => `fixture-${String(i + 1).padStart(2, '0')}`,
  );
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
      this.result.set(
        await this.api.read('/api/v1/simulations', this.#cancel.signal),
      );
    } catch (e) {
      this.error.set(
        e instanceof ApiFailure
          ? e.instruction
          : 'Unable to inspect simulation controls.',
      );
    } finally {
      this.loading.set(false);
    }
  }
  choose(event: Event): void {
    if (event.target instanceof HTMLSelectElement)
      this.chosen.set(event.target.value);
  }
  source(event: SubmitEvent): void {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const data = new FormData(form),
      fixture = formText(data, 'fixture'),
      operation = formText(data, 'operation'),
      value = Number(data.get('value'));
    if (
      !this.fixtures.includes(fixture) ||
      !['create', 'update', 'delete', 'restore'].includes(operation) ||
      !Number.isInteger(value) ||
      value < 0 ||
      value > 1000000
    )
      return;
    this.actions.open({
      title: `${operation[0]?.toUpperCase()}${operation.slice(1)} ${fixture}?`,
      description:
        'Change the selected test record and send the new revision through the pipeline.',
      label: `${operation[0]?.toUpperCase()}${operation.slice(1)} fixture`,
      path: '/api/v1/simulations/source-change',
      method: 'POST',
      body: { fixture, operation, value },
      danger: operation === 'delete',
    });
  }
  corrupt(): void {
    this.actions.open({
      title: `Insert invalid mapped data in ${this.chosen()}?`,
      description:
        'Create an invalid search field on an existing test record to trigger an Elasticsearch rejection. Other deliveries continue.',
      label: 'Create corrupt revision',
      path: '/api/v1/simulations/corrupt-record',
      method: 'POST',
      body: { fixture: this.chosen() },
      danger: true,
    });
  }
  network(
    sink: 'elasticsearch' | 'rabbitmq',
    state: 'connected' | 'disconnected',
  ): void {
    this.actions.open({
      title: `${state === 'connected' ? 'Reconnect' : 'Disconnect'} ${sink === 'elasticsearch' ? 'Elasticsearch' : 'RabbitMQ'} route?`,
      description:
        'Change the receiver connection. Queued and staged work is retained.',
      label: state === 'connected' ? 'Reconnect route' : 'Disconnect route',
      path: `/api/v1/simulations/network/${sink}`,
      method: 'PUT',
      body: { state },
      danger: state === 'disconnected',
    });
  }
}
