import { Component, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { Workspace } from '../core/workspace';
import {
  record,
  records,
  count,
  value,
  label,
  timestamp,
  age,
  sumRows,
} from '../core/value';
import { Badge } from '../shared/badge';
import { Icon } from '../shared/icon';
@Component({
  standalone: true,
  imports: [RouterLink, Badge, Icon],
  templateUrl: './overview.html',
})
export class OverviewPage {
  readonly ws = inject(Workspace);
  readonly record = record;
  readonly records = records;
  readonly count = count;
  readonly value = value;
  readonly label = label;
  readonly timestamp = timestamp;
  readonly age = age;
  readonly stages = [
    { id: 'source', name: 'Source', description: 'Committed change capture' },
    { id: 'pipeline', name: 'Pipeline', description: 'Durable staging' },
    {
      id: 'elasticsearch',
      name: 'Elasticsearch',
      description: 'Current-state search',
    },
    { id: 'rabbitmq', name: 'RabbitMQ', description: 'Broker acceptance' },
    { id: 'consumer', name: 'Consumer', description: 'Committed effects' },
  ];
  state(name: string): unknown {
    return this.ws.error()
      ? 'unknown'
      : (this.ws.dependency(name)['health'] ?? 'unknown');
  }
  delivery(sink: string, states: readonly string[]): string | null {
    return sumRows(this.ws.data('pipeline')['deliveries'], sink, states);
  }
  backfill() {
    return this.ws.snapshot()?.['backfill']
      ? record(this.ws.snapshot()?.['backfill'])
      : null;
  }
  rate(name: string): string {
    const r = this.ws.rates()[name];
    return r === null || r === undefined
      ? 'Warming / unknown'
      : `${r} events / sec`;
  }
}
