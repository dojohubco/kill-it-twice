import { Component, inject } from '@angular/core';
import { Workspace } from '../core/workspace';
import { count, record, value, label, jsonText } from '../core/value';
import { Badge } from '../shared/badge';
import { Icon } from '../shared/icon';
@Component({
  standalone: true,
  imports: [Badge, Icon],
  templateUrl: './configuration.html',
})
export class ConfigurationPage {
  readonly ws = inject(Workspace);
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
