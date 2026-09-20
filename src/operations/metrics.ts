import type { Snapshot } from './service.ts';
import { record } from './validation.ts';
export const metricDefinitions = [
  [
    'pipeline_source_reachable',
    'gauge',
    'Whether the source observation succeeded.',
  ],
  [
    'pipeline_source_last_observation_timestamp_seconds',
    'gauge',
    'Source clock timestamp of the successful observation.',
  ],
  [
    'pipeline_source_pending',
    'gauge',
    'Committed source revisions not yet acknowledged.',
  ],
  [
    'pipeline_source_delayed',
    'gauge',
    'Pending source revisions waiting for eligibility.',
  ],
  [
    'pipeline_source_leased',
    'gauge',
    'Source revisions with a retained lease.',
  ],
  [
    'pipeline_source_blocked',
    'gauge',
    'Source revisions blocked from capture.',
  ],
  [
    'pipeline_source_missing_work',
    'gauge',
    'Committed source revisions missing capture work.',
  ],
  [
    'pipeline_source_oldest_pending_timestamp_seconds',
    'gauge',
    'Oldest unresolved source recorded timestamp; absent when none.',
  ],
  [
    'pipeline_backfill_active',
    'gauge',
    'Whether an unfinished backfill run exists.',
  ],
  [
    'pipeline_backfill_phase',
    'gauge',
    'Current backfill phase as a one-hot bounded enum.',
  ],
  [
    'pipeline_backfill_scan_observed',
    'gauge',
    'Revisions observed by the current backfill scan.',
  ],
  [
    'pipeline_backfill_required_sealed',
    'gauge',
    'Whether the current backfill required membership is sealed.',
  ],
  [
    'pipeline_backfill_required',
    'gauge',
    'Sealed required membership; absent before sealing.',
  ],
  [
    'pipeline_backfill_blocked',
    'gauge',
    'Whether current backfill progress is blocked.',
  ],
  [
    'pipeline_delivery_backlog',
    'gauge',
    'Delivery obligations by sink and current state.',
  ],
  [
    'pipeline_consumer_observation_backlog',
    'gauge',
    'Consumer observations by current state.',
  ],
  [
    'pipeline_dlq_open',
    'gauge',
    'Current unresolved dead letters or retained quarantine by kind.',
  ],
  [
    'pipeline_dependency_health',
    'gauge',
    'Dependency health as a one-hot bounded enum.',
  ],
  [
    'pipeline_observation_fresh_timestamp_seconds',
    'gauge',
    'Timestamp of the successful fresh component observation.',
  ],
  [
    'pipeline_events_staged_total',
    'counter',
    'Immutable canonical events durably staged.',
  ],
  [
    'pipeline_delivery_attempts_total',
    'counter',
    'Completed delivery attempts including retained historical unattributed outcomes.',
  ],
  [
    'pipeline_delivery_settled_total',
    'counter',
    'Durable terminal delivery outcomes including repeated dead letters.',
  ],
  [
    'pipeline_consumer_effects_total',
    'counter',
    'Immutable committed consumer mutation effects.',
  ],
  [
    'pipeline_replay_requests_total',
    'counter',
    'Immutable durably scheduled replay requests.',
  ],
] as const;
const labelValues: Record<string, readonly string[]> = {
  sink: ['elasticsearch', 'rabbitmq'],
  state: [
    'pending',
    'leased',
    'retry_wait',
    'satisfied',
    'dead_letter',
    'processed',
    'quarantined',
    'healthy',
    'degraded',
    'unavailable',
  ],
  phase: [
    'scanning',
    'sealing',
    'importing',
    'draining',
    'complete',
    'complete_with_errors',
  ],
  kind: ['elasticsearch', 'consumer'],
  component: ['source', 'pipeline', 'elasticsearch', 'rabbitmq', 'consumer'],
  outcome_class: [
    'applied',
    'already_applied',
    'superseded',
    'mapping',
    'oversized',
    'transient',
    'auth',
    'configuration',
    'integrity',
    'expired',
    'confirmed',
    'historical_unknown',
  ],
  disposition: [
    'applied',
    'already_applied',
    'superseded',
    'dead_letter',
    'broker_confirmed',
  ],
};
export function metrics(snapshot: Snapshot): string {
  const samples = new Map<string, string[]>();
  const emit = (
    name: string,
    value: unknown,
    labels: Record<string, unknown> = {},
  ) => {
    if (typeof value !== 'string' && typeof value !== 'number')
      throw new Error('Invalid metric value');
    if (!Number.isFinite(Number(value)) || Number(value) < 0)
      throw new Error('Invalid metric scalar');
    const pairs = Object.entries(labels).map(([k, v]) => {
      if (typeof v !== 'string' || !labelValues[k]?.includes(v))
        throw new Error('Metric label outside fixed enum');
      return `${k}="${v}"`;
    });
    const line = `${name}${pairs.length ? `{${pairs.join(',')}}` : ''} ${value}`;
    const prior = samples.get(name) ?? [];
    prior.push(line);
    samples.set(name, prior);
  };
  const timestamp = (
    name: string,
    at: unknown,
    labels: Record<string, unknown> = {},
  ) => {
    if (typeof at !== 'string') return;
    const fraction = at.match(/\.(\d{1,6})(?:Z|[+-])/u)?.[1];
    const seconds =
      Math.floor(Date.parse(at) / 1000) +
      (fraction ? Number('0.' + fraction) : 0);
    emit(name, seconds, labels);
  };
  for (const [component, o] of Object.entries(snapshot.dependencies)) {
    for (const state of ['healthy', 'degraded', 'unavailable'])
      emit('pipeline_dependency_health', Number(o.health === state), {
        component,
        state,
      });
    if (o.freshness === 'fresh')
      timestamp('pipeline_observation_fresh_timestamp_seconds', o.observed_at, {
        component,
      });
  }
  const source = snapshot.dependencies['source'];
  emit('pipeline_source_reachable', Number(source?.freshness === 'fresh'));
  if (source?.freshness === 'fresh' && source.data) {
    const c = record(source.data['counts']);
    timestamp(
      'pipeline_source_last_observation_timestamp_seconds',
      source.observed_at,
    );
    emit('pipeline_source_pending', source.data['pending']);
    emit('pipeline_source_delayed', c['pending_delayed']);
    emit(
      'pipeline_source_leased',
      (
        BigInt(String(c['leased_current'])) +
        BigInt(String(c['leased_expired']))
      ).toString(),
    );
    emit('pipeline_source_blocked', c['blocked']);
    emit('pipeline_source_missing_work', c['missing']);
    timestamp(
      'pipeline_source_oldest_pending_timestamp_seconds',
      source.data['oldest_pending_at'],
    );
  }
  const pipeline = snapshot.dependencies['pipeline'];
  if (pipeline?.freshness === 'fresh' && pipeline.data) {
    const p = pipeline.data;
    emit('pipeline_events_staged_total', p['staged']);
    emit('pipeline_replay_requests_total', p['replays'], {
      sink: 'elasticsearch',
    });
    const dataRows = (key: string): Record<string, unknown>[] => {
      const v = p[key];
      if (!Array.isArray(v)) throw new Error('Invalid metric rows');
      return v.map((x: unknown) => record(x));
    };
    const deliveries = dataRows('deliveries');
    for (const sink of ['elasticsearch', 'rabbitmq'])
      for (const state of [
        'pending',
        'leased',
        'retry_wait',
        'satisfied',
        ...(sink === 'elasticsearch' ? ['dead_letter'] : []),
      ])
        emit(
          'pipeline_delivery_backlog',
          deliveries.find((r) => r['sink'] === sink && r['state'] === state)?.[
            'count'
          ] ?? '0',
          { sink, state },
        );
    for (const state of ['pending', 'processed', 'quarantined'])
      emit(
        'pipeline_consumer_observation_backlog',
        dataRows('observations').find((r) => r['state'] === state)?.['count'] ??
          '0',
        { state },
      );
    for (const r of dataRows('attempts'))
      emit('pipeline_delivery_attempts_total', r['total'], {
        sink: r['sink'],
        outcome_class: r['outcome_class'],
      });
    for (const r of dataRows('settled'))
      emit('pipeline_delivery_settled_total', r['count'], {
        sink: r['sink'],
        disposition: r['disposition'],
      });
    emit(
      'pipeline_dlq_open',
      deliveries.find(
        (r) => r['sink'] === 'elasticsearch' && r['state'] === 'dead_letter',
      )?.['count'] ?? '0',
      { kind: 'elasticsearch' },
    );
    const b = snapshot.backfill;
    emit(
      'pipeline_backfill_active',
      Number(
        Boolean(
          b &&
          !['complete', 'complete_with_errors'].includes(String(b['phase'])),
        ),
      ),
    );
    for (const phase of labelValues['phase'] ?? [])
      emit('pipeline_backfill_phase', Number(b?.['phase'] === phase), {
        phase,
      });
    emit('pipeline_backfill_scan_observed', b?.['scan_observations'] ?? '0');
    emit('pipeline_backfill_blocked', Number(b?.['blocked'] === true));
    emit(
      'pipeline_backfill_required_sealed',
      Number(b?.['required_sealed'] === true),
    );
    if (b?.['required_sealed'] === true)
      emit('pipeline_backfill_required', b['denominator']);
  }
  const consumer = snapshot.dependencies['consumer'];
  if (consumer?.freshness === 'fresh' && consumer.data) {
    emit('pipeline_consumer_effects_total', consumer.data['effects']);
    emit('pipeline_dlq_open', consumer.data['quarantine'], {
      kind: 'consumer',
    });
  }
  return metricDefinitions
    .map(
      ([name, type, help]) =>
        `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n${(samples.get(name) ?? []).map((s) => s + '\n').join('')}`,
    )
    .join('');
}
