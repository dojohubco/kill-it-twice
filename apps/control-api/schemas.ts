import type { SchemaObject } from '@nestjs/swagger';
const text: SchemaObject = { type: 'string' };
const decimal: SchemaObject = { type: 'string', pattern: '^(0|[1-9][0-9]*)$' };
const boolean: SchemaObject = { type: 'boolean' };
const timestamp: SchemaObject = {
  type: 'string',
  format: 'date-time',
  nullable: true,
};
const object = (
  properties: Record<string, SchemaObject>,
  required = Object.keys(properties),
): SchemaObject => ({ type: 'object', properties, required });
const array = (items: SchemaObject): SchemaObject => ({ type: 'array', items });
const enumeration = (...values: string[]): SchemaObject => ({
  type: 'string',
  enum: values,
});
const arbitrary: SchemaObject = { type: 'object', additionalProperties: true };
const observation = object({
  observed_at: timestamp,
  freshness: enumeration('fresh', 'stale', 'unavailable'),
  health: enumeration('healthy', 'degraded', 'unavailable'),
  dependency_health: enumeration('healthy', 'unavailable'),
  data_health: enumeration('healthy', 'degraded', 'unknown'),
  data: { ...arbitrary, nullable: true },
});
const backfill = object({
  run_id: { type: 'string', format: 'uuid' },
  phase: enumeration(
    'scanning',
    'sealing',
    'importing',
    'draining',
    'complete',
    'complete_with_errors',
  ),
  desired_paused: boolean,
  required_sealed: boolean,
  denominator: { ...decimal, nullable: true },
  counts: arbitrary,
});
const projection = object({
  projection_schema: { type: 'string', enum: ['search-v1'] },
  source_epoch: { type: 'string', format: 'uuid' },
  entity_id: decimal,
  entity_version: decimal,
  is_deleted: boolean,
  content_sha256: text,
  search_fields: arbitrary,
});
const schemas: Record<string, SchemaObject> = {
  status: object({
    observed_at: timestamp,
    health: enumeration('healthy', 'degraded', 'unavailable'),
    dependencies: object(
      Object.fromEntries(
        ['source', 'pipeline', 'consumer', 'elasticsearch', 'rabbitmq'].map(
          (k) => [
            k,
            k === 'pipeline'
              ? {
                  ...observation,
                  properties: {
                    ...observation.properties,
                    data: {
                      ...object({
                        es_oldest_unresolved_at: timestamp,
                        es_oldest_unresolved_age_seconds: {
                          type: 'number',
                          minimum: 0,
                          nullable: true,
                        },
                      }),
                      additionalProperties: true,
                      nullable: true,
                    },
                  },
                }
              : observation,
          ],
        ),
      ),
    ),
    backfill: { ...backfill, nullable: true },
    worker_liveness: enumeration('unknown_no_heartbeat_evidence'),
    consumer_identity_validation: enumeration(
      'matched',
      'unknown_or_mismatched',
    ),
    consistency: enumeration(
      'independent_observations_not_atomic_global_state',
    ),
    throughput: arbitrary,
    recovery: arbitrary,
  }),
  backfill_status: backfill,
  entity_search: object({
    observed_at: timestamp,
    freshness: enumeration('near_realtime'),
    items: array({
      ...projection,
      properties: { ...projection.properties, receiver_version: decimal },
      required: [...(projection.required ?? []), 'receiver_version'],
    }),
    next_cursor: { ...text, nullable: true },
  }),
  entity_detail: object({
    observed_at: timestamp,
    freshness: enumeration('realtime'),
    projection,
    receiver_version: decimal,
    source: { ...arbitrary, nullable: true },
    pipeline: array(arbitrary),
    convergence: enumeration('current', 'degraded'),
  }),
  event_detail: object({
    event_id: text,
    source_epoch: text,
    entity_id: decimal,
    entity_version: decimal,
    content_sha256: text,
    deliveries: array(arbitrary),
    es_attempts: { ...array(arbitrary), maxItems: 8 },
    rabbit_attempts: { ...array(arbitrary), maxItems: 8 },
    consumer: { ...arbitrary, nullable: true },
    source_capture: { ...arbitrary, nullable: true },
    observed_at: timestamp,
  }),
  failures: object({
    observed_at: timestamp,
    items: {
      ...array(
        object({
          key: text,
          type: enumeration(
            'elasticsearch_history',
            'elasticsearch_active',
            'consumer_quarantine',
            'capture_block',
            'backfill_block',
          ),
          recorded_at: timestamp,
          classification: text,
          context: { type: 'string', maxLength: 256 },
          state: text,
          replayable: boolean,
          replay_reason: text,
        }),
      ),
      maxItems: 100,
    },
    next_cursor: { ...text, nullable: true },
  }),
  config: object({
    source_epoch: text,
    pipeline_id: text,
    limits: arbitrary,
    receivers: arbitrary,
    immutable: array(text),
    restart_required: array(text),
    quarantine_replay: { type: 'boolean', enum: [false] },
  }),
  simulations: object({
    observed_at: timestamp,
    generator: enumeration('explicit_commands_only'),
    elasticsearch: arbitrary,
    rabbitmq: arbitrary,
    fixtures: { ...array(arbitrary), maxItems: 16 },
  }),
};
export function responseSchema(operation: string): SchemaObject {
  const data = schemas[operation];
  if (!data) throw new Error('Undeclared API response schema');
  return object({ request_id: { type: 'string', format: 'uuid' }, data });
}
export const mutationSchema = object({
  request_id: { type: 'string', format: 'uuid' },
  outcome: enumeration('scheduled', 'already_applied'),
  data: arbitrary,
});
export const errorSchema = object({
  request_id: { type: 'string', format: 'uuid' },
  error: object({ code: text, message: text }),
});
