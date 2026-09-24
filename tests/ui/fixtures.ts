import type { Page } from 'playwright-core';
// Browser-only response fixtures; these are not imported by the application.
const epoch = '11111111-1111-4111-8111-111111111111';
const pipeline = '22222222-2222-4222-8222-222222222222';
const runId = '33333333-3333-4333-8333-333333333333';
export const entityId = '9007199254740993';
export const eventId = `${epoch}:${entityId}:8`;
export const token = 'ui-test-only-operator-token-never-a-real-credential';
const now = () => new Date().toISOString();
const observation = (data: unknown, health = 'healthy') => ({
  observed_at: now(),
  freshness: 'fresh',
  health,
  data,
});
function backfillFixture() {
  return {
    run_id: runId,
    phase: 'scanning',
    blocked: false,
    range_count: 4,
    desired_paused: false,
    effective_paused: false,
    scan_observations: '128',
    required_sealed: false,
    denominator: null,
    sealed_at: null,
    created_at: now(),
    completed_at: null,
    observed_at: now(),
    counts: {
      required: null,
      es_satisfied: '120',
      es_pending: '8',
      es_errors: '0',
      rabbit_satisfied: '128',
      rabbit_pending: '0',
      consumer_processed: '124',
      consumer_pending: '4',
      consumer_errors: '0',
    },
    ranges: [
      {
        range_no: 1,
        state: 'complete',
        lower_key: '0',
        checkpoint: '64',
        upper_key: '64',
        reason: null,
      },
      {
        range_no: 2,
        state: 'leased',
        lower_key: '64',
        checkpoint: '128',
        upper_key: '129',
        reason: null,
      },
      {
        range_no: 3,
        state: 'ready',
        lower_key: '129',
        checkpoint: '129',
        upper_key: '193',
        reason: null,
      },
      {
        range_no: 4,
        state: 'ready',
        lower_key: '193',
        checkpoint: '193',
        upper_key: '257',
        reason: null,
      },
    ],
  };
}
function statusFixture() {
  return {
    observed_at: now(),
    health: 'healthy',
    dependencies: {
      source: observation({
        pending: '3',
        oldest_pending_age_seconds: 1.5,
        counts: {
          pending_due: '3',
          pending_delayed: '0',
          blocked: '0',
          missing: '0',
        },
      }),
      pipeline: observation({
        staged: '144',
        deliveries: [
          { sink: 'elasticsearch', state: 'satisfied', count: '132' },
          { sink: 'elasticsearch', state: 'pending', count: '12' },
          { sink: 'rabbitmq', state: 'satisfied', count: '144' },
        ],
        settled: [
          { sink: 'elasticsearch', disposition: 'applied', count: '132' },
          { sink: 'rabbitmq', disposition: 'broker_confirmed', count: '144' },
        ],
      }),
      consumer: observation({
        processed: '128',
        effects: '63',
        quarantine: '0',
      }),
      elasticsearch: observation({ mode: 'ready' }),
      rabbitmq: observation({ state: 'connected' }),
    },
    backfill: backfillFixture(),
    worker_liveness: 'unknown_no_heartbeat_evidence',
    consumer_identity_validation: 'matched',
    throughput: Object.fromEntries(
      [
        'staged',
        'elasticsearch_satisfied',
        'rabbitmq_confirmed',
        'consumer_processed',
        'mutation_effects',
      ].map((name) => [
        name,
        {
          state: 'warming',
          per_second: null,
          reason: 'first_sample',
          interval_ms: null,
        },
      ]),
    ),
  };
}
function configFixture() {
  return {
    source_epoch: epoch,
    pipeline_id: pipeline,
    limits: {
      page: 100,
      attempt_history: 8,
      body_bytes: 8192,
      ranges: 16,
      fixtures: 16,
      freshness_seconds: 30,
    },
    receivers: {
      elasticsearch: {
        mode: 'ready',
        destination_id: '44444444-4444-4444-8444-444444444444',
        generation: '1',
        index_uuid: 'ui-fixture-index',
      },
      rabbitmq: {
        mode: 'ready',
        destination_id: '55555555-5555-4555-8555-555555555555',
        generation: '1',
        registration_id: 'ui-fixture-registration',
      },
    },
    quarantine_replay: false,
  };
}
const projection = {
  source_epoch: epoch,
  entity_id: entityId,
  entity_version: '8',
  is_deleted: false,
  content_sha256: 'a'.repeat(64),
  search_fields: { name: 'Kartli Trading', country: 'GE', loyalty_points: 42 },
};
export interface UiFixtureState {
  unavailable: boolean;
  empty: boolean;
  ambiguousOnce: boolean;
  configUnavailable: boolean;
  requests: {
    method: string;
    path: string;
    key: string | null;
    body: string | null;
  }[];
}
export async function routeFixtures(page: Page): Promise<UiFixtureState> {
  const state: UiFixtureState = {
    unavailable: false,
    empty: false,
    ambiguousOnce: false,
    configUnavailable: false,
    requests: [],
  };
  let statusReads = 0;
  await page.route('**/api/v1/**', async (route) => {
    const request = route.request(),
      url = new URL(request.url()),
      path = url.pathname;
    state.requests.push({
      method: request.method(),
      path,
      key: request.headers()['idempotency-key'] ?? null,
      body: request.postData(),
    });
    const reply = async (status: number, data: Record<string, unknown>) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify({
          request_id: '66666666-6666-4666-8666-666666666666',
          ...data,
        }),
      });
    if (
      state.unavailable ||
      (state.configUnavailable && path.endsWith('/config'))
    ) {
      await reply(503, {
        error: {
          code: 'unavailable',
          message: 'UI test-only unavailable fixture',
        },
      });
      return;
    }
    if (request.method() !== 'GET') {
      if (state.ambiguousOnce) {
        state.ambiguousOnce = false;
        await reply(503, {
          error: {
            code: 'unavailable',
            message: 'UI test-only ambiguous outcome',
          },
        });
        return;
      }
      await reply(202, { outcome: 'scheduled', data: { replayed: false } });
      return;
    }
    let data: unknown;
    if (path === '/api/v1/status') {
      statusReads += 1;
      const snapshot = statusFixture();
      if (statusReads > 1)
        Object.assign(snapshot, {
          throughput: Object.fromEntries(
            Object.keys(snapshot.throughput).map((name) => [
              name,
              {
                state: 'known',
                per_second: '0.000000',
                reason: null,
                interval_ms: 5000,
              },
            ]),
          ),
        });
      data = snapshot;
    } else if (path === '/api/v1/config/polling')
      data = {
        revision: '1',
        capture_poll_ms: 1000,
        backfill_idle_ms: 1000,
        updated_at: now(),
        observed_at: now(),
        minimum_ms: 50,
        maximum_ms: 30000,
        recent_changes: [],
      };
    else if (path === '/api/v1/config') data = configFixture();
    else if (path.startsWith('/api/v1/backfills/')) data = backfillFixture();
    else if (path === '/api/v1/entities')
      data = {
        observed_at: now(),
        freshness: 'near_realtime',
        items: state.empty
          ? []
          : [
              {
                ...projection,
                entity_id: url.searchParams.has('cursor')
                  ? '9007199254740994'
                  : entityId,
                receiver_version: '8',
              },
            ],
        next_cursor:
          state.empty || url.searchParams.has('cursor')
            ? null
            : 'test-only-second-page',
      };
    else if (path.startsWith('/api/v1/entities/'))
      data = {
        observed_at: now(),
        freshness: 'realtime',
        projection,
        receiver_version: '8',
        source: {
          source_epoch: epoch,
          entity_id: entityId,
          entity_version: '9',
          is_deleted: false,
        },
        pipeline: [
          { event_id: eventId, entity_version: '8', state: 'satisfied' },
        ],
        convergence: 'degraded',
      };
    else if (path === '/api/v1/failures')
      data = {
        observed_at: now(),
        items: state.empty
          ? []
          : [
              {
                key: 'es-active:' + eventId,
                type: 'elasticsearch_active',
                event_id: eventId,
                attempt_id: '77777777-7777-4777-8777-777777777777',
                destination_id: '44444444-4444-4444-8444-444444444444',
                generation: '1',
                recorded_at: now(),
                classification: 'mapping',
                context: 'The receiver rejected the integer field.',
                state: 'dead_letter',
                replayable: true,
                replay_reason: 'current_terminal',
              },
              {
                key: 'quarantine:test-only',
                type: 'consumer_quarantine',
                quarantine_id: '88888888-8888-4888-8888-888888888888',
                recorded_at: now(),
                classification: 'invalid_message',
                context: 'Raw bytes withheld.',
                state: 'quarantined',
                replayable: false,
                replay_reason: 'quarantine_requires_separate_contract',
              },
            ],
        next_cursor: null,
      };
    else if (path.startsWith('/api/v1/events/'))
      data = {
        event_id: eventId,
        source_epoch: epoch,
        entity_id: entityId,
        entity_version: '8',
        kind: 'mutation',
        consumer: { state: 'processed' },
        deliveries: [
          { kind: 'elasticsearch', state: 'dead_letter' },
          { kind: 'rabbitmq', state: 'satisfied' },
        ],
      };
    else if (path === '/api/v1/simulations')
      data = {
        observed_at: now(),
        generator: 'explicit_commands_only',
        elasticsearch: { state: 'connected' },
        rabbitmq: { state: 'connected' },
        fixtures: [
          {
            name: 'fixture-01',
            entity_id: entityId,
            entity_version: '8',
            is_deleted: false,
          },
        ],
      };
    else {
      await reply(404, {
        error: { code: 'not_found', message: 'No fixture for this endpoint' },
      });
      return;
    }
    await reply(200, { data });
  });
  return state;
}
