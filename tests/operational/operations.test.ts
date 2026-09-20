import pg from 'pg';
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { operationalCases } from '../../scripts/required-operational-cases.ts';
import { metricDefinitions } from '../../src/operations/metrics.ts';
import { record, string } from '../../src/operations/validation.ts';
import { SourceCapture } from '../../src/source-capture.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { RabbitDelivery } from '../../src/rabbitmq/worker.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import {
  connections,
  checkStartupFailure,
  migrateAll,
  apiConfig,
  startApi,
  networkRoute,
  data,
  objects,
  crashAfterCommit,
  retained,
} from '../support/operations.ts';
import { bootstrap, recipe, pipelineConfig } from '../support/bootstrap.ts';
import { scan } from '../support/backfill.ts';
import { config } from '../support/staging.ts';
import { drain as captureDrain } from '../support/capture.ts';
import { esConfig, finish, worker, create } from '../support/es.ts';
import {
  drain as rabbitDrain,
  rawPublish,
  consumer,
  publisher,
  observer,
  rabbitLedger,
  amqpConfig,
  metadata,
  consumerConfig,
  topology,
} from '../support/rabbit.ts';
import { required, evidence } from '../support/db.ts';
const name = (id: string) => {
  const c = operationalCases.find((c) => c.id === id);
  assert.ok(c);
  return c.name;
};
let db: Awaited<ReturnType<typeof connections>>,
  api: Awaited<ReturnType<typeof startApi>>,
  route: Awaited<ReturnType<typeof networkRoute>>,
  cfg: ReturnType<typeof apiConfig>;
const epoch = required('SOURCE_EPOCH'),
  run = randomUUID();
let badEvent = '',
  badEntity = '',
  badAttempt = '',
  faultEvent = '';
const allLogs: string[] = [];
before(async () => {
  db = await connections();
  const b = bootstrap(),
    r = { ...recipe, count: '17', chunkSize: 8, seed: 'M6-operations' };
  await b.begin(r);
  for (const first of ['1', '9', '17']) await b.chunk(r.epoch, r.key, first);
  await b.seal(r.epoch, r.key);
  await b.activate(r.epoch, r.key, required('PIPELINE_ID'), pipelineConfig());
  const password = await migrateAll(db.s, db.p, db.c);
  route = await networkRoute();
  cfg = apiConfig(password);
  api = await startApi(cfg);
});
after(async () => {
  if (api) {
    allLogs.push(api.logs);
    await api.close();
  }
  if (route) await route.close();
  if (db) await db.close();
});
const get = async (path: string) => data(await api.request('/api/v1' + path));
const post = async (
  path: string,
  body: unknown,
  key = randomUUID(),
  status = 202,
) => data(await api.request('/api/v1' + path, 'POST', body, key), status);
const change = (
  fixture: string,
  operation = 'create',
  value = 10,
  key = randomUUID(),
  status = 202,
) =>
  post(
    '/simulations/source-change',
    { fixture, operation, value },
    key,
    status,
  );
const event = (r: Record<string, unknown>) =>
  `${epoch}:${string(r['entity_id'])}:${string(r['entity_version'])}`;
async function deliver() {
  await captureDrain();
  await rabbitDrain(db.p, db.c);
  const es = new EsTransport(esConfig());
  try {
    await finish(es, db.p);
  } finally {
    await es.close();
  }
}
async function refresh() {
  const es = new EsTransport(esConfig(true));
  try {
    await es.request('POST', `/${required('ES_INDEX')}/_refresh`);
  } finally {
    await es.close();
  }
}
async function current(id: string) {
  return record(
    (
      await db.p.query<Record<string, unknown>>(
        "SELECT row_to_json(d) value FROM pipeline.delivery_intents d WHERE event_id=$1 AND kind='elasticsearch'",
        [id],
      )
    ).rows[0]?.['value'],
  );
}
async function failure(id: string) {
  return (
    await db.p.query<{ value: string }>(
      'SELECT row_to_json(l)::text value FROM pipeline.es_dead_letters l WHERE event_id=$1 ORDER BY attempt_id',
      [id],
    )
  ).rows;
}
async function replayBody(id: string) {
  const d = await current(id);
  return {
    attempt_id: d['attempt_id'],
    destination_id: d['destination_id'],
    generation: '1',
    reason: 'controlled acceptance replay',
  };
}
void test(name('OP01'), async () => {
  await checkStartupFailure();
  const reply = await api.request('/api/v1/openapi.json');
  assert.equal(reply.status, 200);
  const schema = record(reply.value),
    paths = record(schema['paths']);
  const expected: Record<string, string[]> = {
    '/api/v1/status': ['get'],
    '/api/v1/backfills/{runId}': ['get'],
    '/api/v1/backfills': ['post'],
    '/api/v1/backfills/{runId}/pause': ['post'],
    '/api/v1/backfills/{runId}/resume': ['post'],
    '/api/v1/entities': ['get'],
    '/api/v1/entities/{sourceEpoch}/{entityId}': ['get'],
    '/api/v1/events/{eventId}': ['get'],
    '/api/v1/failures': ['get'],
    '/api/v1/config': ['get'],
    '/api/v1/failures/elasticsearch/{eventId}/replay': ['post'],
    '/api/v1/simulations/source-change': ['post'],
    '/api/v1/simulations/corrupt-record': ['post'],
    '/api/v1/simulations/network/elasticsearch': ['put'],
    '/api/v1/simulations/network/rabbitmq': ['put'],
    '/api/v1/simulations': ['get'],
    '/api/v1/openapi.json': ['get'],
    '/metrics': ['get'],
  };
  assert.deepEqual(Object.keys(paths).sort(), Object.keys(expected).sort());
  for (const [path, methods] of Object.entries(expected)) {
    assert.deepEqual(Object.keys(record(paths[path])).sort(), methods);
    for (const method of methods) {
      const op = record(record(paths[path])[method]),
        responses = record(op['responses']);
      assert.ok(responses[method === 'get' ? '200' : '202']);
      if (method !== 'get') {
        assert.ok(responses['401']);
        assert.ok(responses['409']);
      }
    }
  }
  for (const path of [
    '/entities?limit=0',
    '/entities?limit=101',
    '/entities?limit=1.2',
    '/entities?cursor=%%%',
    '/entities?include_deleted=maybe',
    '/entities?extra=1',
    '/entities/nope/1',
    `/entities/${epoch}/9223372036854775808`,
    '/events/nope',
    '/backfills/nope',
    '/failures?cursor=%%%',
  ]) {
    const r = await api.request('/api/v1' + path);
    assert.equal(r.status, 400, path);
    assert.deepEqual(record(r.value)['error'], {
      code: 'invalid_request',
      message: 'invalid request',
    });
    assert.equal(record(r.value)['request_id'], r.requestId);
  }
  assert.equal(
    (
      await api.request(
        '/api/v1/backfills',
        'POST',
        { run_id: run, ranges: 4 },
        run,
        'wrong',
      )
    ).status,
    401,
  );
  assert.equal(
    (
      await api.request('/api/v1/simulations/network/rabbitmq', 'PUT', {
        state: 'disconnected',
        url: 'http://elsewhere',
      })
    ).status,
    400,
  );
  for (const [body, expectedStatus] of [
    ['{', 400],
    [JSON.stringify({ padding: 'x'.repeat(9000) }), 413],
  ] as const) {
    const response = await fetch(api.url + '/api/v1/backfills', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
    });
    assert.equal(response.status, expectedStatus);
    const result: unknown = await response.json();
    assert.ok(record(result)['request_id']);
    assert.ok(record(record(result)['error'])['code']);
  }
  const configReply = await get('/config');
  assert.equal(configReply['quarantine_replay'], false);
  assert.ok(!JSON.stringify(configReply).includes(cfg.token));
  assert.ok(!JSON.stringify(configReply).includes(cfg.source.password));
  await writeFile(
    join(required('M1_ARTIFACT_DIR'), 'openapi.json'),
    JSON.stringify(schema, null, 2),
  );
  evidence('OP01', { paths: expected, config: configReply });
});
void test(name('OP02'), async () => {
  const idle = await get('/status'),
    dependencies = record(idle['dependencies']),
    source = record(dependencies['source']),
    facts = record(source['data']);
  assert.equal(source['freshness'], 'fresh');
  assert.equal(facts['pending'], '0');
  assert.equal(facts['oldest_pending_at'], null);
  await delay(25);
  assert.equal(
    record(
      record(record((await get('/status'))['dependencies'])['source'])['data'],
    )['oldest_pending_age_seconds'],
    null,
  );
  const first = await change('fixture-01'),
    second = await change('fixture-02'),
    third = await change('fixture-03');
  const capture = new SourceCapture(
    {
      ...config('reader', 'operational-capture'),
      user: 'source_capture',
      password: required('SOURCE_CAPTURE_PASSWORD'),
    },
    { sourceEpoch: epoch, pipelineId: required('PIPELINE_ID') },
  );
  const claims = await capture.claim(randomUUID(), 3, 30000);
  assert.equal(claims.length, 3);
  const a = claims[0],
    b = claims[1];
  assert.ok(a && b);
  await capture.defer(a, 30000);
  await capture.block(b);
  const state = record(
    record(record((await get('/status'))['dependencies'])['source'])['data'],
  );
  const counts = record(state['counts']);
  assert.equal(counts['pending_delayed'], '1');
  assert.equal(counts['leased_current'], '1');
  assert.equal(counts['blocked'], '1');
  assert.equal(state['pending'], '3');
  assert.ok(state['oldest_pending_at']);
  // Privileged fixture releases only the test's two scheduling claims.
  // The blocked row stays retained; production exposes no unblock API.
  await db.s.query<Record<string, unknown>>(
    "UPDATE source.capture_work SET state='pending',owner_id=NULL,lease_until=NULL,reason=NULL,next_eligible_at=clock_timestamp() WHERE state IN ('leased','pending')",
  );
  // Reject actual new source connections while retaining this fixture-control session.
  // Container stop/start can allocate a different ephemeral host port in Docker 29.
  const outageControl = new pg.Client({
    host: '127.0.0.1',
    port: Number(required('M1_PORT')),
    database: 'postgres',
    user: 'm1_admin',
    password: required('M1_ADMIN_PASSWORD'),
  });
  await outageControl.connect();
  await outageControl.query('ALTER DATABASE source_m1 ALLOW_CONNECTIONS false');
  try {
    const r = await get('/status');
    const o = record(record(r['dependencies'])['source']);
    assert.equal(o['freshness'], 'unavailable');
    assert.equal(o['data'], null);
    const m = await api.request('/metrics');
    assert.match(m.text, /pipeline_source_reachable 0/);
    assert.doesNotMatch(m.text, /^pipeline_source_pending [0-9]/m);
  } finally {
    try {
      await outageControl.query(
        'ALTER DATABASE source_m1 ALLOW_CONNECTIONS true',
      );
    } finally {
      await outageControl.end();
    }
  }
  // Fixture block is explicit and retained; use the unaffected command identity for diagnostics.
  faultEvent = event(first);
  evidence('OP02', {
    idle,
    states: state,
    created: [first, second, third],
    unknown: 'source database rejects actual new connections',
  });
});
void test(name('OP03'), async () => {
  const body = { run_id: run, ranges: 4 };
  await crashAfterCommit(cfg, 'backfill_start', '/api/v1/backfills', body, run);
  await post('/backfills', body, run, 200);
  const status = await get(`/backfills/${run}`);
  assert.equal(status['required_sealed'], false);
  assert.equal(status['denominator'], null);
  const pauseKey = randomUUID();
  await crashAfterCommit(
    cfg,
    'backfill_pause',
    `/api/v1/backfills/${run}/pause`,
    {},
    pauseKey,
  );
  assert.equal((await get(`/backfills/${run}`))['desired_paused'], true);
  await post(`/backfills/${run}/pause`, {}, pauseKey, 200);
  await post(`/backfills/${run}/resume`, {});
  await post(`/backfills/${run}/pause`, {}, pauseKey, 200);
  assert.equal((await get(`/backfills/${run}`))['desired_paused'], false);
  const scanner = scan();
  for (let i = 0; i < 40; i++) {
    const state = await scanner.status(run);
    if (state.phase === 'draining') break;
    await scanner.once(run);
  }
  assert.equal((await scanner.status(run)).phase, 'draining');
  await deliver();
  await scanner.once(run);
  assert.equal((await scanner.status(run)).phase, 'complete');
  const original = await retained(db.p, [
    'pipeline.backfill_runs',
    'pipeline.backfill_ranges',
  ]);
  await post('/backfills', body, run, 200);
  assert.deepEqual(
    await retained(db.p, [
      'pipeline.backfill_runs',
      'pipeline.backfill_ranges',
    ]),
    original,
  );
  evidence('OP03', { run, status: await get(`/backfills/${run}`) });
});
void test(name('OP04'), async () => {
  await change('fixture-04');
  await change('fixture-05');
  await change('fixture-05', 'delete', 0);
  await deliver();
  await refresh();
  const first = await get('/entities?limit=1&q=fixture'),
    items = objects(first['items']);
  assert.equal(items.length, 1);
  assert.equal(typeof items[0]?.['entity_id'], 'string');
  assert.equal(typeof items[0]?.['receiver_version'], 'string');
  assert.equal(
    typeof record(items[0]?.['search_fields'])['loyalty_points'],
    'number',
  );
  const found: string[] = [];
  let next: string | null = null;
  do {
    const r = await get(
      '/entities?limit=2&q=fixture' + (next ? `&cursor=${next}` : ''),
    );
    for (const row of objects(r['items'])) {
      assert.equal(row['is_deleted'], false);
      found.push(string(row['entity_id']));
    }
    next = r['next_cursor'] === null ? null : string(r['next_cursor'], 2048);
  } while (next);
  assert.equal(found.length, new Set(found).size);
  const inclusive = await get('/entities?limit=100&include_deleted=true');
  assert.ok(objects(inclusive['items']).some((r) => r['is_deleted'] === true));
  const doc = await get(`/entities/${epoch}/${found[0]}`);
  assert.equal(doc['freshness'], 'realtime');
  assert.equal(typeof doc['receiver_version'], 'string');
  assert.equal(record(doc['projection'])['canonical_body_json'], undefined);
  const newer = await change('fixture-04', 'update', 71);
  const stale = await get(`/entities/${epoch}/${string(newer['entity_id'])}`);
  assert.equal(stale['convergence'], 'degraded');
  assert.notEqual(stale['receiver_version'], newer['entity_version']);
  evidence('OP04', { first, inclusive, detail: doc, expectedDegraded: stale });
});
void test(name('OP05'), async () => {
  const result = await get(`/events/${faultEvent}`);
  const direct = (
    await db.p.query<{ hash: string }>(
      'SELECT content_sha256 hash FROM pipeline.events WHERE event_id=$1',
      [faultEvent],
    )
  ).rows[0];
  assert.equal(result['content_sha256'], direct?.hash);
  assert.ok(objects(result['es_attempts']).length <= 8);
  assert.ok(objects(result['rabbit_attempts']).length <= 8);
  assert.equal(record(result['consumer'])['state'], 'processed');
  assert.equal(
    record(result['source_capture'])['acknowledged_hash'],
    direct?.hash,
  );
  assert.ok(!JSON.stringify(result).includes('payload_json'));
  assert.equal(
    (await api.request(`/api/v1/events/${epoch}:9223372036854775807:1`)).status,
    404,
  );
  evidence('OP05', result);
});
function metricMap(text: string) {
  const m = new Map<string, number>();
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const i = line.lastIndexOf(' ');
    assert.ok(i > 0);
    const key = line.slice(0, i),
      value = Number(line.slice(i + 1));
    assert.ok(Number.isFinite(value));
    assert.ok(!m.has(key));
    m.set(key, value);
  }
  return m;
}
void test(name('OP06'), async () => {
  const before = (await api.request('/metrics')).text;
  for (const [name, type, help] of metricDefinitions) {
    assert.ok(before.includes(`# TYPE ${name} ${type}\n`));
    assert.ok(before.includes(`# HELP ${name} ${help}\n`));
  }
  assert.doesNotMatch(
    before,
    /(?:event_id|entity_id|request_id|run_id|message)=/,
  );
  const counters = (text: string) =>
    new Map(
      [...metricMap(text)].filter(([key]) =>
        key.split('{')[0]?.endsWith('_total'),
      ),
    );
  const old = counters(before);
  allLogs.push(api.logs);
  await api.close();
  api = await startApi(cfg);
  const after = (await api.request('/metrics')).text;
  assert.deepEqual(counters(after), old);
  await change('fixture-06');
  const pending = metricMap((await api.request('/metrics')).text).get(
    'pipeline_source_pending',
  );
  await deliver();
  const settled = metricMap((await api.request('/metrics')).text);
  assert.ok(Number(pending) > Number(settled.get('pipeline_source_pending')));
  const newer = counters((await api.request('/metrics')).text);
  for (const [k, v] of old) assert.ok(Number(newer.get(k)) >= v, k);
  const direct = (
    await db.s.query<{ oldest: string }>(
      "SELECT extract(epoch FROM min(o.recorded_at))::text oldest FROM source.outbox o JOIN source.capture_work w USING(source_epoch,entity_id,entity_version) WHERE w.state<>'acknowledged'",
    )
  ).rows[0];
  assert.equal(
    settled.get('pipeline_source_oldest_pending_timestamp_seconds'),
    Number(direct?.oldest),
  );
  await writeFile(join(required('M1_ARTIFACT_DIR'), 'metrics.prom'), after);
  evidence('OP06', { before, after, postDelivery: [...settled] });
});
void test(name('OP07'), async () => {
  const fresh = await change('fixture-07'),
    id = event(fresh);
  await captureDrain();
  await rabbitDrain(db.p, db.c);
  const es = new EsTransport(esConfig(true)),
    runtime = new EsTransport(esConfig());
  try {
    await es.request(
      'PUT',
      '/_ingest/pipeline/kit-m6-rejection',
      JSON.stringify({
        processors: [
          {
            set: {
              field: 'search_fields.loyalty_points',
              value: 'not-a-number',
            },
          },
        ],
      }),
    );
    await es.request(
      'PUT',
      `/${required('ES_INDEX')}/_settings`,
      JSON.stringify({ 'index.default_pipeline': 'kit-m6-rejection' }),
    );
    await finish(runtime, db.p);
    assert.equal((await current(id))['state'], 'dead_letter');
    const old = await failure(id);
    assert.equal(old.length, 1);
    const canonical = await db.p.query<Record<string, unknown>>(
      'SELECT body_bytes,content_sha256 FROM pipeline.events WHERE event_id=$1',
      [id],
    );
    await es.request(
      'PUT',
      `/${required('ES_INDEX')}/_settings`,
      JSON.stringify({ 'index.default_pipeline': '_none' }),
    );
    await post(`/failures/elasticsearch/${id}/replay`, await replayBody(id));
    await finish(runtime, db.p);
    assert.equal((await current(id))['state'], 'satisfied');
    assert.notEqual(
      (await current(id))['attempt_id'],
      record(JSON.parse(old[0]?.value ?? '{}'))['attempt_id'],
    );
    assert.deepEqual(await failure(id), old);
    assert.deepEqual(
      (
        await db.p.query<Record<string, unknown>>(
          'SELECT body_bytes,content_sha256 FROM pipeline.events WHERE event_id=$1',
          [id],
        )
      ).rows,
      canonical.rows,
    );
    evidence('OP07', {
      event: id,
      old,
      current: await current(id),
      source: canonical.rows.map((r) => ({ hash: r['content_sha256'] })),
    });
  } finally {
    await es.request(
      'PUT',
      `/${required('ES_INDEX')}/_settings`,
      JSON.stringify({ 'index.default_pipeline': '_none' }),
    );
    await es.request('DELETE', '/_ingest/pipeline/kit-m6-rejection');
    await runtime.close();
    await es.close();
  }
});
void test(name('OP08'), async () => {
  const created = await change('fixture-08');
  badEntity = string(created['entity_id']);
  const corrupted = await post('/simulations/corrupt-record', {
    fixture: 'fixture-08',
  });
  badEvent = event(corrupted);
  await deliver();
  const before = await retained(db.p, [
    'pipeline.events',
    'pipeline.consumer_observations',
  ]);
  const source = await retained(db.s, ['source.outbox', 'source.capture_work']);
  const rabbit = (
    await db.p.query<Record<string, unknown>>(
      "SELECT row_to_json(d)::text FROM pipeline.delivery_intents d WHERE kind='rabbitmq'",
    )
  ).rows;
  const old = await failure(badEvent);
  assert.equal(old.length, 1);
  const body = await replayBody(badEvent);
  await post(`/failures/elasticsearch/${badEvent}/replay`, body);
  await deliver();
  const failures = await failure(badEvent);
  assert.equal(failures.length, 2);
  assert.ok(failures.some((f) => f.value === old[0]?.value));
  const d = await current(badEvent);
  assert.equal(d['state'], 'dead_letter');
  badAttempt = string(d['attempt_id']);
  assert.notEqual(badAttempt, body.attempt_id);
  assert.deepEqual(
    await retained(db.p, ['pipeline.events', 'pipeline.consumer_observations']),
    before,
  );
  assert.deepEqual(
    await retained(db.s, ['source.outbox', 'source.capture_work']),
    source,
  );
  assert.deepEqual(
    (
      await db.p.query<Record<string, unknown>>(
        "SELECT row_to_json(d)::text FROM pipeline.delivery_intents d WHERE kind='rabbitmq'",
      )
    ).rows,
    rabbit,
  );
  evidence('OP08', { badEvent, failures, current: d });
});
void test(name('OP09'), async () => {
  const key = randomUUID(),
    body = await replayBody(badEvent);
  await crashAfterCommit(
    cfg,
    'es_replay',
    `/api/v1/failures/elasticsearch/${badEvent}/replay`,
    body,
    key,
  );
  await post(`/failures/elasticsearch/${badEvent}/replay`, body, key, 200);
  const before = await current(badEvent);
  await post(`/failures/elasticsearch/${badEvent}/replay`, body, key, 200);
  assert.deepEqual(await current(badEvent), before);
  await deliver();
  const competing = await replayBody(badEvent),
    keys = [randomUUID(), randomUUID()];
  const results = await Promise.all(
    keys.map((k) =>
      api.request(
        `/api/v1/failures/elasticsearch/${badEvent}/replay`,
        'POST',
        competing,
        k,
      ),
    ),
  );
  assert.deepEqual(results.map((r) => r.status).sort(), [202, 409]);
  await deliver();
  // Satisfied event from receiver-repair case must reject every stale/new scheduling request.
  const satisfied = await get('/failures?limit=100');
  const row = objects(satisfied['items']).find(
    (r) => r['type'] === 'elasticsearch_history' && r['state'] === 'satisfied',
  );
  assert.ok(row);
  assert.equal(
    (
      await api.request(
        `/api/v1/failures/elasticsearch/${string(row['event_id'])}/replay`,
        'POST',
        {
          attempt_id: row['attempt_id'],
          destination_id: row['destination_id'],
          generation: row['generation'],
          reason: 'stale',
        },
      )
    ).status,
    409,
  );
  evidence('OP09', { key, competing, results: results.map((r) => r.value) });
});
void test(name('OP10'), async () => {
  const old = await failure(badEvent);
  const corrected = await change('fixture-08', 'update', 42);
  assert.ok(
    BigInt(string(corrected['entity_version'])) >
      BigInt(badEvent.split(':')[2] ?? '0'),
  );
  await deliver();
  const detail = await get(`/entities/${epoch}/${badEntity}`);
  assert.equal(detail['receiver_version'], corrected['entity_version']);
  assert.deepEqual(await failure(badEvent), old);
  assert.equal((await current(badEvent))['state'], 'dead_letter');
  evidence('OP10', { oldEvent: badEvent, newEvent: event(corrected), detail });
});
void test(name('OP11'), async () => {
  await rawPublish(Buffer.from('M6-invalid-json-private-body'), 'not-an-event');
  const outcome = await consumer().once();
  assert.equal(outcome.quarantined.length, 1);
  const failures = objects((await get('/failures?limit=100'))['items']);
  const q = failures.find((r) => r['type'] === 'consumer_quarantine');
  assert.ok(q);
  assert.equal(q['replayable'], false);
  assert.ok(!JSON.stringify(q).includes('private-body'));
  assert.equal(
    (
      await api.request(
        `/api/v1/failures/consumer/${String(q['quarantine_id'])}/replay`,
        'POST',
        {},
      )
    ).status,
    404,
  );
  const before = (
    await db.c.query<Record<string, unknown>>(
      'SELECT count(*) FROM consumer.quarantine',
    )
  ).rows;
  await change('fixture-09');
  await captureDrain();
  await publisher().once();
  const failedConsumer = new Consumer(
    new ConsumerDatabase(consumerConfig()),
    amqpConfig('consumer'),
    metadata(),
    topology(),
  );
  await db.p.query<Record<string, unknown>>(
    'ALTER DATABASE consumer_m4 ALLOW_CONNECTIONS false',
  );
  try {
    await assert.rejects(failedConsumer.once());
  } finally {
    await db.p.query<Record<string, unknown>>(
      'ALTER DATABASE consumer_m4 ALLOW_CONNECTIONS true',
    );
  }
  assert.deepEqual(
    (
      await db.c.query<Record<string, unknown>>(
        'SELECT count(*) FROM consumer.quarantine',
      )
    ).rows,
    before,
  );
  await deliver();
  evidence('OP11', { quarantine: q, outcome });
});
void test(name('OP12'), async () => {
  const key = randomUUID(),
    body = { fixture: 'fixture-10', operation: 'create', value: 44 };
  const committed = await crashAfterCommit(
    cfg,
    'source_change',
    '/api/v1/simulations/source-change',
    body,
    key,
  );
  const replayed = await post('/simulations/source-change', body, key, 200);
  assert.equal(committed['entity_id'], replayed['entity_id']);
  for (const operation of ['update', 'delete', 'restore'])
    await change('fixture-10', operation, 55);
  await deliver();
  const effects = (
    await db.c.query<{ n: string }>(
      'SELECT count(*)::text n FROM consumer.mutation_effects WHERE event_id LIKE $1',
      [`${epoch}:${string(committed['entity_id'])}:%`],
    )
  ).rows[0];
  assert.equal(effects?.n, '4');
  const oldBad = (
    await db.c.query<{ n: string }>(
      'SELECT count(*)::text n FROM consumer.mutation_effects WHERE event_id=$1',
      [badEvent],
    )
  ).rows[0];
  assert.equal(oldBad?.n, '1');
  assert.equal((await current(badEvent))['state'], 'dead_letter');
  assert.equal(
    (
      await db.p.query<Record<string, unknown>>(
        "SELECT state FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='rabbitmq'",
        [badEvent],
      )
    ).rows[0]?.['state'],
    'satisfied',
  );
  evidence('OP12', {
    committed,
    replayed,
    effects,
    badEvent,
    originalTerminalAttempt: badAttempt,
  });
});
void test(name('OP13'), async () => {
  const fixture = await change('fixture-11');
  await captureDrain();
  for (const sink of ['elasticsearch', 'rabbitmq'])
    data(
      await api.request(`/api/v1/simulations/network/${sink}`, 'PUT', {
        state: 'disconnected',
      }),
      202,
    );
  const state = await get('/simulations');
  assert.equal(record(state['elasticsearch'])['state'], 'disconnected');
  assert.equal(record(state['rabbitmq'])['state'], 'disconnected');
  const es = new EsTransport(esConfig(false, true));
  try {
    await worker(es).once();
  } finally {
    await es.close();
  }
  const delivery = new RabbitDelivery(
    rabbitLedger(),
    new Publisher(
      { ...amqpConfig(), host: route.host, port: route.port },
      metadata(),
    ),
  );
  await delivery.once();
  const rows = (
    await db.p.query<Record<string, unknown>>(
      'SELECT kind,state,error_class FROM pipeline.delivery_intents WHERE event_id=$1',
      [event(fixture)],
    )
  ).rows;
  assert.ok(rows.every((r) => r['state'] !== 'satisfied'));
  assert.ok(rows.every((r) => r['state'] !== 'dead_letter'));
  const status = await get('/status');
  assert.equal(status['health'], 'unavailable');
  assert.equal(
    (
      await api.request(
        `/api/v1/entities/${epoch}/${string(fixture['entity_id'])}`,
      )
    ).status,
    503,
  );
  for (const sink of ['elasticsearch', 'rabbitmq'])
    data(
      await api.request(`/api/v1/simulations/network/${sink}`, 'PUT', {
        state: 'connected',
      }),
      202,
    );
  const recoveredEs = new EsTransport(esConfig(false, true));
  try {
    await finish(recoveredEs, db.p);
  } finally {
    await recoveredEs.close();
  }
  for (let i = 0; i < 50; i++) {
    await delivery.once();
    await consumer().once();
    await observer().once();
    const state = (
      await db.p.query<{ state: string }>(
        "SELECT state FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='rabbitmq'",
        [event(fixture)],
      )
    ).rows[0]?.state;
    if (state === 'satisfied') break;
    await delay(100);
  }
  assert.equal(
    (
      await db.p.query<{ state: string }>(
        "SELECT state FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='rabbitmq'",
        [event(fixture)],
      )
    ).rows[0]?.state,
    'satisfied',
  );
  assert.equal((await current(event(fixture)))['state'], 'satisfied');
  evidence('OP13', {
    disconnected: state,
    status,
    during: rows,
    reconnected: await get('/simulations'),
  });
});
void test(name('OP14'), async () => {
  const key = randomUUID();
  await post(`/backfills/${run}/resume`, {}, key);
  await delay(30);
  const lines = (allLogs.join('\n') + '\n' + api.logs)
    .split('\n')
    .filter(Boolean)
    .map((l) => record(JSON.parse(l)));
  const matched = lines.filter((l) => l['request_id'] === key);
  assert.equal(matched.length, 1);
  assert.equal(matched[0]?.['outcome'], 'scheduled');
  const audit = (
    await db.p.query<Record<string, unknown>>(
      'SELECT request_id,request_correlation,operation,result FROM pipeline.operator_receipts WHERE request_id=$1',
      [key],
    )
  ).rows;
  assert.equal(audit.length, 1);
  assert.equal(audit[0]?.['request_correlation'], key);
  for (const l of lines) {
    for (const k of [
      'timestamp',
      'level',
      'service',
      'operation',
      'request_id',
      'outcome',
      'duration_ms',
      'error_class',
    ])
      assert.ok(k in l);
    assert.ok(JSON.stringify(l).length < 1024);
  }
  const text = JSON.stringify(lines);
  for (const secret of [
    cfg.token,
    cfg.source.password,
    cfg.es.password,
    'M6-invalid-json-private-body',
    'payload_json',
    'canonical_body_json',
  ])
    assert.ok(!text.includes(secret));
  await writeFile(
    join(required('M1_ARTIFACT_DIR'), 'api-logs.jsonl'),
    lines.map((l) => JSON.stringify(l)).join('\n') + '\n',
  );
  evidence('OP14', { matched, audit });
});
void test(name('OP15'), async () => {
  const before = await retained(db.p, [
    'pipeline.replay_requests',
    'pipeline.replay_items',
    'pipeline.backfill_runs',
    'pipeline.attempt_totals',
  ]);
  allLogs.push(api.logs);
  const pid = api.child.pid;
  await api.close('SIGKILL');
  await changeWithSource();
  await captureDrain();
  await rabbitDrain(db.p, db.c);
  const es = new EsTransport(esConfig());
  try {
    await finish(es, db.p);
  } finally {
    await es.close();
  }
  const afterWorker = await retained(db.p, [
    'pipeline.replay_requests',
    'pipeline.replay_items',
    'pipeline.backfill_runs',
    'pipeline.attempt_totals',
  ]);
  api = await startApi(cfg);
  assert.notEqual(api.child.pid, pid);
  await get('/status');
  await api.request('/metrics');
  assert.deepEqual(
    await retained(db.p, [
      'pipeline.replay_requests',
      'pipeline.replay_items',
      'pipeline.backfill_runs',
      'pipeline.attempt_totals',
    ]),
    afterWorker,
  );
  assert.deepEqual(
    afterWorker['pipeline.replay_requests'],
    before['pipeline.replay_requests'],
  );
  evidence('OP15', {
    oldPid: pid,
    newPid: api.child.pid,
    workersIndependent: true,
  });
});
async function changeWithSource() {
  return create(
    '{"name":"API down independent source command","loyalty_points":7}',
  );
}
void test(name('OP16'), async () => {
  const all = objects((await get('/failures?limit=100'))['items']);
  let cursor: string | null = null;
  const seen: Record<string, unknown>[] = [];
  do {
    const p = await get(
      '/failures?limit=2' + (cursor ? `&cursor=${cursor}` : ''),
    );
    const items = objects(p['items']);
    assert.ok(items.length <= 2);
    seen.push(...items);
    cursor = p['next_cursor'] === null ? null : string(p['next_cursor'], 2048);
  } while (cursor);
  assert.deepEqual(seen, all);
  assert.equal(new Set(seen.map((r) => r['key'])).size, seen.length);
  for (const type of [
    'elasticsearch_history',
    'elasticsearch_active',
    'consumer_quarantine',
    'capture_block',
  ])
    assert.ok(seen.some((r) => r['type'] === type));
  for (const row of seen) assert.ok(string(row['context'], 1024).length <= 256);
  await create(
    JSON.stringify({
      name: 'oversized backfill boundary',
      padding: 'x'.repeat(70000),
    }),
  );
  const blockedRun = randomUUID();
  await post('/backfills', { run_id: blockedRun, ranges: 1 }, blockedRun);
  const scanner = scan();
  for (let i = 0; i < 40; i++) {
    if ((await scanner.status(blockedRun)).blocked) break;
    await scanner.once(blockedRun);
  }
  assert.equal((await scanner.status(blockedRun)).blocked, true);
  const blocked = objects((await get('/failures?limit=100'))['items']);
  assert.ok(blocked.some((r) => r['type'] === 'backfill_block'));
  evidence('OP16', { failures: seen });
});
void test(name('OP18'), async () => {
  const status = await get('/status'),
    metrics = (await api.request('/metrics')).text;
  assert.equal(record(status['backfill'])['blocked'], true);
  assert.ok(metrics.includes('pipeline_delivery_settled_total'));
  assert.ok(metrics.includes('pipeline_consumer_effects_total'));
  assert.ok(metrics.includes('pipeline_source_pending'));
  assert.ok(metrics.includes('pipeline_dlq_open'));
  assert.ok(metrics.includes('pipeline_dependency_health'));
  assert.equal(status['health'], 'degraded');
  assert.ok(
    objects(
      record(record(status['dependencies'])['pipeline'])['data'] === null
        ? []
        : record(record(record(status['dependencies'])['pipeline'])['data'])[
            'deliveries'
          ],
    ).some((r) => r['state'] === 'dead_letter'),
  );
  const samplesBefore = metricMap(metrics),
    at = performance.now();
  await create('{"name":"bounded throughput observation","loyalty_points":1}');
  await deliver();
  const samplesAfter = metricMap((await api.request('/metrics')).text),
    seconds = (performance.now() - at) / 1000;
  const effectsDelta =
    Number(samplesAfter.get('pipeline_consumer_effects_total')) -
    Number(samplesBefore.get('pipeline_consumer_effects_total'));
  assert.equal(effectsDelta, 1);
  const successful = (m: Map<string, number>) =>
    [...m]
      .filter(
        ([k]) =>
          k.startsWith('pipeline_delivery_settled_total') &&
          !k.includes('dead_letter'),
      )
      .reduce((n, [, v]) => n + v, 0);
  const settlementsDelta = successful(samplesAfter) - successful(samplesBefore);
  assert.ok(settlementsDelta >= 2);
  evidence('OP18', {
    status,
    metrics,
    throughput: {
      seconds,
      effectsDelta,
      effectsPerSecond: effectsDelta / seconds,
      settlementsDelta,
      successfulSettlementsPerSecond: settlementsDelta / seconds,
    },
    scope: 'backend G5 preparation; no UI or full G5 PASS',
  });
});
