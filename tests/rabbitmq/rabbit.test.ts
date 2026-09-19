import { canonicalEvent } from '../../src/envelope.ts';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pipeline } from '../support/staging.ts';
import {
  reconcile,
  syntheticBaselines,
  checkConsumer,
  type ConsumerSnapshot,
} from '../support/rabbit-oracle.ts';
import { waitFor } from '../../scripts/support.ts';
import pg from 'pg';
import { rabbitConfig } from '../support/rabbit.ts';
import { queueArguments } from '../../src/rabbitmq/metadata.ts';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { proxy, docker } from '../support/rabbit-network.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { RabbitDelivery } from '../../src/rabbitmq/worker.ts';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import {
  rabbitLedger,
  observer,
  consumerConfig,
  rawPublish,
  ledgerWire,
} from '../support/rabbit.ts';
import { launchRabbit } from '../support/rabbit-process.ts';
import { databaseWaitFor, required } from '../support/db.ts';
import { drain as captureDrain } from '../support/capture.ts';
import { publisher, consumer, intent, effects } from '../support/rabbit.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { esConfig, finish as finishEs, remote } from '../support/es.ts';
import {
  declareMappingRejection,
  type ExpectedRejection,
} from '../support/es-expectations.ts';
const rejections: ExpectedRejection[] = [];
import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import { rabbitCases } from '../../scripts/required-rabbit-cases.ts';
import {
  field,
  record,
  validateTopology,
} from '../../src/rabbitmq/metadata.ts';
import { AmqpSession } from '../../src/rabbitmq/session.ts';
import {
  topology,
  metadata,
  amqpConfig,
  setup,
  mutation,
  drain,
  expected,
} from '../support/rabbit.ts';
import { evidence } from '../support/db.ts';
function name(id: string) {
  const c = rabbitCases.find((c) => c.id === id);
  assert.ok(c);
  return c.name;
}
await test(name('MQ01'), async (context) => {
  const { p: pipelineDb, c: consumerDb } = await setup(context);
  const t = topology();
  await validateTopology(metadata(), t);
  const pub = new AmqpSession(),
    con = new AmqpSession();
  try {
    await pub.open(amqpConfig());
    await con.open(amqpConfig('consumer'));
    const p = await pub.channel(true),
      c = await con.channel(false);
    await p.checkExchange(t.exchange);
    const q = await c.checkQueue(t.queue);
    assert.equal(q.messageCount, 0);
    // This intentionally empty queue demonstrates RabbitMQ's residual read/purge ACL.
    assert.equal((await c.purgeQueue(t.queue)).messageCount, 0);
    await assert.rejects(p.assertQueue('forbidden-runtime-topology'));
    const permissions = await metadata(true).request(
      'GET',
      `/api/vhosts/${encodeURIComponent(t.vhost)}/permissions`,
    );
    const config = await docker([
      'exec',
      `${required('RABBIT_PROJECT')}-rabbitmq-1`,
      'rabbitmqctl',
      '-q',
      'eval',
      'application:get_env(rabbit, max_message_size).',
    ]);
    assert.match(config, /131072/);
    const denied = [];
    for (const [connection, statements] of [
      [
        rabbitConfig(),
        [
          'UPDATE pipeline.events SET body_bytes=body_bytes',
          'DELETE FROM pipeline.delivery_intents',
          "UPDATE pipeline.es_target SET mode='ready'",
          'SET ROLE pipeline_owner',
          'SELECT * FROM pipeline.consumer_observations',
        ],
      ],
      [
        consumerConfig(),
        [
          'DELETE FROM consumer.processed_events',
          'UPDATE consumer.entity_totals SET units=0',
          'SET ROLE consumer_owner',
          'DROP SCHEMA consumer CASCADE',
        ],
      ],
    ] as const) {
      const db = new pg.Client(connection);
      await db.connect();
      try {
        for (const sql of statements) {
          await assert.rejects(db.query(sql), { code: '42501' });
          denied.push(sql);
        }
      } finally {
        await db.end();
      }
    }
    const unrelated = new pg.Client({
      ...consumerConfig(),
      database: 'pipeline_m2b',
    });
    await assert.rejects(unrelated.connect(), { code: '42501' });
    await unrelated.end();
    await assert.rejects(
      validateTopology(metadata(), { ...t, vhost: `kit-${randomUUID()}` }),
      /404/,
    );
    const v = encodeURIComponent(t.vhost);
    await metadata(true).request('DELETE', `/api/queues/${v}/${t.queue}`);
    try {
      await assert.rejects(consumer().once(), /404/);
      await assert.rejects(
        metadata(true).request('GET', `/api/queues/${v}/${t.queue}`),
        /404/,
      );
    } finally {
      await metadata(true).request('PUT', `/api/queues/${v}/${t.queue}`, {
        durable: true,
        auto_delete: false,
        arguments: queueArguments,
      });
      await metadata(true).request(
        'POST',
        `/api/bindings/${v}/e/${t.exchange}/q/${t.queue}`,
        { routing_key: t.routingKey, arguments: {} },
      );
    }
    const observerDenied = await metadata()
      .request('DELETE', `/api/queues/${v}/${t.queue}`)
      .then(
        () => false,
        () => true,
      );
    assert.equal(observerDenied, true);
    const functions = (
      await pipelineDb.query<Record<string, unknown>>(
        "SELECT proname,proconfig FROM pg_proc JOIN pg_namespace n ON pronamespace=n.oid WHERE n.nspname='pipeline' AND prosecdef AND proname LIKE 'rabbit_%' ORDER BY proname",
      )
    ).rows;
    for (const f of functions)
      assert.deepEqual(f['proconfig'], ['search_path=pg_catalog, pg_temp']);
    assert.equal(
      (
        await consumerDb.query<{ n: string }>(
          'SELECT count(*)::text n FROM consumer.processed_events',
        )
      ).rows[0]?.n,
      '0',
    );
    evidence('MQ01', {
      topology: t,
      passiveValidated: true,
      permissions,
      denied,
      configuredMaximum: config,
      functions,
      missingQueueDetectedWithoutRecreation: true,
      configureDenied: true,
      emptyQueueReadPurgeResidual: true,
    });
  } finally {
    await pub.close();
    await con.close();
  }
});
await test(name('MQ02'), async (t) => {
  const { s, p, c } = await setup(t);
  const f = await mutation(
    s,
    '{"name":"M4 exact","country":"GE","loyalty_points":7,"precise":9007199254740993,"decimal":1.12345678901234567890123456789}',
  );
  await drain(p, c);
  const e = await expected(s, f.id, '1');
  const inbox = (
    await c.query<{ body: string; hash: string }>(
      "SELECT convert_from(body_bytes,'UTF8') body,content_sha256 hash FROM consumer.processed_events WHERE event_id=$1",
      [f.eventId],
    )
  ).rows;
  assert.deepEqual(inbox, [{ body: e.body, hash: e.hash }]);
  assert.equal(
    (
      await c.query<{ n: string }>(
        'SELECT count(*)::text n FROM consumer.mutation_effects WHERE event_id=$1',
        [f.eventId],
      )
    ).rows[0]?.n,
    '1',
  );
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  await finishEs(es, p);
  const before = await remote(es, `${required('SOURCE_EPOCH')}:${f.id}`);
  const bad = await mutation(
    s,
    '{"name":"M4 invalid ES valid consumer","country":"GE","loyalty_points":"not-a-number"}',
    f.id,
    'update',
  );
  rejections.push(
    await declareMappingRejection(
      s,
      p,
      { ...bad, documentId: `${required('SOURCE_EPOCH')}:${bad.id}` },
      'MQ02',
    ),
  );
  await drain(p, c);
  await finishEs(es, p);
  assert.deepEqual(
    await remote(es, `${required('SOURCE_EPOCH')}:${f.id}`),
    before,
  );
  assert.equal((await effects(c, bad.eventId)).length, 1);
  assert.equal(
    (
      await p.query<{ state: string }>(
        "SELECT state FROM pipeline.delivery_intents WHERE kind='elasticsearch' AND event_id=$1",
        [bad.eventId],
      )
    ).rows[0]?.state,
    'dead_letter',
  );
  evidence('MQ02', {
    eventId: f.eventId,
    ...e,
    inbox,
    rejected: bad.eventId,
    retainedReceiver: before,
    consumerAcceptedBadSearchField: await effects(c, bad.eventId),
  });
});
const publisherBoundaries = [
  'rabbit.after_claim_commit.before_publish',
  'rabbit.after_confirm.before_local_commit',
  'rabbit.after_local_commit.before_success',
];
const consumerBoundaries = [
  'consumer.before_db_commit',
  'consumer.after_db_commit.before_ack',
];
await test(name('MQ03'), async (t) => {
  const { s, p, c } = await setup(t);
  for (const boundary of publisherBoundaries) {
    const f = await mutation(
      s,
      JSON.stringify({ name: boundary, loyalty_points: 1 }),
    );
    await captureDrain();
    const child = launchRabbit(
      t,
      `MQ03-${boundary}`,
      boundary,
      'publisher',
      1500,
    );
    const reached = await child.barrier();
    const before = await intent(p, f.eventId);
    assert.equal(
      before['state'],
      boundary === publisherBoundaries[2] ? 'satisfied' : 'leased',
    );
    assert.equal((await effects(c, f.eventId)).length, 0);
    const killed = await child.finish('kill');
    if (before['state'] === 'leased')
      await databaseWaitFor(
        p,
        () => intent(p, f.eventId),
        (r) => r['expired'] === true,
        'publisher lease expires after SIGKILL',
        5000,
      );
    const retry = await publisher().once();
    const session = new AmqpSession();
    await session.open(amqpConfig('consumer'));
    const physical = [];
    try {
      const ch = await session.channel(false);
      for (let i = 0; i < (boundary === publisherBoundaries[1] ? 2 : 1); i++) {
        const m = await ch.get(topology().queue, { noAck: false });
        assert.ok(m);
        physical.push({
          bytes: m.content.toString('hex'),
          properties: m.properties,
          fields: m.fields,
        });
      }
      assert.equal(await ch.get(topology().queue, { noAck: false }), false);
    } finally {
      await session.close();
    }
    assert.equal(new Set(physical.map((x) => x.bytes)).size, 1);
    if (physical.length === 2)
      assert.notEqual(
        physical[0]?.properties.correlationId,
        physical[1]?.properties.correlationId,
      );
    await drain(p, c);
    assert.equal((await effects(c, f.eventId)).length, 1);
    evidence('MQ03-boundary', {
      boundary,
      eventId: f.eventId,
      reached,
      before,
      killed,
      retry,
      physical,
      after: await intent(p, f.eventId),
      effects: await effects(c, f.eventId),
    });
  }
});
await test(name('MQ06'), async (t) => {
  const { s, p, c } = await setup(t);
  for (const boundary of consumerBoundaries) {
    const f = await mutation(
      s,
      JSON.stringify({ name: boundary, loyalty_points: 2 }),
    );
    await captureDrain();
    await publisher().once();
    const label = `MQ06-${boundary}`,
      child = launchRabbit(t, label, boundary, 'consumer');
    const reached = await child.barrier();
    const sessions = (
      await c.query<Record<string, unknown>>(
        'SELECT pid,application_name,state,backend_xid::text FROM pg_stat_activity WHERE application_name=$1',
        [`${required('M1_RUN_ID')}:${label}`],
      )
    ).rows;
    const before = await effects(c, f.eventId);
    assert.equal(before.length, boundary === consumerBoundaries[0] ? 0 : 1);
    if (boundary === consumerBoundaries[0]) {
      assert.equal(sessions.length, 1);
      assert.equal(sessions[0]?.['state'], 'idle in transaction');
      assert.ok(sessions[0]?.['backend_xid']);
    } else assert.deepEqual(sessions, []);
    const killed = await child.finish('kill');
    await drain(p, c);
    assert.equal((await effects(c, f.eventId)).length, 1);
    evidence('MQ06-boundary', {
      boundary,
      eventId: f.eventId,
      reached,
      sessions,
      before,
      killed,
      after: await effects(c, f.eventId),
    });
  }
});
await test(name('MQ07'), async (t) => {
  const { s, p, c } = await setup(t);
  const f = await mutation(
    s,
    '{"name":"one publication twenty five crashes","loyalty_points":25}',
  );
  await captureDrain();
  const published = await publisher().once();
  assert.equal(published.claimed, 1);
  const attemptsBefore = (
    await p.query('SELECT * FROM pipeline.rabbit_attempts WHERE event_id=$1', [
      f.eventId,
    ])
  ).rows;
  const crashes = [];
  let raw: string | undefined, correlation: unknown;
  for (let i = 0; i < 25; i++) {
    const child = launchRabbit(
      t,
      `MQ07-crash-${i}`,
      'consumer.after_db_commit.before_ack',
      'consumer',
    );
    const reached = await child.barrier();
    const deliveries = reached['deliveries'];
    assert.ok(Array.isArray(deliveries));
    assert.equal(deliveries.length, 1);
    const d = record(deliveries[0]);
    const props = record(d['properties']);
    const headers = record(props['headers']);
    if (i === 0) {
      assert.equal(typeof d['wireHex'], 'string');
      raw = String(d['wireHex']);
      correlation = props['correlationId'];
    }
    assert.equal(d['wireHex'], raw);
    assert.equal(props['correlationId'], correlation);
    assert.equal(headers['x-delivery-count'] ?? 0, i);
    assert.equal((await effects(c, f.eventId)).length, 1);
    const killed = await child.finish('kill');
    crashes.push({ iteration: i, reached, killed });
  }
  const result = await consumer().once();
  assert.equal(result.acknowledged, 1);
  assert.equal((await effects(c, f.eventId)).length, 1);
  assert.deepEqual(
    (
      await p.query(
        'SELECT * FROM pipeline.rabbit_attempts WHERE event_id=$1',
        [f.eventId],
      )
    ).rows,
    attemptsBefore,
  );
  await drain(p, c);
  evidence('MQ07', {
    eventId: f.eventId,
    published,
    physicalPublicationAttempts: attemptsBefore,
    crashes,
    result,
    effects: await effects(c, f.eventId),
  });
});
for (const [i, boundary] of [
  ...publisherBoundaries,
  ...consumerBoundaries,
].entries())
  await test(name(`MQH0${i + 1}`), async (t) => {
    const { s, p, c } = await setup(t);
    const f = await mutation(
      s,
      JSON.stringify({ name: `healthy-${boundary}`, loyalty_points: 3 }),
    );
    await captureDrain();
    const role = i < 3 ? 'publisher' : 'consumer';
    if (role === 'consumer') await publisher().once();
    const child = launchRabbit(t, `MQH0${i + 1}`, boundary, role);
    const reached = await child.barrier();
    const released = await child.finish('release');
    const ordinary = record(record(released.output[0])['result']);
    if (role === 'publisher') {
      assert.equal(ordinary['claimed'], 1);
      const outcomes = ordinary['outcomes'];
      assert.ok(Array.isArray(outcomes));
      assert.equal(outcomes.length, 1);
      assert.equal(record(outcomes[0])['eventId'], f.eventId);
      assert.equal(record(outcomes[0])['outcome'], 'confirmed');
      assert.equal(record(outcomes[0])['status'], 'settled');
    } else {
      assert.equal(ordinary['acknowledged'], 1);
      assert.deepEqual(ordinary['processed'], [f.eventId]);
    }

    await drain(p, c);
    assert.equal((await effects(c, f.eventId)).length, 1);
    assert.deepEqual(
      (
        await c.query(
          "SELECT pid FROM pg_stat_activity WHERE usename='consumer_runtime'",
        )
      ).rows,
      [],
    );
    evidence(`MQH0${i + 1}`, {
      eventId: f.eventId,
      boundary,
      reached,
      released,
      effects: await effects(c, f.eventId),
    });
  });
await test(name('MQ04'), async (t) => {
  const { s, p, c } = await setup(t);
  const f = await mutation(s, '{"name":"lost confirm","loyalty_points":4}');
  await captureDrain();
  const route = await proxy(t, 'amqp');
  let injected = false;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- private fault instrumentation preserves the original session receiver
  const originalChannel = AmqpSession.prototype.channel;
  const hook = mock.method(
    AmqpSession.prototype,
    'channel',
    async function (this: AmqpSession, confirm: false, highWaterMark?: number) {
      const ch = await Reflect.apply(originalChannel, this, [
        confirm,
        highWaterMark,
      ]);
      const check = ch.checkExchange.bind(ch);
      mock.method(ch, 'checkExchange', async (exchange: string) => {
        const result = await check(exchange);
        await route.toxic('timeout', 'downstream', { timeout: 0 });
        injected = true;
        return result;
      });
      return ch;
    },
  );
  t.after(() => hook.mock.restore());
  const worker = new RabbitDelivery(
    rabbitLedger(),
    new Publisher(
      { ...amqpConfig(), host: route.host, port: route.port },
      metadata(),
      1500,
    ),
  );
  const active = worker.once();
  void active.catch(() => undefined);
  const result = await active;
  hook.mock.restore();
  assert.equal(injected, true);
  // This schedule must actually lose the confirm, otherwise the fault case fails.
  assert.ok(result.outcomes.some((o) => o.outcome === 'transient'));
  const session = new AmqpSession();
  await session.open(amqpConfig('consumer'));
  let message;
  try {
    const ch = await session.channel(false);
    message = await ch.get(topology().queue, { noAck: false });
    assert.ok(message);
    assert.deepEqual(message.content, await ledgerWire(p, f.eventId));
  } finally {
    await session.close();
  }
  assert.equal((await intent(p, f.eventId))['state'], 'retry_wait');
  assert.deepEqual(await effects(c, f.eventId), []);
  await route.clear();
  await drain(p, c);
  evidence('MQ04', {
    eventId: f.eventId,
    result,
    independentlyReceived: message,
    after: await effects(c, f.eventId),
  });
});
await test(name('MQ05'), async (context) => {
  const { s, p, c } = await setup(context);
  const t = topology(),
    api = metadata(true),
    v = encodeURIComponent(t.vhost),
    queue = `${t.vhost}-overflow`,
    routing = 'overflow';
  await api.request('PUT', `/api/queues/${v}/${queue}`, {
    durable: true,
    auto_delete: false,
    arguments: {
      'x-queue-type': 'quorum',
      'x-delivery-limit': -1,
      'x-overflow': 'reject-publish',
      'x-max-length-bytes': 1024,
    },
  });
  await api.request('POST', `/api/bindings/${v}/e/${t.exchange}/q/${queue}`, {
    routing_key: routing,
    arguments: {},
  });
  const session = new AmqpSession();
  await session.open(amqpConfig());
  const observed = [];
  try {
    const ch = await session.channel(true, 1);
    let returns = 0;
    ch.on('return', () => returns++);
    const send = async (key: string, bytes: Buffer) => {
      let writable = true;
      let drained = false;
      const drain = () => {
        drained = true;
      };
      ch.once('drain', drain);
      const error = await new Promise<unknown>((resolve) => {
        writable = ch.publish(
          t.exchange,
          key,
          bytes,
          { mandatory: true, persistent: true, correlationId: randomUUID() },
          (e: unknown) => resolve(e),
        );
      });
      ch.off('drain', drain);
      return { writable, drained, confirmed: !error };
    };
    const unrouted = await send('not-bound', Buffer.from('unroutable'));
    assert.equal(unrouted.confirmed, true);
    assert.equal(returns, 1);
    for (let i = 0; i < 12; i++)
      observed.push(await send(routing, Buffer.alloc(768, i)));
    assert.ok(observed.some((o) => o.confirmed));
    assert.ok(observed.some((o) => !o.confirmed));
    assert.ok(observed.some((o) => !o.writable));
    const accepted = observed.filter((o) => o.confirmed).length;
    const setupSession = new AmqpSession();
    await setupSession.open(amqpConfig('setup'));
    try {
      const read = await setupSession.channel(false);
      const bodies = [];
      for (let i = 0; i < 12; i++) {
        const m = await read.get(queue, { noAck: false });
        if (!m) break;
        bodies.push(m.content.toString('hex'));
        read.ack(m, false);
      }
      assert.equal(bodies.length, accepted);
      evidence('MQ05', {
        unrouted,
        returns,
        observed,
        accepted,
        bodies,
        queueBudget: 'ready bodies only; bounded overshoot is allowed',
      });
    } finally {
      await setupSession.close();
    }
  } finally {
    await session.close();
    await api.request('DELETE', `/api/queues/${v}/${queue}`);
  }
  // Actual production adapter: remove the binding only after its real passive
  // exchange check. The returned message is still positively confirmed by Rabbit.
  const f = await mutation(
    s,
    '{"name":"mandatory return adapter","loyalty_points":5}',
  );
  await captureDrain();
  const bindings = await api.request(
    'GET',
    `/api/bindings/${v}/e/${t.exchange}/q/${t.queue}`,
  );
  assert.ok(Array.isArray(bindings));
  assert.equal(bindings.length, 1);
  const key = field(record(bindings[0]), 'properties_key');
  let removed = false;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- private setup barrier preserves the real session receiver
  const original = AmqpSession.prototype.channel;
  const hook = mock.method(
    AmqpSession.prototype,
    'channel',
    async function (this: AmqpSession, confirm: false, highWaterMark?: number) {
      const ch = await Reflect.apply(original, this, [confirm, highWaterMark]);
      const check = ch.checkExchange.bind(ch);
      mock.method(ch, 'checkExchange', async (exchange: string) => {
        const result = await check(exchange);
        await api.request(
          'DELETE',
          `/api/bindings/${v}/e/${t.exchange}/q/${t.queue}/${encodeURIComponent(key)}`,
        );
        removed = true;
        return result;
      });
      return ch;
    },
  );
  let returned;
  try {
    returned = await new Publisher(amqpConfig(), metadata(), 10000, 1).publish(
      t,
      [
        {
          eventId: f.eventId,
          attemptId: randomUUID(),
          wire: await ledgerWire(p, f.eventId),
        },
      ],
    );
  } finally {
    hook.mock.restore();
    if (removed)
      await api.request(
        'POST',
        `/api/bindings/${v}/e/${t.exchange}/q/${t.queue}`,
        { routing_key: t.routingKey, arguments: {} },
      );
  }
  assert.equal(returned.length, 1);
  assert.equal(returned[0]?.returned, true);
  assert.equal(returned[0]?.outcome, 'configuration');
  assert.equal((await intent(p, f.eventId))['state'], 'pending');
  await drain(p, c);
  evidence('MQ05-production-return', { eventId: f.eventId, returned });
});
await test(name('MQ09'), async (t) => {
  const { s, p, c } = await setup(t);
  const f = await mutation(s, '{"name":"stale publisher","loyalty_points":9}');
  await captureDrain();
  const a = launchRabbit(
    t,
    'MQ09-old',
    'rabbit.after_confirm.before_local_commit',
    'publisher',
    900,
    true,
  );
  const reached = await a.barrier();
  await databaseWaitFor(
    p,
    () => intent(p, f.eventId),
    (r) => r['expired'] === true,
    'source clock proves publisher expiry',
    5000,
  );
  const b = await publisher().once();
  const terminal = await intent(p, f.eventId);
  assert.equal(terminal['state'], 'satisfied');
  assert.equal(terminal['claim_generation'], '2');
  const released = await a.finish('release');
  const after = await intent(p, f.eventId);
  delete terminal['observed_at'];
  delete after['observed_at'];
  assert.deepEqual(after, terminal);
  await drain(p, c);
  evidence('MQ09-publisher', {
    eventId: f.eventId,
    reached,
    b,
    terminal,
    released,
    after,
  });
  // Retire the original AMQP connection while its DB COMMIT is held at a private barrier.
  const next = await mutation(
    s,
    '{"name":"old consumer channel","loyalty_points":10}',
  );
  await captureDrain();
  await publisher().once();
  const old = launchRabbit(
    t,
    'MQ09-consumer',
    'consumer.after_db_commit.before_ack',
    'consumer',
  );
  const paused = await old.barrier();
  const deliveries = paused['deliveries'];
  assert.ok(Array.isArray(deliveries));
  const original = field(record(deliveries[0]), 'channelId');
  const connection = await waitFor(
    async (signal) => {
      const all = await metadata(true).request(
        'GET',
        '/api/connections',
        undefined,
        signal,
      );
      assert.ok(Array.isArray(all));
      const found: unknown = all.find(
        (x) =>
          record(record(x)['client_properties'])['connection_name'] ===
          original,
      );
      return found;
    },
    (x) => x !== undefined,
    'Broker observes exact consumer connection',
    10000,
  );
  assert.ok(connection);
  await metadata(true).request(
    'DELETE',
    `/api/connections/${encodeURIComponent(field(record(connection), 'name'))}`,
  );
  const newer = await consumer().once();
  assert.equal(newer.acknowledged, 1);
  const oldResult = await old.finish('release');
  const response = record(record(oldResult.output[0])['result']);
  assert.equal(response['acknowledged'], 0);
  assert.equal(response['retired'], true);
  await drain(p, c);
  evidence('MQ09-consumer', {
    eventId: next.eventId,
    paused,
    original,
    newer,
    oldResult,
    effects: await effects(c, next.eventId),
  });
});
await test(name('MQ11'), async (t) => {
  const { s, p, c } = await setup(t);
  const a = await mutation(s, '{"name":"consumer paused","loyalty_points":11}');
  await captureDrain();
  const pub = await publisher().once();
  assert.equal((await intent(p, a.eventId))['state'], 'satisfied');
  assert.deepEqual(await effects(c, a.eventId), []);
  assert.equal(
    (await observer().once()).observed.some((x) => x.eventId === a.eventId),
    false,
  );
  await drain(p, c);
  const f = await mutation(s, '{"name":"consumer first","loyalty_points":12}');
  await captureDrain();
  const child = launchRabbit(
    t,
    'MQ11-consumer-first',
    'rabbit.after_confirm.before_local_commit',
  );
  const paused = await child.barrier();
  await consumer().once();
  const observation = await observer().once();
  assert.ok(observation.observed.some((x) => x.eventId === f.eventId));
  assert.equal((await intent(p, f.eventId))['state'], 'leased');
  const released = await child.finish('release');
  await drain(p, c);
  evidence('MQ11', {
    pausedPublication: pub,
    firstEvent: a.eventId,
    consumerFirstEvent: f.eventId,
    paused,
    observation,
    released,
  });
});
await test(name('MQ10'), async (t) => {
  const { s, p, c } = await setup(t);
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  const broker = `${required('RABBIT_PROJECT')}-rabbitmq-1`;
  await docker(['stop', '--time', '5', broker]);
  const stopped = performance.now();
  let restarted = false;
  t.after(async () => {
    if (!restarted) await docker(['start', broker]);
  });
  const f = await mutation(
    s,
    '{"name":"broker outage ES progresses","loyalty_points":10}',
  );
  await captureDrain();
  await finishEs(es, p);
  assert.equal(
    record(
      (await remote(es, `${required('SOURCE_EPOCH')}:${f.id}`))['_source'],
    )['entity_version'],
    '1',
  );
  const w = publisher(),
    attempts = [];
  for (let i = 0; i < 6; i++) {
    attempts.push(await w.once());
    await delay(150);
  }
  assert.notEqual((await intent(p, f.eventId))['state'], 'satisfied');
  assert.deepEqual(await effects(c, f.eventId), []);
  const admissionAttempts = attempts.reduce((n, r) => n + r.claimed, 0);
  assert.ok(admissionAttempts >= 1 && admissionAttempts <= 3);
  await docker(['start', broker]);
  restarted = true;
  const brokerStoppedMs = performance.now() - stopped;
  const until = performance.now() + 20000;
  while (true) {
    try {
      await validateTopology(metadata(), topology());
      break;
    } catch (error) {
      if (performance.now() > until) throw error;
      await delay(200);
    }
  }
  const stopPublish = new AbortController();
  const recoveredPublications: Awaited<ReturnType<RabbitDelivery['once']>>[] =
    [];
  const publishDeadline = setTimeout(() => stopPublish.abort(), 15000);
  try {
    await w.follow(stopPublish.signal, (result) => {
      recoveredPublications.push(result);
      if (
        result.outcomes.some(
          (o) => o.eventId === f.eventId && o.status === 'settled',
        )
      )
        stopPublish.abort();
      return Promise.resolve();
    });
  } finally {
    clearTimeout(publishDeadline);
    stopPublish.abort();
  }
  assert.ok(
    recoveredPublications.some((r) =>
      r.outcomes.some(
        (o) =>
          o.eventId === f.eventId &&
          o.status === 'settled' &&
          o.outcome === 'confirmed',
      ),
    ),
  );
  await drain(p, c);
  const route = await proxy(t, 'consumer-db');
  const valid = await mutation(
    s,
    '{"name":"consumer database outage","loyalty_points":11}',
  );
  await captureDrain();
  await publisher().once();
  const qBefore = (await c.query('SELECT * FROM consumer.quarantine')).rows;
  let interruptDatabase = true;
  class InterruptedDatabase extends ConsumerDatabase {
    override async process(...args: Parameters<ConsumerDatabase['process']>) {
      if (interruptDatabase) {
        interruptDatabase = false;
        await route.enabled(false);
      }
      return super.process(...args);
    }
  }
  const failed = new Consumer(
    new InterruptedDatabase({
      ...consumerConfig(),
      host: route.host,
      port: route.port,
    }),
    amqpConfig('consumer'),
    metadata(),
    topology(),
  );
  await assert.rejects(failed.once());
  assert.deepEqual(await effects(c, valid.eventId), []);
  assert.deepEqual(
    (await c.query('SELECT * FROM consumer.quarantine')).rows,
    qBefore,
  );
  const stopRecovery = new AbortController();
  const recovery: (
    Awaited<ReturnType<Consumer['once']>> | { error: string; retryMs: number }
  )[] = [];
  const recoveryDeadline = setTimeout(() => stopRecovery.abort(), 15000);
  try {
    await failed.follow(stopRecovery.signal, async (result) => {
      recovery.push(result);
      if ('error' in result) {
        assert.ok(result.retryMs > 0);
        await route.enabled(true);
      } else if (result.acknowledged === 1) stopRecovery.abort();
    });
  } finally {
    clearTimeout(recoveryDeadline);
    stopRecovery.abort();
  }
  const recovered = recovery.find(
    (r) => 'acknowledged' in r && r.acknowledged === 1,
  );
  assert.ok(
    recovered,
    'Same consumer follow loop reconnects and ACKs the retained message',
  );
  await drain(p, c);
  const esContainer = `${required('ES_PROJECT')}-elasticsearch-1`;
  await docker(['stop', '--time', '10', esContainer]);
  let esStarted = false;
  t.after(async () => {
    if (!esStarted) await docker(['start', esContainer]);
  });
  const unaffected = await mutation(
    s,
    '{"name":"ES outage consumer progresses","loyalty_points":12}',
  );
  await drain(p, c);
  assert.equal((await effects(c, unaffected.eventId)).length, 1);
  await docker(['start', esContainer]);
  esStarted = true;
  evidence('MQ10', {
    brokerStoppedMs,
    attempts,
    admissionAttempts,
    automaticPublisherRecovery: recoveredPublications,
    consumerDatabaseRecovered: recovered,
    automaticConsumerRecovery: recovery,
    esIndependentEvent: unaffected.eventId,
  });
});
await test(name('MQ12'), async (t) => {
  const { s, p, c } = await setup(t);
  const valid = await mutation(
    s,
    '{"name":"poison neighbor","loyalty_points":12}',
  );
  await captureDrain();
  const wire = await ledgerWire(p, valid.eventId);
  const body: unknown = JSON.parse(wire.toString('utf8'));
  const parsed = record(body),
    b = record(parsed['body']);
  const foreignEpoch = randomUUID();
  const wrongEpoch = canonicalEvent({
    ...b,
    source_epoch: foreignEpoch,
    event_id: `${foreignEpoch}:${String(b['entity_id'])}:${String(b['entity_version'])}`,
  });
  const fixtures = [
    { raw: Buffer.alloc(131072, 120), id: 'malformed-at-transport-cap' },
    { raw: wrongEpoch.wireBytes, id: wrongEpoch.body.event_id },
    { raw: Buffer.from([0xc3, 0x28]), id: 'malformed-utf8' },
    {
      raw: Buffer.from(
        wire
          .toString('utf8')
          .replace(String(parsed['content_sha256']), '0'.repeat(64)),
      ),
      id: valid.eventId,
    },
    {
      raw: Buffer.from(
        JSON.stringify({ ...parsed, body: { ...b, schema_version: 2 } }),
      ),
      id: valid.eventId,
    },
    { raw: wire, id: 'wrong-message-id' },
    {
      raw: wire,
      id: valid.eventId,
      headers: {
        pipeline_id: topology().pipelineId,
        registration_id: randomUUID(),
      },
    },
  ];
  const before = (
    await c.query<{ n: string }>(
      'SELECT count(*)::text n FROM consumer.quarantine',
    )
  ).rows[0]?.n;
  for (const f of fixtures)
    await rawPublish(f.raw, f.id, f.headers ? { headers: f.headers } : {});
  await publisher().once();
  const result = await consumer().once();
  assert.equal(result.quarantined.length, fixtures.length);
  assert.equal(result.processed.length, 1);
  assert.equal(result.acknowledged, fixtures.length + 1);
  const diagnostics = (
    await c.query<Record<string, unknown>>(
      "SELECT quarantine_id::text,encode(raw_bytes,'hex') raw,convert_from(metadata,'UTF8') metadata,classification,context FROM consumer.quarantine ORDER BY recorded_at",
    )
  ).rows;
  for (const f of fixtures)
    assert.ok(diagnostics.some((d) => d['raw'] === f.raw.toString('hex')));
  // A self-consistent same-ID different body is an integrity poison, not a mapper rejection.
  const conflict = canonicalEvent({
    ...b,
    payload_json: '{"name": "conflict"}',
  });
  const original = await effects(c, valid.eventId);
  await rawPublish(conflict.wireBytes, valid.eventId);
  const conflicted = await consumer().once();
  assert.equal(conflicted.quarantined.length, 1);
  assert.deepEqual(await effects(c, valid.eventId), original);
  const crashRaw = Buffer.from('not JSON: quarantine survives SIGKILL');
  await rawPublish(crashRaw, 'quarantine-crash');
  const child = launchRabbit(
    t,
    'MQ12-quarantine',
    'consumer.after_quarantine_commit.before_ack',
    'consumer',
  );
  const reached = await child.barrier();
  const retained = (
    await c.query('SELECT * FROM consumer.quarantine WHERE raw_bytes=$1', [
      crashRaw,
    ])
  ).rows;
  assert.equal(retained.length, 1);
  const killed = await child.finish('kill');
  const repeated = await consumer().once();
  assert.equal(repeated.quarantined.length, 1);
  assert.deepEqual(
    (
      await c.query('SELECT * FROM consumer.quarantine WHERE raw_bytes=$1', [
        crashRaw,
      ])
    ).rows,
    retained,
  );
  await drain(p, c);
  evidence('MQ12', {
    before,
    fixtures: fixtures.map((f) => ({ id: f.id, raw: f.raw.toString('hex') })),
    result,
    diagnostics,
    conflicted,
    reached,
    killed,
    repeated,
    retained,
  });
});
await test(name('MQH06'), async (t) => {
  const { c } = await setup(t);
  const raw = Buffer.from('healthy durable quarantine control');
  await rawPublish(raw, 'healthy-quarantine');
  const child = launchRabbit(
    t,
    'MQH06',
    'consumer.after_quarantine_commit.before_ack',
    'consumer',
  );
  const reached = await child.barrier();
  const before = (
    await c.query('SELECT * FROM consumer.quarantine WHERE raw_bytes=$1', [raw])
  ).rows;
  assert.equal(before.length, 1);
  const released = await child.finish('release');
  const ordinary = record(record(released.output[0])['result']);
  assert.equal(ordinary['acknowledged'], 1);
  assert.deepEqual(ordinary['processed'], []);
  assert.ok(
    Array.isArray(ordinary['quarantined']) &&
      ordinary['quarantined'].length === 1,
  );

  assert.deepEqual(
    (
      await c.query('SELECT * FROM consumer.quarantine WHERE raw_bytes=$1', [
        raw,
      ])
    ).rows,
    before,
  );
  evidence('MQH06', { reached, released, before });
});
await test(name('MQ13'), async (t) => {
  const { s, p, c } = await setup(t);
  const missing = await mutation(
    s,
    '{"name":"late receipt","loyalty_points":13}',
  );
  await captureDrain();
  const first = await observer().once();
  assert.ok(first.requested.includes(missing.eventId));
  assert.deepEqual(first.observed, []);
  const later = [];
  for (let i = 0; i < 10; i++)
    later.push(
      await mutation(
        s,
        JSON.stringify({ name: `fair-${i}`, loyalty_points: i }),
      ),
    );
  await captureDrain();
  await publisher().once();
  const child = launchRabbit(
    t,
    'MQ13-late',
    'consumer.before_db_commit',
    'consumer',
  );
  const reached = await child.barrier();
  const absent = await observer().once();
  assert.deepEqual(absent.observed, []);
  const released = await child.finish('release');
  await drain(p, c);
  const rows = (
    await p.query(
      'SELECT event_id,state,consumer_id::text,registration_id::text,receipt_hash FROM pipeline.consumer_observations WHERE event_id=ANY($1::text[]) ORDER BY event_id',
      [[missing.eventId, ...later.map((f) => f.eventId)]],
    )
  ).rows;
  assert.equal(rows.length, 11);
  for (const r of rows) assert.equal(record(r)['state'], 'processed');
  const wire = await ledgerWire(p, missing.eventId);
  const positive = await new ConsumerDatabase(
    consumerConfig('reader'),
  ).receipts([missing.eventId], ['0'.repeat(64)]);
  assert.equal(positive.length, 1);
  assert.equal(positive[0]?.['state'], 'processed');
  const before = JSON.stringify(rows);
  await rawPublish(
    Buffer.from('forged claiming legitimate identity'),
    missing.eventId,
  );
  await consumer().once();
  await observer().once();
  assert.equal(
    JSON.stringify(
      (
        await p.query(
          'SELECT event_id,state,consumer_id::text,registration_id::text,receipt_hash FROM pipeline.consumer_observations WHERE event_id=ANY($1::text[]) ORDER BY event_id',
          [[missing.eventId, ...later.map((f) => f.eventId)]],
        )
      ).rows,
    ),
    before,
  );
  await assert.rejects(
    new ConsumerDatabase(consumerConfig('reader')).receipts(
      Array.from({ length: 9 }, () => missing.eventId),
      Array.from({ length: 9 }, () => '0'.repeat(64)),
    ),
  );
  const forgedPending = await mutation(
    s,
    '{"name":"forged receipt identity","loyalty_points":13}',
  );
  await captureDrain();
  const legitimateWire = await ledgerWire(p, forgedPending.eventId);
  await rawPublish(legitimateWire, forgedPending.eventId, {
    headers: {
      pipeline_id: topology().pipelineId,
      registration_id: randomUUID(),
    },
  });
  const quarantinedForged = await consumer().once();
  assert.equal(quarantinedForged.quarantined.length, 1);
  await assert.rejects(observer().once(), /Forged claimed ID/);
  assert.equal(
    (
      await p.query<{ state: string }>(
        'SELECT state FROM pipeline.consumer_observations WHERE event_id=$1',
        [forgedPending.eventId],
      )
    ).rows[0]?.state,
    'pending',
  );
  const db = new pg.Client(rabbitConfig('observer'));
  await db.connect();
  try {
    const e = canonicalEvent(
      record(JSON.parse(legitimateWire.toString('utf8')))['body'],
    );
    for (const [consumerId, hash] of [
      [randomUUID(), e.contentSha256],
      [topology().consumerId, '0'.repeat(64)],
    ]) {
      await assert.rejects(
        db.query(
          'SELECT pipeline.observe_receipt($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [
            forgedPending.eventId,
            consumerId,
            topology().registrationId,
            topology().epoch,
            topology().pipelineId,
            'processed',
            e.bodyBytes,
            hash,
            forgedPending.eventId,
          ],
        ),
        { code: 'P7001' },
      );
    }
  } finally {
    await db.end();
  }
  await drain(p, c);
  evidence('MQ13', {
    forgedPending: forgedPending.eventId,
    quarantinedForged,
    forgedQuarantineRejected: true,
    wrongReceiptIdentityAndHashRejected: 'P7001',
    first,
    absent,
    reached,
    released,
    rows,
    wire: wire.toString('hex'),
    positive,
  });
});

await test(name('MQ08'), async (t) => {
  const { s, p, c } = await setup(t);
  await drain(p, c);
  const live = await mutation(s, '{"name":"ordered live","loyalty_points":1}');
  const update = await mutation(
    s,
    '{"name":"ordered newer","loyalty_points":2}',
    live.id,
    'update',
  );
  const tombstone = await mutation(s, '{}', live.id, 'delete');
  await captureDrain();
  const tombWire = await ledgerWire(p, tombstone.eventId);
  await rawPublish(tombWire, tombstone.eventId);
  await consumer().once();
  const projected = async () =>
    (
      await c.query<Record<string, unknown>>(
        'SELECT entity_version::text,event_id FROM consumer.entity_projection WHERE entity_id=$1',
        [live.id],
      )
    ).rows;
  assert.deepEqual(await projected(), [
    { entity_version: '3', event_id: tombstone.eventId },
  ]);
  for (const f of [update, live, live])
    await rawPublish(await ledgerWire(p, f.eventId), f.eventId);
  const older = await consumer().once();
  assert.equal(older.acknowledged, 3);
  assert.deepEqual(await projected(), [
    { entity_version: '3', event_id: tombstone.eventId },
  ]);
  await drain(p, c);
  const restored = await mutation(
    s,
    '{"name":"restored","loyalty_points":4}',
    live.id,
    'restore',
  );
  await drain(p, c);
  assert.deepEqual(await projected(), [
    { entity_version: '4', event_id: restored.eventId },
  ]);
  assert.equal(
    (
      await c.query<{ n: string }>(
        'SELECT units::text n FROM consumer.entity_totals WHERE entity_id=$1',
        [live.id],
      )
    ).rows[0]?.n,
    '4',
  );
  // Two actual consumer processes: A retains 64 deliveries, B's later revisions
  // wait on A's per-entity transaction lock. Neither process shares an owner.
  const hot = await mutation(s, '{"name":"hot","loyalty_points":0}');
  for (let i = 1; i < 80; i++)
    await mutation(
      s,
      JSON.stringify({ name: 'hot', loyalty_points: i }),
      hot.id,
      'update',
    );
  await captureDrain();
  const duplicateHot = `${required('SOURCE_EPOCH')}:${hot.id}:80`;
  await rawPublish(await ledgerWire(p, duplicateHot), duplicateHot);
  await publisher().once();
  const a = launchRabbit(t, 'MQ08-A', 'consumer.before_db_commit', 'consumer');
  const aBoundary = await a.barrier();
  const b = launchRabbit(t, 'MQ08-B', 'consumer.before_db_commit', 'consumer');
  const blocked = await databaseWaitFor(
    c,
    async () =>
      (
        await c.query<Record<string, unknown>>(
          'SELECT pid,application_name,wait_event_type,wait_event,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE application_name=$1',
          [`${required('M1_RUN_ID')}:MQ08-B`],
        )
      ).rows,
    (r) =>
      r.some(
        (v) =>
          v['wait_event_type'] === 'Lock' && v['wait_event'] === 'advisory',
      ),
    'Second real consumer waits on same entity',
    12000,
  );
  const releasedA = await a.finish('release');
  const bBoundary = await b.barrier();
  assert.ok(
    Array.isArray(aBoundary['events']) &&
      aBoundary['events'].includes(duplicateHot),
  );
  assert.ok(
    Array.isArray(bBoundary['events']) &&
      bBoundary['events'].includes(duplicateHot),
  );
  const releasedB = await b.finish('release');
  await drain(p, c);
  assert.equal(
    (
      await c.query<{ n: string }>(
        'SELECT units::text n FROM consumer.entity_totals WHERE entity_id=$1',
        [hot.id],
      )
    ).rows[0]?.n,
    '80',
  );
  assert.equal(
    (
      await c.query<{ v: string }>(
        'SELECT entity_version::text v FROM consumer.entity_projection WHERE entity_id=$1',
        [hot.id],
      )
    ).rows[0]?.v,
    '80',
  );
  const baseline = canonicalEvent({
    schema_version: 1,
    source_epoch: required('SOURCE_EPOCH'),
    entity_id: '9223372036854775807',
    entity_version: '9007199254740993',
    event_id: `${required('SOURCE_EPOCH')}:9223372036854775807:9007199254740993`,
    source_change_id: null,
    source_recorded_at: '2026-01-01T00:00:00.000001Z',
    kind: 'baseline',
    is_deleted: false,
    payload_encoding: 'pg18-jsonb-text/v1',
    payload_json: '{"synthetic": true}',
  });
  syntheticBaselines.push({
    eventId: baseline.body.event_id,
    body: baseline.bodyBytes.toString('utf8'),
    hash: baseline.contentSha256,
    entity: baseline.body.entity_id,
    version: baseline.body.entity_version,
  });
  evidence('MQ08-synthetic-baseline-declaration', syntheticBaselines.at(-1));
  await rawPublish(baseline.wireBytes, baseline.body.event_id);
  await consumer().once();
  assert.deepEqual(
    (
      await c.query(
        'SELECT * FROM consumer.mutation_effects WHERE event_id=$1',
        [baseline.body.event_id],
      )
    ).rows,
    [],
  );
  assert.equal(
    (
      await c.query<{ n: string }>(
        'SELECT units::text n FROM consumer.entity_totals WHERE entity_id=$1',
        [baseline.body.entity_id],
      )
    ).rows[0]?.n,
    '0',
  );
  evidence('MQ08', {
    older,
    live,
    update,
    tombstone,
    restored,
    hot,
    aBoundary,
    bBoundary,
    blocked,
    releasedA,
    releasedB,
    syntheticBaseline: syntheticBaselines.at(-1),
  });
});
await test(name('MQ15'), async (t) => {
  const { p, c } = await setup(t);
  await drain(p, c);
  const before = (
    await p.query<{ text: string }>(
      'SELECT row_to_json(i)::text text FROM pipeline.delivery_intents i ORDER BY event_id,kind',
    )
  ).rows;
  const observations = (
    await p.query<{ text: string }>(
      'SELECT row_to_json(i)::text text FROM pipeline.consumer_observations i ORDER BY event_id',
    )
  ).rows;
  const attempts = (
    await p.query<{ text: string }>(
      'SELECT row_to_json(i)::text text FROM pipeline.rabbit_attempts i ORDER BY attempt_id',
    )
  ).rows;
  const events = (
    await p.query<{ body: Buffer; hash: string; id: string }>(
      'SELECT body_bytes body,content_sha256 hash,event_id id FROM pipeline.events ORDER BY event_id',
    )
  ).rows;
  for (let i = 0; i < events.length; i += 16) {
    const batch = events.slice(i, i + 16);
    const results = await pipeline().stage(
      batch.map((e) => ({ bodyBytes: e.body, contentSha256: e.hash })),
    );
    assert.deepEqual(
      results.map((r) => r.status),
      batch.map(() => 'already_staged'),
    );
  }
  assert.deepEqual(
    (
      await p.query<{ text: string }>(
        'SELECT row_to_json(i)::text text FROM pipeline.delivery_intents i ORDER BY event_id,kind',
      )
    ).rows,
    before,
  );
  assert.deepEqual(
    (
      await p.query<{ text: string }>(
        'SELECT row_to_json(i)::text text FROM pipeline.consumer_observations i ORDER BY event_id',
      )
    ).rows,
    observations,
  );
  assert.deepEqual(
    (
      await p.query<{ text: string }>(
        'SELECT row_to_json(i)::text text FROM pipeline.rabbit_attempts i ORDER BY attempt_id',
      )
    ).rows,
    attempts,
  );
  const first = events[0];
  assert.ok(first);
  // Deliberate owner corruption inside a rolled-back private transaction only.
  await p.query('BEGIN');
  try {
    await p.query(
      'ALTER TABLE pipeline.consumer_observations DISABLE TRIGGER USER',
    );
    await p.query(
      'DELETE FROM pipeline.consumer_observations WHERE event_id=$1',
      [first.id],
    );
    await assert.rejects(
      p.query('SELECT pipeline.stage_event($1,$2)', [first.body, first.hash]),
      { code: 'P3002' },
    );
  } finally {
    await p.query('ROLLBACK');
  }
  const upgrade: unknown = JSON.parse(
    await readFile(
      join(required('M1_ARTIFACT_DIR'), 'rabbit-upgrade.json'),
      'utf8',
    ),
  );
  const u = record(upgrade),
    beforeUpgrade = record(u['before']),
    afterUpgrade = record(u['after']);
  for (const table of [
    'events',
    'es_attempts',
    'es_dead_letters',
    'es_target',
    'source_binding',
    'destinations',
  ])
    assert.deepEqual(afterUpgrade[table], beforeUpgrade[table]);
  if (process.env['M4_UPGRADE'] === 'true') {
    assert.ok(
      Array.isArray(beforeUpgrade['events']) &&
        beforeUpgrade['events'].length === 2,
    );
    assert.ok(
      Array.isArray(beforeUpgrade['es_attempts']) &&
        beforeUpgrade['es_attempts'].length === 2,
    );
  }
  evidence('MQ15', {
    upgrade,
    restaged: events.map((e) => e.id),
    unchangedIntents: before,
    unchangedObservations: observations,
    unchangedAttempts: attempts,
    missingRelationRejected: 'P3002; rollback restored test relation',
  });
});
await test(name('MQ16'), async (t) => {
  const { s, p, c } = await setup(t);
  await drain(p, c);
  const tBefore = await rabbitLedger().target();
  const identityBefore = await new ConsumerDatabase(
    consumerConfig(),
  ).identity();
  const f = await mutation(
    s,
    '{"name":"queued retained restart","loyalty_points":16}',
  );
  await captureDrain();
  await publisher().once();
  assert.deepEqual(await effects(c, f.eventId), []);
  const before = await intent(p, f.eventId);
  const broker = `${required('RABBIT_PROJECT')}-rabbitmq-1`;
  await docker(['restart', '--time', '10', broker]);
  await waitFor(
    async (signal) => {
      try {
        await validateTopology(metadata(), topology(), signal);
        return true;
      } catch {
        return false;
      }
    },
    Boolean,
    'Retained Rabbit restart',
    30000,
  );
  assert.deepEqual(await rabbitLedger().target(), tBefore);
  assert.deepEqual(
    await new ConsumerDatabase(consumerConfig()).identity(),
    identityBefore,
  );
  await drain(p, c);
  assert.equal((await effects(c, f.eventId)).length, 1);
  const after = await intent(p, f.eventId);
  assert.equal(after['rabbit_attempt_id'], before['rabbit_attempt_id']);
  assert.equal(after['settled_at'], before['settled_at']);
  const authFixture = await mutation(
    s,
    '{"name":"bad broker credential remains retryable after setup correction","loyalty_points":16}',
  );
  await captureDrain();
  const badCredential = new RabbitDelivery(
    rabbitLedger(),
    new Publisher({ ...amqpConfig(), password: randomUUID() }, metadata()),
  );
  const authResult = await badCredential.once();
  assert.equal(authResult.outcomes.length, 1);
  assert.equal(authResult.outcomes[0]?.outcome, 'auth');
  assert.equal((await rabbitLedger().target()).mode, 'blocked');
  assert.equal((await intent(p, authFixture.eventId))['state'], 'retry_wait');
  await assert.rejects(publisher().once(), /target unavailable/);
  const authAttempts = (
    await p.query(
      'SELECT event_id,outcome,context FROM pipeline.rabbit_attempts WHERE event_id=$1',
      [authFixture.eventId],
    )
  ).rows;
  assert.equal(authAttempts.length, 1);
  // Controlled test setup repairs only this intentionally wrong credential experiment's
  // operational block. Runtime has no rebind, unblock or replay capability.
  await p.query("UPDATE pipeline.rabbit_target SET mode='ready',reason=NULL");
  await drain(p, c);
  const session = new AmqpSession();
  await session.open(amqpConfig());
  const ch = await session.channel(true);
  let oversized: unknown;
  try {
    await new Promise<void>((resolve, reject) =>
      ch.publish(
        topology().exchange,
        topology().routingKey,
        Buffer.alloc(131073, 120),
        { mandatory: true, persistent: true },
        (e) =>
          e
            ? reject(
                e instanceof Error
                  ? e
                  : new Error('Broker rejected oversized message'),
              )
            : resolve(),
      ),
    );
  } catch (error) {
    oversized = error;
  }
  assert.ok(oversized, 'Real configured broker maximum rejects >128 KiB');
  await session.close();
  await assert.rejects(
    rabbitLedger().read(Array.from({ length: 129 }, () => f.eventId)),
  );
  await assert.rejects(
    new ConsumerDatabase(consumerConfig('reader')).receipts(
      Array.from({ length: 9 }, () => f.eventId),
      Array.from({ length: 9 }, () => '0'.repeat(64)),
    ),
  );
  const stopping = new AbortController();
  stopping.abort();
  const stopped = await consumer().once(stopping.signal, 25);
  assert.equal(stopped.acknowledged, 0);
  evidence('MQ16', {
    badCredential: { authResult, authAttempts, eventId: authFixture.eventId },
    eventId: f.eventId,
    target: tBefore,
    identity: identityBefore,
    before,
    after,
    oversizedRejected:
      oversized instanceof Error
        ? oversized.message
        : 'Broker rejected oversized publication',
    stopped,
    limits: {
      unconfirmed: 128,
      bytes: 4194304,
      transport: 131072,
      prefetch: 64,
      batch: 32,
      batchBytes: 1048576,
    },
  });
});
await test(name('MQ14'), async (t) => {
  const { s, p, c } = await setup(t);
  await drain(p, c);
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  await finishEs(es, p);
  const admin = new EsTransport(esConfig(true));
  t.after(() => admin.close());
  await admin.request('POST', `/${required('ES_INDEX')}/_refresh`);
  const checked = await reconcile(s, p, c, es, rejections);
  const mutateSnapshot = (edit: (snapshot: ConsumerSnapshot) => void) => {
    const copy = structuredClone(checked.snapshot);
    edit(copy);
    assert.throws(() => checkConsumer(copy, checked.wanted));
  };
  mutateSnapshot((x) => {
    x.effects.pop();
  });
  mutateSnapshot((x) => {
    const first = x.effects[0];
    assert.ok(first);
    x.effects.push({ ...first });
  });
  mutateSnapshot((x) => {
    const first = x.totals[0];
    assert.ok(first);
    first['units'] = '999999';
  });
  mutateSnapshot((x) => {
    const first = x.projections[0];
    assert.ok(first);
    first['body'] = '{"fabricated":true}';
  });
  evidence('MQ14-negative-controls', {
    kind: 'pure independent snapshot checker; no fabricated receiver error',
    rejected: [
      'missing effect',
      'duplicate effect',
      'altered aggregate',
      'altered projection payload',
    ],
  });
});
