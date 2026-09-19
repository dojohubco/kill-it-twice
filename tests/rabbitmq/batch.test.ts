import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { readFile } from 'node:fs/promises';
import { randomUUID, createHash } from 'node:crypto';
import pg from 'pg';
import { batchCases } from '../../scripts/required-batch-cases.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { canonicalEvent, type CanonicalEvent } from '../../src/envelope.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { migrateConsumerBatch } from '../../scripts/rabbit-setup.ts';
import { object } from '../../scripts/acceptance.ts';
import { evidence, databaseWaitFor } from '../support/db.ts';
import {
  consumerConfig,
  setup,
  topology,
  drain,
  rawPublish,
  consumer,
  mutation,
  amqpConfig,
  metadata,
} from '../support/rabbit.ts';
import {
  cohort,
  cap,
  sqlBytes,
  consumerSnapshot,
  assertCohort,
  publishCohort,
  queueState,
  consumeCohort,
  observedWorker,
  type Cohort,
} from '../support/batch-fixture.ts';
import {
  batchTrace,
  installBatchTiming,
  type BatchTrace,
} from '../support/batch-observation.ts';
import { launchBatch } from '../support/batch-process.ts';
const name = (id: string) => {
  const c = batchCases.find((c) => c.id === id);
  assert.ok(c);
  return c.name;
};
function assertTrace(
  trace: BatchTrace,
  received: number,
  unique: number,
  bytes?: number,
) {
  assert.equal(trace.acknowledgements.length, received);
  const selected = trace.acknowledgements.map((a) => {
    assert.equal(a.multiple, false);
    const m = trace.received.find(
      (r) => r.channelId === a.channelId && r.tag === a.tag,
    );
    assert.ok(m);
    return m;
  });
  assert.ok(selected.length <= 32);
  const total = selected.reduce((n, m) => n + m.bytes, 0);
  assert.ok(total <= cap);
  if (bytes !== undefined) assert.equal(total, bytes);
  assert.equal(trace.batches.length, 1);
  const b = trace.batches[0];
  assert.ok(b);
  assert.equal(b.count, unique);
  assert.equal(b.completion, 'confirmed_commit');
  assert.ok(b.wireBytes <= total);
}
async function directRejection(group: Cohort, label: string) {
  const db = new ConsumerDatabase(consumerConfig('runtime', label));
  let observation: unknown;
  await assert.rejects(db.process(topology(), group.events), (e) => {
    assert.ok(e instanceof TransactionError);
    assert.equal(e.outcome, 'rolled_back');
    assert.equal(e.sqlState, 'P6002');
    observation = {
      sqlState: e.sqlState,
      outcome: e.outcome,
      message: e.message,
    };
    return true;
  });
  return observation;
}
async function outsideSnapshot(s: pg.Client, p: pg.Client) {
  const result: Record<string, unknown> = {};
  for (const [schema, client] of [
    ['source', s],
    ['pipeline', p],
  ] as const) {
    const tables = (
      await client.query<{ tablename: string }>(
        'SELECT tablename FROM pg_tables WHERE schemaname=$1 ORDER BY tablename',
        [schema],
      )
    ).rows;
    for (const { tablename } of tables) {
      assert.match(tablename, /^[a-z_]+$/);
      result[`${schema}.${tablename}`] = (
        await client.query<{ row: string }>(
          `SELECT to_jsonb(t)::text row FROM ${schema}.${tablename} t ORDER BY to_jsonb(t)::text COLLATE "C"`,
        )
      ).rows.map((r) => r.row);
    }
  }
  return result;
}
// Upgrade runs first: the historical database is populated before applying 002.
void test(name('B07'), async (t) => {
  const { s, p, c } = await setup(t);
  const upgrade = process.env['M41_UPGRADE'] === 'true';
  if (upgrade) {
    await mutation(s, '{"name":"before byte migration","loyalty_points":4}');
    await drain(p, c);
    const baseline = canonicalEvent({
      schema_version: 1,
      source_epoch: topology().epoch,
      entity_id: '9223372036854775807',
      entity_version: '9007199254740993',
      event_id: `${topology().epoch}:9223372036854775807:9007199254740993`,
      source_change_id: null,
      source_recorded_at: '2026-01-01T00:00:00.000000Z',
      kind: 'baseline',
      is_deleted: false,
      payload_encoding: 'pg18-jsonb-text/v1',
      payload_json: '{"fixture": "synthetic baseline, no seeding"}',
    });
    await rawPublish(baseline.wireBytes, baseline.body.event_id);
    assert.equal((await consumer().once(undefined, 1000)).acknowledged, 1);
    await rawPublish(Buffer.from('invalid-M41-upgrade'), 'M41-upgrade-poison');
    assert.equal(
      (await consumer().once(undefined, 1000)).quarantined.length,
      1,
    );
    await queueState(0);
    assert.deepEqual(
      (
        await c.query(
          'SELECT units::text FROM consumer.entity_totals WHERE entity_id=9223372036854775807',
        )
      ).rows,
      [{ units: '0' }],
    );
  }
  const before = await consumerSnapshot(c),
    outside = await outsideSnapshot(s, p);
  const catalog = () =>
    c.query(
      "SELECT oid::text,proowner::regrole::text owner,prosecdef,proconfig,proacl::text FROM pg_proc WHERE oid='consumer.process_batch(uuid,uuid,uuid,uuid,jsonb)'::regprocedure",
    );
  const oldCatalog = (await catalog()).rows;
  if (upgrade) await migrateConsumerBatch(c);
  assert.deepEqual(await consumerSnapshot(c), before);
  assert.deepEqual(await outsideSnapshot(s, p), outside);
  assert.deepEqual((await catalog()).rows, oldCatalog);
  const definition = (
    await c.query<{ body: string }>(
      "SELECT pg_get_functiondef('consumer.process_batch(uuid,uuid,uuid,uuid,jsonb)'::regprocedure) body",
    )
  ).rows[0]?.body;
  assert.ok(definition);
  assert.ok(!definition.includes('+108'));
  assert.match(definition, /octet_length\(convert_to/);
  const runtime = new pg.Client(consumerConfig('runtime', 'B07-runtime'));
  await runtime.connect();
  try {
    for (const sql of [
      'SET ROLE consumer_owner',
      'ALTER FUNCTION consumer.process_batch(uuid,uuid,uuid,uuid,jsonb) OWNER TO consumer_runtime',
      'UPDATE consumer.identity SET consumer_id=gen_random_uuid()',
      definition,
    ])
      await assert.rejects(runtime.query(sql), { code: '42501' });
  } finally {
    await runtime.end();
  }
  evidence('B07', {
    upgrade,
    consumerBefore: before,
    consumerAfter: await consumerSnapshot(c),
    outsideBefore: outside,
    outsideAfter: await outsideSnapshot(s, p),
    catalog: oldCatalog,
    migrationSha256: createHash('sha256')
      .update(
        await readFile('migrations/consumer/002-batch-byte-accounting.sql'),
      )
      .digest('hex'),
    runtimeDenied: 4,
  });
});
async function boundary(
  t: TestContext,
  id: 'B02' | 'B06' | 'B06H',
  kill: boolean,
) {
  const { s, p, c } = await setup(t),
    group = await cohort(
      s,
      p,
      c,
      Array.from({ length: 32 }, () => 32768),
    );
  const sizes = await sqlBytes(c, group);
  assert.equal(sizes.wireBytes, cap);
  const outside = await outsideSnapshot(s, p);
  await publishCohort(group);
  await queueState(32);
  const child = launchBatch(t, id, 32, true),
    before = await child.barrier('before_commit');
  const transaction = await databaseWaitFor(
    c,
    async () =>
      (
        await c.query<Record<string, unknown>>(
          "SELECT pid,usename,state,backend_xid::text,application_name FROM pg_stat_activity WHERE application_name=$1 AND usename='consumer_runtime'",
          [child.applicationName],
        )
      ).rows,
    (r) => r.length === 1 && r[0]?.['state'] === 'idle in transaction',
    `${id} actual SQL transaction`,
  );
  const transactionId = transaction[0]?.['backend_xid'];
  assert.ok(typeof transactionId === 'string');
  await assertCohort(s, c, group, false);
  assert.deepEqual(object(before['trace'])['acknowledgements'], []);
  child.release('before_commit');
  const committed = await child.barrier('after_commit');
  assert.deepEqual(object(committed['trace'])['acknowledgements'], []);
  const rows = await assertCohort(s, c, group, true);
  const batchList = object(committed['trace'])['batches'];
  assert.ok(Array.isArray(batchList));
  assert.equal(batchList.length, 1);
  assert.equal(object(batchList[0])['count'], 32);
  assert.equal(object(batchList[0])['wireBytes'], cap);
  assert.equal(object(batchList[0])['completion'], 'confirmed_commit');
  const xids = (
    await c.query<{ xid: string }>(
      'SELECT DISTINCT xmin::text xid FROM consumer.processed_events WHERE event_id=ANY($1::text[])',
      [group.events.map((e) => e.body.event_id)],
    )
  ).rows;
  assert.deepEqual(xids, [{ xid: transactionId }]);
  let exit, replay;
  if (kill) {
    exit = await child.finish('kill');
    await queueState(32);
    replay = await consumeCohort();
    assertTrace(replay.trace, 32, 32, cap);
    assert.ok(replay.trace.received.every((m) => m.redelivered));
    assert.ok(
      replay.trace.batches[0]?.statuses?.every(
        (x) => x === 'already_processed',
      ),
    );
  } else {
    child.release('after_commit');
    exit = await child.finish('run');
    const complete = exit.messages.find((m) => m['type'] === 'batch-complete');
    assert.ok(complete);
    const trace = object(complete['trace']);
    const a = trace['acknowledgements'];
    assert.ok(Array.isArray(a));
    assert.equal(a.length, 32);
    assert.ok(a.every((v) => object(v)['multiple'] === false));
  }
  await queueState(0);
  assert.deepEqual(await assertCohort(s, c, group, true), rows);
  await databaseWaitFor(
    c,
    async () =>
      (
        await c.query<{ pid: number }>(
          'SELECT pid FROM pg_stat_activity WHERE application_name=$1',
          [child.applicationName],
        )
      ).rows,
    (r) => r.length === 0,
    'No boundary child session',
  );
  // Publication changes only its own pipeline obligations; canonical source/event content remains intact.
  const after = await outsideSnapshot(s, p);
  for (const key of Object.keys(outside).filter(
    (k) =>
      k.startsWith('source.') ||
      [
        'pipeline.events',
        'pipeline.es_attempts',
        'pipeline.es_dead_letters',
        'pipeline.es_target',
      ].includes(k),
  ))
    assert.deepEqual(after[key], outside[key]);
  evidence(id, {
    sizes,
    events: group.events.map((e) => e.body.event_id),
    transaction,
    xids,
    before,
    committed,
    exit,
    replay,
    exactContentCompared: true,
  });
}
void test(name('B02'), (t) => boundary(t, 'B02', false));
void test(name('B03'), async (t) => {
  const { s, p, c } = await setup(t),
    group = await cohort(s, p, c, [
      ...Array.from({ length: 31 }, () => 32768),
      32769,
    ]);
  const sizes = await sqlBytes(c, group);
  assert.equal(sizes.wireBytes, cap + 1);
  const before = await consumerSnapshot(c),
    rejected = await directRejection(group, 'B03-over-limit');
  assert.deepEqual(await consumerSnapshot(c), before);
  await publishCohort(group);
  const first = await consumeCohort(32);
  assertTrace(first.trace, 31, 31, 31 * 32768);
  assert.equal(first.trace.received.length, 32);
  await queueState(1);
  const second = await consumeCohort(1);
  assertTrace(second.trace, 1, 1, 32769);
  assert.equal(second.trace.received[0]?.redelivered, true);
  await queueState(0);
  await assertCohort(s, c, group);
  evidence('B03', { sizes, rejected, first, second, allContentCompared: true });
});
void test(name('B04'), async (t) => {
  const { s, p, c } = await setup(t),
    group = await cohort(
      s,
      p,
      c,
      Array.from({ length: 33 }, () => 2048),
    );
  const before = await consumerSnapshot(c);
  const rejected = await directRejection(group, 'B04-too-many');
  assert.deepEqual(await consumerSnapshot(c), before);
  await publishCohort(group);
  const first = await consumeCohort(33);
  assertTrace(first.trace, 32, 32, 32 * 2048);
  assert.equal(first.trace.received.length, 33);
  await queueState(1);
  const second = await consumeCohort(1);
  assertTrace(second.trace, 1, 1, 2048);
  await queueState(0);
  await assertCohort(s, c, group);
  evidence('B04', { rejected, first, second, allContentCompared: true });
});
void test(name('B05'), async (t) => {
  const { s, p, c } = await setup(t);
  const synthetic = [];
  for (const delta of [-1, 0, 1]) {
    const items: CanonicalEvent[] = [];
    const sizes = Array.from(
      { length: 32 },
      (_, i) => 32768 + (i % 2 === 0 ? -4096 : 4096),
    );
    sizes[31] = (sizes[31] ?? 0) + delta;
    for (let i = 0; i < 32; i++) {
      const id = (9223372036854775807n - BigInt(i + 100)).toString();
      const text = async (n: number) => {
        const r = (
          await c.query<{ payload: string }>(
            `SELECT jsonb_build_object('text',$1::text,'precise',9007199254740993::numeric,'decimal',0.12345678901234567890123456789::numeric,'padding',repeat('x',$2))::text payload`,
            ['ქართული 🙂 "quote" \\ slash', n],
          )
        ).rows[0];
        assert.ok(r);
        return r.payload;
      };
      const make = (payload_json: string) =>
        canonicalEvent({
          schema_version: 1,
          source_epoch: topology().epoch,
          entity_id: id,
          entity_version: '9007199254740993',
          event_id: `${topology().epoch}:${id}:9007199254740993`,
          source_change_id: null,
          source_recorded_at: '2026-01-01T00:00:00.000000Z',
          kind: 'baseline',
          is_deleted: false,
          payload_encoding: 'pg18-jsonb-text/v1',
          payload_json,
        });
      const empty = make(await text(0)),
        size = sizes[i];
      assert.ok(size);
      const e = make(await text(size - empty.wireBytes.length));
      assert.equal(e.wireBytes.length, size);
      items.push(e);
    }
    const group = { ids: [], events: items },
      measured = await sqlBytes(c, group);
    assert.equal(measured.wireBytes, cap + delta);
    const before = await consumerSnapshot(c);
    const db = new ConsumerDatabase(consumerConfig('runtime', 'B05-synthetic'));
    let result: unknown;
    if (delta === 1) result = await directRejection(group, 'B05-above');
    else {
      const marker = new Error(
        'Explicit rollback of synthetic representation-only fixture',
      );
      await assert.rejects(
        db.transaction(async (tx) => {
          const rows = await tx.process(topology(), items);
          assert.equal(rows.length, 32);
          assert.ok(rows.every((r) => r.status === 'processed'));
          result = rows;
          throw marker;
        }),
        (e) =>
          e instanceof TransactionError &&
          e.outcome === 'rolled_back' &&
          e.cause === marker,
      );
    }
    assert.deepEqual(await consumerSnapshot(c), before);
    synthetic.push({
      delta,
      measured,
      result,
      syntheticBaseline: true,
      retained: false,
    });
  }
  const group = await cohort(
    s,
    p,
    c,
    Array.from({ length: 16 }, () => 32768),
    true,
  );
  await publishCohort(group);
  const pub = new Publisher(amqpConfig(), metadata());
  const duplicate = await pub.publish(
    topology(),
    group.events.map((e) => ({
      eventId: e.body.event_id,
      attemptId: randomUUID(),
      wire: e.wireBytes,
    })),
  );
  assert.ok(duplicate.every((r) => r.outcome === 'confirmed'));
  const result = await consumeCohort(32);
  assertTrace(result.trace, 32, 16, cap);
  assert.equal(result.trace.batches[0]?.wireBytes, cap / 2);
  await queueState(0);
  await assertCohort(s, c, group);
  evidence('B05', {
    synthetic,
    duplicate,
    result,
    originalBytes: cap,
    uniqueBytes: cap / 2,
    allContentCompared: true,
  });
});
void test(name('B06'), (t) => boundary(t, 'B06', true));
void test(name('B06H'), (t) => boundary(t, 'B06H', false));
void test(name('B08'), async (t) => {
  const { s, p, c } = await setup(t),
    group = await cohort(
      s,
      p,
      c,
      Array.from({ length: 32 }, () => 32768),
    );
  await publishCohort(group);
  const trace = batchTrace(),
    restore = installBatchTiming(trace, [32, 3]),
    worker = observedWorker(trace);
  try {
    const first = await worker.once(undefined, 10000);
    assert.equal(first.received, 32);
    assert.equal(first.acknowledged, 32);
    await queueState(0);
    await assertCohort(s, c, group);
    // Same Consumer object, no external restart, new source commands and ordinary bounded capture.
    const small = [];
    for (let i = 0; i < 3; i++) small.push(await mutation(s));
    const { drain: capture } = await import('../support/capture.ts');
    await capture('B08-small');
    const { publisher } = await import('../support/rabbit.ts');
    assert.equal((await publisher().once()).claimed, 3);
    const second = await worker.once(undefined, 10000);
    assert.equal(second.received, 3);
    assert.equal(second.acknowledged, 3);
    assert.deepEqual(second.quarantined, []);
    assert.equal(trace.batches.length, 2);
    assert.equal(trace.batches[0]?.wireBytes, cap);
    assert.ok((trace.batches[1]?.wireBytes ?? cap) < cap);
    for (const m of small) {
      const rows = (
        await c.query<{ body: string; units: string }>(
          "SELECT convert_from(p.body_bytes,'UTF8') body,t.units::text FROM consumer.processed_events p JOIN consumer.entity_totals t USING(source_epoch,entity_id) WHERE event_id=$1",
          [m.eventId],
        )
      ).rows;
      assert.equal(rows.length, 1);
      assert.equal(rows[0]?.units, '1');
      const { expected } = await import('../support/rabbit.ts');
      assert.equal(rows[0]?.body, (await expected(s, m.id, '1')).body);
    }
    await queueState(0);
    evidence('B08', {
      first,
      second,
      trace,
      small: small.map((m) => m.eventId),
      sameConsumerObject: true,
    });
  } finally {
    restore();
  }
});
