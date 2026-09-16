import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import pg from 'pg';
import { command } from '../../scripts/support.ts';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import { canonicalEvent } from '../../src/envelope.ts';
import { SourceReader } from '../../src/source-reader.ts';
import {
  IntegrityError,
  stageSelected,
  type PipelineWork,
} from '../../src/pipeline.ts';
import {
  TransactionError,
  OwnershipError,
} from '../../src/internal/transaction.ts';
import { limits, LimitError } from '../../src/limits.ts';
import { evidence, required, sourceOwner } from '../support/db.ts';
import { execute, request, sqlReject, counts } from '../support/commands.ts';
import {
  setup,
  fixture,
  reader,
  pipeline,
  oracle,
  snapshot,
  gate,
  blocked,
  rawInsert,
  config,
  pconnect,
} from '../support/staging.ts';

void test('S02 real precision and coherent current/outbox parity', async (t) => {
  const { s, p, epoch } = await setup(t);
  await s.query(
    "SELECT setval('source.entities_entity_id_seq',9007199254740993,false)",
  );
  const f = await fixture(
    epoch,
    '{"n":9007199254740993,"d":0.123456789012345678901234567890,"a":[null,1,2],"u":"ქართული 😀"}',
  );
  assert.equal(f.key.entityId, '9007199254740993');
  const current = first((await reader().current(f.key.entityId)).events);
  assert.deepEqual(current, f.event);
  assert.equal(
    current.body.payload_json,
    '{"a": [null, 1, 2], "d": 0.123456789012345678901234567890, "n": 9007199254740993, "u": "ქართული 😀"}',
  );
  const noOp = await execute(
    request(
      epoch,
      'update',
      f.key.entityId,
      '{ "u":"ქართული 😀", "a":[null,1,2], "d":0.1234567890123456789012345678900,"n":9007199254740993.0}',
    ),
  );
  assert.equal(noOp.result.entity_version, '1');
  assert.deepEqual(
    first((await reader().current(f.key.entityId)).events),
    current,
  );
  await pipeline().stage([current]);
  const observed = await oracle(s, p, current);
  await assert.rejects(
    new SourceReader(
      config('reader', 'wrong-epoch'),
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    ).outbox([f.key]),
    /Owned transaction failed/,
  );
  assert.ok(current.body.payload_json);
  const differentNumericText = canonicalEvent({
    ...current.body,
    payload_json: current.body.payload_json.replace(
      '9007199254740993',
      '9007199254740993.0',
    ),
  });
  await assert.rejects(
    pipeline().stage([differentNumericText]),
    (error) => error instanceof IntegrityError && error.code === 'P3001',
  );
  assert.deepEqual(await snapshot(p, [current.body.event_id]), observed);
  evidence('S02', {
    command: f.command.commandId,
    noOp: noOp.result,
    canonical: current.bodyBytes.toString(),
    hash: current.contentSha256,
    observed,
  });
});
void test('S03 historical outbox revisions stage independently out of order', async (t) => {
  const { s, p, epoch } = await setup(t);
  const f = await fixture(epoch);
  const updated = await execute(
    request(epoch, 'update', f.key.entityId, '{"historical":2}'),
  );
  const deleted = await execute(request(epoch, 'delete', f.key.entityId, null));
  const old = first((await reader().outbox([f.key])).events);
  assert.deepEqual(old, f.event);
  const all = (
    await reader().outbox([
      f.key,
      { entityId: f.key.entityId, version: updated.result.entity_version },
      { entityId: f.key.entityId, version: deleted.result.entity_version },
    ])
  ).events;
  for (const event of [...all].reverse()) await pipeline().stage([event]);
  for (const event of all) await oracle(s, p, event);
  assert.deepEqual(
    all.map((e) => e.body.entity_version),
    ['1', '2', '3'],
  );
  evidence('S03', {
    observed: await snapshot(
      p,
      all.map((e) => e.body.event_id),
    ),
  });
});
void test('S04 initial staging commits one complete immutable obligation set', async (t) => {
  const { s, p, epoch } = await setup(t);
  const f = await fixture(epoch);
  const owner = pipeline();
  let expired: PipelineWork | undefined;
  const results = await owner.transaction(async (tx) => {
    expired = tx;
    return tx.stage([f.event]);
  });
  assert.deepEqual(results, [
    { eventId: f.event.body.event_id, status: 'inserted' },
  ]);
  assert.ok(expired);
  await assert.rejects(expired.stage([f.event]), OwnershipError);
  assert.deepEqual(await owner.stage([f.event]), [
    { eventId: f.event.body.event_id, status: 'already_staged' },
  ]);
  const cli = await command(
    process.execPath,
    ['scripts/stage.ts', `${f.key.entityId}:${f.key.version}`],
    {
      PATH: process.env['PATH'],
      SOURCE_EPOCH: epoch,
      SOURCE_READER_HOST: '127.0.0.1',
      SOURCE_READER_PORT: required('M1_PORT'),
      SOURCE_READER_PASSWORD: required('SOURCE_READER_PASSWORD'),
      PIPELINE_STAGER_HOST: '127.0.0.1',
      PIPELINE_STAGER_PORT: required('PIPELINE_PORT'),
      PIPELINE_STAGER_PASSWORD: required('PIPELINE_STAGER_PASSWORD'),
    },
    10000,
    true,
    {
      secrets: [
        required('SOURCE_READER_PASSWORD'),
        required('PIPELINE_STAGER_PASSWORD'),
      ],
    },
  );
  assert.equal(cli.code, 0);
  assert.equal(cli.signal, null);
  assert.equal(cli.timedOut, false);
  assert.equal(cli.outputOverflow, false);
  assert.deepEqual(cli.cleanupErrors, []);
  assert.equal(cli.stderr, '');
  assert.deepEqual(JSON.parse(cli.stdout), {
    notVisible: [],
    results: [{ eventId: f.event.body.event_id, status: 'already_staged' }],
  });
  evidence('S04', { results, observed: await oracle(s, p, f.event) });
});
void test('S05 batch repeated and concurrent duplicates preserve original evidence', async (t) => {
  const { s, p, epoch } = await setup(t);
  const f = await fixture(epoch);
  const reached = gate(),
    release = gate();
  t.after(() => release.release());
  const winner = pipeline('duplicate-winner').transaction(async (tx) => {
    const result = await tx.stage([f.event, f.event]);
    reached.release();
    await release.promise;
    return result;
  });
  void winner.catch(() => undefined);
  await reached.promise;
  const loser = pipeline('duplicate-loser').stage([f.event]);
  void loser.catch(() => undefined);
  const contention = await blocked(p, 'duplicate-loser', 'duplicate-winner');
  assert.deepEqual(await snapshot(p, [f.event.body.event_id]), {
    events: [],
    deliveries: [],
    observations: [],
  });
  release.release();
  assert.equal(first(await winner).status, 'inserted');
  assert.equal(first(await loser).status, 'already_staged');
  const before = await oracle(s, p, f.event);
  for (let i = 0; i < 4; i++)
    assert.equal(
      first(await pipeline().stage([f.event, f.event])).status,
      'already_staged',
    );
  assert.deepEqual(await snapshot(p, [f.event.body.event_id]), before);
  const takeover = await fixture(epoch),
    held = gate(),
    rollback = gate();
  t.after(() => rollback.release());
  const firstAttempt = pipeline('rollback-holder').transaction(async (tx) => {
    await tx.stage([takeover.event]);
    held.release();
    await rollback.promise;
    throw new Error('Deliberate rollback');
  });
  void firstAttempt.catch(() => undefined);
  await held.promise;
  const waiting = pipeline('rollback-waiter').stage([takeover.event]);
  void waiting.catch(() => undefined);
  const takeoverContention = await blocked(
    p,
    'rollback-waiter',
    'rollback-holder',
  );
  rollback.release();
  await assert.rejects(
    firstAttempt,
    (error) =>
      error instanceof TransactionError && error.outcome === 'rolled_back',
  );
  assert.equal(first(await waiting).status, 'inserted');
  await oracle(s, p, takeover.event);
  evidence('S05-takeover', {
    takeoverContention,
    observed: await snapshot(p, [takeover.event.body.event_id]),
  });
  evidence('S05', {
    contention,
    before,
    after: await snapshot(p, [f.event.body.event_id]),
  });
});
void test('S06 conflicts roll back batches and never repair corrupt evidence', async (t) => {
  const { s, p, epoch } = await setup(t);
  const fresh = await fixture(epoch);
  const f = await fixture(epoch);
  await pipeline().stage([f.event]);
  const before = await oracle(s, p, f.event);
  const sourceBefore = await counts(s);
  const conflict = canonicalEvent({
    ...f.event.body,
    payload_json: '{"changed": true}',
  });
  await assert.rejects(
    pipeline().stage([fresh.event, conflict]),
    IntegrityError,
  );
  assert.deepEqual(await snapshot(p, [fresh.event.body.event_id]), {
    events: [],
    deliveries: [],
    observations: [],
  });
  for (const event of [
    canonicalEvent({
      ...f.event.body,
      source_recorded_at: '2000-01-01T00:00:00.000000Z',
    }),
    conflict,
  ])
    await assert.rejects(pipeline().stage([event]), IntegrityError);
  await assert.rejects(
    pipeline().stage([
      fresh.event,
      canonicalEvent({ ...fresh.event.body, payload_json: '{"other": 1}' }),
    ]),
    IntegrityError,
  );
  assert.deepEqual(await snapshot(p, [f.event.body.event_id]), before);
  assert.deepEqual(await counts(s), sourceBefore);
  // Coordinated competing different bodies. Unique-key loser must read the committed winner.
  const c = await fixture(epoch),
    reached = gate(),
    release = gate();
  t.after(() => release.release());
  const win = pipeline('conflict-winner').transaction(async (tx) => {
    await tx.stage([c.event]);
    reached.release();
    await release.promise;
  });
  void win.catch(() => undefined);
  await reached.promise;
  const lose = pipeline('conflict-loser').stage([
    canonicalEvent({ ...c.event.body, payload_json: '{"different": true}' }),
  ]);
  void lose.catch(() => undefined);
  const contention = await blocked(p, 'conflict-loser', 'conflict-winner');
  release.release();
  await win;
  await assert.rejects(lose, IntegrityError);
  await oracle(s, p, c.event);
  // Privileged corruption is explicitly outside the runtime guarantee. Preserve exact row for restoration.
  await p.query(
    'ALTER TABLE pipeline.consumer_observations DISABLE TRIGGER immutable_rows',
  );
  try {
    await p.query(
      'DELETE FROM pipeline.consumer_observations WHERE event_id=$1',
      [f.event.body.event_id],
    );
  } finally {
    await p.query(
      'ALTER TABLE pipeline.consumer_observations ENABLE TRIGGER immutable_rows',
    );
  }
  const corrupt = await snapshot(p, [f.event.body.event_id]);
  await assert.rejects(
    pipeline().stage([f.event]),
    (error: unknown) =>
      error instanceof IntegrityError && error.code === 'P3002',
  );
  assert.deepEqual(await snapshot(p, [f.event.body.event_id]), corrupt);
  // Diagnostic failure stays secondary to the original conflict and cannot turn it into success.
  await p.query(
    'REVOKE EXECUTE ON FUNCTION pipeline.record_incident(text,text,text) FROM pipeline_stager',
  );
  try {
    await assert.rejects(pipeline().stage([conflict]), (error: unknown) => {
      assert.ok(error instanceof IntegrityError);
      assert.equal(error.code, 'P3001');
      assert.ok(error.primary instanceof TransactionError);
      assert.equal(error.primary.outcome, 'rolled_back');
      assert.ok(error.diagnosticFailure instanceof TransactionError);
      return true;
    });
  } finally {
    await p.query(
      'GRANT EXECUTE ON FUNCTION pipeline.record_incident(text,text,text) TO pipeline_stager',
    );
    await p.query(
      'INSERT INTO pipeline.consumer_observations(event_id,state,created_at) VALUES ($1,$2,$3)',
      [
        f.event.body.event_id,
        first(before.observations)['state'],
        first(before.observations)['created_at'],
      ],
    );
  }
  const incidents = (
    await p.query(
      'SELECT event_id,code,observed_sha256 FROM pipeline.integrity_incidents ORDER BY recorded_at',
    )
  ).rows;
  assert.ok(incidents.length >= 6);
  evidence('S06', {
    contention,
    incidents,
    unchanged: before,
    corruptDetected: corrupt,
  });
});
void test('S07 SQL constraints and actual restricted credentials enforce the boundary', async (t) => {
  const { s, p, epoch } = await setup(t);
  const f = await fixture(epoch);
  const otherEpoch = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  const wrongBinding = canonicalEvent({
    ...f.event.body,
    source_epoch: otherEpoch,
    event_id: `${otherEpoch}:${f.key.entityId}:${f.key.version}`,
  });
  await assert.rejects(
    pipeline().stage([wrongBinding]),
    (error) => error instanceof IntegrityError && error.code === 'P3002',
  );
  assert.deepEqual(await snapshot(p, [wrongBinding.body.event_id]), {
    events: [],
    deliveries: [],
    observations: [],
  });
  for (const missing of ['elasticsearch', 'rabbitmq', 'consumer']) {
    await p.query('BEGIN');
    await rawInsert(p, f.event);
    await p.query(
      'INSERT INTO pipeline.delivery_intents(event_id,kind,destination_id) SELECT $1,kind,destination_id FROM pipeline.destinations WHERE kind<>$2',
      [f.event.body.event_id, missing],
    );
    if (missing !== 'consumer')
      await p.query(
        'INSERT INTO pipeline.consumer_observations(event_id) VALUES ($1)',
        [f.event.body.event_id],
      );
    await sqlReject(p, 'COMMIT', 'P3002');
    assert.deepEqual(await snapshot(p, [f.event.body.event_id]), {
      events: [],
      deliveries: [],
      observations: [],
    });
  }
  for (const overrides of [
    { entity_id: '123' },
    { source_recorded_at: '2000-01-01T00:00:00.000000Z' },
    { is_deleted: true },
    { content_sha256: 'a'.repeat(64) },
    { is_deleted: null },
    { body_bytes: Buffer.from('null') },
    { source_change_id: null },
  ])
    await assert.rejects(
      rawInsert(p, f.event, overrides),
      (error) =>
        error instanceof pg.DatabaseError &&
        ['23514', '23502'].includes(error.code ?? ''),
    );
  // Direct SQL (not the application validator) rejects noncanonical bytes and invalid payload representations.
  for (const event of [
    canonicalEvent({ ...f.event.body, payload_json: '[]' }),
    canonicalEvent({ ...f.event.body, payload_json: '{"x":1}' }),
    canonicalEvent({ ...f.event.body, payload_json: 'not json' }),
  ])
    await assert.rejects(
      rawInsert(p, event),
      (error) => error instanceof pg.DatabaseError && error.code === '23514',
    );
  const bytes = Buffer.from(JSON.stringify(f.event.body));
  await assert.rejects(
    rawInsert(p, f.event, {
      body_bytes: bytes,
      content_sha256: createHash('sha256').update(bytes).digest('hex'),
    }),
    (error) => error instanceof pg.DatabaseError && error.code === '23514',
  );
  await pipeline().stage([f.event]);
  const destination = first(
    (
      await p.query<{ destination_id: string }>(
        "SELECT destination_id FROM pipeline.destinations WHERE kind='rabbitmq'",
      )
    ).rows,
  ).destination_id;
  await sqlReject(
    p,
    'INSERT INTO pipeline.delivery_intents(event_id,kind,destination_id) VALUES ($1,$2,$3)',
    '23505',
    [f.event.body.event_id, 'rabbitmq', destination],
  );
  const another = await fixture(epoch);
  await p.query('BEGIN');
  await rawInsert(p, another.event);
  await sqlReject(
    p,
    'INSERT INTO pipeline.delivery_intents(event_id,kind,destination_id) VALUES ($1,$2,$3)',
    '23503',
    [another.event.body.event_id, 'elasticsearch', destination],
  );
  await p.query('ROLLBACK');
  const read = new pg.Client(config('reader', 'readonly-proof'));
  await read.connect();
  t.after(() => read.end());
  await read.query('SET default_transaction_read_only=off');
  for (const sql of [
    'SELECT * FROM source.command_receipts',
    'SELECT allocation_id FROM source.outbox',
    'UPDATE source.entities SET payload=payload',
    'DELETE FROM source.outbox',
    "SELECT source.create_entity('{}'::jsonb)",
    "SELECT source.execute_command(NULL,NULL,1,'create',NULL,'{}')",
    'SET ROLE source_owner',
    'ALTER TABLE source.entities DISABLE TRIGGER ALL',
  ])
    await sqlReject(read, sql, '42501');
  const runtime = await pconnect('restricted', 'stager');
  t.after(() => runtime.end());
  for (const sql of [
    'SELECT * FROM pipeline.events',
    'DELETE FROM pipeline.events',
    "UPDATE pipeline.delivery_intents SET state='complete'",
    "UPDATE pipeline.consumer_observations SET state='complete'",
    "UPDATE pipeline.destinations SET receiver_identity='fabricated'",
    "INSERT INTO pipeline.events(event_id) VALUES ('forged')",
    'SET ROLE pipeline_owner',
    'SET session_replication_role=replica',
    'TRUNCATE pipeline.events CASCADE',
  ])
    await sqlReject(runtime, sql, '42501');
  await sqlReject(
    p,
    'UPDATE pipeline.events SET body_bytes=body_bytes',
    'P3002',
  );
  const catalog = (
    await p.query<{
      name: string;
      owner: string;
      definer: boolean;
      config: string[];
      public_execute: boolean;
    }>(
      `SELECT proname AS name,pg_get_userbyid(proowner) AS owner,prosecdef AS definer,proconfig AS config,has_function_privilege('public',p.oid,'EXECUTE') AS public_execute FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pipeline' ORDER BY proname`,
    )
  ).rows;
  assert.deepEqual(
    catalog.map((r) => r.name),
    [
      'assert_obligations',
      'check_obligations',
      'immutable',
      'record_incident',
      'stage_event',
      'valid_body',
    ],
  );
  for (const row of catalog) {
    assert.equal(row.owner, 'pipeline_owner');
    assert.equal(row.public_execute, false);
    assert.deepEqual(row.config, ['search_path=pg_catalog, pg_temp']);
  }
  assert.deepEqual(
    catalog.filter((r) => r.definer).map((r) => r.name),
    ['record_incident', 'stage_event'],
  );
  evidence('S07', {
    catalog,
    observed: await oracle(s, p, f.event),
    constraints:
      'missing each required obligation fails deferred COMMIT; direct consistency/NULL/FK/duplicate checks rejected',
  });
});
void test('S11 selected uncommitted revisions remain invisible until source commit', async (t) => {
  const { s, p, epoch } = await setup(t);
  const reached = gate(),
    release = gate();
  t.after(() => release.release());
  let key: { entityId: string; version: string } | undefined;
  const pending = sourceOwner('visibility-command', 'command').transaction(
    async (tx) => {
      const reply = await tx.command(request(epoch));
      key = {
        entityId: reply.result.entity_id,
        version: reply.result.entity_version,
      };
      reached.release();
      await release.promise;
    },
  );
  void pending.catch(() => undefined);
  await reached.promise;
  assert.ok(key);
  const invisible = await stageSelected(reader(), pipeline(), [key]);
  assert.deepEqual(invisible, {
    notVisible: [`${epoch}:${key.entityId}:${key.version}`],
    results: [],
  });
  release.release();
  await pending;
  await execute(request(epoch, 'delete', key.entityId, null));
  const event = first((await reader().outbox([key])).events);
  assert.equal(event.body.is_deleted, false);
  const result = await stageSelected(reader(), pipeline(), [key]);
  assert.equal(first(result.results).status, 'inserted');
  evidence('S11', { invisible, result, observed: await oracle(s, p, event) });
});
void test('S12 bounded transfers and several explicit pages retain all source records', async (t) => {
  const { s, p, epoch } = await setup(t);
  const f = await fixture(epoch);
  assert.throws(
    () =>
      reader().outbox(Array.from({ length: limits.records + 1 }, () => f.key)),
    LimitError,
  );
  await assert.rejects(
    pipeline().stage(Array.from({ length: limits.records + 1 }, () => f.event)),
    (error) =>
      error instanceof TransactionError && error.cause instanceof LimitError,
  );
  await assert.rejects(
    pipeline().transaction(async (tx) => {
      await tx.stage([f.event]);
      await tx.stage([f.event]);
    }),
    (error) =>
      error instanceof TransactionError &&
      error.outcome === 'rolled_back' &&
      error.cause instanceof OwnershipError,
  );
  assert.deepEqual(await snapshot(p, [f.event.body.event_id]), {
    events: [],
    deliveries: [],
    observations: [],
  });
  const large = await execute(
    request(
      epoch,
      'create',
      null,
      `{"large":"${'x'.repeat(limits.recordBytes)}"}`,
    ),
  );
  const largeKey = { entityId: large.result.entity_id, version: '1' };
  // eslint-disable-next-line @typescript-eslint/unbound-method -- preserve the actual driver's explicit receiver below
  const query = pg.Client.prototype.query;
  let projections = 0;
  const observation = t.mock.method(
    pg.Client.prototype,
    'query',
    function (this: pg.Client, ...args: unknown[]) {
      const returned: unknown = Reflect.apply(query, this, args);
      assert.ok(returned instanceof Promise);
      return returned.then((result: unknown) => {
        if (
          typeof args[0] === 'string' &&
          args[0].includes('WITH selected AS MATERIALIZED')
        ) {
          const rows = object(result)['rows'];
          assert.ok(Array.isArray(rows));
          for (const raw of rows) {
            const row = object(raw);
            assert.equal(row['fits'], false);
            assert.equal(row['payload_json'], null);
            projections++;
          }
        }
        return result;
      });
    },
  );
  await assert.rejects(
    reader().outbox([largeKey]),
    (error) =>
      error instanceof TransactionError && error.cause instanceof LimitError,
  );
  observation.mock.restore();
  assert.equal(
    projections,
    1,
    'Oversized payload was withheld by PostgreSQL before client transfer',
  );
  const sqlSize = first(
    (
      await s.query<{ bytes: number; revisions: string }>(
        'SELECT octet_length(payload::text) AS bytes,(SELECT count(*)::text FROM source.outbox WHERE entity_id=$1) AS revisions FROM source.entities WHERE entity_id=$1',
        [largeKey.entityId],
      )
    ).rows,
  );
  assert.ok(sqlSize.bytes > limits.recordBytes);
  assert.equal(sqlSize.revisions, '1');
  const medium = [];
  for (let i = 0; i < 6; i++)
    medium.push(await fixture(epoch, `{"medium":"${'y'.repeat(48000)}"}`));
  await assert.rejects(
    reader().outbox(medium.map((f) => f.key)),
    (error) =>
      error instanceof TransactionError && error.cause instanceof LimitError,
  );
  await assert.rejects(
    pipeline().stage(medium.map((f) => f.event)),
    (error) =>
      error instanceof TransactionError && error.cause instanceof LimitError,
  );
  assert.deepEqual(
    await snapshot(p, [
      ...medium.map((f) => f.event.body.event_id),
      `${epoch}:${largeKey.entityId}:1`,
    ]),
    { events: [], deliveries: [], observations: [] },
  );
  const small = [];
  for (let i = 0; i < 25; i++)
    small.push(await fixture(epoch, `{"pageItem":${i}}`));
  const pages = [];
  for (let offset = 0; offset < small.length; offset += 7)
    pages.push(
      await stageSelected(
        reader(),
        pipeline(),
        small.slice(offset, offset + 7).map((f) => f.key),
      ),
    );
  for (const f of small) await oracle(s, p, f.event);
  assert.equal(pages.flatMap((p) => p.results).length, 25);
  evidence('S12', {
    limits,
    oversizedRetained: { ...largeKey, ...sqlSize },
    transferRejected: medium.map((f) => f.key),
    pages,
    restart:
      'orchestrator independently compares both databases after retained-volume restart',
  });
});
void test('S13 destinations remain unbound and every obligation remains pending', async (t) => {
  const { s, p } = await setup(t);
  const destinations = (
    await p.query(
      'SELECT kind,generation::text,state,receiver_identity FROM pipeline.destinations ORDER BY kind',
    )
  ).rows;
  assert.deepEqual(destinations, [
    {
      kind: 'elasticsearch',
      generation: '1',
      state: 'unbound',
      receiver_identity: null,
    },
    {
      kind: 'rabbitmq',
      generation: '1',
      state: 'unbound',
      receiver_identity: null,
    },
  ]);
  const states = (
    await p.query(
      `SELECT 'delivery' AS kind,state,count(*)::text FROM pipeline.delivery_intents GROUP BY state UNION ALL SELECT 'consumer',state,count(*)::text FROM pipeline.consumer_observations GROUP BY state ORDER BY kind`,
    )
  ).rows as Record<string, unknown>[];
  assert.equal(states.length, 2);
  for (const row of states) assert.equal(row['state'], 'pending');
  const sourceTables = (
    await s.query<{ name: string }>(
      "SELECT tablename AS name FROM pg_tables WHERE schemaname='source' ORDER BY tablename",
    )
  ).rows.map((r) => r.name);
  assert.deepEqual(sourceTables, [
    'command_receipts',
    'entities',
    'outbox',
    'source_identity',
  ]);
  const outboxColumns = (
    await s.query<{ name: string }>(
      "SELECT column_name AS name FROM information_schema.columns WHERE table_schema='source' AND table_name='outbox' ORDER BY ordinal_position",
    )
  ).rows.map((r) => r.name);
  assert.deepEqual(outboxColumns, [
    'allocation_id',
    'source_epoch',
    'entity_id',
    'entity_version',
    'change_id',
    'recorded_at',
    'is_deleted',
    'payload',
  ]);
  evidence('S13', {
    destinations,
    states,
    sourceTables,
    outboxColumns,
    scope:
      'Only the two PostgreSQL endpoints are used; no receiver integration, source ACK, or consumer implementation exists',
  });
});
void test('S-UPGRADE forward reader migration preserves the selected installation fixture', async (t) => {
  const { s } = await setup(t);
  const report = object(
    JSON.parse(
      await readFile(
        join(required('M1_ARTIFACT_DIR'), 'upgrade-evidence.json'),
        'utf8',
      ),
    ),
  );
  assert.equal(report['unchanged'], true);
  assert.deepEqual(report['before'], report['after']);
  const before = object(report['before']);
  assert.ok(
    Array.isArray(before['entities']) &&
      Array.isArray(before['outbox']) &&
      Array.isArray(before['command_receipts']),
  );
  if (report['mode'] === 'populated') {
    assert.equal(before['entities'].length, 1);
    assert.equal(before['outbox'].length, 2);
    assert.equal(before['command_receipts'].length, 3);
    for (const line of before['command_receipts']) {
      assert.equal(typeof line, 'string');
      const receipt = object(JSON.parse(String(line)));
      assert.equal(receipt['completed'], true);
      const retained = first(
        (
          await s.query<{ same: boolean }>(
            'SELECT row_to_json(t)::text=$2 AS same FROM source.command_receipts t WHERE command_id=$1',
            [receipt['command_id'], line],
          )
        ).rows,
      );
      assert.equal(retained.same, true);
    }
  } else {
    assert.equal(report['mode'], 'fresh');
    assert.equal(before['entities'].length, 0);
    assert.equal(before['outbox'].length, 0);
    assert.equal(before['command_receipts'].length, 0);
  }
  evidence('S-UPGRADE', report);
});
