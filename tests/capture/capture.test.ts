import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { test, mock } from 'node:test';
import pg from 'pg';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import { Capture, CaptureFailure } from '../../src/capture.ts';
import { Pipeline, pipelineIdentity } from '../../src/pipeline.ts';
import { SourceCapture } from '../../src/source-capture.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { setup, fixture, snapshot, reader } from '../support/staging.ts';
import { execute, request, sqlReject } from '../support/commands.ts';
import { connect, databaseWaitFor, evidence, required } from '../support/db.ts';
import {
  captureConfig,
  control,
  drain,
  expiry,
  stageClaim,
  work,
  worker,
  reconcile,
} from '../support/capture.ts';

void test('IC01 atomic fresh and populated capture initialization', async (t) => {
  const { s, p, epoch } = await setup(t);
  const upgrade = object(
    JSON.parse(
      await readFile(
        join(required('M1_ARTIFACT_DIR'), 'capture-upgrade.json'),
        'utf8',
      ),
    ),
  );
  assert.equal(upgrade['incompleteSetupRejected'], 'P4001');
  assert.deepEqual(upgrade['sourceAfterMigration'], upgrade['sourceBefore']);
  const registration = object(upgrade['registration']);
  assert.equal(registration['repeatedUnchanged'], true);
  const rows = registration['rows'];
  assert.ok(Array.isArray(rows));
  assert.ok(rows.length >= 2);
  for (const r of rows) {
    const row = object(r);
    assert.equal(row['state'], 'pending');
    assert.equal(row['generation'], '0');
  }
  await drain('init-drain');
  const command = request(epoch, 'create', null, '{"workFailure":true}');
  const before = await work(s);
  await s.query(
    `CREATE FUNCTION source.test_work_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test work insertion failure' USING ERRCODE='P9004'; END $$; CREATE TRIGGER test_work_failure BEFORE INSERT ON source.capture_work FOR EACH ROW EXECUTE FUNCTION source.test_work_failure()`,
  );
  try {
    await assert.rejects(execute(command), {
      sqlState: 'P9004',
      outcome: 'rolled_back',
    });
  } finally {
    await s.query(
      'DROP TRIGGER test_work_failure ON source.capture_work; DROP FUNCTION source.test_work_failure()',
    );
  }
  assert.deepEqual(await work(s), before);
  assert.equal(
    first(
      (
        await s.query<{ n: string }>(
          'SELECT count(*)::text AS n FROM source.command_receipts WHERE command_id=$1',
          [command.commandId],
        )
      ).rows,
    ).n,
    '0',
  );
  const retry = await execute(command);
  await drain();
  await reconcile(s, p, 'IC01', [retry.result.entity_id]);
  evidence('IC01-upgrade', upgrade);
});
void test('IC02 complete immutable transfer and staged acknowledgement', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const command = request(
    epoch,
    'create',
    null,
    '{"n":9007199254740993,"d":0.123456789012345678901234567890,"a":[null,"ქართული"]}',
  );
  const created = await execute(command),
    id = created.result.entity_id;
  await execute(command);
  await execute(
    request(
      epoch,
      'update',
      id,
      '{"d":0.1234567890123456789012345678900,"n":9007199254740993.0,"a":[null,"ქართული"]}',
    ),
  );
  const old = first(
    (await reader().outbox([{ entityId: id, version: '1' }])).events,
  );
  await new Pipeline(
    captureConfig('pre-stage').pipeline,
    required('PIPELINE_ID'),
  ).stage([old]);
  const before = await snapshot(p, [old.body.event_id]);
  for (const [op, payload] of [
    ['update', '{"intermediate":1}'],
    ['delete', null],
    ['restore', '{"restored":true}'],
  ] as const)
    await execute(request(epoch, op, id, payload));
  const replies = await drain();
  assert.deepEqual(await snapshot(p, [old.body.event_id]), before);
  assert.equal((await work(s, id)).length, 4);
  assert.ok(
    replies
      .flatMap((r) => r.staged)
      .some(
        (r) => r.eventId === old.body.event_id && r.status === 'already_staged',
      ),
  );
  await reconcile(s, p, 'IC02', [id]);
});
void test('IC03 automatic selection captures late commits without a watermark', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const a = await connect('writer', 'late-a');
  t.after(() => a.end());
  await a.query('BEGIN');
  const early = first(
    (
      await a.query<{ entity_id: string }>(
        'SELECT entity_id::text FROM source.create_entity(\'{"late":"A"}\')',
      )
    ).rows,
  );
  const later = await execute(request(epoch, 'create', null, '{"late":"B"}'));
  const firstPass = await worker('late-selector').captureOnce();
  assert.ok(
    firstPass.acknowledgements.some((r) =>
      r.eventId.endsWith(`:${later.result.entity_id}:1`),
    ),
  );
  assert.equal((await work(s, early.entity_id)).length, 0);
  await a.query('COMMIT');
  await drain();
  const allocation = (
    await s.query<{ entity_id: string; allocation_id: string }>(
      'SELECT entity_id::text,allocation_id::text FROM source.outbox WHERE entity_id=ANY($1::bigint[]) ORDER BY allocation_id',
      [[early.entity_id, later.result.entity_id]],
    )
  ).rows;
  assert.equal(first(allocation).entity_id, early.entity_id);
  evidence('IC03-order', { allocation, firstPass });
  await reconcile(s, p, 'IC03', [early.entity_id, later.result.entity_id]);
});
void test('IC04 independent concurrent claimers own disjoint work', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  for (let i = 0; i < 8; i++)
    await execute(request(epoch, 'create', null, `{"claim":${i}}`));
  const a = control('claimer-a'),
    b = control('claimer-b');
  const [left, right] = await Promise.all([
    a.claim(randomUUID(), 4, 800),
    b.claim(randomUUID(), 4, 800),
  ]);
  assert.equal(left.length, 4);
  assert.equal(right.length, 4);
  assert.equal(new Set([...left, ...right].map((c) => c.entityId)).size, 8);
  assert.deepEqual(await control('third').claim(randomUUID(), 16, 800), []);
  await expiry(s, first(left));
  await drain();
  const locked = await fixture(epoch),
    unlocked = await fixture(epoch);
  const locker = await connect('admin', 'skip-locked-observer');
  t.after(() => locker.end());
  await locker.query('BEGIN');
  await locker.query(
    'SELECT work_id FROM source.capture_work WHERE entity_id=$1 FOR UPDATE',
    [locked.key.entityId],
  );
  const skipped = await worker('skip-locked-production').captureOnce();
  assert.deepEqual(
    skipped.acknowledgements.map((r) => r.eventId),
    [unlocked.event.body.event_id],
  );
  assert.equal(first(await work(s, locked.key.entityId))['state'], 'pending');
  const held = (
    await locker.query<Record<string, unknown>>(
      'SELECT pg_backend_pid() AS pid,pg_current_xact_id()::text AS xid',
    )
  ).rows;
  await locker.query('COMMIT');
  await drain();
  evidence('IC04-skip-locked', {
    held,
    skipped,
    locked: locked.key,
    unlocked: unlocked.key,
  });
  evidence('IC04-claims', { left, right });
  await reconcile(s, p, 'IC04');
});
void test('IC05 source clock renewal expiry and generation reject stale transitions', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const f = await fixture(epoch);
  const c = control('clock');
  const claim = first(await c.claim(randomUUID(), 1, 700));
  assert.equal(claim.entityId, f.key.entityId);
  const renewed = await c.renew(claim, 1200);
  assert.ok(String(renewed.lease_until) > claim.leaseUntil);
  const unchanged = await work(s, f.key.entityId);
  const wrong = { ...claim, ownerId: randomUUID() };
  for (const invalid of [wrong, { ...claim, generation: '9007199254740993' }])
    for (const operation of [
      () => c.renew(invalid, 700),
      () => c.defer(invalid, 1000),
      () => c.block(invalid),
      () => c.acknowledge(invalid, f.event.contentSha256),
    ])
      await assert.rejects(operation(), { sqlState: 'P4002' });
  assert.deepEqual(await work(s, f.key.entityId), unchanged);
  // Force an actual row-lock wait across expiration: transaction-start time would be unsafe here.
  const locker = await connect('admin', 'expiry-locker');
  t.after(() => locker.end());
  await locker.query('BEGIN');
  await locker.query(
    'SELECT work_id FROM source.capture_work WHERE entity_id=$1 FOR UPDATE',
    [claim.entityId],
  );
  const late = c.renew(claim, 1200);
  void late.catch(() => undefined);
  const waiting = await databaseWaitFor(
    s,
    async () =>
      (
        await s.query<{ pid: number; blockers: number[] }>(
          'SELECT pid,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE application_name=$1',
          [`${required('M1_RUN_ID')}:clock`],
        )
      ).rows,
    (rows) => rows.some((r) => r.blockers.length > 0),
    'renewal waiting for row lock',
  );
  const observed = await expiry(s, claim);
  await locker.query('COMMIT');
  await assert.rejects(late, { sqlState: 'P4002' });
  for (const operation of [
    () => c.defer(claim, 1000),
    () => c.block(claim),
    () => c.acknowledge(claim, f.event.contentSha256),
  ])
    await assert.rejects(operation(), { sqlState: 'P4002' });
  assert.deepEqual(await work(s, f.key.entityId), unchanged);
  const next = first(await c.claim(randomUUID(), 1, 1500));
  assert.ok(BigInt(next.generation) > BigInt(claim.generation));
  await stageClaim(next, 'clock-stage');
  const ack = await c.acknowledge(next, f.event.contentSha256);
  const terminal = await work(s, claim.entityId);
  assert.equal(
    (await c.acknowledge(next, f.event.contentSha256)).status,
    'already_acknowledged',
  );
  assert.deepEqual(await work(s, claim.entityId), terminal);
  await assert.rejects(c.acknowledge(claim, f.event.contentSha256), {
    sqlState: 'P4002',
  });
  await assert.rejects(c.acknowledge(next, '0'.repeat(64)), {
    sqlState: 'P4003',
  });
  const overflow = await fixture(epoch);
  await s.query(
    'UPDATE source.capture_work SET generation=9007199254740992 WHERE entity_id=$1',
    [overflow.key.entityId],
  );
  const exact = first(await c.claim(randomUUID(), 1, 1500));
  assert.equal(exact.generation, '9007199254740993');
  await c.defer(exact, 1);
  evidence('IC05-exact-generation', { exact });

  await s.query(
    'UPDATE source.capture_work SET generation=9223372036854775807 WHERE entity_id=$1',
    [overflow.key.entityId],
  );
  await assert.rejects(c.claim(randomUUID(), 1, 1000), { sqlState: '22003' });
  // Privileged fixture teardown restores its deliberately exhausted counter, without changing history.
  await s.query(
    'ALTER TABLE source.capture_work DISABLE TRIGGER capture_work_guard',
  );
  try {
    await s.query(
      'UPDATE source.capture_work SET generation=0 WHERE entity_id=$1',
      [overflow.key.entityId],
    );
  } finally {
    await s.query(
      'ALTER TABLE source.capture_work ENABLE TRIGGER capture_work_guard',
    );
  }
  await drain();
  evidence('IC05-clock', {
    claim,
    renewed,
    waiting,
    observed,
    next,
    ack,
    terminal,
  });
  await reconcile(s, p, 'IC05');
});
void test('IC13 pipeline instance identity is validated inside staging', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const f = await fixture(epoch);
  const cfg = captureConfig('identity');
  const identity = await pipelineIdentity(cfg.pipeline);
  assert.equal(identity.pipelineId, cfg.binding.pipelineId);
  const wrong = randomUUID();
  await assert.rejects(new Pipeline(cfg.pipeline, wrong).stage([f.event]), {
    sqlState: 'P4001',
    outcome: 'rolled_back',
  });
  assert.equal((await snapshot(p, [f.event.body.event_id])).events.length, 0);
  await assert.rejects(
    new SourceCapture(cfg.source, { ...cfg.binding, pipelineId: wrong }).claim(
      randomUUID(),
      1,
      1000,
    ),
    { sqlState: 'P4001' },
  );
  await sqlReject(s, 'SELECT source.register_capture($1,$2,$3)', 'P4001', [
    wrong,
    epoch,
    'pg18-jsonb-text/v1',
  ]);
  // A distinct empty receiver database on the existing pipeline service has the same epoch but its own immutable ID.
  const replacementName = 'replacement_' + randomUUID().replaceAll('-', '');
  await p.query(`CREATE DATABASE ${replacementName}`);
  const adminConfig = {
    ...cfg.pipeline,
    database: replacementName,
    user: 'pipeline_admin',
    password: required('PIPELINE_ADMIN_PASSWORD'),
  };
  const replacement = new pg.Client(adminConfig);
  let replacementIdentity;
  try {
    await replacement.connect();
    await replacement.query('BEGIN');
    for (const migration of ['001-staging.sql', '002-capture-instance.sql']) {
      const sql = (
        await readFile(
          new URL(`../../migrations/pipeline/${migration}`, import.meta.url),
          'utf8',
        )
      )
        .replace(/^CREATE ROLE .*;$/gm, '')
        .replaceAll(
          'ON DATABASE pipeline_m2b',
          `ON DATABASE ${replacementName}`,
        );
      await replacement.query(sql);
      if (migration === '001-staging.sql')
        await replacement.query(
          'INSERT INTO pipeline.source_binding VALUES(true,$1,$2)',
          [epoch, 'pg18-jsonb-text/v1'],
        );
    }
    await replacement.query('COMMIT');
    const replacementConfig = { ...cfg.pipeline, database: replacementName };
    replacementIdentity = await pipelineIdentity(replacementConfig);
    assert.equal(replacementIdentity.sourceEpoch, epoch);
    assert.notEqual(replacementIdentity.pipelineId, identity.pipelineId);
    await assert.rejects(
      new Capture(
        { ...cfg, pipeline: replacementConfig },
        { leaseMs: 1500, renewalMs: 150, idleMs: 100 },
      ).captureOnce(),
      (error: unknown) =>
        error instanceof CaptureFailure &&
        error.fatal &&
        error.primary instanceof TransactionError &&
        error.primary.sqlState === 'P4001',
    );
    assert.equal(first(await work(s, f.key.entityId))['state'], 'leased');
    assert.equal(
      first(
        (
          await replacement.query<{ n: string }>(
            'SELECT count(*)::text AS n FROM pipeline.events',
          )
        ).rows,
      ).n,
      '0',
    );
  } finally {
    await replacement.end();
    await p.query(`DROP DATABASE ${replacementName}`);
  }
  await drain();
  await reconcile(s, p, 'IC13');
  evidence('IC13-identity', {
    identity,
    replacementIdentity,
    rejectedWrongKey: wrong,
  });
});
void test('IC14 bounded medium batches and oversized work preserve fairness', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const bigReply = await execute(
    request(epoch, 'create', null, `{"large":"${'x'.repeat(66000)}"}`),
  );
  const big = { key: { entityId: bigReply.result.entity_id } };
  const medium = [];
  for (let i = 0; i < 6; i++)
    medium.push(
      (
        await execute(
          request(
            epoch,
            'create',
            null,
            `{"m":${i},"text":"${'m'.repeat(47000)}"}`,
          ),
        )
      ).result.entity_id,
    );
  const small = await execute(
    request(epoch, 'create', null, '{"afterLarge":true}'),
  );
  // Public driver instrumentation observes real responses; it never fabricates SQL results.
  const calls: number[] = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- private test preserves the Pipeline receiver
  const originalStage = Pipeline.prototype.stage;
  const stage = mock.method(
    Pipeline.prototype,
    'stage',
    async function (this: Pipeline, inputs: Parameters<Pipeline['stage']>[0]) {
      calls.push(inputs.length);
      return Reflect.apply(originalStage, this, [inputs]);
    },
  );
  let result;
  try {
    result = await worker('fairness').captureOnce();
  } finally {
    stage.mock.restore();
  }
  assert.deepEqual(calls, [5, 2]);
  assert.equal(result.blocked.length, 1);
  assert.equal(result.acknowledgements.length, 7);
  const blocked = first(await work(s, big.key.entityId));
  assert.equal(blocked['state'], 'blocked');
  assert.equal(blocked['acknowledged_hash'], null);
  const delayed = await fixture(epoch);
  const c = control('delay');
  const claim = first(await c.claim(randomUUID(), 1, 1000));
  await c.defer(claim, 1000);
  const empty = await worker('empty-is-not-caught-up').captureOnce();
  assert.equal(empty.claimed, 0);
  assert.equal(empty.sourceState.blocked, '1');
  assert.equal(empty.sourceState.pending_delayed, '1');
  await drain();
  await reconcile(s, p, 'IC14', [
    ...medium,
    small.result.entity_id,
    delayed.key.entityId,
    big.key.entityId,
  ]);
  evidence('IC14-limits', { calls, result, blocked, empty });
});
void test('IC16 restricted roles immutable history and frozen obligations', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain();
  const cfg = captureConfig('permissions');
  const runtime = new pg.Client(cfg.source),
    pr = new pg.Client(cfg.pipeline);
  await runtime.connect();
  await pr.connect();
  t.after(() => runtime.end());
  t.after(() => pr.end());
  for (const sql of [
    "UPDATE source.capture_work SET state='acknowledged'",
    'DELETE FROM source.capture_work',
    'TRUNCATE source.capture_work',
    'UPDATE source.capture_binding SET pipeline_id=gen_random_uuid()',
    'SELECT * FROM source.command_receipts',
    'SELECT * FROM source.entities',
    "SELECT source.create_entity('{}')",
    "SELECT source.mutate_entity(1,'delete',NULL)",
    "SELECT source.register_capture(gen_random_uuid(),gen_random_uuid(),'pg18-jsonb-text/v1')",
    'SET ROLE source_owner',
    'ALTER TABLE source.outbox DISABLE TRIGGER ALL',
    "SELECT setval('source.capture_work_work_id_seq',1)",
  ])
    await assert.rejects(runtime.query(sql), { code: '42501' });
  for (const sql of [
    'UPDATE pipeline.source_binding SET pipeline_id=gen_random_uuid()',
    'SELECT pipeline.stage_event(NULL,NULL)',
    'DELETE FROM pipeline.events',
    "UPDATE pipeline.delivery_intents SET state='delivered'",
    'SET ROLE pipeline_owner',
  ])
    await assert.rejects(pr.query(sql), { code: '42501' });
  const f = await fixture(epoch);
  await drain();
  const terminal = first(await work(s, f.key.entityId));
  for (const sql of [
    'UPDATE source.capture_work SET acknowledged_at=clock_timestamp() WHERE entity_id=$1',
    'DELETE FROM source.capture_work WHERE entity_id=$1',
  ])
    await assert.rejects(s.query(sql, [f.key.entityId]), { code: 'P4003' });
  const tables = (
    await s.query<{ tablename: string }>(
      "SELECT tablename FROM pg_tables WHERE schemaname='source' ORDER BY tablename",
    )
  ).rows.map((r) => r.tablename);
  assert.deepEqual(tables, [
    'capture_binding',
    'capture_work',
    'command_receipts',
    'entities',
    'outbox',
    'source_identity',
  ]);
  const functions = (
    await s.query<Record<string, unknown>>(
      "SELECT p.proname,p.prosecdef,p.proconfig,r.rolname,has_function_privilege('public',p.oid,'EXECUTE') AS public_execute FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner WHERE n.nspname='source' ORDER BY p.proname",
    )
  ).rows;
  assert.equal(functions.length, 16);
  for (const f of functions) {
    assert.equal(f['rolname'], 'source_owner');
    assert.equal(f['public_execute'], false);
    assert.deepEqual(f['proconfig'], ['search_path=pg_catalog, pg_temp']);
  }
  assert.deepEqual(
    (
      await p.query<{ state: string; receiver_identity: null }>(
        'SELECT state,receiver_identity FROM pipeline.destinations',
      )
    ).rows,
    [
      { state: 'unbound', receiver_identity: null },
      { state: 'unbound', receiver_identity: null },
    ],
  );
  assert.equal(
    first(
      (
        await p.query<{ n: string }>(
          "SELECT (SELECT count(*) FROM pipeline.delivery_intents WHERE state<>'pending')+(SELECT count(*) FROM pipeline.consumer_observations WHERE state<>'pending') AS n",
        )
      ).rows,
    ).n,
    '0',
  );
  assert.deepEqual(first(await work(s, f.key.entityId)), terminal);
  await reconcile(s, p, 'IC16');
  evidence('IC16-catalog', { tables, functions, terminal });
});
