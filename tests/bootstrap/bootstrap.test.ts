import { command } from '../../scripts/support.ts';
import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { Bootstrap } from '../../src/bootstrap.ts';
import { bootstrapCases } from '../../scripts/required-bootstrap-cases.ts';
import { LimitError } from '../../src/limits.ts';
import { EsTransport, object } from '../../src/es/transport.ts';
import { esConfig, remote } from '../support/es.ts';
import { reader, pipeline } from '../support/staging.ts';
import { connect, required, evidence, databaseWaitFor } from '../support/db.ts';
import {
  setup,
  consumer,
  rawPublish,
  ledgerWire,
  metadata,
  topology,
} from '../support/rabbit.ts';
import { execute, request } from '../support/commands.ts';
async function session(s: pg.Client, applicationName: string) {
  const rows = (
    await s.query<{ pid: number; backend_xid: string; state: string }>(
      'SELECT pid,backend_xid::text,state FROM pg_stat_activity WHERE application_name=$1',
      [applicationName],
    )
  ).rows;
  assert.equal(rows.length, 1);
  const row = rows[0];
  assert.ok(row);
  return row;
}
import { launchBootstrap } from '../support/bootstrap-process.ts';
import {
  recipe,
  bootstrap,
  bootstrapConfig,
  pipelineConfig,
  member,
  members,
  checkRecipe,
  seedSnapshot,
  code,
  changed,
  stageBaseline,
  deliver,
  reconcile,
  selected,
  blocked,
  baselineExpected,
  assertSeedMap,
  recipeExpectations,
} from '../support/bootstrap.ts';
const name = (id: string) => {
  const entry = bootstrapCases.find((c) => c.id === id);
  assert.ok(entry);
  return entry.name;
};
await test(name('BS01'), async (t) => {
  const { s, p, c } = await setup(t);
  const b = bootstrap();
  assert.equal((await b.status(recipe.epoch)).phase, 'unselected');
  assert.equal((await b.begin(recipe)).phase, 'bootstrapping');
  await b.chunk(recipe.epoch, recipe.key, '1');
  await b.chunk(recipe.epoch, recipe.key, '33');
  await checkRecipe(s, 64);
  for (const table of ['outbox', 'capture_work', 'command_receipts'])
    assert.equal(
      (
        await s.query<{ n: string }>(
          `SELECT count(*)::text n FROM source.${table}`,
        )
      ).rows[0]?.n,
      '0',
    );
  assert.equal(
    (await p.query<Record<string, unknown>>('SELECT * FROM pipeline.events'))
      .rowCount,
    0,
  );
  assert.equal(
    (
      await c.query<Record<string, unknown>>(
        'SELECT * FROM consumer.processed_events',
      )
    ).rowCount,
    0,
  );
  assert.equal(
    (
      await c.query<Record<string, unknown>>(
        'SELECT * FROM consumer.mutation_effects',
      )
    ).rowCount,
    0,
  );
  const es = new EsTransport(esConfig());
  try {
    assert.equal(
      String(
        object(await es.request('GET', `/${required('ES_INDEX')}/_count`))[
          'count'
        ],
      ),
      '0',
    );
  } finally {
    await es.close();
  }
  const queue = await metadata().request(
    'GET',
    `/api/queues/${encodeURIComponent(topology().vhost)}/${topology().queue}`,
  );
  assert.equal(object(queue)['messages'], 0);
  evidence('BS01', { state: await seedSnapshot(s), queue });
});
await test(name('BS02'), async (t) => {
  const { s } = await setup(t);
  const a = new pg.Client({
    ...bootstrapConfig('seed-A'),
    query_timeout: 15000,
  });
  await a.connect();
  t.after(() => a.end());
  await a.query<Record<string, unknown>>('BEGIN');
  const first = (
    await a.query<Record<string, unknown>>(
      'SELECT * FROM source.seed_chunk($1,$2,65)',
      [recipe.epoch, recipe.key],
    )
  ).rows;
  const contender = new (await import('../../src/bootstrap.ts')).Bootstrap(
    bootstrapConfig('seed-B'),
  );
  const waiting = contender.chunk(recipe.epoch, recipe.key, '65');
  void waiting.catch(() => undefined);
  const observed = await databaseWaitFor(
    s,
    () =>
      s.query<Record<string, unknown>>(
        'SELECT pid,wait_event_type,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE application_name=$1',
        [bootstrapConfig('seed-B').application_name],
      ),
    (r) => r.rows.some((x) => x['wait_event_type'] === 'Lock'),
    'second seed waits',
  );
  assert.equal(
    (await a.query<Record<string, unknown>>('COMMIT')).command,
    'COMMIT',
  );
  const replay = await waiting;
  assert.ok(replay.every((r) => r.replayed));
  assert.deepEqual(
    replay.map((r) => ({
      ordinal: r.ordinal,
      entity_id: r.entity_id,
      recorded_at: r.recorded_at,
    })),
    first.map((r) => ({
      ordinal: r['ordinal'],
      entity_id: r['entity_id'],
      recorded_at: r['recorded_at'],
    })),
  );
  const before = await seedSnapshot(s);
  await bootstrap().begin(recipe);
  await bootstrap().chunk(recipe.epoch, recipe.key, '1');
  for (const change of [
    { key: randomUUID() },
    { seed: 'changed' },
    { count: '256' },
    { version: 2 },
    { chunkSize: 16 },
  ])
    await assert.rejects(
      bootstrap().begin({ ...recipe, ...change }),
      (e) => code('P7002')(e) || code('22023')(e),
    );
  await assert.rejects(
    bootstrap().begin({ ...recipe, epoch: randomUUID() }),
    code('P2002'),
  );
  assert.deepEqual(await seedSnapshot(s), before);
  evidence('BS02', { observed: observed.rows, first, replay });
});
async function chunkFault(
  t: import('node:test').TestContext,
  id: string,
  first: string,
  kill: 'before_commit' | 'after_commit' | null,
) {
  const { s } = await setup(t);
  const before = await seedSnapshot(s);
  const child = launchBootstrap(t, id, 'chunk', recipe.key, first);
  const barrier = await child.barrier('before_commit');
  const tx = await session(s, child.applicationName);
  assert.equal(tx.state, 'idle in transaction');
  assert.ok(tx.backend_xid);
  assert.deepEqual(await seedSnapshot(s), before);
  if (kill === 'before_commit') {
    const exit = await child.finish('kill');
    assert.deepEqual(await seedSnapshot(s), before);
    evidence(id, { barrier, tx, exit, before, after: await seedSnapshot(s) });
    return;
  }
  child.release('before_commit');
  const afterBarrier = await child.barrier('after_commit');
  const committed = await seedSnapshot(s);
  assert.notDeepEqual(committed, before);
  if (kill === 'after_commit') {
    const exit = await child.finish('kill');
    const retry = await bootstrap().chunk(recipe.epoch, recipe.key, first);
    assert.ok(retry.every((r) => r.replayed));
    assert.deepEqual(await seedSnapshot(s), committed);
    evidence(id, { barrier, tx, afterBarrier, exit, retry, committed });
  } else {
    child.release('after_commit');
    const exit = await child.finish('run');
    evidence(id, { barrier, tx, afterBarrier, exit, committed });
  }
}
await test(name('BS03'), (t) => chunkFault(t, 'BS03', '97', 'before_commit'));
await test(name('BS03H'), (t) => chunkFault(t, 'BS03H', '97', null));
await test(name('BS04'), (t) => chunkFault(t, 'BS04', '129', 'after_commit'));
await test(name('BS04H'), (t) => chunkFault(t, 'BS04H', '161', null));
await test(name('BS05'), async (t) => {
  const { s } = await setup(t);
  const b = bootstrap(),
    r = await member(s, '1');
  await assert.rejects(b.seal(recipe.epoch, recipe.key), code('P7003'));
  await assert.rejects(
    b.activate(
      recipe.epoch,
      recipe.key,
      required('PIPELINE_ID'),
      pipelineConfig(),
    ),
    code('P7001'),
  );
  const w = await connect('writer', 'closed-writer');
  t.after(() => w.end());
  for (const sql of [
    "SELECT source.create_entity('{}')",
    `SELECT source.mutate_entity(${r.entity_id},'update',$1::jsonb)`,
  ])
    await assert.rejects(
      w.query<Record<string, unknown>>(
        sql,
        sql.includes('$1') ? [r.payload] : [],
      ),
      code('P7001'),
    );
  await assert.rejects(
    execute(request(recipe.epoch, 'update', r.entity_id, r.payload)),
    code('P7001'),
  );
  const cli = await command(
    process.execPath,
    [
      'scripts/bootstrap.ts',
      'seed',
      recipe.epoch,
      recipe.key,
      String(recipe.version),
      recipe.seed,
      recipe.count,
      String(recipe.chunkSize),
    ],
    {
      ...process.env,
      SOURCE_BOOTSTRAP_HOST: '127.0.0.1',
      SOURCE_BOOTSTRAP_PORT: required('M1_PORT'),
      SOURCE_BOOTSTRAP_PASSWORD: required('SOURCE_BOOTSTRAP_PASSWORD'),
    },
    30000,
    true,
    { secrets: [required('SOURCE_BOOTSTRAP_PASSWORD')] },
  );
  assert.equal(cli.code, 0);
  assert.equal(cli.timedOut, false);
  assert.deepEqual(cli.cleanupErrors, []);
  assert.equal(cli.stderr, '');
  const progress = cli.stdout
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  assert.deepEqual(
    progress.slice(1).map((r) => r['first']),
    ['193', '225', '257'],
  );
  await checkRecipe(s, 257);
  const lock = new pg.Client({
    ...bootstrapConfig('seal-lock'),
    query_timeout: 15000,
  });
  await lock.connect();
  t.after(() => lock.end());
  await lock.query<Record<string, unknown>>('BEGIN');
  await lock.query<Record<string, unknown>>(
    'SELECT source.seal_bootstrap($1,$2)',
    [recipe.epoch, recipe.key],
  );
  const attempt = w.query<Record<string, unknown>>(
    "SELECT source.create_entity('{}')",
  );
  void attempt.catch(() => undefined);
  await databaseWaitFor(
    s,
    () =>
      s.query<Record<string, unknown>>(
        'SELECT wait_event_type FROM pg_stat_activity WHERE application_name=$1',
        [`${required('M1_RUN_ID')}:closed-writer`],
      ),
    (x) => x.rows[0]?.['wait_event_type'] === 'Lock',
    'writer waits on seal',
  );
  await lock.query<Record<string, unknown>>('COMMIT');
  await assert.rejects(attempt, code('P7001'));
  assert.equal((await b.status(recipe.epoch)).phase, 'sealed');
  await assert.rejects(b.chunk(recipe.epoch, recipe.key, '289'), code('22023'));
  evidence('BS05', {
    state: await seedSnapshot(s),
    cli: { code: cli.code, progress },
  });
});
async function activationFault(
  t: import('node:test').TestContext,
  id: string,
  phase: 'before_commit' | 'after_commit' | null,
) {
  const { s } = await setup(t);
  const before = await seedSnapshot(s),
    b = bootstrap();
  const old = await connect('writer', id + '-old-snapshot');
  t.after(() => old.end());
  await old.query<Record<string, unknown>>(
    'BEGIN ISOLATION LEVEL REPEATABLE READ',
  );
  const oldSnapshot = (
    await old.query<Record<string, unknown>>(
      "SELECT source_epoch::text,pg_current_snapshot()::text snapshot,current_setting('transaction_isolation') isolation FROM source.source_identity",
    )
  ).rows;
  const child = launchBootstrap(t, id, 'activate', recipe.key);
  const barrier = await child.barrier('before_commit');
  const tx = await session(s, child.applicationName);
  assert.equal(tx.state, 'idle in transaction');
  if (phase === 'before_commit') {
    const exit = await child.finish('kill');
    assert.deepEqual(await seedSnapshot(s), before);
    assert.equal((await b.status(recipe.epoch)).phase, 'sealed');
    await old.query<Record<string, unknown>>('ROLLBACK');
    evidence(id, { barrier, tx, exit, oldSnapshot, state: before });
    return;
  }
  // RC transaction starts and reads before activation commits. It waits at the real shared lifecycle lock.
  const w = await connect('writer', `${id}-waiting`);
  t.after(() => w.end());
  await w.query<Record<string, unknown>>('BEGIN');
  await w.query<Record<string, unknown>>(
    'SELECT source_epoch FROM source.source_identity',
  );
  const query = w.query<Record<string, unknown>>(
    'SELECT (source.create_entity(\'{"name":"Activation race","loyalty_points":1}\')).entity_id::text id',
  );
  void query.catch(() => undefined);
  await databaseWaitFor(
    s,
    () =>
      s.query<Record<string, unknown>>(
        'SELECT wait_event_type FROM pg_stat_activity WHERE application_name=$1',
        [`${required('M1_RUN_ID')}:${id}-waiting`],
      ),
    (r) => r.rows[0]?.['wait_event_type'] === 'Lock',
    'writer waits on activation',
  );
  child.release('before_commit');
  const after = await child.barrier('after_commit');
  await query;
  await w.query<Record<string, unknown>>('ROLLBACK'); // Real successful RC write is deliberately rolled back; no unjournaled mutation.
  assert.equal((await b.status(recipe.epoch)).phase, 'active');
  await assert.rejects(
    old.query<Record<string, unknown>>(
      'SELECT source.create_entity(\'{"oldSnapshot":true}\')',
    ),
    (e) => code('P7001')(e) || code('25001')(e),
  );
  await old.query<Record<string, unknown>>('ROLLBACK');
  evidence(id + '-snapshot', { oldSnapshot, closedOrUnsupported: true });
  const committed = await seedSnapshot(s);
  if (phase === 'after_commit') {
    const exit = await child.finish('kill');
    await b.activate(
      recipe.epoch,
      recipe.key,
      required('PIPELINE_ID'),
      pipelineConfig(),
    );
    assert.deepEqual(await seedSnapshot(s), committed);
    evidence(id, { barrier, tx, after, exit, committed });
  } else {
    child.release('after_commit');
    evidence(id, {
      barrier,
      tx,
      after,
      exit: await child.finish('run'),
      committed,
    });
  }
}
async function activationHealthy(
  t: import('node:test').TestContext,
  id: string,
) {
  // Clone only this run's closed tiny source for an independent successful transition.
  // No capture/delivery is run against this control DB; it is never adopted as the source.
  const database = 'm5a_control_' + randomUUID().replaceAll('-', '');
  const base = {
    host: '127.0.0.1',
    port: Number(required('M1_PORT')),
    user: 'm1_admin',
    password: required('M1_ADMIN_PASSWORD'),
    connectionTimeoutMillis: 5000,
    query_timeout: 15000,
  };
  const admin = new pg.Client({ ...base, database: 'postgres' });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${database} TEMPLATE source_m1`);
  t.after(async () => {
    try {
      await admin.query(`DROP DATABASE ${database} WITH (FORCE)`);
      evidence(id + '-control-cleanup', { database, dropped: true });
    } finally {
      await admin.end();
    }
  });
  await admin.query(`REVOKE ALL ON DATABASE ${database} FROM PUBLIC`);
  await admin.query(
    `GRANT CONNECT ON DATABASE ${database} TO source_bootstrap`,
  );
  const s = new pg.Client({ ...base, database });
  await s.connect();
  t.after(() => s.end());
  const b = new Bootstrap({ ...bootstrapConfig(id), database });
  assert.equal((await b.status(recipe.epoch)).phase, 'sealed');
  const child = launchBootstrap(t, id, 'activate', recipe.key, '1', database);
  const barrier = await child.barrier('before_commit');
  const tx = await session(s, child.applicationName);
  assert.equal(tx.state, 'idle in transaction');
  assert.equal(
    (
      await s.query<{ phase: string }>(
        'SELECT phase FROM source.bootstrap_manifest',
      )
    ).rows[0]?.phase,
    'sealed',
  );
  assert.equal(
    (await s.query('SELECT * FROM source.capture_binding')).rowCount,
    0,
  );
  child.release('before_commit');
  const after = await child.barrier('after_commit');
  assert.equal((await b.status(recipe.epoch)).phase, 'active');
  const committed = await seedSnapshot(s);
  child.release('after_commit');
  const exit = await child.finish('run');
  evidence(id, {
    database,
    isolatedSealedClone: true,
    barrier,
    tx,
    after,
    exit,
    committed,
  });
  await s.end();
}
await test(name('BS06H'), (t) => activationHealthy(t, 'BS06H'));
await test(name('BS06AH'), (t) => activationHealthy(t, 'BS06AH'));
await test(name('BS06'), (t) => activationFault(t, 'BS06', 'before_commit'));
await test(name('BS06A'), (t) => activationFault(t, 'BS06A', 'after_commit'));
await test(name('BS07'), async (t) => {
  const { s } = await setup(t);
  const r = await member(s, '1');
  const command = request(recipe.epoch, 'update', r.entity_id, r.payload);
  const before = (
    await s.query<Record<string, unknown>>('SELECT * FROM source.outbox')
  ).rows;
  const holder = await connect('command', 'baseline-command-A');
  t.after(() => holder.end());
  await holder.query<Record<string, unknown>>('BEGIN');
  await holder.query<Record<string, unknown>>(
    "SELECT * FROM source.execute_command($1,$2,1,'update',$3,$4)",
    [recipe.epoch, command.commandId, r.entity_id, r.payload],
  );
  const waiting = execute(command, 'baseline-command-B');
  void waiting.catch(() => undefined);
  const blocked = await databaseWaitFor(
    s,
    () =>
      s.query<Record<string, unknown>>(
        'SELECT pid,wait_event_type,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE application_name=$1',
        [`${required('M1_RUN_ID')}:baseline-command-B`],
      ),
    (x) => x.rows[0]?.['wait_event_type'] === 'Lock',
    'baseline command key arbitration',
  );
  await holder.query<Record<string, unknown>>('COMMIT');
  const a = await waiting;
  evidence('BS07-contention', { blocked: blocked.rows });
  assert.equal(a.result.change_id, null);
  assert.equal(a.result.entity_version, '1');
  assert.equal(a.result.payload_json, r.payload);
  assert.deepEqual(
    (await s.query<Record<string, unknown>>('SELECT * FROM source.outbox'))
      .rows,
    before,
  );
  const concurrent = await Promise.all([execute(command), execute(command)]);
  for (const x of concurrent) {
    assert.deepEqual(x.result, a.result);
    assert.equal(x.replayed, true);
  }
  await changed(s, r.entity_id, '{"name":"Changed","loyalty_points":42}');
  await changed(s, r.entity_id, null, 'delete');
  assert.deepEqual((await execute(command)).result, a.result);
  await assert.rejects(
    execute({ ...command, payloadJson: '{"changed":true}' }),
    code('P2001'),
  );
  evidence('BS07', { command, reply: a, replayed: await execute(command) });
});
await test(name('BS08'), async (t) => {
  const { s } = await setup(t);
  const r = await member(s, '1');
  await changed(
    s,
    r.entity_id,
    '{"name":"Restored","loyalty_points":3}',
    'restore',
  );
  await changed(
    s,
    null,
    '{"name":"After active","loyalty_points":9}',
    'create',
  );
  const w = await connect('writer', 'snapshot-write');
  t.after(() => w.end());
  for (const level of ['REPEATABLE READ', 'SERIALIZABLE']) {
    await w.query<Record<string, unknown>>(`BEGIN ISOLATION LEVEL ${level}`);
    await w.query<Record<string, unknown>>(
      'SELECT source_epoch FROM source.source_identity',
    );
    await assert.rejects(
      w.query<Record<string, unknown>>(
        "SELECT source.mutate_entity($1,'update','{\"changed\":true}')",
        [r.entity_id],
      ),
      code('25001'),
    );
    await w.query<Record<string, unknown>>('ROLLBACK');
  }
  const before = await seedSnapshot(s);
  await s.query<Record<string, unknown>>('BEGIN');
  await s.query<Record<string, unknown>>(
    "CREATE FUNCTION source.m5a_fail_work() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled capture insert failure' USING ERRCODE='P7099'; END $$",
  );
  await s.query<Record<string, unknown>>(
    'CREATE TRIGGER m5a_fail_work BEFORE INSERT ON source.capture_work FOR EACH ROW EXECUTE FUNCTION source.m5a_fail_work()',
  );
  await assert.rejects(
    s.query<Record<string, unknown>>(
      "SELECT source.mutate_entity($1,'update','{\"fail\":true}')",
      [r.entity_id],
    ),
    code('P7099'),
  );
  await s.query<Record<string, unknown>>('ROLLBACK');
  assert.deepEqual(await seedSnapshot(s), before);
  evidence('BS08', { state: await seedSnapshot(s) });
});
await test(name('BS09'), async (t) => {
  const { s } = await setup(t);
  const old = await member(s, '1'),
    unchanged = await member(s, '2');
  const baseline = (
      await reader().baseline([{ entityId: unchanged.entity_id, version: '1' }])
    ).events[0],
    current = (await reader().current(unchanged.entity_id)).events[0];
  assert.ok(baseline && current);
  assert.deepEqual(baseline, current);
  assert.equal(baseline.body.kind, 'baseline');
  assert.match(baseline.body.payload_json ?? '', /9007199254740995/);
  assert.match(
    baseline.body.payload_json ?? '',
    /0\.123456789012345678901234567890/,
  );
  const historical = (
    await reader().baseline([{ entityId: old.entity_id, version: '1' }])
  ).events[0];
  assert.ok(historical);
  assert.equal(historical.body.payload_json, old.payload);
  const live = (await reader().current(old.entity_id)).events[0];
  assert.ok(live);
  assert.equal(live.body.kind, 'mutation');
  assert.deepEqual(
    (
      await reader().outbox([
        { entityId: old.entity_id, version: live.body.entity_version },
      ])
    ).events[0],
    live,
  );
  assert.equal(
    (
      await reader().baseline([
        { entityId: '9223372036854775807', version: '1' },
      ])
    ).notVisible.length,
    1,
  );
  await assert.rejects(
    async () =>
      reader().baseline(
        Array.from({ length: 17 }, () => ({
          entityId: old.entity_id,
          version: '1',
        })),
      ),
    LimitError,
  );
  const big = await changed(
    s,
    null,
    JSON.stringify({ padding: 'x'.repeat(70000) }),
    'create',
  );
  blocked.add(big.eventId);
  await assert.rejects(
    reader().current(big.id),
    (e) => e instanceof Error && e.cause instanceof LimitError,
  );
  await assert.rejects(
    reader().outbox([{ entityId: big.id, version: '1' }]),
    (e) => e instanceof Error && e.cause instanceof LimitError,
  );
  evidence('BS09', {
    baseline: baseline.body,
    historical: historical.body,
    current: live.body,
    oversized: big.eventId,
  });
});
await test(name('BS10'), async (t) => {
  const { s, p, c } = await setup(t);
  const ids = (await members(s)).slice(1, 9).map((r) => r.entity_id);
  const events = await stageBaseline(ids);
  await deliver(p, c);
  for (const event of events) {
    assert.equal(
      (
        await c.query<{ n: string }>(
          'SELECT count(*)::text n FROM consumer.mutation_effects WHERE event_id=$1',
          [event.body.event_id],
        )
      ).rows[0]?.n,
      '0',
    );
    assert.equal(
      (
        await c.query<{ units: string }>(
          'SELECT units::text FROM consumer.entity_totals WHERE entity_id=$1',
          [event.body.entity_id],
        )
      ).rows[0]?.units,
      '0',
    );
  }
  const before = (
    await p.query<Record<string, unknown>>(
      'SELECT * FROM pipeline.delivery_intents ORDER BY event_id,kind',
    )
  ).rows;
  assert.ok(
    (await pipeline().stage(events)).every(
      (r) => r.status === 'already_staged',
    ),
  );
  await rawPublish(
    await ledgerWire(p, events[0]?.body.event_id ?? ''),
    events[0]?.body.event_id ?? '',
  );
  const duplicate = await consumer().once(undefined, 1000);
  assert.deepEqual(duplicate.processed, [events[0]?.body.event_id]);
  assert.equal(duplicate.acknowledged, 1);
  assert.deepEqual(
    (
      await p.query<Record<string, unknown>>(
        'SELECT * FROM pipeline.delivery_intents ORDER BY event_id,kind',
      )
    ).rows,
    before,
  );
  evidence('BS10', { ids, eventIds: events.map((e) => e.body.event_id) });
});
await test(name('BS11'), async (t) => {
  const { s, p, c } = await setup(t);
  const r = await member(s, '10');
  await changed(s, r.entity_id, '{"name":"Newer","loyalty_points":12}');
  await changed(s, r.entity_id, null, 'delete');
  await deliver(p, c);
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  const before = await remote(es, `${recipe.epoch}:${r.entity_id}`);
  assert.equal(object(before['_source'])['is_deleted'], true);
  await stageBaseline([r.entity_id, (await member(s, '1')).entity_id]);
  await deliver(p, c);
  assert.deepEqual(await remote(es, `${recipe.epoch}:${r.entity_id}`), before);
  assert.equal(
    (
      await c.query<{ v: string }>(
        'SELECT entity_version::text v FROM consumer.entity_projection WHERE entity_id=$1',
        [r.entity_id],
      )
    ).rows[0]?.v,
    '3',
  );
  await changed(
    s,
    r.entity_id,
    '{"name":"Higher restore","loyalty_points":13}',
    'restore',
  );
  await deliver(p, c);
  evidence('BS11', {
    baseline: await baselineExpected(s, r.entity_id),
    before,
    after: await remote(es, `${recipe.epoch}:${r.entity_id}`),
  });
});
await test(name('BS12'), async (t) => {
  const { s } = await setup(t);
  const r = await member(s, '2'),
    before = await seedSnapshot(s);
  const roles = {
    source_writer: 'M1_WRITER_PASSWORD',
    source_command: 'M2A_COMMAND_PASSWORD',
    source_reader: 'SOURCE_READER_PASSWORD',
    source_capture: 'SOURCE_CAPTURE_PASSWORD',
    source_bootstrap: 'SOURCE_BOOTSTRAP_PASSWORD',
  };
  for (const [role, passwordName] of Object.entries(roles)) {
    const client = new pg.Client({
      ...bootstrapConfig('permissions-' + role),
      user: role,
      password: required(passwordName),
      query_timeout: 5000,
    });
    await client.connect();
    try {
      for (const sql of [
        'SET ROLE source_seed_owner',
        'SET ROLE source_owner',
        "UPDATE source.bootstrap_manifest SET phase='bootstrapping'",
        "UPDATE source.baseline_revisions SET payload='{}'",
        "INSERT INTO source.entities(payload,entity_version,change_id) VALUES('{}',1,NULL)",
        'ALTER TABLE source.entities DISABLE TRIGGER entities_capture',
        'CREATE OR REPLACE FUNCTION source.prepare_revision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$',
      ])
        await assert.rejects(client.query(sql), code('42501'));
      if (role !== 'source_bootstrap')
        await assert.rejects(
          client.query('SELECT source.seed_chunk($1,$2,1)', [
            recipe.epoch,
            recipe.key,
          ]),
          code('42501'),
        );
    } finally {
      await client.end();
    }
  }
  for (const change of [
    'NULL,NULL,NULL,NULLIF($3::timestamptz,$3::timestamptz),NULL,NULL',
    "$2,1,NULL,$3::timestamptz,false,'{}'::jsonb",
    '$2,1,gen_random_uuid(),$3,false,$4::jsonb',
  ]) {
    await s.query<Record<string, unknown>>('BEGIN');
    await assert.rejects(
      async () => {
        await s.query<Record<string, unknown>>(
          `INSERT INTO source.command_receipts(source_epoch,command_id,contract_version,operation,target_id,request_payload,completed,result_entity_id,result_version,result_change_id,result_recorded_at,result_deleted,result_payload) VALUES($1,gen_random_uuid(),1,'update',$2,$4::jsonb,true,${change})`,
          [recipe.epoch, r.entity_id, r.recorded_at, r.payload],
        );
        await s.query<Record<string, unknown>>('COMMIT');
      },
      (e) => code('23514')(e) || code('P2003')(e),
    );
    await s.query<Record<string, unknown>>('ROLLBACK');
  }
  await assert.rejects(
    s.query<Record<string, unknown>>(
      'UPDATE source.bootstrap_manifest SET completed_count=256',
    ),
    code('P7001'),
  );
  await assert.rejects(
    bootstrap().transaction((tx) =>
      tx.activate(recipe.epoch, recipe.key, randomUUID()),
    ),
    code('P4001'),
  );
  await assert.rejects(
    bootstrap().begin({ ...recipe, key: randomUUID() }),
    code('P7002'),
  );
  await assert.rejects(
    bootstrap().activate(
      recipe.epoch,
      recipe.key,
      randomUUID(),
      pipelineConfig(),
    ),
    /pipeline identity mismatch/,
  );
  assert.deepEqual(await seedSnapshot(s), before);
  evidence('BS12', { unchanged: true });
});
await test(name('BS14'), async (t) => {
  const { s, p, c } = await setup(t);
  await deliver(p, c);
  await reconcile(s, p, c);
  const map = await checkRecipe(s, 257),
    expectations = await recipeExpectations(s, 257);
  const check = (rows: typeof map) =>
    assertSeedMap(rows, expectations.payloads, expectations.time);
  assert.throws(() => check(map.slice(1)));
  assert.throws(() =>
    check([
      ...map,
      map[0] ??
        map[1] ?? {
          ordinal: '0',
          entity_id: '0',
          recorded_at: '',
          payload: '',
        },
    ]),
  );
  assert.throws(() =>
    check(map.map((r, i) => (i ? r : { ...r, payload: '{}' }))),
  );
  assert.equal(
    (
      await s.query<{ n: string }>(
        'SELECT count(*)::text n FROM source.capture_work w JOIN source.baseline_revisions b USING(source_epoch,entity_id,entity_version)',
      )
    ).rows[0]?.n,
    '0',
  );
  evidence('BS14', {
    negativeControls: ['missing baseline', 'extra baseline', 'altered content'],
    selected: [...selected],
    snapshot: await seedSnapshot(s),
    retainedRestart:
      'required afterward by orchestration, including added baseline tables',
  });
});
