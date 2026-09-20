import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { TransactionError } from '../../src/internal/transaction.ts';
import { canonicalEvent } from '../../src/envelope.ts';
import { BackfillSource } from '../../src/backfill/source.ts';
import { object, text } from '../../src/backfill/types.ts';
import { backfillCases } from '../../scripts/required-backfill-cases.ts';
import { setup, publisher, observer } from '../support/rabbit.ts';
import { pipeline } from '../support/staging.ts';
import {
  bootstrap,
  recipe,
  pipelineConfig,
  code,
  member,
} from '../support/bootstrap.ts';
import { required, evidence, connect, databaseWaitFor } from '../support/db.ts';
import { backfillConfig, launchBackfill } from '../support/backfill-process.ts';
import {
  runId,
  scan,
  backfillLedger,
  backfillSource,
  session,
  gone,
  snapshot,
  claimPage,
  requestFromBarrier,
  declareMutation,
  rememberLegacy,
  initializeExpected,
  unobservedBaselines,
  deliver,
  scanTo,
  setFence,
  expectedFence,
  reconcile,
  checkSet,
  checkReceiver,
  mutations,
} from '../support/backfill.ts';
import { EsTransport, version } from '../../src/es/transport.ts';
import { esConfig, finish } from '../support/es.ts';
const name = (id: string) => {
  const c = backfillCases.find((c) => c.id === id);
  assert.ok(c);
  return c.name;
};
let held: pg.Client | undefined,
  lateId = '',
  lateVersion = '',
  hotId = '',
  postFenceId = '';
let fenceHeld: pg.Client | undefined,
  fenceLateId = '';
async function expire(p: pg.Client) {
  await databaseWaitFor(
    p,
    () =>
      p.query<{ n: string; at: string }>(
        "SELECT count(*) FILTER(WHERE state='leased' AND lease_until>clock_timestamp())::text n,clock_timestamp()::text at FROM pipeline.backfill_ranges WHERE run_id=$1",
        [runId],
      ),
    (r) => r.rows[0]?.n === '0',
    'Real range lease expiry',
    10000,
  );
}
await test(name('BF01'), async (t) => {
  const { s, p, c } = await setup(t);
  recipe.seed = 'M5B-ქართული';
  const b = bootstrap();
  await b.begin(recipe);
  for (let n = 1; n <= 257; n += 32)
    await b.chunk(recipe.epoch, recipe.key, String(n));
  await b.seal(recipe.epoch, recipe.key);
  await b.activate(
    recipe.epoch,
    recipe.key,
    required('PIPELINE_ID'),
    pipelineConfig(),
  );
  await initializeExpected(s);
  for (const table of ['outbox', 'capture_work', 'command_receipts'])
    assert.equal(
      (
        await s.query<{ n: string }>(
          `SELECT count(*)::text n FROM source.${table}`,
        )
      ).rows[0]?.n,
      '0',
    );
  assert.equal((await p.query('SELECT * FROM pipeline.events')).rowCount, 0);
  assert.equal(
    (await c.query('SELECT * FROM consumer.processed_events')).rowCount,
    0,
  );
  held = await connect('writer', 'BF07-held-early');
  await held.query("SET idle_in_transaction_session_timeout='15min'");
  await held.query('BEGIN ISOLATION LEVEL READ COMMITTED');
  const created = (
    await held.query<{ id: string; v: string }>(
      'SELECT entity_id::text id,entity_version::text v FROM source.create_entity(\'{"name":"Held early allocation","loyalty_points":8}\')',
    )
  ).rows[0];
  assert.ok(created);
  lateId = created.id;
  lateVersion = created.v;
  const later = await declareMutation(s);
  assert.ok(BigInt(later.id) > BigInt(lateId));
  const worker = scan(),
    started = await worker.start(runId),
    replayed = await worker.start(runId);
  assert.deepEqual(
    replayed.evidence['source_observation'],
    started.evidence['source_observation'],
  );
  assert.equal(started.evidence['upper_key'], later.id);
  const ranges = (
    await p.query<{ lower: string; upper: string }>(
      'SELECT lower_key::text lower,upper_key::text upper FROM pipeline.backfill_ranges WHERE run_id=$1 ORDER BY range_no',
      [runId],
    )
  ).rows;
  assert.equal(ranges.length, 4);
  assert.equal(ranges[0]?.lower, '0');
  assert.equal(ranges.at(-1)?.upper, later.id);
  for (let i = 1; i < ranges.length; i++)
    assert.equal(ranges[i]?.lower, ranges[i - 1]?.upper);
  await assert.rejects(worker.start(runId, 3), code('P8001'));
  await assert.rejects(
    new BackfillSource(backfillConfig().source, {
      ...backfillConfig().binding,
      pipelineId: randomUUID(),
    }).identity(),
    code('P8001'),
  );
  await assert.rejects(worker.start(randomUUID()), code('23505'));
  evidence('BF01', {
    started: started.evidence,
    replayed: replayed.evidence,
    ranges,
    lateAllocation: {
      id: lateId,
      session: await session(s, `${required('M1_RUN_ID')}:BF07-held-early`),
    },
  });
});
await test(name('BF02'), async (t) => {
  const { p } = await setup(t),
    r = await claimPage();
  assert.ok(r.page.events.length > 1);
  const first = r.page.events[0];
  assert.ok(first);
  await pipeline().stage([first]);
  const before = await snapshot(p);
  await assert.rejects(
    backfillLedger().transaction(async (tx) => {
      const result = await tx.page(r);
      assert.ok(result['batch_id']);
      assert.deepEqual(await snapshot(p), before);
      throw new Error('Deliberate page callback rollback');
    }),
    (error: unknown) => {
      assert.ok(error instanceof TransactionError);
      assert.equal(error.outcome, 'rolled_back');
      assert.equal(error.sqlState, undefined);
      assert.ok(error.cause instanceof Error);
      assert.equal(error.cause.message, 'Deliberate page callback rollback');
      return true;
    },
  );
  assert.deepEqual(await snapshot(p), before);
  assert.equal((await p.query('SELECT * FROM pipeline.events')).rowCount, 1);
  const invalid = {
    ...r,
    page: {
      ...r.page,
      events: r.page.events.map((e, i) =>
        i === 1 ? { ...e, contentSha256: '0'.repeat(64) } : e,
      ),
    },
  };
  await assert.rejects(backfillLedger().page(invalid));
  assert.deepEqual(await snapshot(p), before);
  const conflict = {
    ...r,
    page: {
      ...r.page,
      events: r.page.events.map((event, i) =>
        i === 0
          ? canonicalEvent({
              ...event.body,
              payload_json: '{"name": "conflicting retained identity"}',
            })
          : event,
      ),
    },
  };
  await assert.rejects(backfillLedger().page(conflict), code('P3001'));
  assert.deepEqual(await snapshot(p), before);
  assert.equal((await p.query('SELECT * FROM pipeline.events')).rowCount, 1);
  const role = backfillConfig().pipeline;
  const runtime = new (await import('pg')).default.Client(role);
  await runtime.connect();
  t.after(() => runtime.end());
  await assert.rejects(
    runtime.query('UPDATE pipeline.backfill_ranges SET checkpoint=upper_key'),
    { code: '42501' },
  );
  await assert.rejects(
    runtime.query(
      'INSERT INTO pipeline.backfill_members SELECT * FROM pipeline.backfill_members',
    ),
    { code: '42501' },
  );
  assert.equal(await backfillLedger().change(r.claim, 'defer', 1), true);
  const short = await backfillLedger().claim(runId, randomUUID(), 300);
  assert.ok(short);
  const smallPage = await backfillSource().page(short.checkpoint, short.upper);
  const expiring = { claim: short, page: smallPage, batchId: randomUUID() };
  const oldProgress = await snapshot(p);
  await assert.rejects(
    backfillLedger().transaction(async (tx) => {
      await tx.page(expiring);
      await databaseWaitFor(
        p,
        () =>
          p.query<{ expired: boolean; at: string }>(
            'SELECT clock_timestamp()>$1::timestamptz expired,clock_timestamp()::text at',
            [short.leaseUntil],
          ),
        (r) => r.rows[0]?.expired === true,
        'Page lease expires before COMMIT',
        3000,
      );
    }),
    code('P8002'),
  );
  assert.deepEqual(await snapshot(p), oldProgress);
  evidence('BF02', {
    before,
    after: await snapshot(p),
    independentStaged: first.body.event_id,
    sqlRollback: true,
  });
});
for (const [id, boundary, kill] of [
  ['BF03', 'backfill.before_page_commit', true],
  ['BF03H', 'backfill.before_page_commit', false],
  ['BF04', 'backfill.after_page_commit.before_success', true],
  ['BF04H', 'backfill.after_page_commit.before_success', false],
] as const) {
  await test(name(id), async (t) => {
    const { p } = await setup(t);
    await expire(p);
    const before = await snapshot(p);
    const child = launchBackfill(t, id, runId, boundary, {
      leaseMs: 3000,
      renewalMs: 500,
    });
    const barrier = await child.barrier(),
      request = requestFromBarrier(barrier['data']);
    let tx: unknown = null;
    if (boundary === 'backfill.before_page_commit') {
      tx = await session(p, child.applicationName);
      assert.equal(object(tx)['state'], 'idle in transaction');
      assert.ok(object(tx)['backend_xid']);
      assert.equal(
        (
          await p.query(
            'SELECT * FROM pipeline.backfill_batches WHERE batch_id=$1',
            [request.batchId],
          )
        ).rowCount,
        0,
      );
    } else
      assert.equal(
        (
          await p.query(
            'SELECT * FROM pipeline.backfill_batches WHERE batch_id=$1',
            [request.batchId],
          )
        ).rowCount,
        1,
      );
    if (!kill) child.release();
    const exit = await child.finish(kill ? 'kill' : 'run');
    const sessionGone = await gone(p, child.applicationName);
    if (kill && boundary === 'backfill.before_page_commit') {
      assert.equal(
        (
          await p.query(
            'SELECT * FROM pipeline.backfill_batches WHERE batch_id=$1',
            [request.batchId],
          )
        ).rowCount,
        0,
      );
      assert.equal(
        (
          await p.query<{ checkpoint: string }>(
            'SELECT checkpoint::text FROM pipeline.backfill_ranges WHERE run_id=$1 AND range_no=$2',
            [runId, request.claim.range],
          )
        ).rows[0]?.checkpoint,
        request.claim.checkpoint,
      );
    }
    let replay: unknown = null;
    if (boundary === 'backfill.after_page_commit.before_success') {
      const stored = await snapshot(p);
      replay = await backfillLedger().page(request);
      assert.equal(object(replay)['replayed'], true);
      assert.deepEqual(await snapshot(p), stored);
      if (id === 'BF04') {
        const event = request.page.events[0];
        assert.ok(event);
        await p.query('BEGIN');
        try {
          await p.query(
            'ALTER TABLE pipeline.backfill_members DISABLE TRIGGER no_rewrite',
          );
          await p.query(
            'DELETE FROM pipeline.backfill_members WHERE run_id=$1 AND event_id=$2',
            [runId, event.body.event_id],
          );
          await assert.rejects(
            p.query(
              'SELECT pipeline.backfill_page($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
              [
                runId,
                request.claim.range,
                request.claim.owner,
                request.claim.generation,
                request.batchId,
                request.claim.checkpoint,
                request.page.next,
                request.page.eof,
                JSON.stringify(
                  request.page.events.map((e, i) => ({
                    key: request.page.keys[i],
                    body: e.bodyBytes.toString('hex'),
                    hash: e.contentSha256,
                  })),
                ),
                JSON.stringify(request.page.observation),
              ],
            ),
            { code: 'P8003' },
          );
        } finally {
          await p.query('ROLLBACK');
        }
        assert.deepEqual(await snapshot(p), stored);
      }
    }
    evidence(id, {
      barrier,
      tx,
      exit: { ...exit, sessionGone },
      before,
      after: await snapshot(p),
      replay,
    });
  });
}
await test(name('BF05'), async (t) => {
  const { p } = await setup(t);
  await expire(p);
  const a = launchBackfill(
    t,
    'BF05-A',
    runId,
    'backfill.after_source_read.before_stage',
    { leaseMs: 1800, renewalMs: 300, quiesce: true },
  );
  const ar = await a.barrier();
  assert.equal(ar['renewalQuiescent'], true);
  a.signal('SIGSTOP');
  const aClaim = (
    await p.query<Record<string, unknown>>(
      "SELECT range_no,generation::text,owner_id::text,lease_until::text,clock_timestamp()::text observed_at FROM pipeline.backfill_ranges WHERE run_id=$1 AND state='leased'",
      [runId],
    )
  ).rows[0];
  assert.ok(aClaim);
  const b = launchBackfill(
    t,
    'BF05-B',
    runId,
    'backfill.after_source_read.before_stage',
  );
  await b.barrier();
  const claims = (
    await p.query<{ range_no: number }>(
      'SELECT range_no FROM pipeline.backfill_ranges WHERE run_id=$1 AND state=$2',
      [runId, 'leased'],
    )
  ).rows;
  assert.equal(claims.length, 2);
  assert.notEqual(claims[0]?.range_no, claims[1]?.range_no);
  b.release();
  const bx = await b.finish();
  const expiry = await databaseWaitFor(
    p,
    () =>
      p.query<{ expired: boolean; at: string }>(
        'SELECT lease_until<=clock_timestamp() expired,clock_timestamp()::text at FROM pipeline.backfill_ranges WHERE run_id=$1 AND range_no=$2',
        [runId, aClaim['range_no']],
      ),
    (r) => r.rows[0]?.expired === true,
    'Actual old scanner expiry',
    6000,
  );
  const next = launchBackfill(
    t,
    'BF05-reclaim',
    runId,
    'backfill.after_page_commit.before_success',
  );
  const nr = await next.barrier(),
    request = requestFromBarrier(nr['data']);
  assert.equal(request.claim.range, aClaim['range_no']);
  assert.ok(
    BigInt(request.claim.generation) > BigInt(text(aClaim['generation'])),
  );
  next.release();
  const nx = await next.finish();
  const retained = await snapshot(p);
  a.signal('SIGCONT');
  a.release();
  const ax = await a.finish('failure');
  assert.deepEqual(await snapshot(p), retained);
  await gone(p, a.applicationName);
  evidence('BF05', {
    oldClaim: aClaim,
    claims,
    expiry: expiry.rows,
    oldExit: ax,
    otherExit: bx,
    reclaimExit: nx,
    reclaimed: request.claim,
    unchanged: retained,
  });
});
await test(name('BF06'), async (t) => {
  const { s, p, c } = await setup(t);
  await expire(p);
  const child = launchBackfill(
    t,
    'BF06-held-page',
    runId,
    'backfill.after_source_read.before_stage',
  );
  const barrier = await child.barrier(),
    raw = object(barrier['data']),
    rows = raw['events'];
  assert.ok(Array.isArray(rows) && rows.length >= 2);
  const first = object(object(rows[0])['body']),
    second = object(object(rows[1])['body']);
  assert.equal(first['kind'], 'baseline');
  hotId = text(first['entity_id']);
  const v2 = await declareMutation(
    s,
    hotId,
    '{"name":"new before old scan","country":"GE","loyalty_points":9}',
  );
  const tomb = await declareMutation(
    s,
    text(second['entity_id']),
    '',
    'delete',
  );
  await deliver(p, c);
  child.release();
  const exit = await child.finish();
  await deliver(p, c);
  const es = new EsTransport(esConfig());
  try {
    for (const r of [v2, tomb]) {
      const remote = object(
        await es.request(
          'GET',
          `/${required('ES_INDEX')}/_doc/${recipe.epoch}:${r.id}`,
        ),
      );
      assert.equal(version(remote['_version']), '2');
      assert.equal(object(remote['_source'])['is_deleted'], r === tomb);
    }
  } finally {
    await es.close();
  }
  await declareMutation(
    s,
    tomb.id,
    '{"name":"restored after old baseline","loyalty_points":10}',
    'restore',
  );
  await deliver(p, c);
  const future = await member(s, '250');
  unobservedBaselines.add(`${recipe.epoch}:${future.entity_id}:1`);
  await declareMutation(
    s,
    future.entity_id,
    '{"name":"already mutation when scanned","loyalty_points":11}',
  );
  evidence('BF06', {
    barrier,
    exit,
    updated: v2,
    deleted: tomb,
    alreadyMutated: future.entity_id,
  });
});
await test(name('BF09'), async (t) => {
  const { s, p } = await setup(t);
  const child = launchBackfill(
    t,
    'BF09-in-flight',
    runId,
    'backfill.after_source_read.before_stage',
  );
  await child.barrier();
  const worker = scan();
  const requested = await worker.pause(runId, true);
  assert.equal(requested.effectivePaused, false);
  assert.equal(await backfillLedger().claim(runId, randomUUID(), 30000), null);
  child.release();
  const exit = await child.finish();
  const paused = await worker.status(runId);
  assert.equal(paused.effectivePaused, true);
  const before = await snapshot(p);
  const replacement = launchBackfill(t, 'BF09-paused-restart', runId, '');
  const pausedExit = await replacement.finish();
  const pausedResult = object(object(JSON.parse(pausedExit.stdout))['result']);
  assert.equal(pausedResult['page'], undefined);
  assert.equal(object(pausedResult['status'])['effectivePaused'], true);
  assert.deepEqual(await snapshot(p), before);
  const retainedRanges = (
    await p.query<{ range_no: number; checkpoint: string; upper_key: string }>(
      'SELECT range_no,checkpoint::text,upper_key::text FROM pipeline.backfill_ranges WHERE run_id=$1 ORDER BY range_no',
      [runId],
    )
  ).rows;
  await scan().pause(runId, false);
  const resumed = launchBackfill(
    t,
    'BF09-resumed-process',
    runId,
    'backfill.after_page_commit.before_success',
  );
  const barrier = await resumed.barrier();
  const request = requestFromBarrier(barrier['data']);
  const retained = retainedRanges.find(
    (r) => r.range_no === request.claim.range,
  );
  assert.ok(retained);
  assert.equal(request.claim.runId, runId);
  assert.equal(request.claim.checkpoint, retained.checkpoint);
  assert.equal(request.claim.upper, retained.upper_key);
  assert.ok(BigInt(request.page.next) > BigInt(retained.checkpoint));
  assert.equal(
    (
      await p.query<{ checkpoint: string }>(
        'SELECT checkpoint::text FROM pipeline.backfill_ranges WHERE run_id=$1 AND range_no=$2',
        [runId, request.claim.range],
      )
    ).rows[0]?.checkpoint,
    request.page.next,
  );
  resumed.release();
  const resumedExit = await resumed.finish();
  const exits = [exit, pausedExit, resumedExit];
  assert.equal(new Set(exits.map((e) => e.pid)).size, 3);
  assert.equal(
    new Set(
      exits.map(
        (e) => object(object(JSON.parse(e.stdout))['result'])['workerId'],
      ),
    ).size,
    3,
  );
  for (const process of [child, replacement, resumed]) {
    await gone(p, process.applicationName);
    await gone(s, process.sourceApplicationName);
  }
  evidence('BF09', {
    requested,
    paused,
    exit,
    pausedExit,
    resumedExit,
    retainedRanges,
    resumedClaim: request.claim,
    next: request.page.next,
    preserved: before,
    sessionsGone: true,
  });
});
await test(name('BF07'), async (t) => {
  const { s, p, c } = await setup(t);
  await scanTo('sealing');
  assert.ok(held);
  assert.equal(
    (await s.query('SELECT * FROM source.outbox WHERE entity_id=$1', [lateId]))
      .rowCount,
    0,
  );
  assert.equal(
    (
      await p.query('SELECT * FROM pipeline.events WHERE entity_id=$1', [
        lateId,
      ])
    ).rowCount,
    0,
  );
  assert.equal((await held.query('COMMIT')).command, 'COMMIT');
  await held.end();
  held = undefined;
  await rememberLegacy(s, lateId, lateVersion);
  await deliver(p, c);
  assert.equal(
    (
      await s.query<{ state: string }>(
        'SELECT state FROM source.capture_work WHERE entity_id=$1',
        [lateId],
      )
    ).rows[0]?.state,
    'acknowledged',
  );
  evidence('BF07', {
    lateId,
    lateVersion,
    ranges: await snapshot(p),
    capture: (
      await s.query(
        'SELECT entity_id::text,entity_version::text,state,acknowledged_hash FROM source.capture_work WHERE entity_id=$1',
        [lateId],
      )
    ).rows,
  });
});
await test(name('BF10'), async (t) => {
  const { s } = await setup(t);
  for (let i = 0; i < 34; i++)
    await declareMutation(
      s,
      hotId,
      `{"name":"fence intermediate ${i}","loyalty_points":${i}}`,
    );
  fenceHeld = await connect('writer', 'BF10-held-across-cut');
  await fenceHeld.query("SET idle_in_transaction_session_timeout='5min'");
  await fenceHeld.query('BEGIN');
  const row = (
    await fenceHeld.query<{ id: string }>(
      'SELECT entity_id::text id FROM source.create_entity(\'{"name":"later commit outside cut","loyalty_points":44}\')',
    )
  ).rows[0];
  assert.ok(row);
  fenceLateId = row.id;
  const later = await declareMutation(s);
  postFenceId = later.id;
  assert.ok(BigInt(fenceLateId) < BigInt(later.id));
  setFence([...mutations.keys()]);
  const visible = (
    await s.query<{ id: string }>(
      "SELECT source_epoch::text||':'||entity_id::text||':'||entity_version::text id FROM source.outbox",
    )
  ).rows.map((r) => r.id);
  checkSet(visible, expectedFence, 'Declared before-cut identities');
  const states = (
    await s.query<{ state: string }>(
      'SELECT DISTINCT state FROM source.capture_work',
    )
  ).rows.map((r) => r.state);
  assert.ok(states.includes('pending') && states.includes('acknowledged'));
  evidence('BF10', {
    expectedFence,
    visible,
    states,
    held: await session(s, `${required('M1_RUN_ID')}:BF10-held-across-cut`),
    heldId: fenceLateId,
    laterCommitted: later.id,
  });
});
for (const [id, boundary, kill, direct] of [
  ['BF11', 'fence.before_source_commit', true, false],
  ['BF11H', 'fence.before_source_commit', false, true],
  ['BF11A', 'fence.after_source_commit.before_pipeline_attach', true, false],
  ['BF11AH', 'fence.after_source_commit.before_pipeline_attach', false, true],
] as const) {
  await test(name(id), async (t) => {
    const { s, p } = await setup(t),
      key = direct ? randomUUID() : runId;
    const child = launchBackfill(t, id, key, boundary, {
        action: direct ? 'fence' : 'once',
      }),
      barrier = await child.barrier();
    let tx: unknown = null;
    let contender: Promise<Record<string, unknown>> | undefined;
    let contention: unknown = null;
    if (boundary === 'fence.before_source_commit') {
      tx = await session(s, child.sourceApplicationName);
      assert.equal(object(tx)['state'], 'idle in transaction');
      assert.equal(
        (
          await s.query(
            'SELECT * FROM source.backfill_fences WHERE run_id=$1',
            [key],
          )
        ).rowCount,
        0,
      );
      if (id === 'BF11H') {
        const cfg = backfillConfig('fence-contender');
        contender = new BackfillSource(cfg.source, cfg.binding).fence(key);
        void contender.catch(() => undefined);
        const observed = await databaseWaitFor(
          s,
          () =>
            s.query<Record<string, unknown>>(
              'SELECT pid,backend_xid::text,state,wait_event_type,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE application_name=$1',
              [cfg.source.application_name],
            ),
          (r) =>
            r.rows.some(
              (row) =>
                row['wait_event_type'] === 'Lock' &&
                Array.isArray(row['blockers']) &&
                row['blockers'].includes(object(tx)['pid']),
            ),
          'Concurrent fence key arbitration',
          5000,
        );
        contention = observed.rows;
        t.after(async () => {
          await contender?.catch(() => undefined);
        });
      }
    } else {
      assert.equal(
        (
          await s.query(
            'SELECT * FROM source.backfill_fences WHERE run_id=$1',
            [key],
          )
        ).rowCount,
        1,
      );
      assert.equal(
        (
          await p.query<{ fence: unknown }>(
            'SELECT fence FROM pipeline.backfill_runs WHERE run_id=$1',
            [runId],
          )
        ).rows[0]?.fence,
        null,
      );
    }
    if (!kill) child.release();
    const exit = await child.finish(kill ? 'kill' : 'run');
    const recovered = contender ? await contender : null;
    if (recovered) assert.deepEqual(recovered, barrier['data']);
    const sessionGone = await gone(s, child.sourceApplicationName);
    const rows = (
      await s.query<{ id: string }>(
        "SELECT source_epoch::text||':'||entity_id::text||':'||entity_version::text id FROM source.backfill_fence_members WHERE run_id=$1",
        [key],
      )
    ).rows.map((r) => r.id);
    if (kill && boundary === 'fence.before_source_commit')
      assert.deepEqual(rows, []);
    else checkSet(rows, expectedFence, 'Exact immutable cut');
    evidence(id, {
      barrier,
      tx,
      exit: { ...exit, sessionGone },
      key,
      members: rows,
      contention,
      recovered,
    });
  });
}
await test(name('BF12'), async (t) => {
  const { s, p } = await setup(t);
  const cut = await backfillSource().fence(runId);
  assert.equal(text(cut['member_count']), String(expectedFence.length));
  await backfillLedger().attach(runId, cut);
  const r = await claimPage();
  assert.equal(r.claim.range, 0);
  const before = await snapshot(p);
  await assert.rejects(
    backfillLedger().transaction(async (tx) => {
      await tx.page(r);
      throw new Error('Deliberate import rollback');
    }),
    (error: unknown) => {
      assert.ok(error instanceof TransactionError);
      assert.equal(error.outcome, 'rolled_back');
      assert.equal(error.sqlState, undefined);
      assert.ok(error.cause instanceof Error);
      assert.equal(error.cause.message, 'Deliberate import rollback');
      return true;
    },
  );
  assert.deepEqual(await snapshot(p), before);
  await backfillLedger().change(r.claim, 'defer', 1);
  const child = launchBackfill(
      t,
      'BF12-import',
      runId,
      'fence.after_import_commit.before_success',
    ),
    barrier = await child.barrier(),
    request = requestFromBarrier(barrier['data']);
  assert.equal(request.claim.range, 0);
  assert.equal(
    (
      await p.query(
        'SELECT * FROM pipeline.backfill_batches WHERE batch_id=$1',
        [request.batchId],
      )
    ).rowCount,
    1,
  );
  const exit = await child.finish('kill');
  const sessionGone = await gone(p, child.applicationName);
  const stored = await snapshot(p);
  assert.equal((await backfillLedger().page(request))['replayed'], true);
  assert.deepEqual(await snapshot(p), stored);
  assert.deepEqual(await backfillSource().fence(runId), cut);
  evidence('BF12', {
    barrier,
    exit: { ...exit, sessionGone },
    cut,
    stored,
    sourceMembers: (
      await s.query(
        'SELECT allocation_id::text,entity_id::text,entity_version::text FROM source.backfill_fence_members WHERE run_id=$1 ORDER BY allocation_id',
        [runId],
      )
    ).rows,
  });
});
await test(name('BF12H'), async (t) => {
  const { p } = await setup(t),
    child = launchBackfill(
      t,
      'BF12H-import',
      runId,
      'fence.after_import_commit.before_success',
    );
  const barrier = await child.barrier();
  child.release();
  const exit = await child.finish();
  const sessionGone = await gone(p, child.applicationName);
  evidence('BF12H', { barrier, exit: { ...exit, sessionGone } });
});
await test(name('BF08'), async (t) => {
  const { p } = await setup(t);
  await scanTo('draining');
  const es = new EsTransport(esConfig());
  try {
    await finish(es, p);
  } finally {
    await es.close();
  }
  await publisher().once();
  await observer().once();
  await backfillLedger().advance(runId);
  const state = await scan().status(runId);
  assert.equal(state.phase, 'draining');
  assert.ok(
    BigInt(text(object(state.evidence['counts'])['consumer_pending'])) > 0n,
  );
  evidence('BF08', {
    state,
    reason:
      'Controlled consumer is paused while real publisher confirms and ES settles',
  });
});
await test(name('BF14'), async (t) => {
  const { s, p, c } = await setup(t);
  const frozen = (
    await p.query<{ event_id: string }>(
      'SELECT event_id FROM pipeline.backfill_members WHERE run_id=$1 ORDER BY event_id',
      [runId],
    )
  ).rows;
  assert.ok(fenceHeld);
  assert.equal((await fenceHeld.query('COMMIT')).command, 'COMMIT');
  await fenceHeld.end();
  fenceHeld = undefined;
  await rememberLegacy(s, fenceLateId, '1');
  const later = await declareMutation(
    s,
    postFenceId,
    '{"name":"after fixed fence","loyalty_points":45}',
  );
  // Isolated sequence-gap fixture: new source-owned identity, never a rewritten row/version.
  await s.query(
    "SELECT setval('source.entities_entity_id_seq',9007199254740992,true)",
  );
  const exactId = await declareMutation(s);
  assert.equal(exactId.id, '9007199254740993');
  await deliver(p, c);
  assert.equal(
    (
      await s.query(
        'SELECT * FROM source.backfill_fence_members WHERE run_id=$1 AND entity_id=$2',
        [runId, fenceLateId],
      )
    ).rowCount,
    0,
  );
  await backfillLedger().advance(runId);
  const completed = await scan().status(runId);
  assert.equal(completed.phase, 'complete');
  assert.deepEqual(
    (
      await p.query(
        'SELECT event_id FROM pipeline.backfill_members WHERE run_id=$1 ORDER BY event_id',
        [runId],
      )
    ).rows,
    frozen,
  );
  evidence('BF14', {
    completed: completed.evidence,
    postCut: later.eventId,
    lateLowerId: fenceLateId,
    frozen,
  });
});
await test(name('BF15'), async (t) => {
  const { s, p, c } = await setup(t);
  const actual = await reconcile(s, p, c);
  assert.ok(actual.requiredIds.length >= 257);
  assert.throws(() =>
    checkSet(
      actual.requiredIds.slice(1),
      actual.requiredIds,
      'missing untouched baseline',
    ),
  );
  assert.throws(() =>
    checkSet(
      [...actual.requiredIds, 'extra'],
      actual.requiredIds,
      'extra membership',
    ),
  );
  const first = actual.receiver[0];
  assert.ok(first);
  const corruptions = [
    { ...first.doc, entity_version: '9223372036854775807' },
    { ...first.doc, is_deleted: !first.doc['is_deleted'] },
    { ...first.doc, canonical_body_json: '{}' },
    { ...first.doc, search_fields: { name: 'corrupted with copied hash' } },
  ];
  for (const corrupted of corruptions)
    assert.throws(() => checkReceiver(corrupted, first.expectedDocument));
  assert.throws(() =>
    checkSet(
      [...mutations.keys(), ...mutations.keys()],
      [...mutations.keys()],
      'duplicate business effect',
    ),
  );
  evidence('BF15', {
    ...actual,
    negativeControls: [
      'missing untouched baseline',
      'extra membership',
      'wrong version',
      'wrong tombstone',
      'changed canonical payload with copied hash',
      'wrong projected field',
      'duplicate business effect',
    ],
  });
});
await test(name('BF13'), async (t) => {
  const { s, p, c } = await setup(t),
    es = new EsTransport(esConfig());
  t.after(() => es.close());
  const before = object(
    await es.request(
      'GET',
      `/${required('ES_INDEX')}/_doc/${recipe.epoch}:${hotId}`,
    ),
  );
  const bad = await declareMutation(
    s,
    hotId,
    '{"name":"declared rejected later revision","country":"GE","loyalty_points":"not-a-number"}',
  );
  evidence('BF13-expected-rejection', {
    eventId: bad.eventId,
    command: bad.command,
    reply: bad.reply,
    expectedClass: 'mapping',
    receiverBefore: before,
  });
  const second = randomUUID();
  await scan().start(second);
  await scanTo('draining', second);
  await deliver(p, c);
  const rejected = (
    await p.query<Record<string, unknown>>(
      'SELECT * FROM pipeline.es_dead_letters WHERE event_id=$1',
      [bad.eventId],
    )
  ).rows[0];
  assert.ok(rejected);
  assert.equal(rejected['error_class'], 'mapping');
  assert.match(
    text(rejected['context']),
    /loyalty_points|document_parsing|mapper/i,
  );
  const after = object(
    await es.request(
      'GET',
      `/${required('ES_INDEX')}/_doc/${recipe.epoch}:${hotId}`,
    ),
  );
  assert.deepEqual(after, before);
  const retained = await snapshot(p, second),
    originalRun = await snapshot(p),
    negative: unknown[] = [];
  for (const mode of [
    'missing_observation',
    'missing_member',
    'corrupt_checkpoint',
    'quarantined_classification',
  ] as const) {
    const targetRun = mode === 'quarantined_classification' ? runId : second;
    await p.query('BEGIN');
    try {
      if (mode === 'missing_observation') {
        await p.query(
          'ALTER TABLE pipeline.consumer_observations DISABLE TRIGGER immutable_rows',
        );
        await p.query(
          'DELETE FROM pipeline.consumer_observations WHERE event_id=$1',
          [bad.eventId],
        );
      }
      if (mode === 'missing_member') {
        await p.query(
          'ALTER TABLE pipeline.backfill_members DISABLE TRIGGER no_rewrite',
        );
        await p.query(
          'DELETE FROM pipeline.backfill_members WHERE run_id=$1 AND event_id=$2',
          [second, bad.eventId],
        );
      }
      if (mode === 'corrupt_checkpoint') {
        await p.query(
          'ALTER TABLE pipeline.backfill_ranges DISABLE TRIGGER backfill_range_guard',
        );
        await p.query(
          'UPDATE pipeline.backfill_ranges SET checkpoint=lower_key WHERE run_id=$1 AND range_no=1',
          [second],
        );
      }
      if (mode === 'quarantined_classification') {
        // Isolate quarantine from the separate real ES rejection. This labelled,
        // rolled-back SQL fixture is classification evidence, not a remote quarantine.
        await p.query('SELECT pipeline.backfill_advance($1)', [second]);
        await p.query(
          'ALTER TABLE pipeline.backfill_runs DISABLE TRIGGER backfill_run_guard',
        );
        await p.query(
          "UPDATE pipeline.backfill_runs SET phase='draining',completed_at=NULL WHERE run_id=$1",
          [runId],
        );
        const member = (
          await p.query<{ event_id: string }>(
            'SELECT event_id FROM pipeline.backfill_members WHERE run_id=$1 ORDER BY event_id LIMIT 1',
            [runId],
          )
        ).rows[0];
        assert.ok(member);
        await p.query(
          'ALTER TABLE pipeline.consumer_observations DISABLE TRIGGER immutable_rows',
        );
        await p.query(
          "UPDATE pipeline.consumer_observations SET state='quarantined' WHERE event_id=$1",
          [member.event_id],
        );
      }
      const result = object(
        (
          await p.query<{ value: unknown }>(
            'SELECT pipeline.backfill_advance($1) value',
            [targetRun],
          )
        ).rows[0]?.value,
      );
      let counts: unknown = null;
      if (mode === 'quarantined_classification') {
        counts = (
          await p.query<{ value: unknown }>(
            'SELECT pipeline.backfill_counts($1) value',
            [runId],
          )
        ).rows[0]?.value;
        assert.equal(object(counts)['es_errors'], '0');
        assert.equal(object(counts)['consumer_errors'], '1');
        assert.equal(result['phase'], 'complete_with_errors');
      } else {
        assert.equal(result['phase'], 'draining');
        assert.ok(result['blocked_reason']);
      }
      negative.push({
        mode,
        result,
        counts,
        privilegedRolledBackFixture: true,
        notAnActualQuarantine: true,
      });
    } finally {
      await p.query('ROLLBACK');
    }
    assert.deepEqual(await snapshot(p, second), retained);
    assert.deepEqual(await snapshot(p), originalRun);
  }
  await backfillLedger().advance(second);
  const completed = await scan().status(second);
  assert.equal(completed.phase, 'complete_with_errors');
  const diagnostic = JSON.stringify(rejected);
  await declareMutation(
    s,
    hotId,
    '{"name":"valid correction keeps old diagnostic","loyalty_points":99}',
  );
  await deliver(p, c);
  assert.equal(
    JSON.stringify(
      (
        await p.query(
          'SELECT * FROM pipeline.es_dead_letters WHERE event_id=$1',
          [bad.eventId],
        )
      ).rows[0],
    ),
    diagnostic,
  );
  assert.equal((await scan().status(runId)).phase, 'complete');
  evidence('BF13', {
    second,
    completed: completed.evidence,
    rejected,
    before,
    after,
    negative,
    oldRun: await snapshot(p),
  });
});
await test(name('BF17'), async (t) => {
  const { s, p, c } = await setup(t);
  for (let ordinal = 90; ordinal < 96; ordinal++) {
    const r = await member(s, String(ordinal));
    await declareMutation(
      s,
      r.entity_id,
      JSON.stringify({
        name: `medium ${ordinal}`,
        loyalty_points: ordinal,
        padding: 'm'.repeat(50000),
      }),
    );
  }
  const page = await backfillSource().page('89', '95');
  assert.ok(page.events.length > 0 && page.events.length < 6);
  assert.equal(page.eof, false);
  assert.equal(page.next, '94');
  assert.ok(page.events.reduce((n, e) => n + e.wireBytes.length, 0) <= 262144);
  const tail = await backfillSource().page(page.next, '95');
  assert.equal(tail.events.length, 1);
  assert.equal(tail.eof, true);
  const huge = await declareMutation(
    s,
    null,
    JSON.stringify({
      name: 'oversized source remains durable',
      padding: 'x'.repeat(90000),
    }),
  );
  const small = await declareMutation(
    s,
    null,
    '{"name":"small after oversized","loyalty_points":1}',
  );
  await deliver(p, c);
  assert.equal(
    (
      await s.query<{ state: string }>(
        'SELECT state FROM source.capture_work WHERE entity_id=$1',
        [huge.id],
      )
    ).rows[0]?.state,
    'blocked',
  );
  assert.equal(
    (
      await s.query<{ state: string }>(
        'SELECT state FROM source.capture_work WHERE entity_id=$1',
        [small.id],
      )
    ).rows[0]?.state,
    'acknowledged',
  );
  const third = randomUUID();
  await scan().start(third);
  for (let i = 0; i < 70; i++) {
    await scan().once(third);
    const st = await scan().status(third);
    const ranges = st.evidence['ranges'];
    assert.ok(Array.isArray(ranges));
    if (
      ranges.every((r: unknown) =>
        ['closed', 'blocked'].includes(text(object(r)['state'])),
      )
    )
      break;
  }
  const blocked = await scan().status(third);
  assert.equal(blocked.phase, 'scanning');
  assert.equal(blocked.blocked, true);
  assert.equal(blocked.evidence['sealed_at'], null);
  const ranges = blocked.evidence['ranges'];
  assert.ok(Array.isArray(ranges));
  assert.ok(ranges.some((r: unknown) => object(r)['state'] === 'closed'));
  assert.ok(
    ranges.some(
      (r: unknown) =>
        object(r)['state'] === 'blocked' &&
        object(r)['reason'] === 'oversized_record',
    ),
  );
  assert.ok(Buffer.byteLength(JSON.stringify(blocked)) <= 65536);
  const runtime = new (await import('pg')).default.Client(
    backfillConfig().source,
  );
  await runtime.connect();
  t.after(() => runtime.end());
  await assert.rejects(
    runtime.query('SELECT source.backfill_page($1,$2,0,257,17)', [
      recipe.epoch,
      required('PIPELINE_ID'),
    ]),
    { code: 'P8003' },
  );
  evidence('BF17', {
    prefix: {
      keys: page.keys,
      bytes: page.events.map((e) => e.wireBytes.length),
      next: page.next,
      eof: page.eof,
    },
    tail: { keys: tail.keys, eof: tail.eof },
    huge: huge.eventId,
    small: small.eventId,
    blocked: blocked.evidence,
  });
});
// Register file teardown after the sequential top-level awaited tests. Registering
// it before them allowed Node's root hook to close the cross-case held session early.
after(async () => {
  await held?.end();
  await fenceHeld?.end();
});
