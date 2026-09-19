import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { batchReproductionCases } from '../../scripts/required-batch-cases.ts';
import { evidence } from '../support/db.ts';
import { consumerConfig, setup, topology } from '../support/rabbit.ts';
import {
  cohort,
  cap,
  sqlBytes,
  consumerSnapshot,
  assertCohort,
  publishCohort,
  queueState,
} from '../support/batch-fixture.ts';
import { launchBatch } from '../support/batch-process.ts';
import { object } from '../../scripts/acceptance.ts';
const name = batchReproductionCases[0]?.name;
assert.ok(name);
void test(name, async (t) => {
  const { s, p, c } = await setup(t),
    group = await cohort(
      s,
      p,
      c,
      Array.from({ length: 32 }, () => 32768),
    );
  const sizes = await sqlBytes(c, group);
  assert.equal(sizes.wireBytes, cap);
  assert.equal(sizes.oldSqlCharge, cap + 480);
  const before = await consumerSnapshot(c);
  const client = new pg.Client({
    ...consumerConfig('runtime', 'm41-direct-repro'),
    query_timeout: 15000,
    connectionTimeoutMillis: 5000,
  });
  await client.connect();
  let identity: unknown, error: unknown, rollback: unknown;
  try {
    await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    identity = (
      await client.query(
        "SELECT current_user,current_setting('transaction_isolation') isolation,pg_backend_pid() pid,pg_current_xact_id()::text xid,pg_current_snapshot()::text snapshot",
      )
    ).rows;
    const target = topology();
    try {
      await client.query(
        'SELECT * FROM consumer.process_batch($1,$2,$3,$4,$5::jsonb)',
        [
          target.consumerId,
          target.epoch,
          target.pipelineId,
          target.registrationId,
          JSON.stringify(
            group.events.map((e) => ({
              body: e.bodyBytes.toString('hex'),
              hash: e.contentSha256,
            })),
          ),
        ],
      );
      assert.fail(
        'Predicted old SQL rejection did not reproduce; stop for review',
      );
    } catch (failure) {
      assert.ok(failure instanceof pg.DatabaseError);
      assert.equal(failure.code, 'P6002');
      assert.equal(failure.message, 'Consumer batch exceeds 1 MiB');
      error = {
        sqlState: failure.code,
        message: failure.message,
        where: failure.where,
      };
    }
    rollback = (await client.query('ROLLBACK')).command;
  } finally {
    await client.end();
  }
  assert.equal(rollback, 'ROLLBACK');
  assert.deepEqual(await consumerSnapshot(c), before);
  await assertCohort(s, c, group, false);
  await publishCohort(group);
  await queueState(32);
  const attempts = [];
  for (let i = 0; i < 2; i++) {
    const child = launchBatch(t, `B01-attempt-${i}`),
      exit = await child.finish('failure');
    const failure = exit.messages.find((m) => m['type'] === 'batch-failure');
    assert.ok(failure);
    const trace = object(failure['trace']);
    assert.deepEqual(trace['acknowledgements'], []);
    const batches = trace['batches'];
    assert.ok(Array.isArray(batches));
    assert.equal(batches.length, 1);
    const batch = object(batches[0]);
    assert.equal(batch['count'], 32);
    assert.equal(batch['wireBytes'], cap);
    assert.equal(batch['oldSqlCharge'], cap + 480);
    assert.equal(batch['sqlState'], 'P6002');
    assert.equal(batch['completion'], 'rolled_back');
    const events = batch['events'];
    assert.ok(
      Array.isArray(events) && events.every((e) => typeof e === 'string'),
    );
    assert.deepEqual(
      new Set(events),
      new Set(group.events.map((e) => e.body.event_id)),
    );
    const received = trace['received'];
    assert.ok(Array.isArray(received));
    assert.equal(received.length, 32);
    if (i) assert.ok(received.every((r) => object(r)['redelivered'] === true));
    const queue = await queueState(32);
    assert.deepEqual(await consumerSnapshot(c), before);
    attempts.push({ exit, queue });
  }
  evidence('B01', {
    intent:
      'historical expected diagnostic failure, no successful v2 processing',
    sizes,
    identity,
    error,
    rollback,
    attempts,
    consumerUnchanged: true,
    quarantineUnchanged: true,
    sourceDerived: true,
  });
});
