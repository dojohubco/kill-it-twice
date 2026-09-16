import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import { first } from '../../scripts/rows.ts';
import { connect, evidence, required } from '../support/db.ts';
import { setup } from '../support/staging.ts';
import { worker, control } from '../support/capture.ts';
import { CaptureFailure } from '../../src/capture.ts';
import {
  transactionSnapshot,
  mutationAttempt,
  markerState,
} from '../support/isolation.ts';

void test('B01 old transaction snapshot commits retained outbox without capture work before correction', async (t) => {
  const { s, p, epoch } = await setup(t);
  const a = await connect('writer', 'old-snapshot');
  t.after(() => a.end());
  const b = await connect('admin', 'registration');
  t.after(() => b.end());
  assert.equal(
    first(
      (
        await s.query<{ n: string }>(
          'SELECT count(*)::text AS n FROM source.capture_binding',
        )
      ).rows,
    ).n,
    '0',
  );
  const definition = first(
    (
      await s.query<{ definition: string }>(
        "SELECT pg_get_functiondef('source.enqueue_capture()'::regprocedure) AS definition",
      )
    ).rows,
  ).definition;
  assert.ok(
    !definition.includes('transaction_isolation'),
    'Reproduction requires uncorrected migration 004',
  );
  assert.equal(
    (await a.query('BEGIN ISOLATION LEVEL REPEATABLE READ')).command,
    'BEGIN',
  );
  const old = await transactionSnapshot(a);
  assert.equal(old.role, 'source_writer');
  assert.equal(old.isolation, 'repeatable read');
  const locks = (
    await s.query<Record<string, unknown>>(
      "SELECT mode FROM pg_locks WHERE pid=$1 AND relation='source.outbox'::regclass",
      [old.pid],
    )
  ).rows;
  assert.deepEqual(locks, []);
  assert.equal(
    (await b.query('BEGIN ISOLATION LEVEL READ COMMITTED')).command,
    'BEGIN',
  );
  const registration = await transactionSnapshot(b);
  const visibility = first(
    (
      await s.query<{ visible: boolean }>(
        'SELECT pg_visible_in_snapshot($1::xid8,$2::pg_snapshot) AS visible',
        [registration.xid, old.snapshot],
      )
    ).rows,
  );
  assert.equal(visibility.visible, false);
  const register = (
    await b.query('SELECT source.register_capture($1,$2,$3)', [
      required('PIPELINE_ID'),
      epoch,
      'pg18-jsonb-text/v1',
    ])
  ).command;
  const registrationCommit = (await b.query('COMMIT')).command;
  assert.equal(registrationCommit, 'COMMIT');
  const marker = randomUUID();
  const before = await markerState(s, marker);
  assert.equal(before.binding.length, 1);
  assert.equal(first(before.binding)['xid'], registration.xid);
  const outcome = await mutationAttempt(
    a,
    'create',
    JSON.stringify({ isolationMarker: marker, precise: 'synthetic' }),
  );
  const after = await markerState(s, marker);
  const summary = await control('reproduction-summary').summary();
  let captureFailure;
  try {
    await worker('reproduction-capture').captureOnce();
  } catch (error) {
    assert.ok(error instanceof CaptureFailure);
    assert.ok(error.primary instanceof Error);
    captureFailure = {
      fatal: error.fatal,
      primary: error.primary.message,
      cleanupCount: error.cleanup.length,
    };
  }
  const pipeline = (
    await p.query<Record<string, unknown>>(
      'SELECT event_id FROM pipeline.events',
    )
  ).rows;
  evidence('B01-observed', {
    old,
    locks,
    registration,
    visibility,
    register,
    registrationCommit,
    marker,
    before,
    outcome,
    after,
    summary,
    captureFailure,
    pipeline,
    definition,
  });
  assert.equal(outcome.failure, null);
  assert.equal(outcome.completion, 'COMMIT');
  assert.equal(after.entities.length, 1);
  assert.equal(after.outbox.length, 1);
  assert.deepEqual(after.work, []);
  assert.equal(summary.missing, '1');
  assert.equal(summary.acknowledged, '0');
  assert.deepEqual(captureFailure, {
    fatal: true,
    primary: 'Required capture work is missing',
    cleanupCount: 0,
  });
  assert.deepEqual(pipeline, []);
});
