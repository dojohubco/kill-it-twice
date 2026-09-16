import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { test } from 'node:test';
import { object } from '../../scripts/acceptance.ts';
import { first } from '../../scripts/rows.ts';
import { evidence, required } from '../support/db.ts';
import { setup } from '../support/staging.ts';
import { markerState } from '../support/isolation.ts';
import { drain, reconcile, worker } from '../support/capture.ts';
async function observations() {
  const upgrade = object(
    JSON.parse(
      await readFile(
        join(required('M1_ARTIFACT_DIR'), 'capture-upgrade.json'),
        'utf8',
      ),
    ),
  );
  const isolation = object(upgrade['isolation']);
  assert.equal(isolation['mode'], 'unregistered');
  return {
    registration: object(upgrade['registration']),
    probe: object(isolation['registrationProbe']),
  };
}
void test('R01 old repeatable-read snapshot is rejected before invisible binding lookup', async (t) => {
  const { s } = await setup(t);
  const { probe, registration } = await observations();
  const old = object(probe['oldRR']),
    tx = object(registration['transaction']);
  assert.equal(old['role'], 'source_writer');
  assert.equal(old['isolation'], 'repeatable read');
  assert.equal(tx['isolation'], 'read committed');
  assert.equal(registration['registrationBegin'], 'BEGIN');
  assert.equal(registration['registrationCommit'], 'COMMIT');
  assert.deepEqual(probe['locks'], []);
  assert.ok(BigInt(String(tx['xid'])) > BigInt(String(old['xid'])));
  assert.deepEqual(probe['visibility'], { visible: false });
  const before = object(probe['before']);
  assert.ok(Array.isArray(before['binding']));
  assert.equal(object(first(before['binding']))['xid'], tx['xid']);
  const attempt = object(probe['rrAttempt']);
  assert.equal(object(attempt['failure'])['sqlState'], '25001');
  assert.equal(attempt['completion'], 'ROLLBACK');
  assert.match(
    String(object(attempt['failure'])['where']),
    /enqueue_capture\(\)/,
  );
  const state = await markerState(s, String(probe['rrMarker']));
  assert.deepEqual(state.entities, []);
  assert.deepEqual(state.outbox, []);
  assert.deepEqual(state.work, []);
  const capture = await worker('R01').captureOnce();
  assert.equal(capture.sourceState.missing, '0');
  evidence('R01', { old, registration: tx, probe, state, capture });
});
void test('R02 long-lived read-committed writer enqueues and captures after registration', async (t) => {
  const { s, p } = await setup(t);
  const { probe } = await observations();
  const old = object(probe['oldRC']);
  assert.equal(old['role'], 'source_writer');
  assert.equal(old['isolation'], 'read committed');
  const attempt = object(probe['rcAttempt']);
  assert.equal(attempt['failure'], null);
  assert.equal(attempt['completion'], 'COMMIT');
  const captured = await drain('R02');
  const state = await markerState(s, String(probe['rcMarker']));
  assert.equal(state.entities.length, 1);
  assert.equal(state.outbox.length, 1);
  assert.equal(state.work.length, 1);
  assert.equal(first(state.work)['state'], 'acknowledged');
  await reconcile(s, p, 'R02-reconciliation', [
    String(first(state.entities)['entity_id']),
  ]);
  evidence('R02', { old, attempt, state, captured });
});
void test('R03 both real registration lock controls retain exact work and acknowledgements', async (t) => {
  const { s, p } = await setup(t);
  const { registration } = await observations();
  for (const [key, blocker] of [
    ['waitA', 'aPid'],
    ['waitB', 'rPid'],
  ] as const) {
    const waits = registration[key];
    assert.ok(Array.isArray(waits));
    const blockers = object(first(waits))['blockers'];
    assert.ok(Array.isArray(blockers));
    assert.ok(blockers.includes(registration[blocker]));
  }
  assert.equal(registration['repeatedUnchanged'], true);
  const rows = registration['rows'];
  assert.ok(Array.isArray(rows));
  assert.equal(
    new Set(rows.map((r) => object(r)['work_id'])).size,
    rows.length,
  );
  for (const raw of rows) assert.equal(object(raw)['state'], 'pending');
  await drain('R03');
  const ids = ['createdA', 'createdB'].map((key) =>
    String(object(registration[key])['entity_id']),
  );
  const records = await reconcile(s, p, 'R03-reconciliation', ids);
  assert.equal(records.length, 2);
  evidence('R03', { registration, records });
});
void test('R05 unsupported pre-registration mutations reject while read-committed work is retained', async (t) => {
  const { s, p } = await setup(t);
  const { probe } = await observations();
  const rejected = probe['rejectedBefore'];
  assert.ok(Array.isArray(rejected));
  assert.equal(rejected.length, 2);
  for (const raw of rejected) {
    const r = object(raw),
      attempt = object(r['attempt']);
    assert.equal(object(attempt['failure'])['sqlState'], '25001');
    assert.equal(attempt['completion'], 'ROLLBACK');
    assert.deepEqual(r['state'], {
      entities: [],
      outbox: [],
      work: [],
      binding: [],
    });
    const now = await markerState(s, String(r['marker']));
    assert.deepEqual(now.entities, []);
    assert.deepEqual(now.outbox, []);
    assert.deepEqual(now.work, []);
  }
  const before = object(probe['preState']);
  assert.deepEqual(before['binding'], []);
  assert.deepEqual(before['work'], []);
  assert.equal(object(probe['preAttempt'])['completion'], 'COMMIT');
  await drain('R05');
  const state = await markerState(s, String(probe['preMarker']));
  assert.equal(state.outbox.length, 1);
  assert.equal(state.work.length, 1);
  assert.equal(first(state.work)['state'], 'acknowledged');
  await reconcile(s, p, 'R05-reconciliation', [
    String(first(state.entities)['entity_id']),
  ]);
  evidence('R05', { rejected, before, preAttempt: probe['preAttempt'], state });
});
