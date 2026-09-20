import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { backfillEmptyCases } from '../../scripts/required-backfill-cases.ts';
import { setup } from '../support/rabbit.ts';
import { scan, scanTo, backfillLedger, snapshot } from '../support/backfill.ts';
import { required, evidence } from '../support/db.ts';
import { bootstrap } from '../support/bootstrap.ts';
const entry = backfillEmptyCases[0];
assert.ok(entry);
await test(entry.name, async (t) => {
  const { s, p } = await setup(t);
  assert.equal((await s.query('SELECT * FROM source.entities')).rowCount, 0);
  const lifecycle = await bootstrap().status(required('SOURCE_EPOCH'));
  assert.equal(lifecycle.phase, 'active');
  assert.equal(lifecycle.bootstrap_key, null);
  const id = randomUUID(),
    start = await scan().start(id);
  assert.equal(start.evidence['upper_key'], '0');
  await scanTo('draining', id);
  await backfillLedger().advance(id);
  const completed = await scan().status(id);
  assert.equal(completed.phase, 'complete');
  assert.equal((await p.query('SELECT * FROM pipeline.events')).rowCount, 0);
  assert.equal(
    (await s.query('SELECT * FROM source.backfill_fence_members')).rowCount,
    0,
  );
  evidence('BF01E', {
    start,
    completed,
    lifecycle,
    retained: await snapshot(p, id),
  });
});
