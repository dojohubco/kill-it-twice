import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { backfillEmptyCases } from '../../scripts/required-backfill-cases.ts';
import { setup } from '../support/rabbit.ts';
import {
  scan,
  scanTo,
  backfillLedger,
  snapshot,
  declareMutation,
  deliver,
} from '../support/backfill.ts';
import { required, evidence } from '../support/db.ts';
import { bootstrap } from '../support/bootstrap.ts';
const entry = backfillEmptyCases[0];
assert.ok(entry);
await test(entry.name, async (t) => {
  const { s, p, c } = await setup(t);
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
  const mutation = await declareMutation(s),
    populatedId = randomUUID(),
    populatedStart = await scan().start(populatedId);
  assert.equal(populatedStart.evidence['upper_key'], mutation.id);
  assert.equal(
    (await bootstrap().status(required('SOURCE_EPOCH'))).bootstrap_key,
    null,
  );
  await scanTo('draining', populatedId);
  await deliver(p, c);
  await backfillLedger().advance(populatedId);
  const populatedCompleted = await scan().status(populatedId);
  assert.equal(populatedCompleted.phase, 'complete');
  assert.equal(
    (await s.query('SELECT * FROM source.baseline_revisions')).rowCount,
    0,
  );
  const canonical = (
    await p.query<{ event_id: string; kind: string }>(
      "SELECT event_id,convert_from(body_bytes,'UTF8')::jsonb->>'kind' kind FROM pipeline.events",
    )
  ).rows;
  assert.deepEqual(canonical, [
    { event_id: mutation.eventId, kind: 'mutation' },
  ]);
  assert.equal(
    (await c.query('SELECT * FROM consumer.mutation_effects')).rowCount,
    1,
  );
  assert.equal((await scan().status(id)).phase, 'complete');
  evidence('BF01E', {
    start,
    completed,
    lifecycle,
    retained: await snapshot(p, id),
    populatedLegacy: {
      mutation,
      start: populatedStart,
      completed: populatedCompleted,
      canonical,
    },
  });
});
