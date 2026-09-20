import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { key, claim, status } from '../../src/backfill/types.ts';
import { Backfill } from '../../src/backfill/worker.ts';
await test('backfill cursor parsing keeps zero and the complete signed BIGINT range exact', () => {
  assert.equal(key('0'), '0');
  assert.equal(key('9007199254740993'), '9007199254740993');
  assert.equal(key('9223372036854775807'), '9223372036854775807');
  for (const bad of ['-1', '00', '1.0', '9223372036854775808', 1, null])
    assert.throws(() => key(bad));
});
await test('backfill status and claim boundaries reject missing or malformed evidence', () => {
  assert.equal(claim(null), null);
  assert.throws(() => claim({ range_no: 17 }));
  assert.throws(() => status({ phase: 'completed' }));
  assert.throws(() =>
    status({ run_id: randomUUID(), phase: 'scanning', desired_paused: false }),
  );
});
await test('backfill lease and follow bounds are explicit and cannot disable ownership checks', () => {
  const sql = {
      host: '127.0.0.1',
      port: 1,
      user: 'unconnected',
      password: 'not-a-credential',
      database: 'unconnected',
      application_name: 'unit-only',
    },
    cfg = {
      source: sql,
      pipeline: sql,
      binding: { sourceEpoch: randomUUID(), pipelineId: randomUUID() },
    };
  for (const options of [
    { leaseMs: 0 },
    { leaseMs: 30001 },
    { renewalMs: 0 },
    { leaseMs: 300, renewalMs: 200 },
    { idleMs: 0 },
  ])
    assert.throws(() => new Backfill(cfg, options));
  assert.equal(new Backfill(cfg).options.leaseMs, 30000);
});
