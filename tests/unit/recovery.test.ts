import test from 'node:test';
import assert from 'node:assert/strict';
import { RateSampler } from '../../src/operations/monitor.ts';
import { recoveryInput } from '../../src/operations/recovery.ts';
const epoch = '11111111-1111-4111-8111-111111111111';
const attempt = '22222222-2222-4222-8222-222222222222';
const request = {
  request_id: epoch,
  actor: 'operator',
  reason: 'repair receiver',
  destination_id: attempt,
  generation: '9223372036854775807',
  selection: [
    { event_id: `${epoch}:2:1`, attempt_id: attempt },
    { event_id: `${epoch}:1:1`, attempt_id: attempt },
  ],
};
void test('recovery input fixes request identity and bounded normalized selection without Number coercion', () => {
  const v = recoveryInput(request, 'replay');
  assert.equal(v.selection[0]?.event_id, `${epoch}:1:1`);
  assert.equal(v.generation, '9223372036854775807');
  assert.equal(request.selection[0]?.event_id, `${epoch}:2:1`);
  assert.equal(
    recoveryInput(
      {
        ...request,
        selection: Array.from({ length: 50 }, (_, i) => ({
          event_id: `${epoch}:${i + 1}:1`,
          attempt_id: attempt,
        })),
      },
      'replay',
    ).selection.length,
    50,
  );
  for (const input of [
    { ...request, selection: [] },
    { ...request, selection: Array(51).fill(request.selection[0]) },
    { ...request, selection: [request.selection[0], request.selection[0]] },
    { ...request, actor: 'ა'.repeat(30) },
    { ...request, sql: 'UPDATE state' },
    { ...request, generation: 1 },
  ])
    assert.throws(() => recoveryInput(input, 'replay'));
  assert.throws(() => recoveryInput(request, 'supersession'));
  assert.deepEqual(
    recoveryInput({ ...request, selection: [] }, 'verify_target').selection,
    [],
  );
});
void test('useful rates distinguish warming idle reset unavailable and exact large integer deltas', () => {
  const r = new RateSampler();
  assert.equal(
    r.sample('staged', 'p:e', '9007199254740993', 100).state,
    'warming',
  );
  assert.equal(
    r.sample('staged', 'p:e', '9007199254740995', 1100).per_second,
    '2.000000',
  );
  assert.equal(
    r.sample('staged', 'p:e', '9007199254740995', 2100).per_second,
    '0.000000',
  );
  assert.equal(r.sample('staged', 'p:e', null, 2200).state, 'unknown');
  assert.equal(
    r.sample('staged', 'p:e', '9007199254740996', 2300).state,
    'warming',
  );
  assert.equal(
    r.sample('staged', 'p:new', '1', 2400).reason,
    'identity_changed',
  );
  assert.equal(
    r.sample('staged', 'p:new', '0', 2500).reason,
    'counter_regressed',
  );
  assert.equal(
    r.sample('staged', 'p:new', '0', 2500).reason,
    'nonpositive_interval',
  );
  assert.equal(r.sample('staged', 'p:new', '0', 2600).state, 'warming');
  assert.equal(
    new RateSampler().sample('staged', 'p:new', '1', 1).state,
    'warming',
  );
});
void test('rate series remain independent and nonfinite clocks cannot yield fabricated throughput', () => {
  const r = new RateSampler();
  r.sample('consumer_processed', 'x', '1', 0);
  r.sample('mutation_effects', 'x', '0', 0);
  assert.equal(
    r.sample('consumer_processed', 'x', '2', 500).per_second,
    '2.000000',
  );
  assert.equal(
    r.sample('mutation_effects', 'x', '0', 500).per_second,
    '0.000000',
  );
  assert.equal(r.sample('consumer_processed', 'x', '3', NaN).state, 'unknown');
  assert.equal(
    r.sample('consumer_processed', 'x', '3', Infinity).state,
    'unknown',
  );
  assert.equal(r.sample('consumer_processed', 'x', '3', 600).state, 'warming');
});
