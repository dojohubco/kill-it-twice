import assert from 'node:assert/strict';
import test from 'node:test';
import { redact, waitFor } from '../../scripts/support.ts';

test('artifact redaction removes every temporary credential occurrence', () => {
  assert.equal(redact('secret-a secret-b secret-a', ['secret-a', 'secret-b', '']), '[REDACTED] [REDACTED] [REDACTED]');
});

test('bounded observation waits for evidence and returns the accepted observation', async () => {
  let count = 0;
  assert.deepEqual(await waitFor(async () => ({ count: ++count }), (value) => value.count === 2, 'unit observation', 1_000), { count: 2 });
});

test('missing boundary evidence fails with its last observation, never skips', async () => {
  await assert.rejects(waitFor(async () => 'not reached', () => false, 'missing boundary', 1), /Deadline waiting for missing boundary; last observation: "not reached"/);
});
