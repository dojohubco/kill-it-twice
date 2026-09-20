import test from 'node:test';
import assert from 'node:assert/strict';
import { ciTimeout } from '../../scripts/ci-timeout.ts';
await test('complete gate deadlines include M4.1 M5A and M5B without broadening arbitrary commands', () => {
  for (const gate of ['verify-m3', 'verify-m4', 'verify-m4-1'])
    assert.equal(ciTimeout('make', [gate]), 3000000);
  assert.equal(ciTimeout('make', ['verify-m5a']), 3600000);
  assert.equal(ciTimeout('make', ['verify-m5b']), 4500000);
  assert.equal(ciTimeout('make', ['verify-m6']), 5400000);
  for (const [executable, args] of [
    ['npm', ['ci']],
    ['make', ['quality']],
    ['make', ['unknown-gate']],
    ['make', ['verify-m5b', 'extra']],
    ['make', ['verify-m6', 'extra']],
    ['npm', ['verify-m6']],
    ['make', ['verify-m5a', 'other']],
    ['other', ['verify-m5a']],
  ] as const)
    assert.equal(ciTimeout(executable, args), 600000);
});
