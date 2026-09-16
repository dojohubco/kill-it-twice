import assert from 'node:assert/strict';
import test from 'node:test';
import { positiveBigint } from '../../src/source.ts';

test('source identifiers preserve unsafe-Number values and the signed BIGINT maximum', () => {
  for (const value of ['1', '9007199254740993', '9223372036854775807'])
    assert.equal(positiveBigint(value), value);
});

test('source identifiers reject numbers, overflow, zero, signs, whitespace and noncanonical decimals', () => {
  for (const value of [
    1,
    9007199254740992,
    1n,
    null,
    '',
    '0',
    '-1',
    '+1',
    '01',
    ' 1',
    '1.0',
    '1e3',
    '9223372036854775808',
    '9999999999999999999999999999999',
  ]) {
    assert.throws(() => positiveBigint(value), TypeError, String(value));
  }
});
