import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  backfillPageRecords,
  runtimePageRecords,
} from '../../src/backfill/bounds.ts';
import { BackfillSource } from '../../src/backfill/source.ts';
import { BackfillLedger } from '../../src/backfill/ledger.ts';
import { Backfill } from '../../src/backfill/worker.ts';

void test('backfill count tuning is explicit while the historical default remains sixteen', () => {
  assert.equal(backfillPageRecords(), 16);
  assert.equal(runtimePageRecords(undefined), 16);
  for (const value of [1, 16, 32, 64]) {
    assert.equal(backfillPageRecords(value), value);
    assert.equal(runtimePageRecords(String(value)), value);
  }
  for (const value of [0, -1, 65, NaN, Infinity, 1.5, '64', null])
    assert.throws(() => backfillPageRecords(value));
  for (const value of [
    '',
    '0',
    '65',
    '064',
    ' 64',
    '64 ',
    '+64',
    '1e1',
    '0x10',
    '1.5',
  ])
    assert.throws(() => runtimePageRecords(value));
});
void test('source and atomic ledger reject invalid page ceilings before opening a session', () => {
  const config = {
    host: '127.0.0.1',
    port: 1,
    database: 'no_connection',
    user: 'none',
    password: 'unused',
    application_name: 'unit-only',
  };
  const binding = {
    sourceEpoch: '11111111-1111-4111-8111-111111111111',
    pipelineId: '22222222-2222-4222-8222-222222222222',
  };
  assert.throws(() => new BackfillSource(config, binding, 65));
  assert.throws(() => new BackfillLedger(config, 65));
  assert.throws(
    () =>
      new Backfill(
        { source: config, pipeline: config, binding },
        { pageRecords: 65 },
      ),
  );
  assert.equal(
    new Backfill({ source: config, pipeline: config, binding }).options
      .pageRecords,
    16,
  );
  assert.equal(
    new Backfill(
      { source: config, pipeline: config, binding },
      { pageRecords: 64 },
    ).options.pageRecords,
    64,
  );
});
