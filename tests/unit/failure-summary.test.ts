import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { failureSummary } from '../../src/internal/failure-summary.ts';
import { TransactionError } from '../../src/internal/transaction.ts';

await test('failure summary retains transaction evidence without messages or SQL', () => {
  const cause = new pg.DatabaseError('password=secret SQL payload', 0, 'error');
  cause.code = '57014';
  cause.detail = 'private payload';
  const error = new TransactionError(
    cause,
    'rolled_back',
    'work',
    [],
    'ROLLBACK',
  );
  const result = failureSummary(new AggregateError([error], 'secret'));
  assert.equal(result[1]?.['sqlState'], '57014');
  assert.equal(result[1]?.['outcome'], 'rolled_back');
  assert.equal(result[2]?.['sqlState'], '57014');
  assert.doesNotMatch(
    JSON.stringify(result),
    /secret|password|private payload/,
  );
});
await test('failure summary bounds cycles, cleanup branches and untrusted errors', () => {
  const cycle = new Error('secret');
  cycle.cause = cycle;
  const error = new AggregateError(
    Array.from({ length: 100 }, () => new Error('secret', { cause: cycle })),
  );
  const result = failureSummary(error);
  assert.ok(result.length <= 12);
  assert.doesNotMatch(JSON.stringify(result), /secret/);
  assert.equal(
    failureSummary({ code: '57014', message: 'secret' })[0]?.['type'],
    'unknown',
  );
});
