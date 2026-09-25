import assert from 'node:assert/strict';
import { mock, test } from 'node:test';
import pg from 'pg';
import { OperationalDatabase } from '../../src/operations/database.ts';
import { TransactionError } from '../../src/internal/transaction.ts';

void test('failed observations identify the bounded read without exposing database details', async () => {
  const secret = 'private-password-and-query-parameter';
  const config = {
    host: 'unused',
    port: 1,
    database: 'unused',
    user: 'unused',
    password: secret,
    application_name: 'unit',
  };
  const failure = new pg.DatabaseError('SQL contains ' + secret, 1, 'error');
  failure.code = '57014';
  failure.detail = secret;
  const connect = mock.method(pg.Client.prototype, 'connect', async () => {});
  const end = mock.method(pg.Client.prototype, 'end', async () => {});
  const calls: string[] = [];
  const query = mock.method(pg.Client.prototype, 'query', (text: string) => {
    calls.push(text);
    if (text.includes('jsonb_build_object')) return Promise.reject(failure);
    return Promise.resolve({
      command: text.startsWith('BEGIN') ? 'BEGIN' : text,
      rows: [],
    });
  });
  const log = mock.method(console, 'error', () => {});
  try {
    const db = new OperationalDatabase({
      source: config,
      pipeline: config,
      consumer: config,
    });
    await assert.rejects(
      db.read('consumer', 'snapshot'),
      (error: unknown) =>
        error instanceof TransactionError && error.cause === failure,
    );
    assert.equal(log.mock.callCount(), 1);
    const encoded = String(log.mock.calls[0]?.arguments[0]);
    assert.ok(!encoded.includes(secret) && !encoded.includes('SELECT'));
    const result = JSON.parse(encoded) as Record<string, unknown>;
    assert.equal(result['event'], 'observation_failed');
    assert.equal(result['store'], 'consumer');
    assert.equal(result['operation'], 'snapshot');
    assert.match(encoded, /57014/);
    assert.equal(typeof result['elapsed_ms'], 'number');
    assert.ok(calls.includes('ROLLBACK'));
    assert.deepEqual(
      calls.filter((text) => text.startsWith('SET LOCAL statement_timeout=')),
      ['SET LOCAL statement_timeout=2500'],
    );
  } finally {
    log.mock.restore();
    query.mock.restore();
    end.mock.restore();
    connect.mock.restore();
  }
});
