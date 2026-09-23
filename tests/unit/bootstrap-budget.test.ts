import assert from 'node:assert/strict';
import { mock, test } from 'node:test';
import pg from 'pg';
import { Bootstrap } from '../../src/bootstrap.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { safeFailure } from '../../scripts/runtime/private.ts';

const id = '11111111-1111-4111-8111-111111111111';
const config = {
  host: 'unused',
  port: 1,
  database: 'unused',
  user: 'unused',
  password: 'unused',
  application_name: 'unit',
};

void test('only full bootstrap sealing receives its finite proof budget', async () => {
  const calls: { text: string; budget: unknown }[] = [];
  const connect = mock.method(pg.Client.prototype, 'connect', () =>
    Promise.resolve(),
  );
  const end = mock.method(pg.Client.prototype, 'end', () => Promise.resolve());
  const query = mock.method(pg.Client.prototype, 'query', (input: unknown) => {
    assert.ok(
      typeof input === 'string' || (input && typeof input === 'object'),
    );
    const obj =
      typeof input === 'string'
        ? { text: input }
        : (input as Record<string, unknown>);
    const text = String(obj['text']);
    calls.push({
      text,
      budget: 'query_timeout' in obj ? obj['query_timeout'] : undefined,
    });
    return Promise.resolve({
      command: text.startsWith('BEGIN') ? 'BEGIN' : text,
      rows: text.includes('source.seed_chunk')
        ? [
            {
              ordinal: '1',
              entity_id: '1',
              recorded_at: '2026-09-23T00:00:00Z',
              replayed: false,
            },
          ]
        : [
            {
              value: {
                source_epoch: id,
                phase: 'bootstrapping',
                bootstrap_key: id,
                requested_count: '1',
                completed_count: '1',
                chunk_size: 1,
              },
            },
          ],
    });
  });
  try {
    const bootstrap = new Bootstrap(config);
    await bootstrap.status(id);
    await bootstrap.begin({
      epoch: id,
      key: id,
      version: 1,
      seed: 'unit',
      count: '1',
      chunkSize: 1,
    });
    await bootstrap.chunk(id, id, '1');
    await bootstrap.seal(id, id);
    await bootstrap.transaction((tx) => tx.activate(id, id, id));
    assert.equal(
      calls.filter((c) => c.text === 'SET LOCAL statement_timeout=60000')
        .length,
      1,
    );
    assert.deepEqual(
      calls.filter((c) => c.budget !== undefined),
      [{ text: 'SELECT source.seal_bootstrap($1,$2) value', budget: 65000 }],
    );
    assert.equal(calls.filter((c) => c.text === 'COMMIT').length, 5);
  } finally {
    query.mock.restore();
    end.mock.restore();
    connect.mock.restore();
  }
});

void test('runtime diagnostics retain SQL state and unknown outcome without private cause text', () => {
  const cause = new pg.DatabaseError('private diagnostic content', 0, 'error');
  cause.code = '57014';
  const error = new TransactionError(cause, 'unknown', 'commit', [
    new Error('private cleanup'),
  ]);
  const result = safeFailure(error);
  assert.equal(result['sql_state'], '57014');
  assert.equal(result['transaction_outcome'], 'unknown');
  assert.equal(result['transaction_phase'], 'commit');
  assert.equal(result['cleanup_error_count'], 1);
  assert.ok(!JSON.stringify(result).includes('private'));
});
