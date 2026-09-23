import assert from 'node:assert/strict';
import { mock, test } from 'node:test';
import pg from 'pg';
import { BackfillLedger } from '../../src/backfill/ledger.ts';
import { OperationalDatabase } from '../../src/operations/database.ts';
const config = {
  host: 'unused',
  port: 1,
  database: 'unused',
  user: 'unused',
  password: 'unused',
  application_name: 'unit',
};
const id = '11111111-1111-4111-8111-111111111111';
void test('only finite backfill audit operations receive the explicit longer query budget', async () => {
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
    assert.equal(typeof obj['text'], 'string');
    const text = String(obj['text']);
    calls.push({
      text,
      budget: 'query_timeout' in obj ? obj['query_timeout'] : undefined,
    });
    return Promise.resolve({
      command: text.startsWith('BEGIN') ? 'BEGIN' : text,
      rows: [{ value: {} }],
    });
  });
  try {
    const ledger = new BackfillLedger(config);
    await ledger.advance(id);
    await ledger.advanceIfReady(id);
    await ledger.pause(id, true);
    assert.equal(
      calls.filter((c) => c.text === 'SET LOCAL statement_timeout=180000')
        .length,
      2,
    );
    assert.equal(calls.filter((c) => c.budget === 185000).length, 2);
    assert.ok(
      calls
        .filter((c) => c.budget !== undefined)
        .every((c) => c.text.includes('backfill_advance')),
    );
    assert.equal(calls.filter((c) => c.text === 'COMMIT').length, 3);
  } finally {
    query.mock.restore();
    end.mock.restore();
    connect.mock.restore();
  }
});
for (const installed of [false, true]) {
  void test(`bounded operations discover the explicit observation interface: installed=${installed}`, async () => {
    const calls: string[] = [];
    const connect = mock.method(pg.Client.prototype, 'connect', () =>
      Promise.resolve(),
    );
    const end = mock.method(pg.Client.prototype, 'end', () =>
      Promise.resolve(),
    );
    const query = mock.method(pg.Client.prototype, 'query', (text: string) => {
      calls.push(text);
      return Promise.resolve({
        command: text.startsWith('BEGIN') ? 'BEGIN' : text,
        rows: text.includes('to_regprocedure')
          ? [{ present: installed }]
          : [
              {
                value: {
                  evidence_scope: installed
                    ? 'durable_state_observation_not_revalidation'
                    : 'historical_full_status',
                },
              },
            ],
      });
    });
    try {
      const db = new OperationalDatabase({
        source: config,
        pipeline: config,
        consumer: config,
      });
      const rows = await db.read('pipeline', 'backfill', [id]);
      assert.equal(rows.length, 1);
      assert.ok(
        calls.includes(
          installed
            ? 'SELECT pipeline.backfill_observation($1) value'
            : 'SELECT pipeline.backfill_status($1) value',
        ),
      );
      assert.ok(
        calls.includes('SET TRANSACTION READ ONLY') &&
          calls.includes('SET LOCAL statement_timeout=2500'),
      );
      assert.deepEqual(
        calls.filter((c) => c.startsWith('SET LOCAL statement_timeout=')),
        installed
          ? [
              'SET LOCAL statement_timeout=2500',
              'SET LOCAL statement_timeout=8000',
            ]
          : ['SET LOCAL statement_timeout=2500'],
      );
      assert.ok(!calls.some((c) => c.includes('180000')));
      calls.length = 0;
      await db.read('pipeline', 'snapshot');
      assert.deepEqual(
        calls.filter((c) => c.startsWith('SET LOCAL statement_timeout=')),
        ['SET LOCAL statement_timeout=2500'],
      );
    } finally {
      query.mock.restore();
      end.mock.restore();
      connect.mock.restore();
    }
  });
}
