import { migrateSource } from '../../scripts/migrate.ts';
import { CleanupFailure } from '../../scripts/support.ts';
import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import pg from 'pg';
import {
  Source,
  SourceOwnershipError,
  SourceTransactionError,
} from '../../src/source.ts';

const config = {
  host: 'unused',
  port: 1,
  database: 'unused',
  user: 'unused',
  password: 'unused',
  application_name: 'unit',
};
const databaseError = new pg.DatabaseError(
  'primary PostgreSQL failure',
  0,
  'error',
);
databaseError.code = '40P01';

for (const scenario of [
  'connect',
  'begin',
  'rollback',
  'commit',
  'close',
] as const) {
  void test(`transaction lifecycle preserves ${scenario} outcome and primary/cleanup errors`, async () => {
    const primary = new Error(`${scenario} transport failure`);
    const cleanup = new Error('cleanup failure');
    const commands: string[] = [];
    const connect = mock.method(pg.Client.prototype, 'connect', () =>
      scenario === 'connect' ? Promise.reject(primary) : Promise.resolve(),
    );
    const query = mock.method(pg.Client.prototype, 'query', (sql: string) => {
      commands.push(sql);
      if (sql === 'BEGIN' && scenario === 'begin')
        return Promise.reject(primary);
      if (sql === 'COMMIT' && scenario === 'commit')
        return Promise.reject(primary);
      if (sql === 'ROLLBACK' && scenario === 'rollback')
        return Promise.reject(cleanup);
      return Promise.resolve({ command: sql, rows: [] });
    });
    const end = mock.method(pg.Client.prototype, 'end', () =>
      ['connect', 'begin', 'rollback', 'close'].includes(scenario)
        ? Promise.reject(cleanup)
        : Promise.resolve(),
    );
    const owner = new Source(config);
    try {
      await assert.rejects(
        owner.transaction(() =>
          scenario === 'rollback'
            ? Promise.reject(databaseError)
            : Promise.resolve(42),
        ),
        (error: unknown) => {
          assert.ok(error instanceof SourceTransactionError);
          assert.equal(
            error.cause,
            scenario === 'rollback'
              ? databaseError
              : scenario === 'close'
                ? cleanup
                : primary,
          );
          assert.equal(
            error.outcome,
            scenario === 'close'
              ? 'committed'
              : ['connect', 'begin'].includes(scenario)
                ? 'not_started'
                : 'unknown',
          );
          if (scenario === 'rollback') {
            assert.equal(error.sqlState, '40P01');
            assert.deepEqual(error.cleanupErrors, [cleanup, cleanup]);
          }
          if (scenario === 'commit') {
            assert.deepEqual(commands, ['BEGIN', 'COMMIT', 'ROLLBACK']);
            assert.deepEqual(error.cleanupErrors, []);
          }
          return true;
        },
      );
      await assert.rejects(
        owner.transaction(() => Promise.resolve()),
        SourceOwnershipError,
      );
    } finally {
      connect.mock.restore();
      query.mock.restore();
      end.mock.restore();
    }
  });
}

void test('migration rollback failure preserves its primary PostgreSQL cause', async () => {
  const client = new pg.Client();
  const cleanup = new Error('migration rollback failed');
  const query = mock.method(client, 'query', (sql: string) => {
    if (sql === 'BEGIN') return Promise.resolve({ command: 'BEGIN' });
    return Promise.reject(sql === 'ROLLBACK' ? cleanup : databaseError);
  });
  try {
    await assert.rejects(
      migrateSource(client, '0'.repeat(48)),
      (error: unknown) => {
        assert.ok(error instanceof CleanupFailure);
        assert.equal(error.cause, databaseError);
        assert.deepEqual(error.cleanupErrors, [cleanup]);
        return true;
      },
    );
  } finally {
    query.mock.restore();
  }
});
