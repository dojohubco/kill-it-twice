import assert from 'node:assert/strict';
import test from 'node:test';
import { connect, databaseWaitFor, evidence } from '../support/db.ts';

test('M11-OBSERVE timeout disposes a real in-flight PostgreSQL query and session', async () => {
  const slow = await connect('writer', 'observation-slow');
  const observer = await connect('admin', 'observation-observer');
  try {
    const identity = (
      await slow.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')
    ).rows[0];
    assert.ok(identity);
    const start = performance.now();
    await assert.rejects(
      databaseWaitFor(
        slow,
        () => slow.query('SELECT pg_sleep(30)'),
        () => true,
        'slow query',
        100,
      ),
      /Deadline waiting/,
    );
    assert.ok(performance.now() - start < 3_000);
    const ended = await databaseWaitFor(
      observer,
      async () =>
        (
          await observer.query<{ pid: number }>(
            'SELECT pid FROM pg_stat_activity WHERE pid=$1',
            [identity.pid],
          )
        ).rows,
      (rows) => rows.length === 0,
      'disposed observation backend',
    );
    evidence('M11-OBSERVE', {
      backendPid: identity.pid,
      elapsedMs: performance.now() - start,
      ended,
    });
    await assert.rejects(slow.query('SELECT 1'), /closed|not queryable/);
  } finally {
    await Promise.all([slow.end(), observer.end()]);
  }
});
