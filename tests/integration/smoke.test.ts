import assert from 'node:assert/strict';
import test from 'node:test';
import { connect, evidence } from '../support/db.ts';

test('infrastructure: real PostgreSQL 18.6 connection with durable settings', async () => {
  const client = await connect('admin', 'smoke');
  try {
    const row = (await client.query("SELECT current_database() AS database, session_user, current_user, pg_backend_pid() AS backend_pid, current_setting('server_version') AS version, current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes")).rows[0];
    assert.equal(row.database, 'source_m1');
    assert.match(row.version as string, /^18\.6(?:\s|$)/);
    assert.equal(row.fsync, 'on');
    assert.equal(row.synchronous_commit, 'on');
    assert.equal(row.full_page_writes, 'on');
    evidence('infrastructure', row);
  } finally { await client.end(); }
});
