// Private test process. This file is not a public failpoint or source command API.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import pg from 'pg';
import { createEntity } from '../../src/source.ts';
import { deadline } from './fault-protocol.ts';
import type { BarrierTelemetry, StartWriter } from './fault-protocol.ts';

if (!process.send) throw new Error('Writer child requires its private parent IPC channel');
// An orphan fails; a disconnect is never reported as a successful SIGKILL test.
process.once('disconnect', () => process.exit(72));
let client: pg.Client | undefined;
try {
  const [message] = await deadline(once(process, 'message'), 'start message');
  const start = message as StartWriter;
  assert.equal(start.type, 'start');
  assert.match(start.runId, /^m1-[0-9]+-[a-f0-9]+$/);
  assert.ok(['source.after_mutation.before_commit', 'source.after_commit.before_caller_success'].includes(start.barrier));
  client = new pg.Client({ host: '127.0.0.1', port: start.port, database: 'source_m1', user: 'source_writer', password: start.password, application_name: start.applicationName, connectionTimeoutMillis: 5_000, statement_timeout: 12_000, query_timeout: 15_000, idle_in_transaction_session_timeout: 30_000 });
  await client.connect();
  await client.query('BEGIN');
  const identity = (await client.query('SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid, session_user, current_user')).rows[0];
  const entity = await createEntity(client, start.payloadJson);
  const outbox = (await client.query('SELECT allocation_id::text, source_epoch::text, entity_id::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload_json FROM source.outbox WHERE entity_id=$1', [entity.entity_id])).rows;
  assert.equal(outbox.length, 1);
  assert.equal(outbox[0].change_id, entity.change_id);
  assert.equal(outbox[0].entity_version, '1');
  if (start.barrier === 'source.after_commit.before_caller_success') await client.query('COMMIT');
  const telemetry: BarrierTelemetry = { type: 'barrier', name: start.barrier, runId: start.runId, writerPid: process.pid, backendPid: identity.pid as number, transactionId: identity.xid as string, applicationName: start.applicationName, sessionUser: identity.session_user as string, effectiveUser: identity.current_user as string, entity, outbox };
  // Install the private release listener before advertising the boundary.
  const release = once(process, 'message');
  await new Promise<void>((resolve, reject) => process.send!(telemetry, (error) => error ? reject(error) : resolve()));
  const [reply] = await deadline(release, start.barrier);
  assert.deepEqual(reply, { type: 'release', runId: start.runId, barrier: start.barrier });
  if (start.barrier === 'source.after_mutation.before_commit') await client.query('COMMIT');
  // Ordinary caller success is stdout, never the private telemetry channel.
  process.stdout.write(JSON.stringify({ type: 'caller-success', entity }) + '\n');
  await client.end();
  process.disconnect();
} catch (error) {
  // Never log the connection configuration/password.
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  await client?.end().catch(() => undefined);
  process.exit(1);
}
