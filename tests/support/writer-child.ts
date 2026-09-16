// Private test process: the production owner executes every transaction control statement.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mock } from 'node:test';
import pg from 'pg';
import { Source } from '../../src/source.ts';
import { deadline } from './fault-protocol.ts';
import type { BarrierTelemetry, StartWriter } from './fault-protocol.ts';

if (!process.send) throw new Error('Writer child requires its private parent IPC channel');
let intentionalDisconnect = false, orphaned = false;
const abort = new AbortController();
process.once('disconnect', () => {
  if (!intentionalDisconnect) { orphaned = true; abort.abort(new Error('Unexpected parent loss')); }
});
let telemetry: BarrierTelemetry | undefined;
let start: StartWriter | undefined;
async function barrier(): Promise<void> {
  assert.ok(start && telemetry && process.send);
  const release = once(process, 'message', { signal: abort.signal });
  // Handle a rejected release even if sending telemetry fails first.
  const released = deadline(release, start.barrier);
  void released.catch(() => undefined);
  await new Promise<void>((resolve, reject) => {
    assert.ok(process.send);
    process.send(telemetry, (error) => error ? reject(error) : resolve());
  });
  const [reply] = await released;
  assert.deepEqual(reply, { type: 'release', runId: start.runId, barrier: start.barrier });
}
const originalEnd = pg.Client.prototype.end;
// Test-only pause after production checked COMMIT, before its owned connection closes.
const close = mock.method(pg.Client.prototype, 'end', async function (this: pg.Client) {
  try {
    if (start?.barrier === 'source.after_commit.before_caller_success' && telemetry && !orphaned) await barrier();
  } finally { await (Reflect.apply(originalEnd, this, []) as Promise<void>); }
});
try {
  const [message]: unknown[] = await deadline(once(process, 'message', { signal: abort.signal }), 'start message');
  // The parent constructs this private protocol; validate all fields before using them.
  assert.ok(message && typeof message === 'object');
  assert.ok('type' in message && message.type === 'start');
  assert.ok('runId' in message && typeof message.runId === 'string' && /^m1-[0-9]+-[a-f0-9]+$/.test(message.runId));
  assert.ok('barrier' in message && (message.barrier === 'source.after_mutation.before_commit' || message.barrier === 'source.after_commit.before_caller_success'));
  assert.ok('payloadJson' in message && typeof message.payloadJson === 'string');
  assert.ok('port' in message && typeof message.port === 'number');
  assert.ok('password' in message && typeof message.password === 'string');
  assert.ok('applicationName' in message && typeof message.applicationName === 'string');
  start = { type: 'start', runId: message.runId, barrier: message.barrier, payloadJson: message.payloadJson, port: message.port, password: message.password, applicationName: message.applicationName };
  const config = start;
  const owner = new Source({ host: '127.0.0.1', port: config.port, database: 'source_m1', user: 'source_writer', password: config.password, application_name: config.applicationName });
  const entity = await owner.transaction(async (tx) => {
    const row = await tx.create(config.payloadJson);
    const inspection = await tx.inspect(row.entity_id);
    assert.equal(inspection.outbox.length, 1);
    assert.equal(inspection.outbox[0]?.change_id, row.change_id);
    telemetry = { type: 'barrier', name: config.barrier, runId: config.runId, writerPid: process.pid, backendPid: inspection.session.pid, transactionId: inspection.session.xid, applicationName: config.applicationName, sessionUser: inspection.session.session_user, effectiveUser: inspection.session.current_user, entity: row, outbox: inspection.outbox };
    if (config.barrier === 'source.after_mutation.before_commit') await barrier();
    return row;
  });
  if (orphaned) throw new Error('Unexpected parent loss');
  process.stdout.write(JSON.stringify({ type: 'caller-success', entity }) + '\n');
  intentionalDisconnect = true;
  process.disconnect();
} catch (error) {
  process.stderr.write(`${orphaned ? 'Unexpected parent loss; owned session closed' : error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = orphaned ? 72 : 1;
} finally { abort.abort(); close.mock.restore(); }
