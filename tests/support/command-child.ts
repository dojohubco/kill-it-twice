// Private process barriers only. Source owns BEGIN/COMMIT/ROLLBACK and connection disposal.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mock } from 'node:test';
import pg from 'pg';
import { Source, type CommandReply } from '../../src/source.ts';
import { object } from '../../scripts/acceptance.ts';
import { deadline } from './fault-protocol.ts';

const abort = new AbortController();
let intentionalDisconnect = false;
let orphaned = false;
process.once('disconnect', () => {
  if (!intentionalDisconnect) {
    orphaned = true;
    abort.abort(new Error('Unexpected parent loss'));
  }
});
let result: CommandReply | undefined;
let stage: string | undefined;
async function barrier(): Promise<void> {
  assert.ok(result);
  const release = deadline(
    once(process, 'message', { signal: abort.signal }).then(
      (values: unknown[]) => values,
    ),
    'command release',
  );
  void release.catch(() => undefined);
  await new Promise<void>((resolve, reject) => {
    if (typeof process.send !== 'function') {
      reject(new Error('Parent channel unavailable'));
      return;
    }
    process.send(
      { type: 'command-barrier', pid: process.pid, stage, reply: result },
      (error) => (error ? reject(error) : resolve()),
    );
  });
  const [message]: unknown[] = await release;
  assert.deepEqual(message, { type: 'release', stage });
}
// Public driver method, rebound with its explicit receiver for private post-COMMIT instrumentation.
// eslint-disable-next-line @typescript-eslint/unbound-method
const originalEnd = pg.Client.prototype.end;
const close = mock.method(
  pg.Client.prototype,
  'end',
  async function (this: pg.Client) {
    try {
      if (stage === 'post' && result && !orphaned) await barrier();
    } finally {
      await (Reflect.apply(originalEnd, this, []) as Promise<void>);
    }
  },
);
try {
  const [input]: unknown[] = await deadline(
    once(process, 'message', { signal: abort.signal }).then(
      (values: unknown[]) => values,
    ),
    'command start',
  );
  const message = object(input);
  const command = object(message['command']);
  assert.ok(message['stage'] === 'pre' || message['stage'] === 'post');
  stage = message['stage'];
  assert.ok(
    typeof command['sourceEpoch'] === 'string' &&
      typeof command['commandId'] === 'string' &&
      command['contractVersion'] === 1 &&
      command['operation'] === 'create' &&
      command['entityId'] === null &&
      typeof command['payloadJson'] === 'string',
  );
  // The fault fixture intentionally supports one create request; lifecycle behavior has separate real tests.
  assert.ok(
    typeof message['port'] === 'number' &&
      typeof message['password'] === 'string' &&
      typeof message['applicationName'] === 'string',
  );
  const source = new Source({
    host: '127.0.0.1',
    port: message['port'],
    password: message['password'],
    database: 'source_m1',
    user: 'source_command',
    application_name: message['applicationName'],
  });
  const validatedCommand = {
    sourceEpoch: command['sourceEpoch'],
    commandId: command['commandId'],
    contractVersion: 1,
    operation: 'create' as const,
    entityId: null,
    payloadJson: command['payloadJson'],
  };
  const reply = await source.transaction(async (tx) => {
    result = await tx.command(validatedCommand);
    if (stage === 'pre') await barrier();
    return result;
  });
  if (orphaned) throw new Error('Unexpected parent loss');
  process.stdout.write(
    JSON.stringify({ type: 'caller-success', reply }) + '\n',
  );
  intentionalDisconnect = true;
  process.disconnect();
} catch (error) {
  process.stderr.write(
    `${orphaned ? 'Unexpected parent loss; owned session closed' : error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = orphaned ? 72 : 1;
} finally {
  abort.abort();
  close.mock.restore();
}
