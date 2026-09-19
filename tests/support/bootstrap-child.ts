// Private fault surface around the actual bootstrap owner; credentials arrive only via IPC.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { Bootstrap } from '../../src/bootstrap.ts';
import { pipelineIdentity } from '../../src/pipeline.ts';
import {
  TransactionError,
  type ConnectionConfig,
} from '../../src/internal/transaction.ts';
import { field, record } from '../../src/rabbitmq/metadata.ts';
import { deadline } from './fault-protocol.ts';
const stop = new AbortController();
let intentional = false;
process.once('disconnect', () => {
  if (!intentional) stop.abort();
});
async function send(value: unknown) {
  await new Promise<void>((resolve, reject) =>
    process.send?.(value, (e) => (e ? reject(e) : resolve())),
  );
}
async function barrier(phase: string, result: unknown) {
  const release = deadline(
    once(process, 'message', { signal: stop.signal }),
    'Bootstrap release',
    25000,
  );
  void release.catch(() => undefined);
  await send({ type: 'bootstrap-barrier', phase, pid: process.pid, result });
  assert.deepEqual((await release)[0], { type: 'release', phase });
}
function config(value: unknown): ConnectionConfig {
  const r = record(value);
  assert.equal(typeof r['port'], 'number');
  const port = r['port'];
  if (typeof port !== 'number') throw new Error('Invalid port');
  return {
    host: field(r, 'host'),
    port,
    database: field(r, 'database'),
    user: field(r, 'user'),
    password: field(r, 'password'),
    application_name: field(r, 'application_name'),
  };
}
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message').then((v: unknown[]) => v),
    'Bootstrap start',
    10000,
  );
  const input = record(raw),
    epoch = field(input, 'epoch'),
    key = field(input, 'key'),
    action = field(input, 'action');
  const b = new Bootstrap(config(input['sql']));
  const pipeline = field(input, 'pipeline');
  if (action === 'activate') {
    const identity = await pipelineIdentity(config(input['pipelineConfig']));
    assert.equal(identity.pipelineId, pipeline);
    assert.equal(identity.sourceEpoch, epoch);
  }
  const result = await b.transaction(async (tx) => {
    const value =
      action === 'chunk'
        ? await tx.chunk(epoch, key, field(input, 'first'))
        : await tx.activate(epoch, key, pipeline);
    await barrier('before_commit', value);
    return value;
  });
  await barrier('after_commit', result);
  if (stop.signal.aborted) throw new Error('Orphaned bootstrap');
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(
      JSON.stringify({ type: 'bootstrap-success', result }) + '\n',
      (e) => (e ? reject(e) : resolve()),
    ),
  );
} catch (error) {
  console.error(
    JSON.stringify({
      type: 'bootstrap-failure',
      sqlState: error instanceof TransactionError ? error.sqlState : null,
      message: error instanceof Error ? error.message : 'Unknown failure',
    }),
  );
  process.exitCode = 1;
} finally {
  intentional = true;
  if (process.connected) process.disconnect();
}
