// Private process fault surface; production Consumer and its owned SQL facade do all work.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import {
  BrokerMetadata,
  field,
  record,
  type Topology,
} from '../../src/rabbitmq/metadata.ts';
import {
  TransactionError,
  type ConnectionConfig,
} from '../../src/internal/transaction.ts';
import { deadline } from './fault-protocol.ts';
import {
  batchTrace,
  installBatchTiming,
  ObservedConsumerDatabase,
} from './batch-observation.ts';
const trace = batchTrace();
const stop = new AbortController();
let intentional = false;
let restore: (() => void) | undefined;
process.once('disconnect', () => {
  if (!intentional) stop.abort();
});
for (const signal of ['SIGTERM', 'SIGINT'])
  process.once(signal, () => stop.abort());
async function send(value: unknown) {
  await new Promise<void>((resolve, reject) =>
    process.send?.(value, (error) => (error ? reject(error) : resolve())),
  );
}
async function barrier(phase: string) {
  const release = deadline(
    once(process, 'message', { signal: stop.signal }).then((v: unknown[]) => v),
    'Consumer batch release',
    25000,
  );
  void release.catch(() => undefined);
  await send({ type: 'batch-barrier', phase, pid: process.pid, trace });
  assert.deepEqual((await release)[0], { type: 'release', phase });
}
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message').then((v: unknown[]) => v),
    'Consumer batch child start',
    10000,
  );
  const input = record(raw),
    sql = record(input['sql']),
    a = record(input['amqp']),
    m = record(input['metadata']),
    t = record(input['target']);
  assert.ok(typeof sql['port'] === 'number' && typeof a['port'] === 'number');
  const config: ConnectionConfig = {
    host: field(sql, 'host'),
    port: sql['port'],
    database: field(sql, 'database'),
    user: field(sql, 'user'),
    password: field(sql, 'password'),
    application_name: field(sql, 'application_name'),
  };
  const target: Topology = {
    registrationId: field(t, 'registrationId'),
    consumerId: field(t, 'consumerId'),
    epoch: field(t, 'epoch'),
    pipelineId: field(t, 'pipelineId'),
    vhost: field(t, 'vhost'),
    queue: field(t, 'queue'),
    exchange: field(t, 'exchange'),
    routingKey: field(t, 'routingKey'),
  };
  const minimum = input['minimum'];
  assert.ok(typeof minimum === 'number');
  restore = installBatchTiming(trace, [minimum]);
  const pause = input['pause'] === true;
  const db = new ObservedConsumerDatabase(
    config,
    trace,
    pause ? () => barrier('before_commit') : undefined,
    pause ? () => barrier('after_commit') : undefined,
  );
  const worker = new Consumer(
    db,
    {
      host: field(a, 'host'),
      port: a['port'],
      username: field(a, 'username'),
      password: field(a, 'password'),
      ca: field(a, 'ca'),
      vhost: field(a, 'vhost'),
    },
    new BrokerMetadata({
      url: field(m, 'url'),
      username: field(m, 'username'),
      password: field(m, 'password'),
      ca: field(m, 'ca'),
    }),
    target,
  );
  const result = await worker.once(stop.signal, 10000);
  if (stop.signal.aborted)
    throw new Error('Consumer batch child interrupted or orphaned');
  await send({ type: 'batch-complete', pid: process.pid, trace, result });
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(
      JSON.stringify({ type: 'batch-success', result }) + '\n',
      (error) => (error ? reject(error) : resolve()),
    ),
  );
} catch (error) {
  await send({
    type: 'batch-failure',
    pid: process.pid,
    trace,
    error:
      error instanceof TransactionError
        ? {
            outcome: error.outcome,
            sqlState: error.sqlState,
            message: error.message,
          }
        : {
            message:
              error instanceof Error ? error.message : 'Batch child failure',
          },
  }).catch(() => undefined);
  console.error(
    error instanceof TransactionError
      ? `Consumer batch ${error.outcome} ${error.sqlState ?? 'no SQLSTATE'}`
      : 'Consumer batch child failure',
  );
  process.exitCode = 1;
} finally {
  restore?.();
  intentional = true;
  if (process.connected) process.disconnect();
}
