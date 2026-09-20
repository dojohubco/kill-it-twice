// Private instrumentation around real production methods; no business API failpoint.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mock } from 'node:test';
import { Backfill } from '../../src/backfill/worker.ts';
import { BackfillSource } from '../../src/backfill/source.ts';
import { BackfillLedger } from '../../src/backfill/ledger.ts';
import {
  TransactionError,
  type ConnectionConfig,
} from '../../src/internal/transaction.ts';
import { object, text } from '../../src/backfill/types.ts';
import { deadline } from './fault-protocol.ts';
const stop = new AbortController();
let intentional = false,
  boundary = '',
  reached = false,
  quiesce = false;
let gate: ReturnType<typeof Promise.withResolvers<void>> | undefined;
const renewals = new Set<Promise<unknown>>();
process.once('disconnect', () => {
  if (!intentional) stop.abort();
});
function config(value: unknown): ConnectionConfig {
  const r = object(value),
    port = r['port'];
  assert.equal(typeof port, 'number');
  if (typeof port !== 'number') throw new Error('Port');
  return {
    host: text(r['host']),
    port,
    database: text(r['database']),
    user: text(r['user']),
    password: text(r['password']),
    application_name: text(r['application_name']),
  };
}
async function barrier(name: string, data: unknown) {
  if (boundary !== name || reached) return;
  reached = true;
  if (quiesce) {
    gate = Promise.withResolvers<void>();
    await deadline(Promise.all(renewals), 'Backfill renewal quiescence', 15000);
  }
  const release = deadline(
    once(process, 'message', { signal: stop.signal }),
    'Backfill release',
    25000,
  );
  void release.catch(() => undefined);
  await new Promise<void>((resolve, reject) =>
    process.send?.(
      {
        type: 'backfill-barrier',
        boundary: name,
        pid: process.pid,
        data,
        renewalQuiescent: quiesce && renewals.size === 0,
      },
      (e) => (e ? reject(e) : resolve()),
    ),
  );
  try {
    assert.deepEqual((await release)[0], { type: 'release', boundary: name });
  } finally {
    gate?.resolve();
  }
}
// eslint-disable-next-line @typescript-eslint/unbound-method -- private instrumentation explicitly preserves each original receiver
const originalChange = BackfillLedger.prototype.change;
mock.method(
  BackfillLedger.prototype,
  'change',
  async function (
    this: BackfillLedger,
    ...args: Parameters<BackfillLedger['change']>
  ) {
    if (args[1] === 'renew' && gate) {
      await gate.promise;
      stop.signal.throwIfAborted();
    }
    const pending = Reflect.apply(originalChange, this, args);
    if (args[1] === 'renew') renewals.add(pending);
    try {
      return await pending;
    } finally {
      renewals.delete(pending);
    }
  },
);
// eslint-disable-next-line @typescript-eslint/unbound-method -- real bounded read is completed before the private barrier
const originalRead = BackfillSource.prototype.page;
mock.method(
  BackfillSource.prototype,
  'page',
  async function (
    this: BackfillSource,
    ...args: Parameters<BackfillSource['page']>
  ) {
    const p = await Reflect.apply(originalRead, this, args);
    await barrier('backfill.after_source_read.before_stage', p);
    return p;
  },
);
mock.method(
  BackfillLedger.prototype,
  'page',
  async function (
    this: BackfillLedger,
    request: Parameters<BackfillLedger['page']>[0],
  ) {
    const result = await this.transaction(async (tx) => {
      const value = await tx.page(request);
      await barrier('backfill.before_page_commit', { request, result: value });
      return value;
    });
    await barrier(
      request.claim.range === 0
        ? 'fence.after_import_commit.before_success'
        : 'backfill.after_page_commit.before_success',
      { request, result },
    );
    return result;
  },
);
mock.method(
  BackfillSource.prototype,
  'fence',
  async function (this: BackfillSource, run: string) {
    const result = await this.transaction(async (tx) => {
      const value = await tx.fence(run);
      await barrier('fence.before_source_commit', value);
      return value;
    });
    await barrier('fence.after_source_commit.before_pipeline_attach', result);
    return result;
  },
);
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message').then((x: unknown[]) => x),
    'Backfill child start',
    10000,
  );
  const r = object(raw),
    binding = object(r['binding']),
    sql = config(r['pipeline']),
    source = config(r['source']),
    run = text(r['run']);
  boundary = text(r['boundary']);
  quiesce = r['quiesce'] === true;
  const b = {
    sourceEpoch: text(binding['sourceEpoch']),
    pipelineId: text(binding['pipelineId']),
  };
  let result: unknown;
  if (r['action'] === 'fence')
    result = await new BackfillSource(source, b).fence(run);
  else {
    const opts = object(r['options']);
    result = await new Backfill(
      { source, pipeline: sql, binding: b },
      {
        leaseMs: Number(opts['leaseMs']),
        renewalMs: Number(opts['renewalMs']),
        idleMs: 50,
      },
    ).once(run, stop.signal);
  }
  stop.signal.throwIfAborted();
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(
      JSON.stringify({ type: 'backfill-success', result }) + '\n',
      (e) => (e ? reject(e) : resolve()),
    ),
  );
} catch (error) {
  const primary =
    error instanceof Error && 'primary' in error ? error.primary : error;
  console.error(
    JSON.stringify({
      type: 'backfill-failure',
      message: primary instanceof Error ? primary.message : 'Unknown error',
      sqlState: primary instanceof TransactionError ? primary.sqlState : null,
    }),
  );
  process.exitCode = 1;
} finally {
  intentional = true;
  mock.restoreAll();
  if (process.connected) process.disconnect();
}
