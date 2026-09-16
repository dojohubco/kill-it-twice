// Private barriers surround real Pipeline work/completion; production owns all transaction control.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mock } from 'node:test';
import pg from 'pg';
import { object } from '../../scripts/acceptance.ts';
import { Pipeline, type StageResult } from '../../src/pipeline.ts';
import { SourceReader } from '../../src/source-reader.ts';
import { positiveBigint } from '../../src/source.ts';
import { deadline } from './fault-protocol.ts';
const abort = new AbortController();
let intentional = false,
  orphaned = false;
process.once('disconnect', () => {
  if (!intentional) {
    orphaned = true;
    abort.abort(new Error('Unexpected parent loss'));
  }
});
let results: StageResult[] | undefined, stage: string | undefined;
async function barrier() {
  const release = deadline(
    once(process, 'message', { signal: abort.signal }).then(
      (values: unknown[]) => values,
    ),
    'staging release',
  );
  void release.catch(() => undefined);
  await new Promise<void>((resolve, reject) => {
    if (!process.send) {
      reject(new Error('Parent channel unavailable'));
      return;
    }
    process.send(
      { type: 'staging-barrier', pid: process.pid, stage, results },
      (error) => (error ? reject(error) : resolve()),
    );
  });
  const [message] = await release;
  assert.deepEqual(message, { type: 'release', stage });
}
// eslint-disable-next-line @typescript-eslint/unbound-method -- private instrumentation preserves explicit pg receiver
const originalEnd = pg.Client.prototype.end;
const close = mock.method(
  pg.Client.prototype,
  'end',
  async function (this: pg.Client) {
    try {
      if (stage === 'post' && results && !orphaned) await barrier();
    } finally {
      await (Reflect.apply(originalEnd, this, []) as Promise<void>);
    }
  },
);
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message', { signal: abort.signal }).then(
      (values: unknown[]) => values,
    ),
    'staging start',
  );
  const input = object(raw);
  assert.ok(
    input['stage'] === 'pre' ||
      input['stage'] === 'post' ||
      input['stage'] === 'run',
  );
  stage = input['stage'];
  assert.ok(
    typeof input['sourcePort'] === 'number' &&
      typeof input['pipelinePort'] === 'number' &&
      typeof input['readerPassword'] === 'string' &&
      typeof input['stagerPassword'] === 'string' &&
      typeof input['epoch'] === 'string' &&
      typeof input['applicationName'] === 'string',
  );
  const key = object(input['key']);
  const reader = new SourceReader(
    {
      host: '127.0.0.1',
      port: input['sourcePort'],
      database: 'source_m1',
      user: 'source_reader',
      password: input['readerPassword'],
      application_name: `${input['applicationName']}:read`,
    },
    input['epoch'],
  );
  const selected = await reader.outbox([
    {
      entityId: positiveBigint(key['entityId']),
      version: positiveBigint(key['version']),
    },
  ]);
  assert.equal(selected.notVisible.length, 0);
  assert.equal(selected.events.length, 1);
  const pipeline = new Pipeline({
    host: '127.0.0.1',
    port: input['pipelinePort'],
    database: 'pipeline_m2b',
    user: 'pipeline_stager',
    password: input['stagerPassword'],
    application_name: input['applicationName'],
  });
  const reply = await pipeline.transaction(async (tx) => {
    results = await tx.stage(selected.events);
    if (stage === 'pre') await barrier();
    return results;
  });
  if (orphaned) throw new Error('Unexpected parent loss');
  process.stdout.write(
    JSON.stringify({ type: 'caller-success', results: reply }) + '\n',
  );
  intentional = true;
  process.disconnect();
} catch (error) {
  process.stderr.write(
    (orphaned
      ? 'Unexpected parent loss; session closed'
      : error instanceof Error
        ? error.message
        : String(error)) + '\n',
  );
  process.exitCode = orphaned ? 72 : 1;
} finally {
  abort.abort();
  close.mock.restore();
}
