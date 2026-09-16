// Private IPC-only instrumentation. Every transaction and result is the production implementation.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mock } from 'node:test';
import pg from 'pg';
import { object } from '../../scripts/acceptance.ts';
import { withCleanup } from '../../scripts/support.ts';
import {
  Capture,
  CaptureFailure,
  type CaptureConfig,
  type CaptureResult,
} from '../../src/capture.ts';
import { SourceCapture, type Claim } from '../../src/source-capture.ts';
import {
  Pipeline,
  type EventInput,
  type StageResult,
} from '../../src/pipeline.ts';
import { deadline } from './fault-protocol.ts';
const stop = new AbortController();
let intentional = false,
  orphan = false,
  boundary = '',
  reached = false,
  inPipeline = false;
let claims: Claim[] = [],
  staged: StageResult[] = [],
  sourcePid: number | undefined,
  pipelinePid: number | undefined;
process.once('disconnect', () => {
  if (!intentional) {
    orphan = true;
    stop.abort();
  }
});
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => stop.abort());
async function barrier(name: string) {
  if (boundary !== name || reached) return;
  reached = true;
  const release = deadline(
    once(process, 'message', { signal: stop.signal }).then((v: unknown[]) => v),
    'capture release',
    20000,
  );
  void release.catch(() => undefined);
  await new Promise<void>((resolve, reject) => {
    if (!process.send)
      return reject(new Error('Missing private parent channel'));
    process.send(
      {
        type: 'capture-barrier',
        boundary: name,
        pid: process.pid,
        sourcePid,
        pipelinePid,
        claims,
        staged,
      },
      (error) => (error ? reject(error) : resolve()),
    );
  });
  const [message] = await release;
  assert.deepEqual(message, { type: 'release', boundary: name });
}
// eslint-disable-next-line @typescript-eslint/unbound-method -- test instrumentation explicitly preserves each original receiver
const originalClaim = SourceCapture.prototype.claim;
const claimMock = mock.method(
  SourceCapture.prototype,
  'claim',
  async function (
    this: SourceCapture,
    ...args: Parameters<SourceCapture['claim']>
  ) {
    claims = await Reflect.apply(originalClaim, this, args);
    if (claims.length) await barrier('capture.after_claim_commit.before_stage');
    return claims;
  },
);
// eslint-disable-next-line @typescript-eslint/unbound-method -- test instrumentation explicitly preserves the owner facade receiver
const originalAck = SourceCapture.prototype.acknowledge;
const ackMock = mock.method(
  SourceCapture.prototype,
  'acknowledge',
  async function (
    this: SourceCapture,
    ...args: Parameters<SourceCapture['acknowledge']>
  ) {
    const result = await Reflect.apply(originalAck, this, args);
    await barrier('capture.after_source_ack_commit.before_success');
    return result;
  },
);
const stageMock = mock.method(
  Pipeline.prototype,
  'stage',
  async function (this: Pipeline, inputs: readonly EventInput[]) {
    inPipeline = true;
    try {
      return await this.transaction(async (tx) => {
        staged = await tx.stage(inputs);
        await barrier('capture.before_pipeline_commit');
        return staged;
      });
    } finally {
      inPipeline = false;
    }
  },
);
// eslint-disable-next-line @typescript-eslint/unbound-method -- private pg receiver retained; real COMMIT has already returned before end
const originalEnd = pg.Client.prototype.end;
const endMock = mock.method(
  pg.Client.prototype,
  'end',
  async function (this: pg.Client) {
    await withCleanup(
      async () => {
        if (!orphan && boundary) {
          const observed = (
            await this.query<{ pid: number; role: string }>(
              'SELECT pg_backend_pid() AS pid,current_user AS role',
            )
          ).rows[0];
          if (observed?.role === 'pipeline_capture') {
            pipelinePid = observed.pid;
            if (inPipeline && staged.length)
              await barrier('capture.after_pipeline_commit.before_source_ack');
          } else sourcePid = observed?.pid;
        }
      },
      async () => {
        await (Reflect.apply(originalEnd, this, []) as Promise<void>);
      },
    );
  },
);
function str(value: unknown) {
  assert.equal(typeof value, 'string');
  return String(value);
}
function config(value: unknown): CaptureConfig {
  const r = object(value),
    b = object(r['binding']);
  const connection = (value: unknown) => {
    const c = object(value);
    assert.equal(typeof c['port'], 'number');
    return {
      host: str(c['host']),
      port: Number(c['port']),
      database: str(c['database']),
      user: str(c['user']),
      password: str(c['password']),
      application_name: str(c['application_name']),
    };
  };
  return {
    source: connection(r['source']),
    pipeline: connection(r['pipeline']),
    binding: {
      pipelineId: str(b['pipelineId']),
      sourceEpoch: str(b['sourceEpoch']),
    },
  };
}
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message', { signal: stop.signal }).then((v: unknown[]) => v),
    'capture start',
  );
  const input = object(raw);
  boundary = str(input['boundary']);
  const follow = input['follow'] === true;
  const leaseMs =
    typeof input['leaseMs'] === 'number' ? input['leaseMs'] : 1500;
  const worker = new Capture(config(input['config']), {
    leaseMs,
    renewalMs: 100,
    idleMs: 100,
  });
  const report = (result: CaptureResult | CaptureFailure) => {
    if (orphan) throw new Error('Unexpected parent loss');
    if (result instanceof CaptureFailure)
      process.stdout.write(
        JSON.stringify({
          type: 'capture-failure',
          fatal: result.fatal,
          cleanup: result.cleanup.length,
        }) + '\n',
      );
    else
      process.stdout.write(
        JSON.stringify({ type: 'caller-success', result }) + '\n',
      );
  };
  if (follow) await worker.follow(report, stop.signal);
  else report(await worker.captureOnce(stop.signal));
  if (orphan) throw new Error('Unexpected parent loss');
  intentional = true;
  process.disconnect();
} catch (error) {
  process.stderr.write(
    JSON.stringify({
      type: 'capture-error',
      orphan,
      message: error instanceof Error ? error.message : 'unknown',
    }) + '\n',
  );
  process.exitCode = orphan ? 72 : 1;
} finally {
  stop.abort();
  claimMock.mock.restore();
  ackMock.mock.restore();
  stageMock.mock.restore();
  endMock.mock.restore();
  if (process.connected) {
    intentional = true;
    process.disconnect();
  }
}
