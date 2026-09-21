// Test-only timing hooks call the real driver and production owner; no database result is simulated.
import pg from 'pg';
import { mock } from 'node:test';
import { once } from 'node:events';
import { loadOperationsConfig } from '../../src/operations/config.ts';
import { RecoveryService } from '../../src/operations/recovery.ts';
import { record } from '../../src/operations/validation.ts';
import { writeCaptureReport } from '../../src/internal/capture-report.ts';
const abort = new AbortController();
let intentional = false,
  phase = 'run',
  admission: unknown;
process.once('disconnect', () => {
  if (!intentional) abort.abort(new Error('Parent disconnected'));
});
async function barrier() {
  const release = once(process, 'message', {
    signal: AbortSignal.any([abort.signal, AbortSignal.timeout(15000)]),
  });
  void release.catch(() => undefined);
  await new Promise<void>((resolve, reject) => {
    if (!process.send) {
      reject(new Error('No parent'));
      return;
    }
    process.send(
      { type: 'barrier', phase, pid: process.pid, result: admission },
      (error) => (error ? reject(error) : resolve()),
    );
  });
  const response: unknown[] = await release;
  if (record(response[0])['type'] !== 'release')
    throw new Error('Invalid private release');
}
// eslint-disable-next-line @typescript-eslint/unbound-method -- private wrapper forwards every call with the original pg receiver
const original = pg.Client.prototype.query;
const hook = mock.method(
  pg.Client.prototype,
  'query',
  async function (this: pg.Client, ...args: unknown[]) {
    const pending: unknown = Reflect.apply(original, this, args);
    if (!(pending instanceof Promise))
      throw new Error('Expected promise query');
    const result: unknown = await pending;
    if (
      typeof args[0] === 'string' &&
      args[0].startsWith('SELECT pipeline.request_batch_replay')
    ) {
      const rows = record(result)['rows'];
      if (!Array.isArray(rows)) throw new Error('Missing query rows');
      admission = record(rows[0])['value'];
      if (phase === 'pre') await barrier();
    }
    if (args[0] === 'COMMIT' && admission !== undefined && phase === 'post')
      await barrier();
    return result;
  },
);
try {
  const message: unknown[] = await once(process, 'message', {
    signal: AbortSignal.timeout(15000),
  });
  const r = record(message[0]);
  if (!['pre', 'post', 'run'].includes(String(r['phase'])))
    throw new Error('Invalid phase');
  phase = String(r['phase']);
  const config = await loadOperationsConfig();
  const label = r['label'];
  if (typeof label !== 'string') throw new Error('Missing label');
  const result = await new RecoveryService({
    ...config,
    pipeline: { ...config.pipeline, application_name: label },
  }).replay(r['request']);
  await writeCaptureReport(process.stdout, { type: 'caller-success', result });
  intentional = true;
  if (process.connected) process.disconnect();
} catch {
  process.exitCode = abort.signal.aborted ? 72 : 1;
  await writeCaptureReport(process.stderr, {
    error: 'private-recovery-child-failed',
  }).catch(() => undefined);
  intentional = true;
  if (process.connected) process.disconnect();
} finally {
  hook.mock.restore();
}
