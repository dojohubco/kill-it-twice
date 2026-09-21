import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { environment, required } from './environment.ts';
import { existing, safeFailure } from './private.ts';
const start = new AbortController();
const stop = () => start.abort();
for (const s of ['SIGINT', 'SIGTERM']) process.once(s, stop);
try {
  const role = await environment();
  console.log(
    JSON.stringify({
      type: 'worker_start',
      role,
      pid: process.pid,
      state: 'waiting_for_explicit_seed',
    }),
  );
  while (!start.signal.aborted) {
    const marker = await existing('/ready/active.json');
    if (marker) {
      assert.equal(marker['sourceEpoch'], required('SOURCE_EPOCH'));
      assert.equal(marker['pipelineId'], required('PIPELINE_ID'));
      break;
    }
    await delay(1000, undefined, { signal: start.signal }).catch((e) => {
      if (!start.signal.aborted) throw e;
    });
  }
  for (const s of ['SIGINT', 'SIGTERM']) process.removeListener(s, stop);
  if (!start.signal.aborted) {
    const runners: Record<string, () => Promise<unknown>> = {
      capture: () => import('../capture.ts'),
      elasticsearch: () => import('../deliver-es.ts'),
      publisher: () => import('../deliver-rabbit.ts'),
      consumer: () => import('../consume.ts'),
      observer: () => import('../observe-consumer.ts'),
      backfill: () => import('./dispatch.ts'),
    };
    const runner = runners[role];
    assert.ok(runner, 'Not a worker role');
    process.argv = [process.execPath, `runtime:${role}`, 'follow'];
    await runner();
  }
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
