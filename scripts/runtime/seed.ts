import assert from 'node:assert/strict';
import { Bootstrap } from '../../src/bootstrap.ts';
import { Backfill } from '../../src/backfill/worker.ts';
import { environment, required, connection } from './environment.ts';
import { atomic, database, safeFailure } from './private.ts';
const stop = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => stop.abort());
let phase = 'input';
try {
  await environment('seed');
  const count = process.argv[2] ?? '1024';
  assert.ok(
    process.argv.length <= 3 &&
      /^[1-9][0-9]{0,6}$/.test(count) &&
      BigInt(count) <= 2000000n,
    'Seed count must be 1..2000000',
  );
  const epoch = required('SOURCE_EPOCH'),
    key = required('BOOTSTRAP_KEY'),
    pipeline = required('PIPELINE_ID');
  const bootstrap = new Bootstrap(connection('source_bootstrap'));
  await database(connection('source_bootstrap'), async (guard) => {
    const lock = (
      await guard.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(1769235802) AS locked',
      )
    ).rows[0];
    assert.equal(lock?.locked, true, 'Another explicit seed command is active');
    phase = 'seed';
    let state = await bootstrap.begin({
      epoch,
      key,
      version: 1,
      seed: 'local-runtime-v1',
      count,
      chunkSize: 64,
    });
    console.log(JSON.stringify({ type: 'seed-progress', ...state }));
    while (
      state.phase === 'bootstrapping' &&
      BigInt(state.completed_count) < BigInt(count) &&
      !stop.signal.aborted
    ) {
      await bootstrap.chunk(
        epoch,
        key,
        String(BigInt(state.completed_count) + 1n),
      );
      state = await bootstrap.status(epoch);
      if (
        BigInt(state.completed_count) % 4096n === 0n ||
        state.completed_count === count
      )
        console.log(JSON.stringify({ type: 'seed-progress', ...state }));
    }
    if (stop.signal.aborted) {
      process.exitCode = 130;
      return;
    }
    phase = 'seal';
    await bootstrap.seal(epoch, key);
    phase = 'activate';
    state = await bootstrap.activate(
      epoch,
      key,
      pipeline,
      connection('pipeline_capture'),
    );
    assert.equal(state.phase, 'active');
    phase = 'initial-backfill';
    const backfill = new Backfill({
      source: connection('source_backfill'),
      pipeline: connection('pipeline_backfill'),
      binding: { sourceEpoch: epoch, pipelineId: pipeline },
    });
    const run = await backfill.start(key, 4);
    await atomic('/ready/active.json', {
      sourceEpoch: epoch,
      pipelineId: pipeline,
      initialRun: key,
    });
    console.log(
      JSON.stringify({
        type: 'seed-active',
        ...state,
        initial_run: key,
        backfill_phase: run.phase,
        note: 'Activation and scheduled backfill are not receiver completion',
      }),
    );
  });
} catch (error) {
  console.error(JSON.stringify({ ...safeFailure(error), phase }));
  process.exitCode = 1;
}
