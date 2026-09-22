import { setTimeout as delay } from 'node:timers/promises';
import { runtimePageRecords } from '../../src/backfill/bounds.ts';
import { Backfill, BackfillFailure } from '../../src/backfill/worker.ts';
import { writeCaptureReport } from '../../src/internal/capture-report.ts';
import { connection, required } from './environment.ts';
import { database, safeFailure } from './private.ts';
const stop = new AbortController();
for (const s of ['SIGINT', 'SIGTERM']) process.once(s, () => stop.abort());
const worker = new Backfill(
  {
    source: connection('source_backfill'),
    pipeline: connection('pipeline_backfill'),
    binding: {
      sourceEpoch: required('SOURCE_EPOCH'),
      pipelineId: required('PIPELINE_ID'),
    },
  },
  { pageRecords: runtimePageRecords(process.env['BACKFILL_PAGE_RECORDS']) },
);
const zero = '00000000-0000-0000-0000-000000000000';
let at = '-infinity',
  id = zero;
while (!stop.signal.aborted) {
  const runs = await database(
    connection('pipeline_backfill'),
    async (c) =>
      (
        await c.query<{ run_id: string; created_at: string }>(
          'SELECT * FROM pipeline.discover_backfills($1,$2,16)',
          [at, id],
        )
      ).rows,
  );
  let progressed = false;
  for (const run of runs) {
    if (stop.signal.aborted) break;
    try {
      const result = await worker.once(run.run_id, stop.signal, 'admission');
      progressed ||= result.page !== undefined;
      await writeCaptureReport(process.stdout, {
        type: 'backfill',
        run_id: run.run_id,
        ...result,
      });
    } catch (error) {
      if (!(error instanceof BackfillFailure) || error.fatal) throw error;
      await writeCaptureReport(process.stderr, {
        ...safeFailure(error),
        role: 'backfill',
        fatal: false,
      });
    }
    at = run.created_at;
    id = run.run_id;
  }
  if (runs.length < 16) {
    at = '-infinity';
    id = zero;
  }
  if (!progressed)
    await delay(1000, undefined, { signal: stop.signal }).catch((e) => {
      if (!stop.signal.aborted) throw e;
    });
}
