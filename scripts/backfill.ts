import { Backfill, BackfillFailure } from '../src/backfill/worker.ts';
import { TransactionError } from '../src/internal/transaction.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
import { uuid } from '../src/envelope.ts';
function required(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}
function config(prefix: string, database: string, user: string) {
  const port = Number(required(`${prefix}_PORT`));
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new Error('Invalid PostgreSQL port');
  return {
    host: required(`${prefix}_HOST`),
    port,
    database,
    user,
    password: required(`${prefix}_PASSWORD`),
    application_name: `backfill:${process.pid}`,
  };
}
const [mode, run, partitions] = process.argv.slice(2);
if (
  !mode ||
  !run ||
  !['start', 'status', 'pause', 'resume', 'once', 'follow'].includes(mode) ||
  process.argv.length > 5 ||
  (partitions !== undefined && mode !== 'start')
)
  throw new Error(
    'Use backfill start|status|pause|resume|once|follow <run-uuid> [start range-count]',
  );
uuid(run);
const worker = new Backfill({
  source: config('SOURCE_BACKFILL', 'source_m1', 'source_backfill'),
  pipeline: config('PIPELINE_BACKFILL', 'pipeline_m2b', 'pipeline_backfill'),
  binding: {
    sourceEpoch: required('SOURCE_EPOCH'),
    pipelineId: required('PIPELINE_ID'),
  },
});
const stop = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => stop.abort());
function failure(e: unknown) {
  const primary = e instanceof BackfillFailure ? e.primary : e;
  return {
    type: 'backfill-failure',
    workerId: worker.workerId,
    sqlState: primary instanceof TransactionError ? primary.sqlState : null,
    outcome: primary instanceof TransactionError ? primary.outcome : 'unknown',
    fatal: e instanceof BackfillFailure ? e.fatal : true,
    cleanupFailures: e instanceof BackfillFailure ? e.cleanup.length : 0,
  };
}
try {
  if (mode === 'follow')
    await worker.follow(
      run,
      (v) =>
        writeCaptureReport(
          process.stdout,
          v instanceof BackfillFailure ? failure(v) : v,
        ),
      stop.signal,
    );
  else
    await writeCaptureReport(
      process.stdout,
      mode === 'start'
        ? await worker.start(
            run,
            partitions === undefined ? 4 : Number(partitions),
          )
        : mode === 'status'
          ? await worker.status(run)
          : mode === 'pause' || mode === 'resume'
            ? await worker.pause(run, mode === 'pause')
            : await worker.once(run, stop.signal),
    );
} catch (e) {
  await writeCaptureReport(process.stderr, failure(e));
  process.exitCode = 1;
}
