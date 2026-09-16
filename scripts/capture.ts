import { Capture, CaptureFailure } from '../src/capture.ts';
import { IntegrityError } from '../src/pipeline.ts';
import { TransactionError } from '../src/internal/transaction.ts';
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
    application_name: 'm2c-capture',
  };
}
const mode = process.argv[2];
if ((mode !== 'once' && mode !== 'follow') || process.argv.length !== 3)
  throw new Error('Select capture once or capture follow explicitly');
const capture = new Capture({
  source: config('SOURCE_CAPTURE', 'source_m1', 'source_capture'),
  pipeline: config('PIPELINE_CAPTURE', 'pipeline_m2b', 'pipeline_capture'),
  binding: {
    sourceEpoch: required('SOURCE_EPOCH'),
    pipelineId: required('PIPELINE_ID'),
  },
});
const stop = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => stop.abort());
function failure(error: unknown) {
  // Do not serialize driver details, credentials, arbitrary input or full causes into worker logs.
  const e = error instanceof CaptureFailure ? error.primary : error;
  const sqlState =
    e instanceof TransactionError
      ? e.sqlState
      : e instanceof IntegrityError
        ? e.code
        : undefined;
  const transaction = e instanceof IntegrityError ? e.primary : e;
  return {
    type: 'capture-failure',
    workerId: capture.workerId,
    diagnosis:
      sqlState === 'P4001'
        ? 'binding_mismatch'
        : sqlState === 'P3001'
          ? 'canonical_conflict'
          : sqlState === 'P4002'
            ? 'lost_ownership'
            : sqlState === '22003'
              ? 'generation_exhausted'
              : error instanceof CaptureFailure && error.fatal
                ? 'integrity_failure'
                : 'transaction_or_connectivity_failure',
    fatal: error instanceof CaptureFailure ? error.fatal : true,
    sourceState: 'unknown',
    sqlState,
    outcome:
      transaction instanceof TransactionError ? transaction.outcome : undefined,
    incidentWriteFailed:
      e instanceof IntegrityError && e.diagnosticFailure !== undefined,
    cleanupFailures: error instanceof CaptureFailure ? error.cleanup.length : 0,
  };
}
try {
  if (mode === 'once')
    console.log(
      JSON.stringify({
        type: 'capture-success',
        ...(await capture.captureOnce(stop.signal)),
      }),
    );
  else
    await capture.follow(
      (value) =>
        console.log(
          JSON.stringify(
            value instanceof CaptureFailure
              ? failure(value)
              : { type: 'capture-observation', ...value },
          ),
        ),
      stop.signal,
    );
} catch (error) {
  console.error(JSON.stringify(failure(error)));
  process.exitCode = 1;
}
