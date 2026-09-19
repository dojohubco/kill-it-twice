import { Bootstrap } from '../src/bootstrap.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
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
    application_name: 'm5a-bootstrap',
  };
}
const [mode, epoch, key, version, seed, count, bound] = process.argv.slice(2);
if (!epoch || !['seed', 'status', 'seal', 'activate'].includes(mode ?? ''))
  throw new Error(
    'Use bootstrap seed <epoch> <key> <recipe-version> <seed> <count> <chunk-size>, or status <epoch>, seal|activate <epoch> <key>',
  );
const bootstrap = new Bootstrap(
  config('SOURCE_BOOTSTRAP', 'source_m1', 'source_bootstrap'),
);
const stop = new AbortController();
for (const signal of ['SIGTERM', 'SIGINT'])
  process.once(signal, () => stop.abort());
try {
  if (mode === 'seed') {
    if (!key || !version || !seed || !count || !bound)
      throw new Error('All explicit seed parameters are required');
    const recipe = {
      epoch,
      key,
      version: Number(version),
      seed,
      count,
      chunkSize: Number(bound),
    };
    let state = await bootstrap.begin(recipe);
    await writeCaptureReport(process.stdout, {
      type: 'seed-progress',
      ...state,
    });
    while (
      state.phase === 'bootstrapping' &&
      state.requested_count !== null &&
      BigInt(state.completed_count) < BigInt(state.requested_count) &&
      !stop.signal.aborted
    ) {
      const members = await bootstrap.chunk(
        epoch,
        key,
        String(BigInt(state.completed_count) + 1n),
      );
      state = await bootstrap.status(epoch);
      await writeCaptureReport(process.stdout, {
        type: 'seed-chunk-committed',
        first: members[0]?.ordinal,
        count: members.length,
        ...state,
      });
    }
  } else if (mode === 'status')
    await writeCaptureReport(process.stdout, {
      type: 'seed-status',
      ...(await bootstrap.status(epoch)),
    });
  else {
    if (!key) throw new Error('Bootstrap key is required');
    const state =
      mode === 'seal'
        ? await bootstrap.seal(epoch, key)
        : await bootstrap.activate(
            epoch,
            key,
            required('PIPELINE_ID'),
            config('PIPELINE_CAPTURE', 'pipeline_m2b', 'pipeline_capture'),
          );
    await writeCaptureReport(process.stdout, { type: 'seed-status', ...state });
  }
} catch (error) {
  console.error(
    JSON.stringify({
      type: 'bootstrap-failure',
      sqlState: error instanceof TransactionError ? error.sqlState : undefined,
      outcome: error instanceof TransactionError ? error.outcome : undefined,
      cleanupFailures:
        error instanceof TransactionError ? error.cleanupErrors.length : 0,
    }),
  );
  process.exitCode = 1;
}
