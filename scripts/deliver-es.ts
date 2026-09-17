import { readFile } from 'node:fs/promises';
import { EsTransport } from '../src/es/transport.ts';
import { EsLedger } from '../src/es/ledger.ts';
import { EsAdapter, EsFailure } from '../src/es/adapter.ts';
import { Delivery } from '../src/es/worker.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
function required(key: string) {
  const v = process.env[key];
  if (!v) throw new Error(`Missing ${key}`);
  return v;
}
const mode = process.argv[2];
if (
  !['once', 'follow', 'status'].includes(mode ?? '') ||
  process.argv.length !== 3
)
  throw new Error('Select once, follow or status');
const ledger = new EsLedger({
  host: required('PIPELINE_ES_HOST'),
  port: Number(required('PIPELINE_ES_PORT')),
  database: 'pipeline_m2b',
  user: 'pipeline_es',
  password: required('PIPELINE_ES_PASSWORD'),
  application_name: 'm3-es-delivery',
});
if (mode === 'status')
  await writeCaptureReport(process.stdout, await ledger.status());
else {
  const transport = new EsTransport({
    node: required('ES_URL'),
    username: required('ES_USERNAME'),
    password: required('ES_PASSWORD'),
    ca: await readFile(required('ES_CA_FILE'), 'utf8'),
  });
  const delivery = new Delivery(ledger, new EsAdapter(transport));
  const stop = new AbortController();
  for (const signal of ['SIGINT', 'SIGTERM'])
    process.once(signal, () => stop.abort());
  try {
    if (mode === 'once')
      await writeCaptureReport(process.stdout, await delivery.once());
    else
      await delivery.follow(stop.signal, (r) =>
        writeCaptureReport(process.stdout, r),
      );
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'es-delivery-failure',
        classification:
          error instanceof EsFailure
            ? error.classification
            : 'transaction_or_cleanup',
        workerId: delivery.workerId,
      }),
    );
    process.exitCode = 1;
  } finally {
    await transport.close();
  }
}
