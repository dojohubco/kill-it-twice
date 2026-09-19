import { Consumer } from '../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../src/rabbitmq/consumer-db.ts';
import { BrokerFailure } from '../src/rabbitmq/metadata.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
import {
  sqlConfig,
  brokerConfig,
  cliMode,
  stopSignal,
  expectedTopology,
} from './rabbit-cli-config.ts';
const mode = cliMode(),
  db = new ConsumerDatabase(sqlConfig('consumer_runtime'));
try {
  if (mode === 'status')
    await writeCaptureReport(process.stdout, await db.status());
  else {
    const c = await brokerConfig(),
      worker = new Consumer(db, c.connection, c.metadata, expectedTopology());
    if (mode === 'once')
      await writeCaptureReport(process.stdout, await worker.once(stopSignal()));
    else
      await worker.follow(stopSignal(), (r) =>
        writeCaptureReport(process.stdout, r),
      );
  }
} catch (error) {
  console.error(
    JSON.stringify({
      type: 'consumer-failure',
      classification:
        error instanceof BrokerFailure
          ? error.classification
          : 'transaction_or_cleanup',
      acknowledgement: 'unconfirmed deliveries remain recoverable',
    }),
  );
  process.exitCode = 1;
}
