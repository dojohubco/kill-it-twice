import { RabbitLedger } from '../src/rabbitmq/ledger.ts';
import { RabbitDelivery } from '../src/rabbitmq/worker.ts';
import { Publisher } from '../src/rabbitmq/publisher.ts';
import { BrokerFailure } from '../src/rabbitmq/metadata.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
import {
  sqlConfig,
  brokerConfig,
  cliMode,
  stopSignal,
  expectedTopology,
} from './rabbit-cli-config.ts';
const mode = cliMode();
const ledger = new RabbitLedger(sqlConfig('pipeline_rabbit'));
try {
  if (mode === 'status')
    await writeCaptureReport(process.stdout, await ledger.status());
  else {
    const actual = await ledger.target(),
      expected = expectedTopology();
    for (const key of [
      'registrationId',
      'consumerId',
      'pipelineId',
      'epoch',
      'vhost',
      'exchange',
      'queue',
      'routingKey',
    ] as const)
      if (actual[key] !== expected[key])
        throw new BrokerFailure(
          'configuration',
          'Configured registration differs from durable target',
        );
    const config = await brokerConfig();
    const worker = new RabbitDelivery(
      ledger,
      new Publisher(config.connection, config.metadata),
      {
        databaseBatchSize: Number(
          process.env['SINK_DATABASE_BATCH_SIZE'] ?? '1',
        ),
      },
    );
    if (mode === 'once')
      await writeCaptureReport(process.stdout, await worker.once());
    else
      await worker.follow(stopSignal(), (r) =>
        writeCaptureReport(process.stdout, r),
      );
  }
} catch (error) {
  console.error(
    JSON.stringify({
      type: 'rabbit-publication-failure',
      classification:
        error instanceof BrokerFailure
          ? error.classification
          : 'transaction_or_cleanup',
    }),
  );
  process.exitCode = 1;
}
