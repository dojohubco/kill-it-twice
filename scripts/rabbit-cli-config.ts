import { readFile } from 'node:fs/promises';
import type { ConnectionConfig } from '../src/internal/transaction.ts';
import { BrokerMetadata, type Topology } from '../src/rabbitmq/metadata.ts';
function requiredRabbit(key: string) {
  const v = process.env[key];
  if (!v) throw new Error(`Missing ${key}`);
  return v;
}
export function sqlConfig(
  role:
    | 'pipeline_rabbit'
    | 'pipeline_receipts'
    | 'consumer_runtime'
    | 'consumer_receipt_reader',
): ConnectionConfig {
  const prefix = role.toUpperCase();
  return {
    host: requiredRabbit(`${prefix}_HOST`),
    port: Number(requiredRabbit(`${prefix}_PORT`)),
    database: role.startsWith('consumer') ? 'consumer_m4' : 'pipeline_m2b',
    user: role,
    password: requiredRabbit(`${prefix}_PASSWORD`),
    application_name: `m4-${role}`,
  };
}
export function expectedTopology(): Topology {
  const registrationId = requiredRabbit('RABBIT_REGISTRATION_ID'),
    vhost = `kit-${registrationId}`;
  return {
    registrationId,
    vhost,
    exchange: `${vhost}-events`,
    queue: `${vhost}-consumer`,
    routingKey: 'revision-v1',
    consumerId: requiredRabbit('CONSUMER_ID'),
    pipelineId: requiredRabbit('PIPELINE_ID'),
    epoch: requiredRabbit('SOURCE_EPOCH'),
  };
}
export async function brokerConfig() {
  const ca = await readFile(requiredRabbit('RABBIT_CA_FILE'), 'utf8');
  return {
    connection: {
      host: requiredRabbit('RABBIT_HOST'),
      port: Number(requiredRabbit('RABBIT_PORT')),
      username: requiredRabbit('RABBIT_USERNAME'),
      password: requiredRabbit('RABBIT_PASSWORD'),
      ca,
      vhost: expectedTopology().vhost,
    },
    metadata: new BrokerMetadata({
      url: requiredRabbit('RABBIT_METADATA_URL'),
      username: requiredRabbit('RABBIT_METADATA_USERNAME'),
      password: requiredRabbit('RABBIT_METADATA_PASSWORD'),
      ca,
    }),
  };
}
export function cliMode() {
  const mode = process.argv[2];
  if (
    !['once', 'follow', 'status'].includes(mode ?? '') ||
    process.argv.length !== 3
  )
    throw new Error('Select once, follow or status');
  return mode;
}
export function stopSignal() {
  const stop = new AbortController();
  for (const signal of ['SIGINT', 'SIGTERM'])
    process.once(signal, () => stop.abort());
  return stop.signal;
}
