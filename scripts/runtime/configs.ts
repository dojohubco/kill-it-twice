import assert from 'node:assert/strict';
import { readFile, writeFile, chown } from 'node:fs/promises';
import type { Topology } from '../../src/rabbitmq/metadata.ts';
import { atomic, existing, password, sql } from './private.ts';
export interface Installation {
  installationId: string;
  sourceEpoch: string;
  pipelineId: string;
  topology: Topology;
}
export interface BrokerSecrets {
  publisher: string;
  consumer: string;
  observer: string;
}
async function connection(role: string): Promise<Record<string, string>> {
  const c = sql(role, await password(role)),
    p = role.toUpperCase();
  return {
    [`${p}_HOST`]: String(c.host),
    [`${p}_PORT`]: String(c.port),
    [`${p}_PASSWORD`]: String(c.password),
  };
}
export async function configs(i: Installation, rabbit: BrokerSecrets) {
  const ca = await readFile('/es/server.crt', 'utf8');
  const base = { SOURCE_EPOCH: i.sourceEpoch, PIPELINE_ID: i.pipelineId };
  const es = {
    ES_URL: 'https://toxiproxy:8666',
    ES_USERNAME: 'kit_runtime',
    ES_PASSWORD: await password('es_runtime'),
    ES_CA_FILE: '/config/ca.crt',
  };
  const broker = {
    RABBIT_HOST: 'toxiproxy',
    RABBIT_PORT: '8667',
    RABBIT_CA_FILE: '/config/ca.crt',
    RABBIT_REGISTRATION_ID: i.topology.registrationId,
    CONSUMER_ID: i.topology.consumerId,
    RABBIT_METADATA_URL: 'https://rabbitmq:15671',
    RABBIT_METADATA_USERNAME: `observer-${i.topology.registrationId}`,
    RABBIT_METADATA_PASSWORD: rabbit.observer,
  };
  async function save(role: string, env: Record<string, string>) {
    const file = `/out/${role}/runtime.json`,
      value = { role, env: { ...base, ...env } };
    const before = await existing(file);
    if (before)
      assert.deepEqual(before, value, `Changed retained ${role} configuration`);
    else await atomic(file, value);
    await writeFile(`/out/${role}/ca.crt`, ca, { mode: 0o644 });
    await chown(`/out/${role}/ca.crt`, 1000, 0);
  }
  await save('capture', {
    ...(await connection('source_capture')),
    ...(await connection('pipeline_capture')),
  });
  await save('backfill', {
    ...(await connection('source_backfill')),
    ...(await connection('pipeline_backfill')),
  });
  await save('elasticsearch', { ...(await connection('pipeline_es')), ...es });
  await save('publisher', {
    ...(await connection('pipeline_rabbit')),
    ...broker,
    RABBIT_USERNAME: `publisher-${i.topology.registrationId}`,
    RABBIT_PASSWORD: rabbit.publisher,
  });
  await save('consumer', {
    ...(await connection('consumer_runtime')),
    ...broker,
    RABBIT_USERNAME: `consumer-${i.topology.registrationId}`,
    RABBIT_PASSWORD: rabbit.consumer,
  });
  await save('observer', {
    ...(await connection('pipeline_receipts')),
    ...(await connection('consumer_receipt_reader')),
  });
  await save('seed', {
    ...(await connection('source_bootstrap')),
    ...(await connection('pipeline_capture')),
    ...(await connection('source_backfill')),
    ...(await connection('pipeline_backfill')),
    BOOTSTRAP_KEY: i.installationId,
  });
  await save('writer', { ...(await connection('source_command')) });
  const control = {
    source: sql('source_operator', await password('source_operator')),
    pipeline: sql('pipeline_operator', await password('pipeline_operator')),
    consumer: sql('consumer_operator', await password('consumer_operator')),
    sourceEpoch: i.sourceEpoch,
    pipelineId: i.pipelineId,
    es: {
      node: es.ES_URL,
      username: es.ES_USERNAME,
      password: es.ES_PASSWORD,
      ca,
    },
    rabbit: {
      url: broker.RABBIT_METADATA_URL,
      username: broker.RABBIT_METADATA_USERNAME,
      password: rabbit.observer,
      ca,
    },
    proxyApi: 'http://toxiproxy:8474',
    proxies: { elasticsearch: 'es', rabbitmq: 'rabbit' },
    token: await password('operator_token'),
  };
  const before = await existing('/out/control/control.json');
  if (before)
    assert.deepEqual(
      before,
      control,
      'Changed retained operational configuration',
    );
  else await atomic('/out/control/control.json', control);
}
