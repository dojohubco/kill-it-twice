import { readFile } from 'node:fs/promises';
import type { ConnectionConfig } from '../internal/transaction.ts';
import type { MetadataConnection } from '../rabbitmq/metadata.ts';
import type { EsConnection } from '../es/transport.ts';
import { id, string, integer, record } from './validation.ts';
export interface OperationsConfig {
  source: ConnectionConfig;
  pipeline: ConnectionConfig;
  consumer: ConnectionConfig;
  sourceEpoch: string;
  pipelineId: string;
  es: EsConnection;
  rabbit: MetadataConnection;
  proxyApi: string;
  proxies: { elasticsearch: string; rabbitmq: string };
  token: string;
}
export async function loadOperationsConfig(): Promise<OperationsConfig> {
  const raw: unknown = JSON.parse(
    await readFile(string(process.env['CONTROL_CONFIG_FILE'], 4096), 'utf8'),
  );
  const r = record(raw);
  const sql = (key: string, database: string): ConnectionConfig => {
    const c = record(r[key]);
    return {
      host: string(c['host']),
      port: integer(c['port'], 1, 65535),
      database,
      user: `${key}_operator`,
      password: string(c['password'], 1024),
      application_name: `control-api:${key}`,
    };
  };
  const es = record(r['es']),
    rabbit = record(r['rabbit']),
    proxies = record(r['proxies']);
  const proxyApi = string(r['proxyApi'], 256),
    url = new URL(proxyApi);
  if (
    url.protocol !== 'http:' ||
    url.username ||
    url.password ||
    url.pathname !== '/'
  )
    throw new Error('Invalid fixed proxy API');
  const names = {
    elasticsearch: string(proxies['elasticsearch']),
    rabbitmq: string(proxies['rabbitmq']),
  };
  if (Object.values(names).some((n) => !/^[a-z0-9-]{1,64}$/.test(n)))
    throw new Error('Invalid fixed proxy names');
  const token = string(r['token'], 256);
  if (token.length < 32)
    throw new Error('Operator token must contain at least 32 characters');
  return {
    source: sql('source', 'source_m1'),
    pipeline: sql('pipeline', 'pipeline_m2b'),
    consumer: sql('consumer', 'consumer_m4'),
    sourceEpoch: id(r['sourceEpoch']),
    pipelineId: id(r['pipelineId']),
    es: {
      node: string(es['node'], 256),
      username: string(es['username']),
      password: string(es['password'], 1024),
      ca: pem(es['ca']),
    },
    rabbit: {
      url: string(rabbit['url'], 256),
      username: string(rabbit['username']),
      password: string(rabbit['password'], 1024),
      ca: pem(rabbit['ca']),
    },
    proxyApi: proxyApi.replace(/\/$/, ''),
    proxies: names,
    token,
  };
}

function pem(v: unknown): string {
  if (
    typeof v !== 'string' ||
    v.length > 8192 ||
    !v.startsWith('-----BEGIN CERTIFICATE-----')
  )
    throw new Error('Invalid CA');
  return v;
}
