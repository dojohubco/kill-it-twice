import assert from 'node:assert/strict';
import type { TestContext } from 'node:test';
import { command } from '../../scripts/support.ts';
import { required, evidence } from './db.ts';
import { record, field } from '../../src/rabbitmq/metadata.ts';
export async function docker(args: string[]) {
  const r = await command('docker', args, process.env, 60000, true);
  assert.equal(r.code, 0, r.stderr);
  assert.equal(r.timedOut, false);
  assert.equal(r.outputOverflow, false);
  assert.deepEqual(r.cleanupErrors, []);
  return r.stdout.trim();
}
export async function proxy(t: TestContext, kind: 'amqp' | 'consumer-db') {
  const network = `${required('ES_PROJECT')}_default`,
    container =
      kind === 'amqp'
        ? `${required('RABBIT_PROJECT')}-rabbitmq-1`
        : required('M2C_PIPELINE_CONTAINER');
  await docker([
    'network',
    'connect',
    '--gw-priority',
    '-1',
    network,
    container,
  ]);
  t.after(async () => {
    await docker(['network', 'disconnect', network, container]);
  });
  const raw: unknown = JSON.parse(
    await docker([
      'inspect',
      '--format',
      '{{json .NetworkSettings.Networks}}',
      `${required('ES_PROJECT')}-toxiproxy-1`,
    ]),
  );
  const host = field(record(record(raw)[network]), 'IPAddress');
  const port = kind === 'amqp' ? 8667 : 8668,
    name = `m4-${kind}`;
  const api = required('ES_PROXY_API');
  const request = async (path: string, method: string, data?: unknown) => {
    const r = await fetch(`${api}${path}`, {
      method,
      signal: AbortSignal.timeout(5000),
      ...(data === undefined
        ? {}
        : {
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(data),
          }),
    });
    assert.ok(r.ok, `${method} ${path}: ${r.status}`);
  };
  const internal = kind === 'amqp' ? 5671 : 5432;
  const connected: unknown = JSON.parse(
    await docker([
      'inspect',
      '--format',
      '{{json .NetworkSettings.Networks}}',
      container,
    ]),
  );
  const upstreamHost = field(record(record(connected)[network]), 'IPAddress');
  await request('/proxies', 'POST', {
    name,
    listen: `0.0.0.0:${port}`,
    upstream: `${upstreamHost}:${internal}`,
  });
  t.after(() => request(`/proxies/${name}`, 'DELETE'));
  evidence('MQ-proxy', { kind, network, container, host, port, name });
  return {
    host,
    port,
    name,
    async toxic(
      type: 'timeout' | 'latency',
      stream: 'upstream' | 'downstream',
      attributes: Record<string, number>,
    ) {
      await request(`/proxies/${name}/toxics`, 'POST', {
        name: 'fault',
        type,
        stream,
        toxicity: 1,
        attributes,
      });
    },
    async clear() {
      await request(`/proxies/${name}/toxics/fault`, 'DELETE');
    },
    async enabled(value: boolean) {
      await request(`/proxies/${name}`, 'POST', { enabled: value });
    },
  };
}
