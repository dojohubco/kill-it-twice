import { randomBytes, randomUUID } from 'node:crypto';
import { fork, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import assert from 'node:assert/strict';
import pg from 'pg';
import { migrateOperations } from '../../scripts/migrate-operations.ts';
import { config } from './staging.ts';
import { consumerConfig, topology } from './rabbit.ts';
import { esConfig } from './es.ts';
import { required, evidence, connect } from './db.ts';
import { record, string } from '../../src/operations/validation.ts';
import type { OperationsConfig } from '../../src/operations/config.ts';
import { docker } from './rabbit-network.ts';
export function objects(value: unknown): Record<string, unknown>[] {
  assert.ok(Array.isArray(value));
  return value.map((v: unknown) => record(v));
}
export async function migrateAll(s: pg.Client, p: pg.Client, c: pg.Client) {
  const password = randomBytes(24).toString('hex');
  for (const [store, client] of [
    ['source', s],
    ['pipeline', p],
    ['consumer', c],
  ] as const)
    await migrateOperations(client, store, password);
  return password;
}
export async function connections() {
  const s = await connect('admin', 'operations'),
    p = new pg.Client(config('pipelineAdmin', 'operations')),
    c = new pg.Client(consumerConfig('admin', 'operations'));
  await p.connect();
  await c.connect();
  return {
    s,
    p,
    c,
    async close() {
      await Promise.all([this.s.end(), this.p.end(), this.c.end()]);
    },
  };
}
export async function networkRoute() {
  const network = `${required('ES_PROJECT')}_default`,
    container = `${required('RABBIT_PROJECT')}-rabbitmq-1`;
  await docker([
    'network',
    'connect',
    '--gw-priority',
    '-1',
    network,
    container,
  ]);
  const read = async (container: string) => {
    const raw: unknown = JSON.parse(
      await docker([
        'inspect',
        '--format',
        '{{json .NetworkSettings.Networks}}',
        container,
      ]),
    );
    return string(record(record(raw)[network])['IPAddress']);
  };
  const host = await read(`${required('ES_PROJECT')}-toxiproxy-1`),
    upstream = await read(container);
  const result = await fetch(`${required('ES_PROXY_API')}/proxies`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: 'm6-rabbit',
      listen: '0.0.0.0:8667',
      upstream: `${upstream}:5671`,
    }),
    signal: AbortSignal.timeout(5000),
  });
  assert.equal(result.status, 201);
  return {
    host,
    port: 8667,
    async close() {
      await fetch(`${required('ES_PROXY_API')}/proxies/m6-rabbit`, {
        method: 'DELETE',
        signal: AbortSignal.timeout(5000),
      });
      await docker(['network', 'disconnect', network, container]);
    },
  };
}
export function apiConfig(password: string): OperationsConfig {
  return {
    source: { ...config('reader', 'api'), user: 'source_operator', password },
    pipeline: {
      ...config('pipelineAdmin', 'api'),
      user: 'pipeline_operator',
      password,
    },
    consumer: {
      ...consumerConfig('admin', 'api'),
      user: 'consumer_operator',
      password,
    },
    sourceEpoch: required('SOURCE_EPOCH'),
    pipelineId: required('PIPELINE_ID'),
    es: esConfig(false, true),
    rabbit: {
      url: required('RABBIT_API'),
      username: `observer-${topology().registrationId}`,
      password: required('RABBIT_OBSERVER_PASSWORD'),
      ca: required('RABBIT_CA'),
    },
    proxyApi: required('ES_PROXY_API') + '/',
    proxies: { elasticsearch: 'es', rabbitmq: 'm6-rabbit' },
    token: randomBytes(32).toString('hex'),
  };
}
export async function startApi(config: OperationsConfig, barrier?: string) {
  const directory = await mkdtemp(join(tmpdir(), 'kit-control-')),
    path = join(directory, 'config.json');
  await writeFile(path, JSON.stringify(config), { mode: 0o600 });
  const child = fork(
    resolve('artifacts/control-api-build/apps/control-api/test-child.js'),
    [],
    {
      env: {
        ...process.env,
        CONTROL_CONFIG_FILE: path,
        ...(barrier ? { CONTROL_TEST_BARRIER: barrier } : {}),
      },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    },
  );
  let logs = '',
    stderr = '';
  child.stdout?.on('data', (b: Buffer) => {
    logs += b.toString();
    if (logs.length > 2 * 1024 * 1024) child.kill('SIGKILL');
  });
  child.stderr?.on('data', (b: Buffer) => {
    stderr += b.toString();
    if (stderr.length > 65536) child.kill('SIGKILL');
  });
  const ready = await message(child, 'ready');
  const url = string(ready['url'], 256);
  let closed = false;
  return {
    child,
    url,
    get logs() {
      return logs;
    },
    get stderr() {
      return stderr;
    },
    async close(signal: NodeJS.Signals = 'SIGTERM') {
      if (closed) return;
      closed = true;
      const exited =
        child.exitCode !== null || child.signalCode !== null
          ? Promise.resolve([child.exitCode, child.signalCode])
          : once(child, 'exit');
      if (child.exitCode === null && child.signalCode === null)
        child.kill(signal);
      const result: unknown[] = await exited;
      await rm(directory, { recursive: true, force: true });
      return result;
    },
    async request(
      path: string,
      method = 'GET',
      body?: unknown,
      key: string = randomUUID(),
      token = config.token,
    ) {
      const response = await fetch(url + path, {
        method,
        headers: {
          'content-type': 'application/json',
          authorization: `Bearer ${token}`,
          'idempotency-key': key,
          'x-request-id': key,
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(25000),
      });
      const text = await response.text();
      const value: unknown = path === '/metrics' ? text : JSON.parse(text);
      return {
        status: response.status,
        value,
        text,
        requestId: response.headers.get('x-request-id'),
      };
    },
  };
}
function message(
  child: ChildProcess,
  type: string,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`API ${type} deadline`));
    }, 15000);
    const cleanup = () => {
      clearTimeout(timer);
      child.off('message', listener);
      child.off('exit', exit);
    };
    const listener = (v: unknown) => {
      const r = record(v);
      if (r['type'] === type) {
        cleanup();
        resolve(r);
      }
    };
    const exit = () => {
      cleanup();
      reject(new Error(`API exited before ${type}`));
    };
    child.on('message', listener);
    child.once('exit', exit);
  });
}
export async function crashAfterCommit(
  config: OperationsConfig,
  operation: string,
  path: string,
  body: unknown,
  key: string,
) {
  const api = await startApi(config, operation);
  const reached = message(api.child, 'barrier');
  const request = api.request(path, 'POST', body, key).catch(() => null);
  try {
    const event = await reached;
    const result = await api.close('SIGKILL');
    assert.equal(result?.[1], 'SIGKILL');
    assert.equal(await request, null);
    evidence('OP-process-death', {
      operation,
      pid: api.child.pid,
      request_id: key,
      exit: result,
      barrier: event,
    });
    return record(event['result']);
  } finally {
    await api.close();
  }
}
export function data(reply: { status: number; value: unknown }, status = 200) {
  assert.equal(reply.status, status, JSON.stringify(reply.value));
  return record(record(reply.value)['data']);
}
export async function retained(client: pg.Client, tables: string[]) {
  const result: Record<string, unknown> = {};
  for (const table of tables)
    result[table] = (
      await client.query<{ value: string }>(
        `SELECT row_to_json(t)::text value FROM ${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows;
  return result;
}
