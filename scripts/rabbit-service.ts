// Isolated acceptance setup only. Workers receive no Docker/admin capability.
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { randomBytes } from 'node:crypto';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { command, withCleanup } from './support.ts';
import { BrokerMetadata, record } from '../src/rabbitmq/metadata.ts';
export async function startRabbit(project: string) {
  assert.match(project, /^m(?:4|41|5a)-[a-z0-9-]+$/);
  assert.ok(process.getuid, 'Rabbit acceptance requires local Linux');
  const privateDir = await mkdtemp(join(tmpdir(), 'm4-rabbit-'));
  const password = randomBytes(24).toString('hex');
  const secrets = [password];
  const reserve = async () => {
    const server = createServer();
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    const address = server.address();
    assert.ok(address && typeof address === 'object');
    await new Promise<void>((resolve, reject) =>
      server.close((e) => (e ? reject(e) : resolve())),
    );
    return String(address.port);
  };
  const env = {
    ...process.env,
    M4_PRIVATE_DIR: privateDir,
    M4_UID: String(process.getuid()),
    M4_AMQP_PORT: await reserve(),
    M4_API_PORT: await reserve(),
  };
  const args = ['compose', '-p', project, '-f', resolve('compose.m4.yaml')];
  const compose = async (tail: string[], timeout = 90000) => {
    const r = await command('docker', [...args, ...tail], env, timeout, true, {
      secrets,
    });
    assert.equal(r.code, 0, r.stderr);
    assert.equal(r.timedOut, false);
    assert.equal(r.outputOverflow, false);
    assert.deepEqual(r.cleanupErrors, []);
    return r.stdout.trim();
  };
  const cleanup = () =>
    withCleanup(
      async () => {
        await compose(['down', '-v', '--remove-orphans']);
      },
      () => rm(privateDir, { recursive: true, force: true }),
    );
  try {
    const cert = await command(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-keyout',
        join(privateDir, 'server.key'),
        '-out',
        join(privateDir, 'server.crt'),
        '-days',
        '2',
        '-subj',
        '/CN=localhost',
        '-addext',
        'subjectAltName=DNS:localhost,IP:127.0.0.1',
      ],
      env,
      30000,
      true,
    );
    assert.equal(cert.code, 0, cert.stderr);
    await writeFile(
      join(privateDir, 'rabbitmq.conf'),
      `listeners.tcp = none
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/private/server.crt
ssl_options.certfile = /etc/rabbitmq/private/server.crt
ssl_options.keyfile = /etc/rabbitmq/private/server.key
ssl_options.versions.1 = tlsv1.3
ssl_options.versions.2 = tlsv1.2
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
management.ssl.port = 15671
management.ssl.versions.1 = tlsv1.3
management.ssl.versions.2 = tlsv1.2
management.ssl.cacertfile = /etc/rabbitmq/private/server.crt
management.ssl.certfile = /etc/rabbitmq/private/server.crt
management.ssl.keyfile = /etc/rabbitmq/private/server.key
default_user = m4_setup
default_pass = ${password}
max_message_size = 131072
heartbeat = 15
vm_memory_high_watermark.absolute = 512MiB
disk_free_limit.absolute = 512MB
`,
      { mode: 0o600 },
    );
    await writeFile(
      join(privateDir, 'enabled_plugins'),
      '[rabbitmq_management].\n',
      { mode: 0o600 },
    );
    await compose(['up', '-d']);
    const port = async (n: string) =>
      Number((await compose(['port', 'rabbitmq', n])).split(':').at(-1));
    const ca = await readFile(join(privateDir, 'server.crt'), 'utf8');
    const amqpPort = await port('5671');
    const config = {
      url: `https://127.0.0.1:${await port('15671')}`,
      username: 'm4_setup',
      password,
      ca,
    };
    const api = new BrokerMetadata(config);
    const deadline = performance.now() + 60000;
    let info: Record<string, unknown> | undefined;
    while (performance.now() < deadline) {
      try {
        info = record(await api.request('GET', '/api/overview'));
        break;
      } catch {
        await delay(400);
      }
    }
    assert.ok(
      info,
      `Broker startup deadline: ${await compose(['logs', '--tail', '60'])}`,
    );
    assert.equal(info['rabbitmq_version'], '4.3.6');
    return {
      project,
      privateDir,
      password,
      secrets,
      config,
      api,
      ca,
      amqpPort,
      compose,
      cleanup,
      info,
    };
  } catch (error) {
    return withCleanup(
      () =>
        Promise.reject(
          error instanceof Error
            ? error
            : new Error('Rabbit startup failed', { cause: error }),
        ),
      cleanup,
    );
  }
}
