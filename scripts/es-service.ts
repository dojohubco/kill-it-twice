import { createServer } from 'node:net';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { command, withCleanup } from './support.ts';
import { EsTransport } from '../src/es/transport.ts';

export async function startEs(project: string) {
  assert.match(project, /^m3-[a-z0-9-]+$/);
  const privateDir = await mkdtemp(join(tmpdir(), 'm3-es-'));
  const password = randomBytes(24).toString('hex');
  const reservation = createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once('error', reject);
    reservation.listen(0, '127.0.0.1', resolve);
  });
  const address = reservation.address();
  assert.ok(address && typeof address !== 'string');
  const reservedPort = address.port;
  await new Promise<void>((resolve, reject) =>
    reservation.close((e) => (e ? reject(e) : resolve())),
  );
  const env = {
    ...process.env,
    M3_PRIVATE_DIR: privateDir,
    M3_ES_PORT: String(reservedPort),
  };
  const args = ['compose', '-p', project, '-f', resolve('compose.m3.yaml')];
  async function compose(tail: string[], timeout = 90_000) {
    const r = await command('docker', [...args, ...tail], env, timeout, true, {
      secrets: [password],
    });
    assert.equal(r.code, 0, r.stderr);
    assert.equal(r.outputOverflow, false);
    assert.equal(r.timedOut, false);
    assert.deepEqual(r.cleanupErrors, []);
    return r.stdout;
  }
  const cleanup = async () => {
    await withCleanup(
      () => compose(['down', '-v', '--remove-orphans']).then(() => {}),
      () => rm(privateDir, { recursive: true, force: true }),
    );
  };
  try {
    await writeFile(join(privateDir, 'password'), password, { mode: 0o600 });
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
      30_000,
      true,
    );
    assert.equal(cert.code, 0, cert.stderr);
    await compose(['up', '-d']);
    const port = async (service: string, internal: string) =>
      (await compose(['port', service, internal])).trim().split(':').at(-1);
    const node = `https://127.0.0.1:${await port('elasticsearch', '9200')}`;
    const proxyApi = `http://127.0.0.1:${await port('toxiproxy', '8474')}`;
    const proxyNode = `https://127.0.0.1:${await port('toxiproxy', '8666')}`;
    const config = {
      node,
      username: 'elastic',
      password,
      ca: await readFile(join(privateDir, 'server.crt'), 'utf8'),
    };
    const client = new EsTransport(config);
    const deadline = performance.now() + 90_000;
    let ready = false;
    while (performance.now() < deadline) {
      try {
        await client.request('GET', '/');
        ready = true;
        break;
      } catch {
        await delay(500);
      }
    }
    if (!ready) {
      await client.close();
      throw new Error(
        `ES startup deadline; ${await compose(['logs', '--tail', '40', 'elasticsearch'])}`,
      );
    }
    const response = await fetch(`${proxyApi}/proxies`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'es',
        listen: '0.0.0.0:8666',
        upstream: 'elasticsearch:9200',
      }),
      signal: AbortSignal.timeout(5000),
    });
    assert.equal(response.status, 201);
    return {
      client,
      config,
      proxyNode,
      proxyApi,
      compose,
      cleanup: () => withCleanup(() => client.close(), cleanup),
    };
  } catch (primary) {
    return withCleanup(
      () =>
        Promise.reject(
          primary instanceof Error
            ? primary
            : new Error('Non-error failure', { cause: primary }),
        ),
      cleanup,
    );
  }
}
