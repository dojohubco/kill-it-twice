import { createServer } from 'node:net';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { command, withCleanup } from './support.ts';
import { EsTransport, object } from '../src/es/transport.ts';

export async function startEs(project: string) {
  assert.match(project, /^m3-[a-z0-9-]+$/);
  const maxMap = Number(
    (await readFile('/proc/sys/vm/max_map_count', 'utf8')).trim(),
  );
  assert.ok(
    maxMap >= 1048576,
    'Elasticsearch prerequisite: vm.max_map_count >= 1048576; no host setting is changed by this runner',
  );
  assert.ok(process.getuid, 'M3 acceptance requires a Linux host identity');
  const privateDir = await mkdtemp(join(tmpdir(), 'm3-es-'));
  const password = randomBytes(24).toString('hex');
  const secrets = [password];
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
    M3_UID: String(process.getuid()),
  };
  const args = ['compose', '-p', project, '-f', resolve('compose.m3.yaml')];
  async function compose(tail: string[], timeout = 90_000) {
    const r = await command('docker', [...args, ...tail], env, timeout, true, {
      secrets,
    });
    assert.equal(r.code, 0, r.stderr);
    assert.equal(r.outputOverflow, false);
    assert.equal(r.timedOut, false);
    assert.deepEqual(r.cleanupErrors, []);
    return r.stdout;
  }
  let client: EsTransport | undefined;
  const cleanup = async () => {
    await withCleanup(
      () =>
        withCleanup(
          async () => {
            await client?.close();
          },
          () => compose(['down', '-v', '--remove-orphans']).then(() => {}),
        ),
      () => rm(privateDir, { recursive: true, force: true }),
    );
  };
  try {
    await writeFile(join(privateDir, 'password'), password, { mode: 0o600 });
    for (const file of ['users', 'users_roles', 'roles.yml'])
      await writeFile(join(privateDir, file), '', { mode: 0o600 });
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
    client = new EsTransport(config);
    const deadline = performance.now() + 90_000;
    let ready = false;
    while (performance.now() < deadline) {
      try {
        const identity = object(await client.request('GET', '/'))[
          'cluster_uuid'
        ];
        assert.ok(
          typeof identity === 'string' &&
            identity !== '_na_' &&
            identity.length > 0,
        );
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
    const setupObserver = client;
    async function provisionRuntime(
      username: string,
      runtimePassword: string,
      index: string,
    ) {
      assert.match(username, /^worker-[a-f0-9-]{36}$/);
      assert.match(runtimePassword, /^[a-f0-9]{48}$/);
      assert.match(index, /^kit-[a-f0-9-]+$/);
      secrets.push(runtimePassword);
      // The supported ES tool produces the password hash. Only controlled setup
      // sees these private files; the worker receives an ordinary restricted login.
      const build = join(privateDir, 'realm-build');
      await rm(build, { recursive: true, force: true });
      await mkdir(build, { mode: 0o700 });
      for (const file of ['users', 'users_roles'])
        await writeFile(join(build, file), '', { mode: 0o600 });
      const roles = `${username}:\n  cluster: ['cluster:monitor/main']\n  indices:\n    - names: ['${index}']\n      privileges: ['indices:data/write/index', 'indices:data/write/bulk', 'read', 'view_index_metadata']\n`;
      await writeFile(join(build, 'roles.yml'), roles, { mode: 0o600 });
      await writeFile(
        join(build, 'elasticsearch.yml'),
        'xpack.security.enabled: true\n',
        { mode: 0o600 },
      );
      const images = (await compose(['config', '--images'])).trim().split('\n');
      const toolImage = images.find((image) =>
        image.startsWith('docker.elastic.co/elasticsearch/elasticsearch:'),
      );
      assert.ok(toolImage, 'Receiver image is configured');
      assert.ok(
        toolImage.includes('@sha256:'),
        'Password tool uses the pinned receiver image',
      );
      const toolName = `${project}-realm-tool`;
      await withCleanup(
        async () => {
          // A separate one-shot tool mounts only its own build directory. Reusing
          // the service mounts would relabel the live TLS files on SELinux hosts.
          const result = await command(
            'docker',
            [
              'run',
              '--rm',
              '--name',
              toolName,
              '--label',
              `kill-it-twice.run=${project}`,
              '--network',
              'none',
              '--user',
              `${String(process.getuid?.())}:0`,
              '--memory',
              '256m',
              '-e',
              'ES_PATH_CONF=/tmp/realm',
              '-e',
              'CLI_JAVA_OPTS=-Xms16m -Xmx64m',
              '--volume',
              `${build}:/tmp/realm:rw,Z`,
              '--entrypoint',
              '/usr/share/elasticsearch/bin/elasticsearch-users',
              toolImage,
              'useradd',
              username,
              '-p',
              runtimePassword,
              '-r',
              username,
            ],
            env,
            30000,
            true,
            { secrets },
          );
          assert.equal(result.code, 0, result.stderr);
          assert.equal(result.timedOut, false);
          assert.equal(result.outputOverflow, false);
          assert.deepEqual(result.cleanupErrors, []);
        },
        async () => {
          const removed = await command(
            'docker',
            ['rm', '-f', toolName],
            env,
            30000,
            true,
            { secrets },
          );
          assert.ok(
            removed.code === 0 ||
              (removed.code === 1 &&
                removed.stderr.includes('No such container:')),
            removed.stderr,
          );
          assert.equal(removed.timedOut, false);
          assert.deepEqual(removed.cleanupErrors, []);
        },
      );
      // Keep mounted inodes, then use a controlled initialization restart: host
      // bind-file updates do not reliably notify the receiver's directory watcher.
      for (const file of ['roles.yml', 'users_roles', 'users'])
        await writeFile(
          join(privateDir, file),
          await readFile(join(build, file)),
          { mode: 0o600 },
        );
      await compose(['restart', 'elasticsearch']);
      const runtime = new EsTransport({
        ...config,
        username,
        password: runtimePassword,
      });
      await withCleanup(
        async () => {
          const deadline = performance.now() + 90000;
          let last: unknown;
          while (performance.now() < deadline) {
            try {
              const result = await runtime.request(
                'GET',
                '/_security/_authenticate',
              );
              assert.ok(
                result &&
                  typeof result === 'object' &&
                  'authentication_realm' in result,
              );
              const realm: unknown = result.authentication_realm;
              assert.ok(realm && typeof realm === 'object' && 'type' in realm);
              assert.equal(realm.type, 'file');
              await runtime.request('GET', `/${index}`);
              // Initialization still performs privileged setup/verification after
              // this returns. File-runtime readiness cannot stand in for the
              // native setup identity while its security index is recovering.
              await setupObserver.request('GET', '/_security/_authenticate');
              return;
            } catch (error) {
              last = error;
              await delay(100);
            }
          }
          throw new Error('Restricted file-realm setup deadline', {
            cause: last,
          });
        },
        () => runtime.close(),
      );
    }
    return {
      client,
      config,
      proxyNode,
      proxyApi,
      compose,
      provisionRuntime,
      cleanup,
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
