// Real native-realm restart reproducer. This does not change delivery policy.
import assert from 'node:assert/strict';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { errors } from '@elastic/elasticsearch';
import { startEs } from './es-service.ts';
import { withCleanup, errorText } from './support.ts';
import { EsTransport, object } from '../src/es/transport.ts';

const run = `m3-auth-restart-${Date.now()}`;
const directory = `artifacts/m3/${run}`;
await mkdir(directory, { recursive: true });
const inputs = [];
for (const path of [
  'scripts/es-auth-restart.ts',
  'scripts/es-service.ts',
  'src/es/transport.ts',
  'compose.m3.yaml',
  'package-lock.json',
])
  inputs.push({
    path,
    sha256: createHash('sha256')
      .update(await readFile(path))
      .digest('hex'),
  });
await writeFile(
  `${directory}/inputs.json`,
  JSON.stringify({ developmental: true, inputs }, null, 2) + '\n',
);
const service = await startEs(run);
const observations: Record<string, unknown>[] = [];
const fileControl = process.argv.includes('--file');
try {
  await withCleanup(
    async () => {
      const password = randomBytes(24).toString('hex');
      await service.client.request(
        'PUT',
        '/_security/role/restart-fixture',
        '{"cluster":["cluster:monitor/main"]}',
      );
      await service.client.request(
        'PUT',
        '/_security/user/restart-fixture',
        JSON.stringify({ password, roles: ['restart-fixture'] }),
      );
      let username = 'restart-fixture';
      if (fileControl) {
        username = `worker-${randomUUID()}`;
        const index = `kit-${randomUUID()}`;
        await service.client.request(
          'PUT',
          `/${index}`,
          '{"settings":{"number_of_shards":1,"number_of_replicas":0}}',
        );
        await service.provisionRuntime(username, password, index);
      }
      const client = new EsTransport({ ...service.config, username, password });
      await withCleanup(
        async () => {
          const original = object(await client.request('GET', '/'))[
            'cluster_uuid'
          ];
          for (let cycle = 1; cycle <= 3; cycle++) {
            await service.compose(['stop', '--timeout', '10', 'elasticsearch']);
            const start = performance.now();
            const restart = service.compose(['start', 'elasticsearch']);
            // Consume startup failure immediately; always await the owned operation.
            let startupError: unknown;
            void restart.catch((error: unknown) => {
              startupError = error;
            });
            try {
              let healthy = 0;
              while (performance.now() < start + 90000 && healthy < 5) {
                if (startupError)
                  throw new Error('Owned receiver restart failed', {
                    cause: startupError,
                  });
                try {
                  const body = object(await client.request('GET', '/'));
                  observations.push({
                    cycle,
                    at: new Date().toISOString(),
                    elapsedMs: performance.now() - start,
                    status: 200,
                    clusterUuid: body['cluster_uuid'],
                  });
                  assert.equal(body['cluster_uuid'], original);
                  healthy++;
                } catch (error) {
                  healthy = 0;
                  observations.push({
                    cycle,
                    at: new Date().toISOString(),
                    elapsedMs: performance.now() - start,
                    status:
                      error instanceof errors.ResponseError
                        ? error.statusCode
                        : null,
                    response:
                      error instanceof errors.ResponseError ? error.body : null,
                    error:
                      error instanceof errors.ResponseError
                        ? error.message
                        : errorText(error),
                  });
                }
                await delay(30);
              }
              assert.equal(
                healthy,
                5,
                'Same valid credential must recover without changes',
              );
            } finally {
              await restart;
            }
            if (
              !fileControl &&
              observations.some((row) => row['status'] === 401)
            )
              break;
          }
          if (fileControl)
            assert.equal(
              observations.some((row) => row['status'] === 401),
              false,
              'File identity must remain available across retained restarts',
            );
          else
            assert.ok(
              observations.some((row) => row['status'] === 401),
              'No transient native authentication rejection reproduced in these three schedules',
            );
        },
        () => client.close(),
      );
    },
    async () => {
      await withCleanup(
        async () => {
          await writeFile(
            `${directory}/es.log`,
            await service.compose(['logs', '--no-color', 'elasticsearch']),
          );
        },
        async () => {
          await service.cleanup();
          await writeFile(
            `${directory}/cleanup.json`,
            '{"status":"PASS","ownedResourcesRemoved":true}\n',
          );
        },
      );
    },
  );
} finally {
  // ResponseError redaction clones lossless token objects; keep that diagnostic
  // object representation without asking the numeric serializer to emit it.
  await writeFile(
    `${directory}/observations.json`,
    JSON.stringify({ run, fileControl, observations }, null, 2) + '\n',
  );
  console.log(`Recorded: ${directory}`);
}
