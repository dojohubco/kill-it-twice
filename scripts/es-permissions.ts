// Isolated reproduction of the named-write-privilege mapping boundary; no production credential.
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { startEs } from './es-service.ts';
import { withCleanup } from './support.ts';
import { EsTransport, object, exactInteger } from '../src/es/transport.ts';
const run = `m3-permission-probe-${Date.now()}`;
const directory = `artifacts/m3/${run}`;
await mkdir(directory, { recursive: true });
const service = await startEs(run);
await withCleanup(async () => {
  const index = 'kit-permission-fixture',
    password = randomBytes(24).toString('hex');
  await service.client.request(
    'PUT',
    `/${index}`,
    JSON.stringify({
      settings: { number_of_shards: 1, number_of_replicas: 0 },
      mappings: { dynamic: 'strict' },
    }),
  );
  const records: Record<string, unknown>[] = [];
  let revision = 0;
  for (const privileges of [
    ['index', 'read', 'view_index_metadata'],
    ['create', 'read', 'view_index_metadata'],
    [
      'indices:data/write/index',
      'indices:data/write/bulk',
      'read',
      'view_index_metadata',
    ],
  ]) {
    await service.client.request(
      'PUT',
      '/_security/role/fixture',
      JSON.stringify({
        cluster: ['cluster:monitor/main'],
        indices: [{ names: [index], privileges }],
      }),
    );
    await service.client.request(
      'PUT',
      '/_security/user/fixture',
      JSON.stringify({ password, roles: ['fixture'] }),
    );
    const runtime = new EsTransport({
      ...service.config,
      username: 'fixture',
      password,
    });
    await withCleanup(
      async () => {
        const authenticated = await runtime.request(
          'GET',
          '/_security/_authenticate',
        );
        let status = 200;
        try {
          await runtime.request(
            'PUT',
            `/${index}/_mapping`,
            '{"dynamic":false}',
          );
        } catch (error) {
          assert.ok(
            error && typeof error === 'object' && 'statusCode' in error,
          );
          assert.equal(error.statusCode, 403);
          status = 403;
        }
        assert.equal(status, privileges[0]?.startsWith('indices:') ? 403 : 200);
        const actual = object(
          object(await service.client.request('GET', `/${index}`))[index],
        );
        assert.equal(
          object(actual['mappings'])['dynamic'],
          status === 200 ? 'false' : 'strict',
        );
        const response = object(
          await runtime.request(
            'POST',
            `/${index}/_bulk`,
            `{"index":{"_id":"versioned","version":${++revision},"version_type":"external"}}\n{}\n`,
          ),
        );
        assert.equal(response['errors'], false);
        assert.ok(Array.isArray(response['items']));
        const item = object(object(response['items'][0])['index']);
        assert.ok(['200', '201'].includes(exactInteger(item['status'])));
        records.push({
          privileges,
          authenticated,
          mappingPutStatus: status,
          actualMapping: actual['mappings'],
          externalVersion: revision,
          bulkItem: item,
        });
      },
      () => runtime.close(),
    );
    await service.client.request(
      'PUT',
      `/${index}/_mapping`,
      '{"dynamic":"strict"}',
    );
  }
  await writeFile(
    `${directory}/permission-probe.json`,
    JSON.stringify({ run, records, isolatedReproduction: true }, null, 2) +
      '\n',
  );
  console.log(`Recorded: ${directory}/permission-probe.json`);
}, service.cleanup);
