import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { startEs } from './es-service.ts';
import { withCleanup } from './support.ts';
import { object, exactInteger, version } from '../src/es/transport.ts';
const run = `m3-protocol-${Date.now()}`;
const dir = `artifacts/m3/${run}`;
await mkdir(dir, { recursive: true });
const service = await startEs(run);
await withCleanup(async () => {
  const info = object(await service.client.request('GET', '/'));
  const index = `protocol-${randomUUID()}`;
  await service.client.request(
    'PUT',
    `/${index}`,
    JSON.stringify({
      settings: {
        number_of_shards: 1,
        number_of_replicas: 0,
        'index.translog.durability': 'request',
      },
      mappings: {
        dynamic: 'strict',
        properties: {
          loyalty_points: {
            type: 'integer',
            coerce: false,
            ignore_malformed: false,
          },
        },
      },
    }),
  );
  const observed: unknown[] = [];
  for (const v of ['9007199254740993', '9223372036854775807']) {
    for (const [sent, expected] of [
      [v, '201'],
      [v, '409'],
      [(BigInt(v) - 1n).toString(), '409'],
    ] as const) {
      const body = `{"index":{"_id":"${v}","version":${sent},"version_type":"external"}}\n{"loyalty_points":42}\n`;
      const response = object(
        await service.client.request('POST', `/${index}/_bulk`, body),
      );
      const items = response['items'];
      assert.ok(Array.isArray(items));
      assert.equal(items.length, 1);
      const item = object(object(items[0])['index']);
      assert.equal(exactInteger(item['status']), expected);
      const got = object(
        await service.client.request('GET', `/${index}/_doc/${v}`),
      );
      assert.equal(version(got['_version']), v);
      observed.push({
        sent,
        expected,
        actualStatus: exactInteger(item['status']),
        actualVersion: version(got['_version']),
        request: body,
      });
    }
  }
  const rejected = object(
    await service.client.request(
      'POST',
      `/${index}/_bulk`,
      '{"index":{"_id":"bad","version":1,"version_type":"external"}}\n{"loyalty_points":"not-a-number"}\n',
    ),
  );
  assert.ok(Array.isArray(rejected['items']));
  const bad = object(object(rejected['items'][0])['index']);
  assert.equal(exactInteger(bad['status']), '400');
  assert.equal(object(bad['error'])['type'], 'document_parsing_exception');
  await writeFile(
    `${dir}/protocol.json`,
    JSON.stringify(
      {
        run,
        clusterUuid: info['cluster_uuid'],
        version: object(info['version'])['number'],
        observed,
        mappingError: bad['error'],
        syntheticVersions: true,
      },
      null,
      2,
    ) + '\n',
  );
  console.log(`Protocol PASS: ${dir}/protocol.json`);
}, service.cleanup);
