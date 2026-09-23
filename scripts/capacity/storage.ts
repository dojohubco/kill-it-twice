// Read-only installation storage observations; no fixture creation or receiver mutation.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { stringify } from 'lossless-json';
import { EsTransport, object, exactInteger } from '../../src/es/transport.ts';
import {
  database,
  adminConfig,
  password,
  safeFailure,
} from '../runtime/private.ts';
import { withCleanup } from '../support.ts';
try {
  const databases: Record<string, unknown>[] = [];
  const payload = await database(
    await adminConfig('source'),
    async (client) => {
      await client.query('BEGIN READ ONLY');
      return (
        await client.query<Record<string, unknown>>(
          `SELECT count(*)::text AS entities,sum(octet_length(payload::text))::text AS total_bytes,min(octet_length(payload::text))::text AS minimum_bytes,max(octet_length(payload::text))::text AS maximum_bytes,avg(octet_length(payload::text))::text AS average_bytes FROM source.baseline_revisions`,
        )
      ).rows[0];
    },
  );
  for (const store of ['source', 'pipeline', 'consumer'] as const) {
    databases.push(
      await database(await adminConfig(store), async (client) => {
        await client.query('BEGIN READ ONLY');
        const total = (
          await client.query<{ database: string; bytes: string }>(
            'SELECT current_database() AS database,pg_database_size(current_database())::text AS bytes',
          )
        ).rows[0];
        assert.ok(total);
        const relations = (
          await client.query<Record<string, unknown>>(
            `SELECT schemaname,relname,pg_table_size(relid)::text AS table_bytes,pg_indexes_size(relid)::text AS index_bytes,pg_total_relation_size(relid)::text AS total_bytes FROM pg_stat_user_tables WHERE schemaname=$1 ORDER BY relname`,
            [store],
          )
        ).rows;
        return { ...total, relations };
      }),
    );
  }
  const index = await database(
    await adminConfig('pipeline'),
    async (client) =>
      (
        await client.query<{ index_name: string }>(
          'SELECT index_name FROM pipeline.es_target',
        )
      ).rows[0]?.index_name,
  );
  assert.ok(index && /^kit-[a-f0-9-]+$/.test(index));
  const transport = new EsTransport({
    node: 'https://elasticsearch:9200',
    username: 'kit_setup',
    password: await password('es_setup'),
    ca: await readFile('/private/server.crt', 'utf8'),
  });
  const receiver = await withCleanup(
    async () => {
      const response = object(
        await transport.request(
          'GET',
          `/${index}/_stats/store,docs?filter_path=_shards,_all.primaries`,
        ),
      );
      assert.equal(exactInteger(object(response['_shards'])['failed']), '0');
      const primary = object(object(response['_all'])['primaries']);
      return {
        index,
        primary_store_bytes: exactInteger(
          object(primary['store'])['size_in_bytes'],
        ),
        lucene_documents: exactInteger(object(primary['docs'])['count']),
        note: 'Physical index statistic; independent exported documents prove logical correctness',
      };
    },
    () => transport.close(),
  );
  const output = stringify({
    observed_at: new Date().toISOString(),
    databases,
    baseline_payload: payload,
    elasticsearch: receiver,
  });
  assert.ok(output && Buffer.byteLength(output) <= 262144);
  console.log(output);
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
