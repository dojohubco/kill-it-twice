// Explicit local operator tool. Never imported by workers or the HTTP API.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { stringify } from 'lossless-json';
import { EsTransport, object } from '../../src/es/transport.ts';
import {
  database,
  adminConfig,
  password,
  readObject,
  safeFailure,
} from './private.ts';
import { withCleanup } from '../support.ts';
async function emit(value: unknown) {
  const line = stringify(value);
  assert.ok(
    typeof line === 'string' && Buffer.byteLength(line) <= 262144,
    'Bounded export row',
  );
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('Export output deadline')),
      5000,
    );
    process.stdout.write(line + '\n', (error) => {
      clearTimeout(timer);
      if (error) reject(error);
      else resolve();
    });
  });
}
const collections = {
  baselines: {
    store: 'source',
    query: `SELECT entity_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,is_deleted,payload::text AS payload_json,change_id,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at FROM source.baseline_revisions WHERE entity_id>$1::bigint ORDER BY source.baseline_revisions.entity_id LIMIT 32`,
    initial: '0',
  },
  source: {
    store: 'source',
    query: `SELECT entity_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,is_deleted,payload::text AS payload_json,change_id,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at FROM source.entities WHERE entity_id>$1::bigint ORDER BY source.entities.entity_id LIMIT 32`,
    initial: '0',
  },
  mutations: {
    store: 'source',
    query: `SELECT allocation_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,change_id,is_deleted,payload::text AS payload_json,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at FROM source.outbox WHERE allocation_id>$1::bigint ORDER BY allocation_id LIMIT 32`,
    initial: '0',
  },
  pipeline: {
    store: 'pipeline',
    query: `SELECT e.event_id AS cursor,e.event_id,convert_from(e.body_bytes,'UTF8') AS body,e.content_sha256,d.state AS es_state,d.disposition AS es_disposition,r.state AS rabbit_state,o.state AS consumer_state FROM pipeline.events e LEFT JOIN pipeline.delivery_intents d ON d.event_id=e.event_id AND d.kind='elasticsearch' LEFT JOIN pipeline.delivery_intents r ON r.event_id=e.event_id AND r.kind='rabbitmq' LEFT JOIN pipeline.consumer_observations o ON o.event_id=e.event_id WHERE e.event_id COLLATE "C">$1 COLLATE "C" ORDER BY e.event_id COLLATE "C" LIMIT 32`,
    initial: '',
  },
  consumer: {
    store: 'consumer',
    query: `SELECT e.event_id AS cursor,e.event_id,convert_from(e.body_bytes,'UTF8') AS body,e.content_sha256,(p.event_id IS NOT NULL) AS is_current,(m.event_id IS NOT NULL) AS has_effect,t.units::text FROM consumer.processed_events e LEFT JOIN consumer.entity_projection p ON p.event_id=e.event_id LEFT JOIN consumer.mutation_effects m ON m.event_id=e.event_id LEFT JOIN consumer.entity_totals t ON t.source_epoch=e.source_epoch AND t.entity_id=e.entity_id WHERE e.event_id COLLATE "C">$1 COLLATE "C" ORDER BY e.event_id COLLATE "C" LIMIT 32`,
    initial: '',
  },
  commands: {
    store: 'source',
    query: `SELECT command_id::text AS cursor,command_id,source_epoch,operation,target_id::text,request_payload::text AS request_payload,completed,result_entity_id::text,result_version::text,result_change_id,to_char(result_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS result_recorded_at,result_deleted,result_payload::text FROM source.command_receipts WHERE command_id>$1::uuid ORDER BY command_id LIMIT 32`,
    initial: '00000000-0000-0000-0000-000000000000',
  },
  work: {
    store: 'source',
    query: `SELECT work_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,pipeline_id,state,generation::text,acknowledged_generation::text,acknowledged_hash,reason FROM source.capture_work WHERE work_id>$1::bigint ORDER BY work_id LIMIT 32`,
    initial: '0',
  },
  totals: {
    store: 'consumer',
    query: `SELECT entity_id::text AS cursor,source_epoch,entity_id::text,units::text FROM consumer.entity_totals WHERE entity_id>$1::bigint ORDER BY consumer.entity_totals.entity_id LIMIT 32`,
    initial: '0',
  },
  projection: {
    store: 'consumer',
    query: `SELECT entity_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,event_id FROM consumer.entity_projection WHERE entity_id>$1::bigint ORDER BY consumer.entity_projection.entity_id LIMIT 32`,
    initial: '0',
  },
} as const;
async function summary() {
  const source = await database(await adminConfig('source'), async (c) => {
    await c.query('BEGIN READ ONLY');
    return {
      manifest: (
        await c.query<Record<string, unknown>>(
          'SELECT source_epoch,phase,bootstrap_key,requested_count::text,completed_count::text FROM source.bootstrap_manifest',
        )
      ).rows,
      entities: (
        await c.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM source.entities',
        )
      ).rows[0]?.count,
      mutations: (
        await c.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM source.outbox',
        )
      ).rows[0]?.count,
    };
  });
  const pipeline = await database(await adminConfig('pipeline'), async (c) => {
    await c.query('BEGIN READ ONLY');
    return {
      identity: (
        await c.query<Record<string, unknown>>(
          'SELECT * FROM pipeline.source_binding',
        )
      ).rows,
      events: (
        await c.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM pipeline.events',
        )
      ).rows[0]?.count,
      deliveries: (
        await c.query<Record<string, unknown>>(
          'SELECT kind,state,count(*)::text AS count FROM pipeline.delivery_intents GROUP BY kind,state ORDER BY kind,state',
        )
      ).rows,
      runs: (
        await c.query<Record<string, unknown>>(
          'SELECT run_id,phase,upper_key::text,completed_at::text FROM pipeline.backfill_runs ORDER BY created_at DESC LIMIT 16',
        )
      ).rows,
    };
  });
  const consumer = await database(await adminConfig('consumer'), async (c) => {
    await c.query('BEGIN READ ONLY');
    return (
      await c.query<Record<string, unknown>>(
        `SELECT (SELECT count(*)::text FROM consumer.processed_events) AS processed,(SELECT count(*)::text FROM consumer.mutation_effects) AS effects,(SELECT coalesce(sum(units),0)::text FROM consumer.entity_totals) AS units,(SELECT count(*)::text FROM consumer.quarantine) AS quarantine`,
      )
    ).rows[0];
  });
  await emit({ source, pipeline, consumer });
}
async function exportDatabase(name: keyof typeof collections) {
  const collection = collections[name];
  await database(await adminConfig(collection.store), async (c) => {
    await c.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    let cursor: string = collection.initial;
    for (;;) {
      const rows = (
        await c.query<Record<string, unknown>>(collection.query, [cursor])
      ).rows;
      for (const row of rows) {
        assert.equal(typeof row['cursor'], 'string');
        cursor = String(row['cursor']);
        await emit(row);
      }
      if (rows.length < 32) break;
    }
  });
}
async function exportReceiver() {
  const index = await database(
    await adminConfig('pipeline'),
    async (c) =>
      (
        await c.query<{ index_name: string }>(
          'SELECT index_name FROM pipeline.es_target',
        )
      ).rows[0]?.index_name,
  );
  assert.ok(index && /^kit-[a-f0-9-]+$/.test(index));
  const client = new EsTransport({
    node: 'https://elasticsearch:9200',
    username: 'kit_setup',
    password: await password('es_setup'),
    ca: await readFile('/private/server.crt', 'utf8'),
  });
  await withCleanup(
    async () => {
      let after: unknown[] | undefined;
      for (;;) {
        const result = object(
          await client.request(
            'POST',
            `/${index}/_search`,
            JSON.stringify({
              size: 32,
              version: true,
              track_total_hits: false,
              sort: [{ source_epoch: 'asc' }, { entity_id: 'asc' }],
              ...(after ? { search_after: after } : {}),
            }),
          ),
        );
        assert.equal(result['timed_out'], false);
        assert.equal(stringify(object(result['_shards'])['failed']), '0');
        const hits = object(result['hits'])['hits'];
        assert.ok(Array.isArray(hits));
        for (const value of hits as unknown[]) {
          const row = object(value);
          assert.ok(Array.isArray(row['sort']));
          after = row['sort'];
          await emit({
            document_id: row['_id'],
            receiver_version: row['_version'],
            source: row['_source'],
          });
        }
        if (hits.length < 32) break;
      }
    },
    () => client.close(),
  );
}
try {
  const mode = process.argv[2] ?? 'summary';
  await readObject('/private/initialized.json');
  if (mode === 'summary') await summary();
  else if (mode === 'receiver') await exportReceiver();
  else if (mode === 'token')
    process.stdout.write((await password('operator_token')) + '\n');
  else {
    assert.ok(
      mode === 'baselines' ||
        mode === 'source' ||
        mode === 'mutations' ||
        mode === 'pipeline' ||
        mode === 'consumer' ||
        mode === 'commands' ||
        mode === 'work' ||
        mode === 'totals' ||
        mode === 'projection',
    );
    await exportDatabase(mode);
  }
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
