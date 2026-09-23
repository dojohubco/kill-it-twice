// Independent read-only verification observations; not imported by workers or the API.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { stringify } from 'lossless-json';
import {
  database,
  adminConfig,
  password,
  readObject,
  safeFailure,
} from '../runtime/private.ts';
import { BrokerMetadata } from '../../src/rabbitmq/metadata.ts';
async function emit(data: unknown) {
  const line = stringify(data);
  assert.ok(
    typeof line === 'string' && Buffer.byteLength(line) <= 4 * 1024 * 1024,
  );
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(line + '\n', (e) => (e ? reject(e) : resolve())),
  );
}
const queries: Record<string, string> = {
  backfill: `SELECT r.run_id,r.phase,r.upper_key::text,r.sealed_at::text,r.completed_at::text,q.range_no,q.lower_key::text,q.upper_key::text AS range_upper,q.checkpoint::text,q.state,q.generation::text,q.owner_id,q.last_batch,q.lease_until::text,(SELECT count(*)::text FROM pipeline.backfill_batches b WHERE b.run_id=r.run_id AND b.range_no=q.range_no) AS committed_batches FROM pipeline.backfill_runs r JOIN pipeline.backfill_ranges q USING(run_id) ORDER BY r.created_at,q.range_no LIMIT 64`,
  sessions: `SELECT pid,application_name,state,backend_xid::text,query,query_start::text FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND usename NOT IN ('pipeline_admin') ORDER BY pid LIMIT 128`,
  failures: `SELECT l.event_id,l.attempt_id,l.error_class,l.context,l.recorded_at::text,a.outcome,a.finished_at::text,d.state,d.attempt_id=l.attempt_id AS current FROM pipeline.es_dead_letters l JOIN pipeline.es_attempts a USING(attempt_id) JOIN pipeline.delivery_intents d ON d.event_id=l.event_id AND d.destination_id=l.destination_id ORDER BY l.event_id COLLATE "C",l.attempt_id LIMIT 1000`,
  attempts: `SELECT outcome,count(*)::text AS count FROM pipeline.es_attempts GROUP BY outcome ORDER BY outcome NULLS FIRST`,
  delivery: `SELECT kind,state,count(*)::text AS count FROM pipeline.delivery_intents GROUP BY kind,state ORDER BY kind,state`,
};
try {
  const mode = process.argv[2] ?? 'summary';
  const query = queries[mode];
  if (query) {
    await emit(
      await database(await adminConfig('pipeline'), async (c) => {
        await c.query('BEGIN READ ONLY');
        return (await c.query<Record<string, unknown>>(query)).rows;
      }),
    );
  } else if (mode === 'event') {
    const key = process.argv[3];
    assert.ok(key && /^[a-f0-9-]{36}:[1-9][0-9]*:[1-9][0-9]*$/.test(key));
    const pipeline = await database(
      await adminConfig('pipeline'),
      async (c) => {
        await c.query('BEGIN READ ONLY');
        return {
          event: (
            await c.query<Record<string, unknown>>(
              "SELECT event_id,content_sha256,encode(body_bytes,'hex') AS body_bytes,staged_at::text FROM pipeline.events WHERE event_id=$1",
              [key],
            )
          ).rows,
          deliveries: (
            await c.query<Record<string, unknown>>(
              'SELECT kind,state,claim_generation::text,attempt_id,rabbit_attempt_id,disposition,created_at::text,settled_at::text FROM pipeline.delivery_intents WHERE event_id=$1 ORDER BY kind',
              [key],
            )
          ).rows,
          observation: (
            await c.query<Record<string, unknown>>(
              'SELECT event_id,state,receipt_id,observed_at::text FROM pipeline.consumer_observations WHERE event_id=$1',
              [key],
            )
          ).rows,
        };
      },
    );
    const consumer = await database(
      await adminConfig('consumer'),
      async (c) => {
        await c.query('BEGIN READ ONLY');
        return {
          inbox: (
            await c.query<Record<string, unknown>>(
              'SELECT event_id,content_sha256,xmin::text FROM consumer.processed_events WHERE event_id=$1',
              [key],
            )
          ).rows,
          effects: (
            await c.query<Record<string, unknown>>(
              'SELECT event_id,source_epoch,source_change_id FROM consumer.mutation_effects WHERE event_id=$1',
              [key],
            )
          ).rows,
        };
      },
    );
    await emit({ pipeline, consumer });
  } else if (mode === 'queue') {
    const installation = await readObject('/private/initialized.json');
    assert.ok(
      installation['topology'] && typeof installation['topology'] === 'object',
    );
    const topology = installation['topology'] as Record<string, unknown>;
    assert.equal(typeof topology['vhost'], 'string');
    assert.equal(typeof topology['queue'], 'string');
    const client = new BrokerMetadata({
      url: 'https://rabbitmq:15671',
      username: 'm4_setup',
      password: await password('rabbit_setup'),
      ca: await readFile('/private/server.crt', 'utf8'),
    });
    const value = await client.request(
      'GET',
      `/api/queues/${encodeURIComponent(String(topology['vhost']))}/${encodeURIComponent(String(topology['queue']))}`,
    );
    assert.ok(value && typeof value === 'object');
    const q = value as Record<string, unknown>;
    await emit({
      name: q['name'],
      messages: q['messages'],
      ready: q['messages_ready'],
      unacknowledged: q['messages_unacknowledged'],
      consumers: q['consumers'],
    });
  } else {
    await import('../runtime/inspect.ts');
  }
} catch (e) {
  console.error(JSON.stringify(safeFailure(e)));
  process.exitCode = 1;
}
