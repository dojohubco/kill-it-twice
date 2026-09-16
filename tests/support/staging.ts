import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import type { TestContext } from 'node:test';
import pg from 'pg';
import { first } from '../../scripts/rows.ts';
import { Pipeline } from '../../src/pipeline.ts';
import { SourceReader } from '../../src/source-reader.ts';
import type { CanonicalEvent } from '../../src/envelope.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import { connect, required, databaseWaitFor } from './db.ts';
import { activity, epoch, execute, request } from './commands.ts';
export function config(
  role: 'reader' | 'stager' | 'pipelineAdmin',
  label: string,
): ConnectionConfig {
  const source = role === 'reader';
  return {
    host: '127.0.0.1',
    port: Number(required(source ? 'M1_PORT' : 'PIPELINE_PORT')),
    database: source ? 'source_m1' : 'pipeline_m2b',
    user: source
      ? 'source_reader'
      : role === 'stager'
        ? 'pipeline_stager'
        : 'pipeline_admin',
    password: required(
      source
        ? 'SOURCE_READER_PASSWORD'
        : role === 'stager'
          ? 'PIPELINE_STAGER_PASSWORD'
          : 'PIPELINE_ADMIN_PASSWORD',
    ),
    application_name: `${required('M1_RUN_ID')}:${label}`,
  };
}
export function pipeline(label = 'stager') {
  return new Pipeline(config('stager', label));
}
export function reader(label = 'reader') {
  return new SourceReader(config('reader', label), required('SOURCE_EPOCH'));
}
export async function pconnect(
  label: string,
  role: 'stager' | 'pipelineAdmin' = 'pipelineAdmin',
) {
  const c = new pg.Client({
    ...config(role, label),
    connectionTimeoutMillis: 5000,
    query_timeout: 15000,
    statement_timeout: 12000,
  });
  await c.connect();
  return c;
}
export async function setup(t: TestContext) {
  const s = await connect('admin', t.name);
  t.after(() => s.end());
  const p = await pconnect(t.name);
  t.after(() => p.end());
  return { s, p, epoch: await epoch(s) };
}
export async function fixture(epoch: string, payload = '{"tiny": true}') {
  const command = request(epoch, 'create', null, payload);
  const reply = await execute(command);
  const key = {
    entityId: reply.result.entity_id,
    version: reply.result.entity_version,
  };
  const event = first((await reader().outbox([key])).events);
  return { command, reply, key, event };
}
// Independent SQL oracle; no JS numeric payload parsing and no production hash/serializer.
export async function snapshot(p: pg.Client, ids: readonly string[]) {
  const events = (
    await p.query<Record<string, unknown>>(
      `SELECT event_id,source_epoch::text,entity_id::text,entity_version::text,source_change_id::text,
    to_char(source_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS source_recorded_at,kind,is_deleted,
    encode(body_bytes,'hex') AS body_hex,content_sha256,encode(sha256(body_bytes),'hex') AS independent_hash,
    convert_from(body_bytes,'UTF8')::jsonb->>'payload_json' AS payload_json,staged_at::text,xmin::text AS xid
    FROM pipeline.events WHERE event_id=ANY($1::text[]) ORDER BY event_id`,
      [ids],
    )
  ).rows;
  const deliveries = (
    await p.query<Record<string, unknown>>(
      'SELECT event_id,kind,destination_id::text,state,created_at::text,xmin::text AS xid FROM pipeline.delivery_intents WHERE event_id=ANY($1::text[]) ORDER BY event_id,kind',
      [ids],
    )
  ).rows;
  const observations = (
    await p.query<Record<string, unknown>>(
      'SELECT event_id,state,created_at::text,xmin::text AS xid FROM pipeline.consumer_observations WHERE event_id=ANY($1::text[]) ORDER BY event_id',
      [ids],
    )
  ).rows;
  return { events, deliveries, observations };
}
export async function oracle(
  s: pg.Client,
  p: pg.Client,
  event: CanonicalEvent,
) {
  const state = await snapshot(p, [event.body.event_id]);
  const stored = first(state.events);
  assert.equal(state.events.length, 1);
  assert.equal(state.deliveries.length, 2);
  assert.equal(state.observations.length, 1);
  assert.deepEqual(
    state.deliveries.map((row) => row['kind']),
    ['elasticsearch', 'rabbitmq'],
  );
  for (const row of [...state.deliveries, ...state.observations])
    assert.equal(row['state'], 'pending');
  assert.equal(stored['body_hex'], event.bodyBytes.toString('hex'));
  assert.equal(stored['content_sha256'], stored['independent_hash']);
  assert.equal(
    stored['content_sha256'],
    createHash('sha256')
      .update(Buffer.from(String(stored['body_hex']), 'hex'))
      .digest('hex'),
  );
  const source = first(
    (
      await s.query<Record<string, unknown>>(
        `SELECT source_epoch::text,entity_id::text,entity_version::text,change_id::text AS source_change_id,
    to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS source_recorded_at,is_deleted,payload::text AS payload_json
    FROM source.outbox WHERE source_epoch=$1 AND entity_id=$2 AND entity_version=$3`,
        [
          event.body.source_epoch,
          event.body.entity_id,
          event.body.entity_version,
        ],
      )
    ).rows,
  );
  for (const [key, value] of Object.entries(source))
    assert.deepEqual(stored[key], value, key);
  return state;
}
export function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, release: () => resolve() };
}
export async function blocked(p: pg.Client, waiter: string, holder: string) {
  const winner = first(await activity(p, holder));
  const loser = first(
    await databaseWaitFor(
      p,
      () => activity(p, waiter),
      (rows) => rows.length === 1 && first(rows).blockers.includes(winner.pid),
      `${waiter} blocked by ${holder}`,
    ),
  );
  assert.equal(loser.usename, 'pipeline_stager');
  assert.equal(loser.wait_event_type, 'Lock');
  return { winner, loser };
}
export async function rawInsert(
  p: pg.Client,
  event: CanonicalEvent,
  overrides: Record<string, unknown> = {},
) {
  const b = event.body;
  const row: Record<string, unknown> = {
    event_id: b.event_id,
    source_epoch: b.source_epoch,
    entity_id: b.entity_id,
    entity_version: b.entity_version,
    source_change_id: b.source_change_id,
    source_recorded_at: b.source_recorded_at,
    kind: b.kind,
    is_deleted: b.is_deleted,
    body_bytes: event.bodyBytes,
    content_sha256: event.contentSha256,
    ...overrides,
  };
  await p.query(
    `INSERT INTO pipeline.events(${Object.keys(row).join(',')}) VALUES (${Object.keys(
      row,
    )
      .map((_, i) => `$${i + 1}`)
      .join(',')})`,
    Object.values(row),
  );
}
