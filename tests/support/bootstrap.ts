import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import type pg from 'pg';
import { parse } from 'lossless-json';
import { Bootstrap, type BootstrapRecipe } from '../../src/bootstrap.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { SourceReader } from '../../src/source-reader.ts';
import { config, pipeline } from './staging.ts';
import { evidence, required } from './db.ts';
import { execute, request } from './commands.ts';
import {
  drain as rabbitDrain,
  expected as mutationExpected,
} from './rabbit.ts';
import { drain as captureDrain } from './capture.ts';
import { esConfig, finish } from './es.ts';
import { EsTransport, object, version } from '../../src/es/transport.ts';
export const recipe: BootstrapRecipe = {
  epoch: required('SOURCE_EPOCH'),
  key: randomUUID(),
  version: 1,
  seed: 'M5A-ქართული',
  count: '257',
  chunkSize: 32,
};
export function bootstrapConfig(label = 'bootstrap') {
  return {
    ...config('reader', label),
    user: 'source_bootstrap',
    password: required('SOURCE_BOOTSTRAP_PASSWORD'),
  };
}
export function pipelineConfig(label = 'activation-identity') {
  return {
    ...config('stager', label),
    user: 'pipeline_capture',
    password: required('PIPELINE_CAPTURE_PASSWORD'),
  };
}
export const bootstrap = () => new Bootstrap(bootstrapConfig());
export const selected = new Set<string>();
const mutationJournal: {
  eventId: string;
  id: string;
  version: string;
  changeId: string;
  payload: string | null;
  deleted: boolean;
}[] = [];
export const blocked = new Set<string>();
function expectedRecipe(ordinal: string) {
  const n = BigInt(ordinal);
  // Independent literal request specification, never source.seed_payload or numeric JSON.parse.
  return `{"name":${JSON.stringify(`Seed ${recipe.seed} #${ordinal}`)},"country":"${n % 2n === 0n ? 'GE' : 'FR'}","loyalty_points":${n % 1000n},"seed_ordinal":"${ordinal}","tags":["ქართული","café",${n % 7n},null],"optional":${n % 3n === 0n ? 'null' : '"value"'},"exact":${9007199254740993n + n},"decimal":0.123456789012345678901234567890,"padding":"${String.fromCharCode(97 + Number(n % 26n)).repeat(768)}"}`;
}
export async function members(s: pg.Client) {
  return (
    await s.query<{
      ordinal: string;
      entity_id: string;
      recorded_at: string;
      payload: string;
    }>(
      'SELECT ordinal::text,entity_id::text,to_char(recorded_at AT TIME ZONE \'UTC\',\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\') recorded_at,payload::text payload FROM source.baseline_revisions b ORDER BY b.ordinal',
    )
  ).rows;
}
export async function member(s: pg.Client, ordinal: string) {
  const row = (await members(s)).find((r) => r.ordinal === ordinal);
  assert.ok(row);
  return row;
}
export async function seedSnapshot(s: pg.Client) {
  const r: Record<string, unknown> = {};
  for (const name of [
    'bootstrap_manifest',
    'bootstrap_chunks',
    'baseline_revisions',
    'entities',
    'outbox',
    'capture_work',
    'command_receipts',
    'capture_binding',
  ])
    r[name] = (
      await s.query<{ r: string }>(
        `SELECT row_to_json(t)::text r FROM source.${name} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((x) => x.r);
  return r;
}
export function assertSeedMap(
  rows: {
    ordinal: string;
    entity_id: string;
    recorded_at: string;
    payload: string;
  }[],
  expectedPayloads: string[],
  recordedAt: string,
) {
  assert.equal(rows.length, expectedPayloads.length);
  assert.equal(new Set(rows.map((r) => r.entity_id)).size, rows.length);
  for (const [i, r] of rows.entries()) {
    assert.equal(r.ordinal, String(i + 1));
    assert.equal(r.payload, expectedPayloads[i]);
    assert.equal(r.recorded_at, recordedAt);
  }
}
export async function recipeExpectations(s: pg.Client, count: number) {
  const payloads = [];
  for (let i = 1; i <= count; i++) {
    const row = (
      await s.query<{ payload: string }>('SELECT $1::jsonb::text payload', [
        expectedRecipe(String(i)),
      ])
    ).rows[0];
    assert.ok(row);
    payloads.push(row.payload);
  }
  const clock = (
    await s.query<{ time: string }>(
      'SELECT to_char(baseline_at AT TIME ZONE \'UTC\',\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\') time FROM source.bootstrap_manifest',
    )
  ).rows[0];
  assert.ok(clock);
  return { payloads, time: clock.time };
}
export async function checkRecipe(s: pg.Client, count: number) {
  const rows = await members(s),
    expected = await recipeExpectations(s, count);
  assertSeedMap(rows, expected.payloads, expected.time);
  evidence('M5A-independent-seed-map', { recipe, rows });
  return rows;
}
export const code = (sqlState: string) => (error: unknown) =>
  error instanceof TransactionError
    ? error.sqlState === sqlState
    : typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      error.code === sqlState;
export async function changed(
  s: pg.Client,
  id: string | null,
  payload: string | null,
  operation: 'create' | 'update' | 'delete' | 'restore' = 'update',
) {
  const command = request(recipe.epoch, operation, id, payload);
  evidence('M5A-command-declaration', command);
  const reply = await execute(command, 'm5a-command');
  assert.equal(typeof reply.result.change_id, 'string');
  assert.ok(reply.result.change_id);
  const entry = {
    eventId: `${recipe.epoch}:${reply.result.entity_id}:${reply.result.entity_version}`,
    id: reply.result.entity_id,
    version: reply.result.entity_version,
    changeId: reply.result.change_id,
    payload: reply.result.payload_json,
    deleted: reply.result.is_deleted,
  };
  const observed = await mutationExpected(s, entry.id, entry.version);
  assert.equal(observed.source['change_id'], entry.changeId);
  assert.equal(observed.source['payload_json'], entry.payload);
  mutationJournal.push(entry);
  evidence('M5A-command-observed', { command, reply, entry });
  return entry;
}
export async function baselineExpected(s: pg.Client, id: string) {
  const r = (
    await s.query<Record<string, unknown>>(
      'SELECT source_epoch::text,entity_id::text,entity_version::text,change_id::text,is_deleted,payload::text payload_json,to_char(recorded_at AT TIME ZONE \'UTC\',\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\') recorded_at FROM source.baseline_revisions WHERE entity_id=$1',
      [id],
    )
  ).rows[0];
  assert.ok(r);
  const body = JSON.stringify({
    entity_id: r['entity_id'],
    entity_version: r['entity_version'],
    event_id: `${String(r['source_epoch'])}:${String(r['entity_id'])}:${String(r['entity_version'])}`,
    is_deleted: r['is_deleted'],
    kind: 'baseline',
    payload_encoding: 'pg18-jsonb-text/v1',
    payload_json: r['payload_json'],
    schema_version: 1,
    source_change_id: null,
    source_epoch: r['source_epoch'],
    source_recorded_at: r['recorded_at'],
  });
  return {
    body,
    hash: createHash('sha256').update(body).digest('hex'),
    source: r,
  };
}
export async function stageBaseline(ids: string[]) {
  const r = new SourceReader(config('reader', 'm5a-read'), recipe.epoch);
  const result = await r.baseline(
    ids.map((entityId) => ({ entityId, version: '1' })),
  );
  assert.deepEqual(result.notVisible, []);
  const staged = await pipeline().stage(result.events);
  for (const e of result.events) selected.add(e.body.event_id);
  evidence('M5A-baseline-selection', { ids, staged });
  return result.events;
}
export async function deliver(p: pg.Client, c: pg.Client) {
  await captureDrain('m5a-capture');
  await rabbitDrain(p, c);
  const es = new EsTransport(esConfig());
  try {
    await finish(es, p);
  } finally {
    await es.close();
  }
}
export async function reconcile(s: pg.Client, p: pg.Client, c: pg.Client) {
  await checkRecipe(s, 257);
  const out = (
    await s.query<{ event_id: string }>(
      "SELECT source_epoch::text||':'||entity_id::text||':'||entity_version::text event_id FROM source.outbox ORDER BY entity_id,entity_version",
    )
  ).rows.map((r) => r.event_id);
  assert.deepEqual(
    new Set(out),
    new Set(mutationJournal.map((r) => r.eventId)),
    'Source mutation journal differs',
  );
  const expectedIds = [
    ...selected,
    ...out.filter((id) => !blocked.has(id)),
  ].sort();
  const actual = (
    await p.query<{ event_id: string }>(
      'SELECT event_id FROM pipeline.events ORDER BY event_id COLLATE "C"',
    )
  ).rows.map((r) => r.event_id);
  assert.deepEqual(actual, expectedIds);
  const inbox = (
    await c.query<{ event_id: string }>(
      'SELECT event_id FROM consumer.processed_events ORDER BY event_id COLLATE "C"',
    )
  ).rows.map((r) => r.event_id);
  assert.deepEqual(inbox, expectedIds);
  const es = new EsTransport(esConfig());
  const latest = new Map<
    string,
    {
      id: string;
      version: string;
      body: string;
      hash: string;
      source: Record<string, unknown>;
    }
  >();
  try {
    for (const eventId of expectedIds) {
      const [, id, v] = eventId.split(':');
      assert.ok(id && v);
      const exp = selected.has(eventId)
        ? await baselineExpected(s, id)
        : await mutationExpected(s, id, v);
      const event = (
        await p.query<{ body: string; hash: string }>(
          "SELECT convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash FROM pipeline.events WHERE event_id=$1",
          [eventId],
        )
      ).rows[0];
      assert.deepEqual(event, { body: exp.body, hash: exp.hash });
      const stored = (
        await c.query<{ body: string; hash: string }>(
          "SELECT convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash FROM consumer.processed_events WHERE event_id=$1",
          [eventId],
        )
      ).rows[0];
      assert.deepEqual(stored, event);
      const intents = (
        await p.query<{ kind: string; state: string }>(
          'SELECT kind,state FROM pipeline.delivery_intents WHERE event_id=$1 ORDER BY kind',
          [eventId],
        )
      ).rows;
      assert.deepEqual(intents, [
        { kind: 'elasticsearch', state: 'satisfied' },
        { kind: 'rabbitmq', state: 'satisfied' },
      ]);
      assert.equal(
        (
          await p.query<{ state: string }>(
            'SELECT state FROM pipeline.consumer_observations WHERE event_id=$1',
            [eventId],
          )
        ).rows[0]?.state,
        'processed',
      );
      if (!selected.has(eventId))
        assert.equal(
          (
            await s.query<{ hash: string }>(
              'SELECT acknowledged_hash hash FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2',
              [id, v],
            )
          ).rows[0]?.hash,
          exp.hash,
        );
      const prior = latest.get(id);
      if (!prior || BigInt(v) > BigInt(prior.version))
        latest.set(id, { id, version: v, ...exp });
    }
    const effectRows = (
      await c.query<{ event_id: string }>(
        'SELECT event_id FROM consumer.mutation_effects ORDER BY event_id COLLATE "C"',
      )
    ).rows.map((r) => r.event_id);
    assert.deepEqual(effectRows, out.filter((id) => !blocked.has(id)).sort());
    for (const e of latest.values()) {
      const doc = object(
        await es.request(
          'GET',
          `/${required('ES_INDEX')}/_doc/${recipe.epoch}:${e.id}`,
        ),
      );
      assert.equal(version(doc['_version']), e.version);
      const actual = object(doc['_source']);
      assert.equal(actual['canonical_body_json'], e.body);
      assert.equal(actual['content_sha256'], e.hash);
      assert.equal(actual['is_deleted'], e.source['is_deleted']);
      assert.equal(actual['entity_version'], e.version);
      const projected = (
        await s.query<{ fields: string }>(
          `SELECT CASE WHEN $1::boolean THEN '{}'::jsonb ELSE (SELECT coalesce(jsonb_object_agg(key,value),'{}'::jsonb) FROM jsonb_each($2::jsonb) WHERE key IN ('name','country','loyalty_points')) END::text fields`,
          [e.source['is_deleted'], e.source['payload_json']],
        )
      ).rows[0];
      assert.ok(projected);
      assert.deepEqual(actual['search_fields'], parse(projected.fields));
      const state = (
        await c.query<{ v: string; units: string; body: string; hash: string }>(
          "SELECT p.entity_version::text v,t.units::text,convert_from(i.body_bytes,'UTF8') body,i.content_sha256 hash FROM consumer.entity_projection p JOIN consumer.entity_totals t USING(source_epoch,entity_id) JOIN consumer.processed_events i ON i.event_id=p.event_id WHERE p.entity_id=$1",
          [e.id],
        )
      ).rows[0];
      assert.deepEqual(state, {
        v: e.version,
        body: e.body,
        hash: e.hash,
        units: String(
          effectRows.filter((id) => id.startsWith(`${recipe.epoch}:${e.id}:`))
            .length,
        ),
      });
    }
    const admin = new EsTransport(esConfig(true));
    try {
      await admin.request('POST', `/${required('ES_INDEX')}/_refresh`);
    } finally {
      await admin.close();
    }
    const search = object(
      await es.request(
        'POST',
        `/${required('ES_INDEX')}/_search`,
        JSON.stringify({ size: 1000, query: { match_all: {} } }),
      ),
    );
    const hits = object(search['hits'])['hits'];
    assert.ok(Array.isArray(hits));
    assert.deepEqual(
      hits.map((h) => String(object(h)['_id'])).sort(),
      [...latest.keys()].map((id) => `${recipe.epoch}:${id}`).sort(),
    );
    evidence('M5A-independent-reconciliation', {
      recipe,
      baselineIds: [...selected],
      mutationJournal,
      blocked: [...blocked],
      expectedIds,
      effectRows,
      latest: [...latest.values()],
    });
  } finally {
    await es.close();
  }
}
