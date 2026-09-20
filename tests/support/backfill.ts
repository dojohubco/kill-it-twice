import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { parse } from 'lossless-json';
import { Backfill } from '../../src/backfill/worker.ts';
import { BackfillLedger, type PageRequest } from '../../src/backfill/ledger.ts';
import { BackfillSource } from '../../src/backfill/source.ts';
import { canonicalEvent } from '../../src/envelope.ts';
import { object, text, type Claim } from '../../src/backfill/types.ts';
import { backfillConfig } from './backfill-process.ts';
import { required, evidence, databaseWaitFor } from './db.ts';
import {
  expected as mutationExpected,
  mutation,
  drain as rabbitDrain,
} from './rabbit.ts';
import { baselineExpected, recipe, checkRecipe } from './bootstrap.ts';
import { EsTransport, version } from '../../src/es/transport.ts';
import { esConfig, finish } from './es.ts';
export const runId = randomUUID();
export const scan = () => new Backfill(backfillConfig());
export const backfillLedger = () =>
  new BackfillLedger(backfillConfig().pipeline);
export const backfillSource = () =>
  new BackfillSource(backfillConfig().source, backfillConfig().binding);
const scanObservations = new Map<string, string>();
export const mutations = new Map<
  string,
  Awaited<ReturnType<typeof mutationExpected>>
>();
export const unobservedBaselines = new Set<string>();
const initialBaselines = new Map<
  string,
  Awaited<ReturnType<typeof baselineExpected>>
>();
export let expectedFence: string[] = [];
export function setFence(ids: string[]) {
  expectedFence = [...ids].sort();
}
export async function declareMutation(
  s: pg.Client,
  id: string | null = null,
  payload = '{"name":"Backfill change","country":"GE","loyalty_points":7}',
  operation: 'create' | 'update' | 'delete' | 'restore' = id === null
    ? 'create'
    : 'update',
) {
  const result = await mutation(s, payload, id, operation),
    expected = await mutationExpected(
      s,
      result.id,
      result.reply.result.entity_version,
    );
  assert.equal(
    expected.source['payload_json'],
    result.reply.result.payload_json,
  );
  mutations.set(result.eventId, expected);
  return result;
}
export async function rememberLegacy(
  s: pg.Client,
  id: string,
  version: string,
) {
  const e = await mutationExpected(s, id, version);
  mutations.set(`${recipe.epoch}:${id}:${version}`, e);
  return e;
}
export async function session(c: pg.Client, prefix: string) {
  const rows = (
    await c.query<Record<string, unknown>>(
      'SELECT pid,backend_xid::text,state,wait_event_type,wait_event,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE application_name LIKE $1',
      [prefix + '%'],
    )
  ).rows;
  assert.equal(rows.length, 1);
  return object(rows[0]);
}
export async function gone(c: pg.Client, prefix: string) {
  await databaseWaitFor(
    c,
    () =>
      c.query(
        'SELECT pid FROM pg_stat_activity WHERE application_name LIKE $1',
        [prefix + '%'],
      ),
    (r) => r.rowCount === 0,
    'Backfill owned session ended',
    6000,
  );
  return true;
}
export async function snapshot(p: pg.Client, run = runId) {
  const data: Record<string, unknown> = {};
  for (const table of [
    'backfill_runs',
    'backfill_ranges',
    'backfill_batches',
    'backfill_members',
  ])
    data[table] = (
      await p.query<{ r: string }>(
        `SELECT row_to_json(t)::text r FROM pipeline.${table} t WHERE run_id=$1 ORDER BY row_to_json(t)::text COLLATE "C"`,
        [run],
      )
    ).rows.map((r) => r.r);
  return data;
}
export async function claimPage(run = runId) {
  const claim = await backfillLedger().claim(run, randomUUID(), 30000);
  assert.ok(claim);
  const page = await backfillSource().page(
    claim.checkpoint,
    claim.upper,
    claim.range === 0 ? run : undefined,
  );
  return { claim, page, batchId: randomUUID() };
}
export function requestFromBarrier(value: unknown): PageRequest {
  const raw = object(object(value)['request']),
    r = object(raw['claim']),
    p = object(raw['page']);
  const c: Claim = {
    runId: text(r['runId']),
    range: Number(r['range']),
    owner: text(r['owner']),
    generation: text(r['generation']),
    checkpoint: text(r['checkpoint']),
    upper: text(r['upper']),
    leaseUntil: text(r['leaseUntil']),
  };
  const events = p['events'],
    keys = p['keys'];
  assert.ok(Array.isArray(events) && Array.isArray(keys));
  const o = object(p['observation']);
  return {
    claim: c,
    batchId: text(raw['batchId']),
    page: {
      events: events.map((e: unknown) => canonicalEvent(object(e)['body'])),
      keys: keys.map(text),
      next: text(p['next']),
      eof: p['eof'] === true,
      observation: {
        observed_at: text(o['observed_at']),
        snapshot: text(o['snapshot']),
      },
      blocked: null,
    },
  };
}
async function collectObservations(p: pg.Client, run = runId) {
  const rows = (
    await p.query<{ event_id: string; body: string }>(
      "SELECT x->>'event_id' event_id,convert_from(e.body_bytes,'UTF8') body FROM pipeline.backfill_batches b CROSS JOIN LATERAL jsonb_array_elements(b.items) x JOIN pipeline.events e ON e.event_id=x->>'event_id' WHERE b.run_id=$1 AND b.range_no>0",
      [run],
    )
  ).rows;
  for (const r of rows) scanObservations.set(r.event_id, r.body);
}
export async function scanTo(phase: string, run = runId) {
  const b = scan();
  for (let i = 0; i < 150; i++) {
    const status = await b.status(run);
    if (status.phase === phase) return status;
    assert.equal(status.blocked, false);
    await b.once(run);
  }
  throw new Error('Bounded backfill progress did not reach phase');
}
export async function deliver(p: pg.Client, c: pg.Client) {
  await rabbitDrain(p, c);
  const es = new EsTransport(esConfig());
  try {
    await finish(es, p);
  } finally {
    await es.close();
  }
}
export async function initializeExpected(s: pg.Client) {
  await checkRecipe(s, 257);
  const rows = (
    await s.query<{ id: string }>(
      'SELECT entity_id::text id FROM source.baseline_revisions ORDER BY ordinal',
    )
  ).rows;
  for (const r of rows)
    initialBaselines.set(
      `${recipe.epoch}:${r.id}:1`,
      await baselineExpected(s, r.id),
    );
}
export function checkReceiver(actual: unknown, expected: unknown) {
  assert.deepEqual(
    actual,
    expected,
    'Independent complete receiver projection',
  );
}
export function checkSet(actual: string[], expected: string[], label: string) {
  assert.deepEqual([...actual].sort(), [...new Set(expected)].sort(), label);
}
export async function reconcile(s: pg.Client, p: pg.Client, c: pg.Client) {
  await collectObservations(p);
  const requiredIds = [
    ...new Set([
      ...[...initialBaselines.keys()].filter(
        (id) => !unobservedBaselines.has(id),
      ),
      ...expectedFence,
    ]),
  ].sort();
  const members = (
    await p.query<{ event_id: string }>(
      'SELECT event_id FROM pipeline.backfill_members WHERE run_id=$1 ORDER BY event_id COLLATE "C"',
      [runId],
    )
  ).rows.map((r) => r.event_id);
  checkSet(members, requiredIds, 'Finite required membership');
  const sourceMutations = (
    await s.query<{ id: string }>(
      "SELECT source_epoch::text||':'||entity_id::text||':'||entity_version::text id FROM source.outbox",
    )
  ).rows.map((r) => r.id);
  checkSet(
    sourceMutations,
    [...mutations.keys()],
    'Verifier-owned mutation journal',
  );
  const allExpected = new Map(
    [...initialBaselines].filter(([id]) => !unobservedBaselines.has(id)),
  );
  for (const [id, e] of mutations) allExpected.set(id, e);
  const ids = [...allExpected.keys()].sort();
  checkSet(
    (
      await p.query<{ event_id: string }>(
        'SELECT event_id FROM pipeline.events',
      )
    ).rows.map((r) => r.event_id),
    ids,
    'Ledger exact set',
  );
  checkSet(
    (
      await c.query<{ event_id: string }>(
        'SELECT event_id FROM consumer.processed_events',
      )
    ).rows.map((r) => r.event_id),
    ids,
    'Inbox exact set',
  );
  checkSet(
    (
      await c.query<{ event_id: string }>(
        'SELECT event_id FROM consumer.mutation_effects',
      )
    ).rows.map((r) => r.event_id),
    [...mutations.keys()],
    'Every mutation has one effect',
  );
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
  for (const [id, e] of allExpected) {
    const actual = (
      await p.query<{ body: string; hash: string }>(
        "SELECT convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash FROM pipeline.events WHERE event_id=$1",
        [id],
      )
    ).rows[0];
    assert.deepEqual(actual, { body: e.body, hash: e.hash });
    assert.deepEqual(
      (
        await c.query<{ body: string; hash: string }>(
          "SELECT convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash FROM consumer.processed_events WHERE event_id=$1",
          [id],
        )
      ).rows[0],
      actual,
    );
    const entity = text(e.source['entity_id']),
      v = text(e.source['entity_version']),
      old = latest.get(entity);
    if (!old || BigInt(v) > BigInt(old.version))
      latest.set(entity, { id: entity, version: v, ...e });
    const states = (
      await p.query<{ kind: string; state: string }>(
        'SELECT kind,state FROM pipeline.delivery_intents WHERE event_id=$1 ORDER BY kind',
        [id],
      )
    ).rows;
    assert.deepEqual(states, [
      { kind: 'elasticsearch', state: 'satisfied' },
      { kind: 'rabbitmq', state: 'satisfied' },
    ]);
    assert.equal(
      (
        await p.query<{ state: string }>(
          'SELECT state FROM pipeline.consumer_observations WHERE event_id=$1',
          [id],
        )
      ).rows[0]?.state,
      'processed',
    );
    if (mutations.has(id))
      assert.equal(
        (
          await s.query<{ hash: string }>(
            'SELECT acknowledged_hash hash FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2',
            [entity, v],
          )
        ).rows[0]?.hash,
        e.hash,
      );
  }
  const es = new EsTransport(esConfig()),
    receiver = [];
  try {
    for (const e of latest.values()) {
      const remote = object(
          await es.request(
            'GET',
            `/${required('ES_INDEX')}/_doc/${recipe.epoch}:${e.id}`,
          ),
        ),
        doc = object(remote['_source']);
      assert.equal(version(remote['_version']), e.version);
      assert.equal(doc['canonical_body_json'], e.body);
      assert.equal(doc['content_sha256'], e.hash);
      assert.equal(doc['is_deleted'], e.source['is_deleted']);
      const projected = (
        await c.query<{ version: string; body: string; units: string }>(
          "SELECT p.entity_version::text version,convert_from(e.body_bytes,'UTF8') body,t.units::text FROM consumer.entity_projection p JOIN consumer.processed_events e USING(event_id) JOIN consumer.entity_totals t ON t.source_epoch=p.source_epoch AND t.entity_id=p.entity_id WHERE p.entity_id=$1",
          [e.id],
        )
      ).rows[0];
      assert.deepEqual(projected, {
        version: e.version,
        body: e.body,
        units: String(
          [...mutations.values()].filter((x) => x.source['entity_id'] === e.id)
            .length,
        ),
      });
      const fields = (
        await s.query<{ fields: string }>(
          `SELECT CASE WHEN $1::boolean THEN '{}'::jsonb ELSE (SELECT coalesce(jsonb_object_agg(key,value),'{}'::jsonb) FROM jsonb_each($2::jsonb) WHERE key IN ('name','country','loyalty_points')) END::text fields`,
          [e.source['is_deleted'], e.source['payload_json']],
        )
      ).rows[0];
      assert.ok(fields);
      const expectedDocument = {
        projection_schema: 'search-v1',
        source_epoch: recipe.epoch,
        entity_id: e.id,
        entity_version: e.version,
        is_deleted: e.source['is_deleted'],
        canonical_body_json: e.body,
        content_sha256: e.hash,
        search_fields: parse(fields.fields),
      };
      checkReceiver(doc, expectedDocument);
      receiver.push({ id: e.id, version: e.version, doc, expectedDocument });
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
      ),
      hits = object(search['hits'])['hits'];
    assert.ok(Array.isArray(hits));
    checkSet(
      hits.map((h: unknown) => text(object(h)['_id'])),
      [...latest.keys()].map((id) => `${recipe.epoch}:${id}`),
      'Independent receiver identity set',
    );
  } finally {
    await es.close();
  }
  const sourceCurrent = (
    await s.query<{ id: string; version: string }>(
      'SELECT entity_id::text id,entity_version::text version FROM source.entities ORDER BY entity_id',
    )
  ).rows;
  assert.equal(latest.size, sourceCurrent.length);
  for (const r of sourceCurrent)
    assert.equal(latest.get(r.id)?.version, r.version);
  evidence('M5B-independent-reconciliation', {
    runId,
    requiredIds,
    expectedFence,
    scan: [...scanObservations],
    mutationJournal: [...mutations],
    baselines: [...initialBaselines],
    sourceCurrent,
    receiver,
  });
  return { requiredIds, ids, receiver };
}
