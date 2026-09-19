import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { parse } from 'lossless-json';
import type pg from 'pg';
import type { TestContext } from 'node:test';
import { EsTransport, object, version } from '../../src/es/transport.ts';
import { EsLedger } from '../../src/es/ledger.ts';
import { EsAdapter } from '../../src/es/adapter.ts';
import { Delivery, type DeliveryOptions } from '../../src/es/worker.ts';
import { config, pconnect } from './staging.ts';
import { required, connect, evidence } from './db.ts';
import { execute, request } from './commands.ts';
import { drain as captureDrain } from './capture.ts';
// Ordinary M3 workloads use production capture leases. Private expiry/fault children
// retain their explicitly shorter timings and independent database-clock evidence.
export function drain(label = 'es-capture') {
  return captureDrain(label, { leaseMs: 30000, renewalMs: 5000, idleMs: 1000 });
}
export function esConfig(setup = false, proxy = false) {
  return {
    node: required(proxy ? 'ES_PROXY_URL' : 'ES_URL'),
    username: setup ? 'elastic' : required('ES_USERNAME'),
    password: required(setup ? 'ES_SETUP_PASSWORD' : 'ES_PASSWORD'),
    ca: required('ES_CA'),
  };
}
export function ledgerConfig(label = 'es-runtime') {
  return {
    ...config('pipelineAdmin', label),
    user: 'pipeline_es',
    password: required('PIPELINE_ES_PASSWORD'),
  };
}
export function ledger(label = 'es-runtime') {
  return new EsLedger(ledgerConfig(label));
}
export function worker(
  transport: EsTransport,
  options: Partial<DeliveryOptions> = {},
) {
  return new Delivery(ledger(), new EsAdapter(transport), options);
}
export async function setup(t: TestContext) {
  const p = await pconnect(t.name);
  t.after(() => p.end());
  const s = await connect('admin', t.name);
  t.after(() => s.end());
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  const admin = new EsTransport(esConfig(true));
  t.after(() => admin.close());
  return { p, s, es, admin, target: await ledger().target() };
}
export async function create(
  payload = '{"name":"Fixture","country":"GE","loyalty_points":42}',
  entityId: string | null = null,
  operation: 'create' | 'update' | 'delete' | 'restore' = 'create',
) {
  const command = request(
    required('SOURCE_EPOCH'),
    operation,
    entityId,
    operation === 'delete' ? null : payload,
  );
  const reply = await execute(command);
  return {
    command,
    reply,
    id: reply.result.entity_id,
    eventId: `${reply.result.source_epoch}:${reply.result.entity_id}:${reply.result.entity_version}`,
    documentId: `${reply.result.source_epoch}:${reply.result.entity_id}`,
  };
}
export async function staged(payload?: string) {
  const f = await create(payload);
  await drain('es-capture');
  return f;
}
export async function deliveries(p: pg.Client, ids?: string[]) {
  return (
    await p.query<Record<string, unknown>>(
      `SELECT event_id,state,claim_generation::text,owner_id::text,lease_until::text,next_retry_at::text,disposition,remote_version::text,witness_event_id,attempt_id::text,settled_at::text,error_class,created_at::text FROM pipeline.delivery_intents WHERE kind='elasticsearch' ${ids ? 'AND event_id=ANY($1::text[])' : ''} ORDER BY event_id`,
      ids ? [ids] : [],
    )
  ).rows;
}
export async function finish(es: EsTransport, p: pg.Client) {
  const w = worker(es);
  const results = [];
  const end = performance.now() + 45000;
  while (performance.now() < end) {
    results.push(await w.once());
    const remaining = (await deliveries(p)).filter(
      (r) => !['satisfied', 'dead_letter'].includes(String(r['state'])),
    );
    if (!remaining.length) return results;
    await delay(100);
  }
  throw new Error('Delivery drain deadline');
}
export async function untouched(p: pg.Client) {
  return {
    rabbit: (
      await p.query<{ text: string }>(
        "SELECT row_to_json(d)::text AS text FROM pipeline.delivery_intents d WHERE kind='rabbitmq' ORDER BY event_id",
      )
    ).rows,
    observations: (
      await p.query<{ text: string }>(
        'SELECT row_to_json(o)::text AS text FROM pipeline.consumer_observations o ORDER BY event_id',
      )
    ).rows,
    events: (
      await p.query<{ text: string }>(
        'SELECT row_to_json(e)::text AS text FROM pipeline.events e ORDER BY event_id',
      )
    ).rows,
  };
}
export async function remote(es: EsTransport, id: string) {
  return object(
    await es.request(
      'GET',
      `/${required('ES_INDEX')}/_doc/${encodeURIComponent(id)}`,
    ),
  );
}
// Independently written SQL and literal projection keys; no production projection/normalizer.
export async function oracle(s: pg.Client, p: pg.Client, es: EsTransport) {
  const revisions = (
    await s.query<{
      event_id: string;
      body: string;
      state: string;
      acknowledged_hash: string;
    }>(`SELECT o.source_epoch::text||':'||o.entity_id::text||':'||o.entity_version::text event_id,
    jsonb_build_object('schema_version',1,'source_epoch',o.source_epoch::text,'entity_id',o.entity_id::text,'entity_version',o.entity_version::text,
    'event_id',o.source_epoch::text||':'||o.entity_id::text||':'||o.entity_version::text,'source_change_id',o.change_id::text,
    'source_recorded_at',to_char(o.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'kind','mutation',
    'is_deleted',o.is_deleted,'payload_encoding','pg18-jsonb-text/v1','payload_json',o.payload::text)::text body,
    w.state,w.acknowledged_hash FROM source.outbox o LEFT JOIN source.capture_work w USING(source_epoch,entity_id,entity_version) ORDER BY event_id`)
  ).rows;
  const ledgerRows = (
    await p.query<{
      event_id: string;
      body: string;
      hash: string;
      recorded_hash: string;
      revision: string;
      epoch: string;
      entity: string;
    }>(`SELECT event_id,convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash,content_sha256 recorded_hash,
    entity_version::text revision,source_epoch::text epoch,entity_id::text entity FROM pipeline.events ORDER BY event_id`)
  ).rows;
  assert.deepEqual(
    ledgerRows.map((r) => r.event_id),
    revisions.map((r) => r.event_id),
  );
  const obligations = (
    await p.query<{ event_id: string; kind: string; state: string }>(
      'SELECT event_id,kind,state FROM pipeline.delivery_intents ORDER BY event_id,kind',
    )
  ).rows;
  const observations = (
    await p.query<{ event_id: string; state: string }>(
      'SELECT event_id,state FROM pipeline.consumer_observations ORDER BY event_id',
    )
  ).rows;
  assert.equal(obligations.length, revisions.length * 2);
  assert.equal(observations.length, revisions.length);
  const history = [];
  for (const revision of revisions) {
    // This envelope contains payload text as a string; no arbitrary payload number
    // enters JSON.parse. Independent literal SQL export and lexical field sorting.
    const fields = object(JSON.parse(revision.body));
    const body = JSON.stringify(
      Object.fromEntries(
        Object.entries(fields).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      ),
    );
    const hash = createHash('sha256').update(body).digest('hex');
    const stored = ledgerRows.find((r) => r.event_id === revision.event_id);
    assert.ok(stored);
    assert.equal(stored.body, body);
    assert.equal(stored.hash, hash);
    assert.equal(stored.recorded_hash, hash);
    assert.equal(stored.revision, fields['entity_version']);
    assert.equal(stored.entity, fields['entity_id']);
    assert.equal(stored.epoch, fields['source_epoch']);
    assert.equal(revision.state, 'acknowledged');
    assert.equal(revision.acknowledged_hash, hash);
    const intents = obligations.filter((r) => r.event_id === revision.event_id);
    assert.deepEqual(
      intents.map((r) => r.kind),
      ['elasticsearch', 'rabbitmq'],
    );
    assert.equal(intents[1]?.state, 'pending');
    assert.ok(['satisfied', 'dead_letter'].includes(intents[0]?.state ?? ''));
    const observed = observations.filter(
      (r) => r.event_id === revision.event_id,
    );
    assert.equal(observed.length, 1);
    assert.equal(observed[0]?.state, 'pending');
    history.push({
      eventId: revision.event_id,
      body,
      hash,
      ack: revision.acknowledged_hash,
      intents,
      observed,
    });
  }
  const rows = (
    await s.query<{
      entity_id: string;
      source_epoch: string;
      entity_version: string;
      is_deleted: boolean;
      payload: string | null;
      search_fields: string;
      change_id: string;
      recorded_at: string;
    }>(`SELECT entity_id::text,source_epoch::text,entity_version::text,is_deleted,payload::text,change_id::text,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') recorded_at,
 CASE WHEN is_deleted THEN '{}' ELSE (SELECT COALESCE(jsonb_object_agg(key,value),'{}')::text FROM jsonb_each(payload) WHERE key IN ('name','country','loyalty_points')) END AS search_fields FROM source.entities ORDER BY entity_id`)
  ).rows;
  const evidenceRows: {
    id: string;
    unresolved?: boolean;
    version?: string;
    source?: unknown;
    delivery: Record<string, unknown>;
  }[] = [];
  for (const r of rows) {
    const id = `${r.source_epoch}:${r.entity_id}:${r.entity_version}`;
    const d = (await deliveries(p, [id]))[0];
    assert.ok(d);
    if (d['state'] === 'dead_letter') {
      evidenceRows.push({ id, unresolved: true, delivery: d });
      continue;
    }
    assert.equal(d['state'], 'satisfied');
    const doc = await remote(es, `${r.source_epoch}:${r.entity_id}`);
    assert.equal(version(doc['_version']), r.entity_version);
    const body = JSON.stringify({
      entity_id: r.entity_id,
      entity_version: r.entity_version,
      event_id: id,
      is_deleted: r.is_deleted,
      kind: 'mutation',
      payload_encoding: 'pg18-jsonb-text/v1',
      payload_json: r.payload,
      schema_version: 1,
      source_change_id: r.change_id,
      source_epoch: r.source_epoch,
      source_recorded_at: r.recorded_at,
    });
    const expected = {
      projection_schema: 'search-v1',
      source_epoch: r.source_epoch,
      entity_id: r.entity_id,
      entity_version: r.entity_version,
      is_deleted: r.is_deleted,
      canonical_body_json: body,
      content_sha256: createHash('sha256').update(body).digest('hex'),
      search_fields: parse(r.search_fields),
    };
    assert.deepEqual(doc['_source'], expected);
    const canonical = (
      await p.query<{ body: string; hash: string; ack: string | null }>(
        `SELECT convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash,NULL::text ack FROM pipeline.events WHERE event_id=$1`,
        [id],
      )
    ).rows[0];
    assert.equal(canonical?.body, body);
    assert.equal(canonical.hash, expected.content_sha256);
    const ack = (
      await s.query<{ hash: string }>(
        'SELECT acknowledged_hash AS hash FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2',
        [r.entity_id, r.entity_version],
      )
    ).rows[0];
    assert.equal(ack?.hash, expected.content_sha256);
    evidenceRows.push({
      id,
      version: version(doc['_version']),
      source: doc['_source'],
      delivery: d,
    });
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
  const expectedIds = rows
    .filter(
      (r) =>
        evidenceRows.find(
          (e) =>
            e.id === `${r.source_epoch}:${r.entity_id}:${r.entity_version}`,
        )?.unresolved !== true,
    )
    .map((r) => `${r.source_epoch}:${r.entity_id}`)
    .sort();
  assert.deepEqual(hits.map((h) => object(h)['_id']).sort(), expectedIds);
  evidence('ES14-reconciliation', {
    history,
    rows: evidenceRows,
    searchIds: expectedIds,
  });
  return evidenceRows;
}
