// Historical M3 oracle from c3cb8f7c8398d8db4d158f2307f790244f4199e6.
// Retained only for the isolated counterexample; never used for acceptance.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { parse } from 'lossless-json';
import type pg from 'pg';
import { EsTransport, object, version } from '../../src/es/transport.ts';
import { required, evidence } from './db.ts';
import { deliveries, remote } from './es.ts';
// Independently written SQL and literal projection keys; no production projection/normalizer.
export async function historicalM3Oracle(
  s: pg.Client,
  p: pg.Client,
  es: EsTransport,
) {
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
