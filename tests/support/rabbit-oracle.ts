// Verifier-owned finite workload oracle. Does not import consumer effects/projection code.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { parse } from 'lossless-json';
import type pg from 'pg';
import { EsTransport, object } from '../../src/es/transport.ts';
import { journal, expected } from './rabbit.ts';
import { required, evidence } from './db.ts';
import {
  checkReceiver,
  checkSearchIdentities,
  checkExpectedOutcome,
  type ExpectedRejection,
  type ExpectedReceiver,
} from './es-expectations.ts';
import { remote } from './es.ts';
export const syntheticBaselines: {
  eventId: string;
  body: string;
  hash: string;
  entity: string;
  version: string;
}[] = [];
export interface ConsumerSnapshot {
  inbox: Record<string, unknown>[];
  effects: Record<string, unknown>[];
  totals: Record<string, unknown>[];
  projections: Record<string, unknown>[];
}
async function consumerSnapshot(c: pg.Client): Promise<ConsumerSnapshot> {
  return {
    inbox: (
      await c.query<Record<string, unknown>>(
        "SELECT event_id,source_epoch::text,entity_id::text,entity_version::text,source_change_id::text,kind,is_deleted,convert_from(body_bytes,'UTF8') body,content_sha256 hash FROM consumer.processed_events ORDER BY event_id",
      )
    ).rows,
    effects: (
      await c.query<Record<string, unknown>>(
        'SELECT event_id,source_epoch::text,source_change_id::text FROM consumer.mutation_effects ORDER BY event_id',
      )
    ).rows,
    totals: (
      await c.query<Record<string, unknown>>(
        'SELECT entity_id::text,units::text FROM consumer.entity_totals ORDER BY entity_id',
      )
    ).rows,
    projections: (
      await c.query<Record<string, unknown>>(
        "SELECT p.entity_id::text,p.entity_version::text,p.event_id,convert_from(e.body_bytes,'UTF8') body FROM consumer.entity_projection p JOIN consumer.processed_events e USING(event_id) ORDER BY p.entity_id",
      )
    ).rows,
  };
}
export function checkConsumer(
  actual: ConsumerSnapshot,
  wanted: {
    eventId: string;
    body: string;
    hash: string;
    entity: string;
    version: string;
    change: string | null;
  }[],
) {
  assert.deepEqual(
    actual.inbox.map((r) => r['event_id']).sort(),
    wanted.map((e) => e.eventId).sort(),
    'Inbox identity set differs from declared workload',
  );
  assert.deepEqual(
    actual.effects.map((r) => r['event_id']).sort(),
    wanted
      .filter((e) => e.change !== null)
      .map((e) => e.eventId)
      .sort(),
    'Business effect identity set differs',
  );
  const entities = new Map<string, typeof wanted>();
  for (const e of wanted) {
    const a = actual.inbox.find((r) => r['event_id'] === e.eventId);
    assert.ok(a);
    assert.equal(a['body'], e.body);
    assert.equal(a['hash'], e.hash);
    assert.equal(a['entity_id'], e.entity);
    assert.equal(a['entity_version'], e.version);
    assert.equal(a['source_change_id'], e.change);
    const body = object(JSON.parse(e.body));
    assert.equal(a['source_epoch'], body['source_epoch']);
    assert.equal(a['kind'], body['kind']);
    assert.equal(a['is_deleted'], body['is_deleted']);
    if (e.change !== null)
      assert.equal(
        actual.effects.find((r) => r['event_id'] === e.eventId)?.[
          'source_change_id'
        ],
        e.change,
      );
    const list = entities.get(e.entity) ?? [];
    list.push(e);
    entities.set(e.entity, list);
  }
  assert.equal(actual.totals.length, entities.size);
  assert.equal(actual.projections.length, entities.size);
  for (const [entity, list] of entities) {
    const latest = [...list].sort((a, b) =>
      BigInt(a.version) > BigInt(b.version) ? -1 : 1,
    )[0];
    assert.ok(latest);
    assert.equal(
      actual.totals.find((r) => r['entity_id'] === entity)?.['units'],
      String(list.filter((e) => e.change !== null).length),
      'Aggregate differs from independently expected mutation set',
    );
    const p = actual.projections.find((r) => r['entity_id'] === entity);
    assert.ok(p);
    assert.equal(p['entity_version'], latest.version);
    assert.equal(p['event_id'], latest.eventId);
    assert.equal(
      p['body'],
      latest.body,
      'Current consumer payload/tombstone differs',
    );
  }
}
export async function reconcile(
  s: pg.Client,
  p: pg.Client,
  c: pg.Client,
  es: EsTransport,
  rejections: readonly ExpectedRejection[],
) {
  const declared = journal.map(({ reply }) => ({
    entity: reply.result.entity_id,
    version: reply.result.entity_version,
    change: reply.result.change_id,
  }));
  if (process.env['M4_UPGRADE'] === 'true') {
    const raw: unknown = JSON.parse(
      await readFile(
        join(required('M1_ARTIFACT_DIR'), 'm4-initial-journal.json'),
        'utf8',
      ),
    );
    assert.ok(Array.isArray(raw));
    for (const row of raw) {
      const r = object(object(row)['reply']);
      const v = object(r['result']);
      assert.equal(typeof v['entity_id'], 'string');
      assert.equal(typeof v['entity_version'], 'string');
      assert.equal(typeof v['change_id'], 'string');
      declared.push({
        entity: String(v['entity_id']),
        version: String(v['entity_version']),
        change: String(v['change_id']),
      });
    }
  }
  const mutations = [
    ...new Map(
      declared.map((e) => [
        `${required('SOURCE_EPOCH')}:${e.entity}:${e.version}`,
        e,
      ]),
    ).entries(),
  ];
  const ids = mutations.map(([id]) => id).sort();
  const outbox = (
    await s.query<{ id: string }>(
      "SELECT source_epoch::text||':'||entity_id::text||':'||entity_version::text id FROM source.outbox ORDER BY id",
    )
  ).rows.map((r) => r.id);
  assert.deepEqual(
    outbox.sort(),
    ids,
    'Source history differs from command journal',
  );
  const ledger = (
    await p.query<Record<string, unknown>>(
      "SELECT event_id,convert_from(body_bytes,'UTF8') body,content_sha256 hash,encode(sha256(body_bytes),'hex') independent_hash,entity_id::text,entity_version::text,source_change_id::text FROM pipeline.events ORDER BY event_id",
    )
  ).rows;
  assert.deepEqual(ledger.map((r) => r['event_id']).sort(), ids);
  const intents = (
    await p.query<Record<string, unknown>>(
      'SELECT event_id,kind,state,disposition,attempt_id::text,rabbit_attempt_id::text FROM pipeline.delivery_intents ORDER BY event_id,kind',
    )
  ).rows;
  const observations = (
    await p.query<Record<string, unknown>>(
      "SELECT event_id,state,consumer_id::text,registration_id::text,receipt_id,encode(receipt_bytes,'hex') body_hex,receipt_hash hash FROM pipeline.consumer_observations ORDER BY event_id",
    )
  ).rows;
  const diagnostics = (
    await p.query<Record<string, unknown>>(
      `SELECT d.*,a.event_id attempt_event,a.destination_id attempt_destination,a.outcome attempt_outcome,a.context attempt_context,a.finished_at::text finished_at,i.attempt_id::text terminal_attempt FROM pipeline.es_dead_letters d JOIN pipeline.es_attempts a USING(attempt_id) JOIN pipeline.delivery_intents i ON i.event_id=d.event_id AND i.destination_id=d.destination_id ORDER BY d.event_id`,
    )
  ).rows;
  assert.deepEqual(
    diagnostics.map((r) => r['event_id']).sort(),
    rejections.map((r) => r.eventId).sort(),
  );
  assert.equal(intents.length, ids.length * 2);
  assert.deepEqual(observations.map((r) => r['event_id']).sort(), ids);
  const registration = (
    await p.query<Record<string, unknown>>(
      'SELECT consumer_id::text,registration_id::text,pipeline_id::text,source_epoch::text FROM pipeline.rabbit_target',
    )
  ).rows;
  const consumerIdentity = (
    await c.query<Record<string, unknown>>(
      'SELECT consumer_id::text,registration_id::text,pipeline_id::text,source_epoch::text FROM consumer.identity',
    )
  ).rows;
  assert.deepEqual(consumerIdentity, registration);
  assert.equal(registration.length, 1);
  assert.equal(registration[0]?.['source_epoch'], required('SOURCE_EPOCH'));
  assert.equal(registration[0]?.['pipeline_id'], required('PIPELINE_ID'));
  const wanted = [];
  const receiver = new Map<string, ExpectedReceiver>();
  const latestSource = new Map<string, string>();
  for (const [eventId, d] of mutations) {
    const e = await expected(s, d.entity, d.version);
    assert.equal(e.source['change_id'], d.change);
    const stored = ledger.find((r) => r['event_id'] === eventId);
    assert.ok(stored);
    assert.equal(stored['body'], e.body);
    assert.equal(stored['hash'], e.hash);
    assert.equal(stored['independent_hash'], e.hash);
    assert.equal(stored['entity_id'], d.entity);
    assert.equal(stored['entity_version'], d.version);
    assert.equal(stored['source_change_id'], d.change);
    const work = (
      await s.query<Record<string, unknown>>(
        'SELECT state,acknowledged_hash FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2',
        [d.entity, d.version],
      )
    ).rows;
    assert.deepEqual(work, [
      { state: 'acknowledged', acknowledged_hash: e.hash },
    ]);
    const mq = intents.find(
      (r) => r['event_id'] === eventId && r['kind'] === 'rabbitmq',
    );
    assert.ok(mq);
    assert.equal(mq['state'], 'satisfied');
    assert.equal(mq['disposition'], 'broker_confirmed');
    const attempt = (
      await p.query<Record<string, unknown>>(
        'SELECT event_id,outcome,channel_id::text,finished_at::text FROM pipeline.rabbit_attempts WHERE attempt_id=$1',
        [mq['rabbit_attempt_id']],
      )
    ).rows[0];
    assert.ok(attempt);
    assert.equal(attempt['event_id'], eventId);
    assert.equal(attempt['outcome'], 'confirmed');
    assert.ok(attempt['channel_id']);
    assert.ok(attempt['finished_at']);
    const obs = observations.find((r) => r['event_id'] === eventId);
    assert.ok(obs);
    assert.equal(obs['state'], 'processed');
    assert.equal(obs['consumer_id'], registration[0]?.['consumer_id']);
    assert.equal(obs['registration_id'], registration[0]?.['registration_id']);
    assert.equal(obs['hash'], e.hash);
    assert.equal(obs['body_hex'], Buffer.from(e.body).toString('hex'));
    assert.equal(obs['receipt_id'], eventId);
    wanted.push({ eventId, body: e.body, hash: e.hash, ...d });
    const rejected = rejections.find((r) => r.eventId === eventId);
    const esIntent = intents.find(
      (r) => r['event_id'] === eventId && r['kind'] === 'elasticsearch',
    );
    assert.ok(esIntent);
    assert.equal(typeof esIntent['state'], 'string');
    checkExpectedOutcome(
      eventId,
      String(esIntent['state']),
      rejected,
      diagnostics.find((r) => r['event_id'] === eventId),
    );
    if (rejected) {
      assert.equal(e.source['payload_json'], rejected.payloadText);
      assert.equal(
        createHash('sha256').update(rejected.payloadText).digest('hex'),
        rejected.payloadSha256,
      );
    }
    const documentId = `${required('SOURCE_EPOCH')}:${d.entity}`;
    if (BigInt(d.version) > BigInt(latestSource.get(documentId) ?? '0'))
      latestSource.set(documentId, d.version);
    if (
      !rejected &&
      BigInt(d.version) > BigInt(receiver.get(documentId)?.version ?? '0')
    ) {
      const fields = (
        await s.query<{ fields: string }>(
          "SELECT CASE WHEN is_deleted THEN '{}' ELSE (SELECT coalesce(jsonb_object_agg(key,value),'{}')::text FROM jsonb_each(payload) WHERE key IN ('name','country','loyalty_points')) END fields FROM source.outbox WHERE entity_id=$1 AND entity_version=$2",
          [d.entity, d.version],
        )
      ).rows[0];
      assert.ok(fields);
      receiver.set(documentId, {
        eventId,
        documentId,
        version: d.version,
        source: {
          projection_schema: 'search-v1',
          source_epoch: required('SOURCE_EPOCH'),
          entity_id: d.entity,
          entity_version: d.version,
          is_deleted: e.source['is_deleted'],
          canonical_body_json: e.body,
          content_sha256: e.hash,
          search_fields: parse(fields.fields),
        },
      });
    }
  }
  const allWanted = [
    ...wanted,
    ...syntheticBaselines.map((e) => ({ ...e, change: null })),
  ];
  const snapshot = await consumerSnapshot(c);
  checkConsumer(snapshot, allWanted);
  const sourceCurrent = (
    await s.query<Record<string, unknown>>(
      'SELECT entity_id::text,entity_version::text,is_deleted,payload::text payload FROM source.entities ORDER BY entity_id',
    )
  ).rows;
  for (const row of sourceCurrent) {
    const latest: (typeof wanted)[number] | undefined = wanted
      .filter((e) => e.entity === row['entity_id'])
      .sort((a, b) => (BigInt(a.version) > BigInt(b.version) ? -1 : 1))[0];
    assert.ok(latest);
    const body = object(JSON.parse(latest.body));
    assert.equal(row['entity_version'], latest.version);
    assert.equal(row['is_deleted'], body['is_deleted']);
    assert.equal(row['payload'], body['payload_json']);
  }
  const receivers = [];
  for (const [documentId, sourceVersion] of latestSource) {
    const admissible = receiver.get(documentId);
    const actual = await remote(es, documentId);
    checkReceiver(documentId, admissible, actual);
    receivers.push({
      documentId,
      sourceVersion,
      expectedReceiverVersion: admissible?.version ?? null,
      currentConverged: sourceVersion === admissible?.version,
      actual,
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
  checkSearchIdentities([...receiver.keys()], hits);
  const report = {
    declaredMutations: ids,
    syntheticBaselines,
    ledger,
    sourceCurrent,
    intents,
    observations,
    expectedRejections: rejections,
    diagnostics,
    snapshot,
    receivers,
  };
  evidence('MQ14-reconciliation', report);
  return { snapshot, wanted: allWanted, report };
}
