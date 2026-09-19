import assert from 'node:assert/strict';
import type pg from 'pg';
import { createHash } from 'node:crypto';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { decodeWire } from '../../src/rabbitmq/protocol.ts';
import { record } from '../../src/rabbitmq/metadata.ts';
import { waitFor } from '../../scripts/support.ts';
import { drain as captureDrain } from './capture.ts';
import { evidence } from './db.ts';
import {
  mutation,
  expected,
  drain,
  ledgerWire,
  publisher,
  metadata,
  topology,
  consumerConfig,
  amqpConfig,
} from './rabbit.ts';
import {
  batchTrace,
  installBatchTiming,
  ObservedConsumerDatabase,
  type BatchTrace,
} from './batch-observation.ts';
export const cap = 1048576;
export interface Cohort {
  ids: string[];
  events: ReturnType<typeof decodeWire>[];
}
const payload = (padding: number, unicode = false) =>
  `{"name":${JSON.stringify(unicode ? 'ქართული 🙂 "quote" \\ slash' : 'M4.1 size fixture')},"country":"GE","loyalty_points":7,"precise":9007199254740993,"decimal":0.12345678901234567890123456789,"padding":"${'x'.repeat(padding)}"}`;
export async function cohort(
  s: pg.Client,
  p: pg.Client,
  c: pg.Client,
  sizes: number[],
  unicode = false,
): Promise<Cohort> {
  const seeds = [];
  for (const size of sizes) {
    assert.ok(size >= 2048 && size <= 60000);
    const seed = await mutation(s, payload(0, unicode));
    const original = await expected(s, seed.id, '1');
    const wire = Buffer.from(
      `{"body":${original.body},"content_sha256":"${original.hash}"}`,
    );
    seeds.push({ id: seed.id, padding: size - wire.length, size });
  }
  await drain(p, c); // Ordinary bounded capture/publication/processing of the small sizing revisions.
  const ids = [];
  for (const seed of seeds) {
    assert.ok(seed.padding > 0);
    const update = await mutation(
      s,
      payload(seed.padding, unicode),
      seed.id,
      'update',
    );
    assert.equal(update.reply.result.entity_version, '2');
    const actual = await expected(s, seed.id, '2');
    assert.equal(
      Buffer.byteLength(
        `{"body":${actual.body},"content_sha256":"${actual.hash}"}`,
      ),
      seed.size,
      'Measured source-export wire, not guessed metadata length',
    );
    ids.push(seed.id);
  }
  await captureDrain('m41-cohort');
  const events = [];
  for (const id of ids) {
    const source = await expected(s, id, '2');
    const eventId = `${topology().epoch}:${id}:2`;
    const wire = await ledgerWire(p, eventId);
    assert.equal(
      wire.toString('utf8'),
      `{"body":${source.body},"content_sha256":"${source.hash}"}`,
    );
    events.push(decodeWire(wire));
  }
  evidence('M41-source-cohort', {
    kind: 'actual source commands and immutable outbox exports',
    ids,
    events: events.map((e) => ({
      eventId: e.body.event_id,
      bodyBytes: e.bodyBytes.length,
      wireBytes: e.wireBytes.length,
      bodyHash: e.contentSha256,
      wireHash: createHash('sha256').update(e.wireBytes).digest('hex'),
    })),
  });
  return { ids, events };
}
export async function publishCohort(group: Cohort) {
  const result = await publisher().once();
  assert.equal(result.claimed, group.events.length);
  assert.ok(result.outcomes.every((r) => r.outcome === 'confirmed'));
  evidence('M41-production-publication', {
    events: group.events.map((e) => e.body.event_id),
    result,
  });
  return result;
}
export async function queueState(ready: number, unacked = 0) {
  const m = metadata(),
    t = topology();
  return waitFor(
    async (signal) =>
      record(
        await m.request(
          'GET',
          `/api/queues/${encodeURIComponent(t.vhost)}/${encodeURIComponent(t.queue)}`,
          undefined,
          signal,
        ),
      ),
    (r) =>
      r['messages_ready'] === ready && r['messages_unacknowledged'] === unacked,
    'Independent broker queue state',
    15000,
  );
}
export async function consumerSnapshot(c: pg.Client) {
  const result: Record<string, string[]> = {};
  for (const table of [
    'identity',
    'processed_events',
    'mutation_effects',
    'entity_totals',
    'entity_projection',
    'quarantine',
  ]) {
    result[table] = (
      await c.query<{ row: string }>(
        `SELECT to_jsonb(t)::text row FROM consumer.${table} t ORDER BY to_jsonb(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.row);
  }
  return result;
}
export async function assertCohort(
  s: pg.Client,
  c: pg.Client,
  group: Cohort,
  processed = true,
) {
  const observed = [];
  for (const id of group.ids) {
    const rows = (
      await c.query<{
        event_id: string;
        version: string;
        body: string;
        hash: string;
        change_id: string;
        units: string;
        projected: string;
      }>(
        `SELECT p.event_id,p.entity_version::text version,convert_from(p.body_bytes,'UTF8') body,p.content_sha256 hash,m.source_change_id::text change_id,t.units::text,x.entity_version::text projected FROM consumer.processed_events p JOIN consumer.mutation_effects m USING(event_id) JOIN consumer.entity_totals t USING(source_epoch,entity_id) JOIN consumer.entity_projection x USING(source_epoch,entity_id) WHERE p.entity_id=$1 ORDER BY p.entity_version`,
        [id],
      )
    ).rows;
    assert.equal(rows.length, processed ? 2 : 1);
    for (const row of rows) {
      const src = await expected(s, id, row.version);
      assert.equal(row.body, src.body);
      assert.equal(row.hash, src.hash);
      assert.equal(row.change_id, src.source['change_id']);
      assert.equal(row.units, processed ? '2' : '1');
      assert.equal(row.projected, processed ? '2' : '1');
    }
    observed.push(...rows);
  }
  evidence('M41-independent-effects', { processed, observed });
  return observed;
}
export async function sqlBytes(c: pg.Client, group: Cohort) {
  const rows = (
    await c.query<{
      event_id: string;
      body_bytes: number;
      wire_bytes: number;
      old_charge: number;
    }>(
      `SELECT convert_from(decode(i->>'body','hex'),'UTF8')::jsonb->>'event_id' event_id,octet_length(decode(i->>'body','hex')) body_bytes,octet_length(convert_to('{"body":','UTF8')||decode(i->>'body','hex')||convert_to(',"content_sha256":"'||(i->>'hash')||'"}','UTF8')) wire_bytes,octet_length(decode(i->>'body','hex'))+108 old_charge FROM jsonb_array_elements($1::jsonb) i`,
      [
        JSON.stringify(
          group.events.map((e) => ({
            body: e.bodyBytes.toString('hex'),
            hash: e.contentSha256,
          })),
        ),
      ],
    )
  ).rows;
  assert.equal(rows.length, group.events.length);
  for (const [i, row] of rows.entries()) {
    const e = group.events[i];
    assert.ok(e);
    assert.equal(row.wire_bytes, e.wireBytes.length);
    assert.equal(row.body_bytes, e.bodyBytes.length);
    assert.equal(row.wire_bytes - row.body_bytes, 93);
  }
  return {
    rows,
    wireBytes: rows.reduce((n, r) => n + r.wire_bytes, 0),
    oldSqlCharge: rows.reduce((n, r) => n + r.old_charge, 0),
  };
}
export function observedWorker(trace: BatchTrace) {
  return new Consumer(
    new ObservedConsumerDatabase(
      consumerConfig('runtime', 'm41-observed'),
      trace,
    ),
    amqpConfig('consumer'),
    metadata(),
    topology(),
  );
}
export async function consumeCohort(minimum = 32) {
  const trace = batchTrace(),
    restore = installBatchTiming(trace, [minimum]);
  try {
    const result = await observedWorker(trace).once(undefined, 10000);
    return { trace, result };
  } finally {
    restore();
  }
}
