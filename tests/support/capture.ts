import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';
import { first } from '../../scripts/rows.ts';
import {
  Capture,
  type CaptureOptions,
  type CaptureConfig,
} from '../../src/capture.ts';
import { SourceCapture, type Claim } from '../../src/source-capture.ts';
import { Pipeline } from '../../src/pipeline.ts';
import { SourceReader } from '../../src/source-reader.ts';
import { config, snapshot } from './staging.ts';
import { required, databaseWaitFor, evidence } from './db.ts';
const fast = { leaseMs: 1500, renewalMs: 150, idleMs: 100 };
export function captureConfig(label: string): CaptureConfig {
  return {
    source: {
      ...config('reader', label),
      user: 'source_capture',
      password: required('SOURCE_CAPTURE_PASSWORD'),
    },
    pipeline: {
      ...config('stager', label),
      user: 'pipeline_capture',
      password: required('PIPELINE_CAPTURE_PASSWORD'),
    },
    binding: {
      pipelineId: required('PIPELINE_ID'),
      sourceEpoch: required('SOURCE_EPOCH'),
    },
  };
}
export function worker(label: string, options: Partial<CaptureOptions> = fast) {
  return new Capture(captureConfig(label), options);
}
export function control(label: string) {
  const c = captureConfig(label);
  return new SourceCapture(c.source, c.binding);
}
export async function work(s: pg.Client, entity?: string) {
  return (
    await s.query<Record<string, unknown>>(
      `SELECT work_id::text,pipeline_id::text,source_epoch::text,entity_id::text,entity_version::text,state,generation::text,owner_id::text,
    lease_until::text,next_eligible_at::text,reason,acknowledged_hash,acknowledged_at::text,acknowledged_generation::text,xmin::text AS xid
    FROM source.capture_work ${entity ? 'WHERE entity_id=$1' : ''} ORDER BY work_id`,
      entity ? [entity] : [],
    )
  ).rows;
}
export async function drain(
  label = 'drain',
  options?: Partial<CaptureOptions>,
) {
  const w = worker(label, options);
  const results = [];
  for (let i = 0; i < 120; i++) {
    const result = await w.captureOnce();
    results.push(result);
    const q = result.sourceState;
    if (
      [
        q.pending_due,
        q.pending_delayed,
        q.leased_current,
        q.leased_expired,
      ].every((value) => value === '0')
    )
      return results;
    await delay(50);
  }
  throw new Error('Capture did not quiesce within bounded drain');
}
export async function expiry(s: pg.Client, claim: Claim) {
  return first(
    await databaseWaitFor(
      s,
      async () =>
        (
          await s.query<{
            expired: boolean;
            now: string;
            lease: string;
            generation: string;
          }>(
            'SELECT clock_timestamp()>lease_until AS expired,clock_timestamp()::text AS now,lease_until::text AS lease,generation::text FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2',
            [claim.entityId, claim.version],
          )
        ).rows,
      (rows) => first(rows).expired,
      'source clock proves lease expiry',
      10000,
    ),
  );
}
export async function stageClaim(claim: Claim, label: string) {
  const c = captureConfig(label);
  const event = first(
    (await new SourceReader(c.source, c.binding.sourceEpoch).outbox([claim]))
      .events,
  );
  const results = await new Pipeline(c.pipeline, c.binding.pipelineId).stage([
    event,
  ]);
  return { event, results };
}
// Independent tiny-fixture reconciliation: SQL exports source payload text; no production serializer/reader.
export async function reconcile(
  s: pg.Client,
  p: pg.Client,
  label: string,
  ids?: string[],
) {
  const revisions = (
    await s.query<{
      source_epoch: string;
      entity_id: string;
      entity_version: string;
      change_id: string;
      recorded_at: string;
      is_deleted: boolean;
      payload_json: string | null;
    }>(
      `SELECT source_epoch::text,entity_id::text,entity_version::text,change_id::text,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at,is_deleted,payload::text AS payload_json FROM source.outbox ${ids ? 'WHERE entity_id=ANY($1::bigint[])' : ''} ORDER BY entity_id,entity_version`,
      ids ? [ids] : [],
    )
  ).rows;
  const states = await work(s);
  const expectedIds = revisions.map(
    (r) => `${r.source_epoch}:${r.entity_id}:${r.entity_version}`,
  );
  const observed = await snapshot(p, expectedIds);
  const records = [];
  for (const r of revisions) {
    const eventId = `${r.source_epoch}:${r.entity_id}:${r.entity_version}`;
    const ack = states.find(
      (v) =>
        v['entity_id'] === r.entity_id &&
        v['entity_version'] === r.entity_version,
    );
    assert.ok(ack);
    const event = observed.events.find((v) => v['event_id'] === eventId);
    if (ack['state'] === 'blocked') {
      assert.equal(event, undefined);
      continue;
    }
    assert.ok(event, `Missing staged ${eventId}`);
    const bytes = Buffer.from(
      JSON.stringify({
        entity_id: r.entity_id,
        entity_version: r.entity_version,
        event_id: eventId,
        is_deleted: r.is_deleted,
        kind: 'mutation',
        payload_encoding: 'pg18-jsonb-text/v1',
        payload_json: r.payload_json,
        schema_version: 1,
        source_change_id: r.change_id,
        source_epoch: r.source_epoch,
        source_recorded_at: r.recorded_at,
      }),
    );
    const hash = createHash('sha256').update(bytes).digest('hex');
    assert.equal(event['body_hex'], bytes.toString('hex'));
    assert.equal(event['payload_json'], r.payload_json);
    assert.equal(event['independent_hash'], hash);
    assert.equal(event['content_sha256'], hash);
    assert.equal(ack['state'], 'acknowledged');
    assert.equal(ack['acknowledged_hash'], hash);
    assert.equal(ack['acknowledged_generation'], ack['generation']);
    assert.equal(ack['pipeline_id'], required('PIPELINE_ID'));
    const deliveries = observed.deliveries.filter(
      (d) => d['event_id'] === eventId,
    );
    assert.deepEqual(
      deliveries.map((d) => d['kind']),
      ['elasticsearch', 'rabbitmq'],
    );
    const observations = observed.observations.filter(
      (o) => o['event_id'] === eventId,
    );
    assert.equal(observations.length, 1);
    for (const obligation of [...deliveries, ...observations])
      assert.equal(obligation['state'], 'pending');
    records.push({ source: r, event, ack, deliveries, observations });
  }
  if (!ids) {
    const all = (
      await p.query<{ event_id: string }>(
        'SELECT event_id FROM pipeline.events ORDER BY event_id',
      )
    ).rows.map((r) => r.event_id);
    const supported = revisions
      .filter(
        (r) =>
          states.find(
            (v) =>
              v['entity_id'] === r.entity_id &&
              v['entity_version'] === r.entity_version,
          )?.['state'] !== 'blocked',
      )
      .map((r) => `${r.source_epoch}:${r.entity_id}:${r.entity_version}`)
      .sort();
    assert.deepEqual(
      all.sort(),
      supported,
      'No missing or extra pipeline identity',
    );
    assert.equal(
      states.length,
      revisions.length,
      'Exactly one work row per revision',
    );
  }
  evidence(label, {
    expectedIds,
    records,
    blocked: states.filter((r) => r['state'] === 'blocked'),
  });
  return records;
}
