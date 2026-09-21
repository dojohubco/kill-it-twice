import { readFile } from 'node:fs/promises';
import { RecoveryService } from '../../src/operations/recovery.ts';
import type { OperationsConfig } from '../../src/operations/config.ts';
import { recoveryCases } from '../../scripts/required-recovery-cases.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { operationalUpgradeCases } from '../../scripts/required-operational-cases.ts';
import {
  connections,
  migrateAll,
  retained,
  apiConfig,
  startApi,
  networkRoute,
  data,
} from '../support/operations.ts';
import { bootstrap, recipe, pipelineConfig } from '../support/bootstrap.ts';
import { scan, deliver } from '../support/backfill.ts';
import { create, esConfig } from '../support/es.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { required, evidence } from '../support/db.ts';
import { record, string } from '../../src/operations/validation.ts';
let completedFixture:
  | {
      config: OperationsConfig;
      run: string;
      oldCounts: unknown;
      oldHistorical: string;
    }
  | undefined;
const entry = operationalUpgradeCases[0];
assert.ok(entry);
await test(entry.name, async (t) => {
  const db = await connections();
  t.after(() => db.close());
  const b = bootstrap(),
    r = { ...recipe, count: '17', chunkSize: 8, seed: 'M6-upgrade' };
  await b.begin(r);
  for (const first of ['1', '9', '17']) await b.chunk(r.epoch, r.key, first);
  await b.seal(r.epoch, r.key);
  await b.activate(r.epoch, r.key, required('PIPELINE_ID'), pipelineConfig());
  const scanner = scan();
  async function complete(run: string) {
    await scanner.start(run, 4);
    for (let i = 0; i < 40; i++) {
      if ((await scanner.status(run)).phase === 'draining') break;
      await scanner.once(run);
    }
    await deliver(db.p, db.c);
    await scanner.once(run);
    return scanner.status(run);
  }
  const healthy = randomUUID();
  assert.equal((await complete(healthy)).phase, 'complete');
  const bad = await create(
    '{"name":"retained receiver rejection","loyalty_points":12}',
  );
  const degraded = randomUUID();
  const receiver = new EsTransport(esConfig(true));
  try {
    await receiver.request(
      'PUT',
      '/_ingest/pipeline/kit-upgrade-rejection',
      JSON.stringify({
        processors: [
          {
            set: {
              field: 'search_fields.loyalty_points',
              value: 'not-a-number',
            },
          },
        ],
      }),
    );
    await receiver.request(
      'PUT',
      `/${required('ES_INDEX')}/_settings`,
      JSON.stringify({ 'index.default_pipeline': 'kit-upgrade-rejection' }),
    );
    assert.equal((await complete(degraded)).phase, 'complete_with_errors');
  } finally {
    await receiver.request(
      'PUT',
      `/${required('ES_INDEX')}/_settings`,
      JSON.stringify({ 'index.default_pipeline': '_none' }),
    );
    await receiver.request('DELETE', '/_ingest/pipeline/kit-upgrade-rejection');
    await receiver.close();
  }
  const sourceTables = [
    'source.entities',
    'source.outbox',
    'source.command_receipts',
    'source.capture_work',
    'source.bootstrap_manifest',
    'source.baseline_revisions',
    'source.backfill_fences',
    'source.backfill_fence_members',
  ];
  const pipelineTables = [
    'pipeline.events',
    'pipeline.delivery_intents',
    'pipeline.consumer_observations',
    'pipeline.es_target',
    'pipeline.rabbit_target',
    'pipeline.es_attempts',
    'pipeline.es_dead_letters',
    'pipeline.backfill_runs',
    'pipeline.backfill_ranges',
    'pipeline.backfill_batches',
    'pipeline.backfill_members',
  ];
  const consumerTables = [
    'consumer.processed_events',
    'consumer.entity_projection',
    'consumer.mutation_effects',
    'consumer.entity_totals',
    'consumer.quarantine',
  ];
  const before = {
    source: await retained(db.s, sourceTables),
    pipeline: await retained(db.p, pipelineTables),
    consumer: await retained(db.c, consumerTables),
  };
  const password = await migrateAll(db.s, db.p, db.c, false);
  assert.deepEqual(
    {
      source: await retained(db.s, sourceTables),
      pipeline: await retained(db.p, pipelineTables),
      consumer: await retained(db.c, consumerTables),
    },
    before,
  );
  const routes = await networkRoute();
  t.after(() => routes.close());
  const api = await startApi(apiConfig(password));
  t.after(() => api.close().then(() => {}));
  const d = record(
    (
      await db.p.query<Record<string, unknown>>(
        "SELECT row_to_json(d) value FROM pipeline.delivery_intents d WHERE event_id=$1 AND kind='elasticsearch'",
        [bad.eventId],
      )
    ).rows[0]?.['value'],
  );
  completedFixture = {
    config: apiConfig(password),
    run: degraded,
    oldCounts: (
      await db.p.query<{ value: unknown }>(
        'SELECT pipeline.backfill_counts($1) value',
        [degraded],
      )
    ).rows[0]?.value,
    oldHistorical: string(
      (
        await db.p.query<{ value: string }>(
          'SELECT row_to_json(r)::text value FROM pipeline.backfill_runs r WHERE run_id=$1',
          [degraded],
        )
      ).rows[0]?.value,
      16384,
    ),
  };
  const history = (
    await db.p.query<Record<string, unknown>>(
      'SELECT row_to_json(l)::text value FROM pipeline.es_dead_letters l WHERE event_id=$1',
      [bad.eventId],
    )
  ).rows;
  data(
    await api.request(
      `/api/v1/failures/elasticsearch/${bad.eventId}/replay`,
      'POST',
      {
        attempt_id: d['attempt_id'],
        destination_id: d['destination_id'],
        generation: '1',
        reason: 'upgrade replay',
      },
    ),
    202,
  );
  await deliver(db.p, db.c);
  const afterHistory = (
    await db.p.query<Record<string, unknown>>(
      'SELECT row_to_json(l)::text value FROM pipeline.es_dead_letters l WHERE event_id=$1',
      [bad.eventId],
    )
  ).rows;
  assert.deepEqual(afterHistory, history);
  assert.equal(
    (
      await db.p.query<{ state: string }>(
        "SELECT state FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='elasticsearch'",
        [bad.eventId],
      )
    ).rows[0]?.state,
    'satisfied',
  );
  assert.ok(afterHistory.some((r) => r['value'] === history[0]?.['value']));
  assert.deepEqual(await retained(db.s, sourceTables), before.source);
  assert.deepEqual(await retained(db.c, consumerTables), before.consumer);
  assert.deepEqual(
    await retained(db.p, [
      'pipeline.backfill_runs',
      'pipeline.backfill_ranges',
      'pipeline.backfill_batches',
      'pipeline.backfill_members',
    ]),
    Object.fromEntries(
      Object.entries(before.pipeline).filter(([k]) =>
        k.startsWith('pipeline.backfill_'),
      ),
    ),
  );
  const status = data(await api.request(`/api/v1/backfills/${degraded}`));
  assert.equal(status['phase'], 'complete_with_errors');
  assert.equal(
    string(record(status['counts'])['required']),
    string(
      record((await scanner.status(degraded)).evidence['counts'])['required'],
    ),
  );
  const runtime = new (await import('pg')).default.Client({
    ...apiConfig(password).pipeline,
  });
  await runtime.connect();
  t.after(() => runtime.end());
  for (const sql of [
    "UPDATE pipeline.es_dead_letters SET context='changed'",
    'DELETE FROM pipeline.replay_requests',
    "UPDATE pipeline.delivery_intents SET state='pending'",
    'SET ROLE pipeline_owner',
  ])
    await assert.rejects(runtime.query<Record<string, unknown>>(sql), {
      code: '42501',
    });
  evidence('OP17', {
    healthy,
    degraded,
    badEvent: bad.eventId,
    oldFailure: history,
    newHistory: afterHistory,
    status,
    retainedBefore: before,
  });
});

const recoveryName = (id: string) => {
  const e = recoveryCases.find((c) => c.id === id);
  assert.ok(e);
  return e.name;
};
await test(recoveryName('RC09'), async () => {
  assert.ok(completedFixture);
  const db = await connections();
  try {
    const tables = [
      'pipeline.events',
      'pipeline.es_dead_letters',
      'pipeline.replay_requests',
      'pipeline.replay_items',
      'pipeline.operator_receipts',
      'pipeline.es_target',
      'pipeline.rabbit_target',
      'pipeline.backfill_runs',
      'pipeline.backfill_members',
      'pipeline.consumer_observations',
    ];
    const before = await retained(db.p, tables);
    const deliveries = async () =>
      (
        await db.p.query<{ value: string }>(
          "SELECT (to_jsonb(d)-'replay_request_id')::text value FROM pipeline.delivery_intents d ORDER BY event_id,kind",
        )
      ).rows;
    const oldDeliveries = await deliveries();
    await db.p.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    try {
      await db.p.query(
        await readFile(
          new URL(
            '../../migrations/pipeline/008-recovery-controls.sql',
            import.meta.url,
          ),
          'utf8',
        ),
      );
      assert.equal((await db.p.query('COMMIT')).command, 'COMMIT');
    } catch (error) {
      await db.p.query('ROLLBACK');
      throw error;
    }
    assert.deepEqual(await retained(db.p, tables), before);
    assert.deepEqual(await deliveries(), oldDeliveries);
    assert.equal(
      (
        await db.p.query<{ n: string }>(
          'SELECT count(*)::text n FROM pipeline.delivery_intents WHERE replay_request_id IS NOT NULL',
        )
      ).rows[0]?.n,
      '0',
    );
    const overview = await new RecoveryService(
      completedFixture.config,
    ).overview();
    evidence('RC09', {
      preservedTables: tables,
      deliveries: oldDeliveries,
      overview,
      addition: 'nullable replay correlation only; historical rows retained',
    });
  } finally {
    await db.close();
  }
});
await test(recoveryName('RC05'), async () => {
  assert.ok(completedFixture);
  const db = await connections();
  try {
    const status = await new RecoveryService(completedFixture.config).run(
      completedFixture.run,
    );
    assert.equal(record(status['historical'])['phase'], 'complete_with_errors');
    assert.equal(record(status['current_recovery'])['state'], 'satisfied');
    assert.ok(
      BigInt(string(record(completedFixture.oldCounts)['es_errors'])) > 0n,
    );
    const actual = (
      await db.p.query<{ value: string }>(
        'SELECT row_to_json(r)::text value FROM pipeline.backfill_runs r WHERE run_id=$1',
        [completedFixture.run],
      )
    ).rows[0]?.value;
    assert.equal(actual, completedFixture.oldHistorical);
    assert.equal(
      record(record(status['current_recovery'])['counts'])['required'],
      record(completedFixture.oldCounts)['required'],
    );
    let missingEvidence: unknown;
    await db.p.query('BEGIN');
    try {
      // Rollback-only privileged negative fixture: runtime cannot remove immutable membership.
      await db.p.query(
        'ALTER TABLE pipeline.backfill_members DISABLE TRIGGER USER',
      );
      await db.p.query(
        'DELETE FROM pipeline.backfill_members WHERE run_id=$1 AND event_id=(SELECT min(event_id) FROM pipeline.backfill_members WHERE run_id=$1)',
        [completedFixture.run],
      );
      missingEvidence = (
        await db.p.query<{ value: unknown }>(
          'SELECT pipeline.recovery_run($1) value',
          [completedFixture.run],
        )
      ).rows[0]?.value;
      assert.equal(
        record(record(missingEvidence)['current_recovery'])['state'],
        'integrity_blocked',
      );
    } finally {
      await db.p.query('ROLLBACK');
    }
    assert.equal(
      record(
        (
          await new RecoveryService(completedFixture.config).run(
            completedFixture.run,
          )
        )['current_recovery'],
      )['state'],
      'satisfied',
    );
    evidence('RC05', {
      historicalBefore: completedFixture.oldHistorical,
      countsAtFailure: completedFixture.oldCounts,
      current: status,
      historyUnchanged: true,
      missingEvidence,
    });
  } finally {
    await db.close();
  }
});
