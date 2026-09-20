import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { backfillUpgradeCases } from '../../scripts/required-backfill-cases.ts';
import {
  migrateBackfillSource,
  migrateBackfillPipeline,
} from '../../scripts/migrate-backfill.ts';
import { setup, rabbitLedger, metadata, topology } from '../support/rabbit.ts';
import { docker } from '../support/rabbit-network.ts';
import { esConfig, ledger, remote } from '../support/es.ts';
import { EsTransport, object } from '../../src/es/transport.ts';
import { EsAdapter } from '../../src/es/adapter.ts';
import { validateTopology } from '../../src/rabbitmq/metadata.ts';
import { waitFor } from '../../scripts/support.ts';
import {
  bootstrap,
  recipe,
  pipelineConfig,
  seedSnapshot,
} from '../support/bootstrap.ts';
import {
  sourceSnapshot,
  pipelineSnapshot,
} from '../../scripts/migrate-staging.ts';
import { captureSnapshot } from '../support/capture-upgrade.ts';
import { esSnapshot } from '../../scripts/es-setup.ts';
import {
  rabbitSnapshot,
  consumerSnapshot,
} from '../../scripts/rabbit-setup.ts';
import { required, evidence } from '../support/db.ts';
import {
  declareMutation,
  deliver,
  scan,
  scanTo,
  backfillLedger,
  snapshot,
} from '../support/backfill.ts';
import { backfillConfig } from '../support/backfill-process.ts';
import { BackfillSource } from '../../src/backfill/source.ts';
import { config } from '../support/staging.ts';
import { SourceReader } from '../../src/source-reader.ts';
import { Pipeline } from '../../src/pipeline.ts';
import pg from 'pg';
const entry = backfillUpgradeCases[0];
assert.ok(entry);
await test(entry.name, async (t) => {
  const { s, p, c } = await setup(t),
    b = bootstrap(),
    r = { ...recipe, seed: 'M5B-upgrade', count: '17', chunkSize: 8 };
  await b.begin(r);
  for (const first of ['1', '9', '17']) await b.chunk(r.epoch, r.key, first);
  await b.seal(r.epoch, r.key);
  await b.activate(r.epoch, r.key, required('PIPELINE_ID'), pipelineConfig());
  const ids = (
    await s.query<{ id: string }>(
      'SELECT entity_id::text id FROM source.entities ORDER BY entity_id LIMIT 3',
    )
  ).rows.map((r) => r.id);
  const selected = await new SourceReader(
    config('reader', 'upgrade-baselines'),
    r.epoch,
  ).baseline(ids.map((entityId) => ({ entityId, version: '1' })));
  await new Pipeline(pipelineConfig(), required('PIPELINE_ID')).stage(
    selected.events,
  );
  await declareMutation(s, ids[0] ?? null);
  await deliver(p, c);
  const all = async () => ({
    source: await sourceSnapshot(s),
    capture: await captureSnapshot(s),
    bootstrap: await seedSnapshot(s),
    pipeline: await pipelineSnapshot(p),
    es: await esSnapshot(p),
    rabbit: await rabbitSnapshot(p),
    consumer: await consumerSnapshot(c),
  });
  const before = await all();
  await migrateBackfillSource(s, required('SOURCE_BACKFILL_PASSWORD'));
  await migrateBackfillPipeline(p, required('PIPELINE_BACKFILL_PASSWORD'));
  const after = await all();
  assert.deepEqual(after, before);
  const cfg = backfillConfig('permissions'),
    runtime = new pg.Client(cfg.source),
    pruntime = new pg.Client(cfg.pipeline);
  await runtime.connect();
  await pruntime.connect();
  t.after(() => runtime.end());
  t.after(() => pruntime.end());
  for (const sql of [
    'UPDATE source.capture_binding SET pipeline_id=gen_random_uuid()',
    'DELETE FROM source.outbox',
    'SET ROLE source_owner',
    "SELECT source.create_entity('{}')",
    'ALTER FUNCTION source.seal_backfill_fence(uuid,uuid,uuid) RENAME TO forbidden',
  ])
    await assert.rejects(runtime.query(sql), { code: '42501' });
  for (const sql of [
    'UPDATE pipeline.backfill_ranges SET checkpoint=upper_key',
    'DELETE FROM pipeline.events',
    'INSERT INTO pipeline.backfill_members SELECT * FROM pipeline.backfill_members',
    'SET ROLE pipeline_owner',
    'ALTER FUNCTION pipeline.backfill_page(uuid,integer,uuid,bigint,uuid,bigint,bigint,boolean,jsonb,jsonb) RENAME TO forbidden',
  ])
    await assert.rejects(pruntime.query(sql), { code: '42501' });
  const id = randomUUID();
  await scan().start(id);
  await scanTo('draining', id);
  await deliver(p, c);
  await backfillLedger().advance(id);
  assert.equal((await scan().status(id)).phase, 'complete');
  const es = new EsTransport(esConfig()),
    admin = new EsTransport(esConfig(true));
  t.after(() => es.close());
  t.after(() => admin.close());
  const esTarget = await ledger().target(),
    rabbitTarget = await rabbitLedger().target(),
    progressBefore = await snapshot(p, id),
    stateBefore = await all();
  const documents = async () => {
    const rows = (
      await s.query<{ id: string }>(
        'SELECT entity_id::text id FROM source.entities ORDER BY entity_id',
      )
    ).rows;
    const result = [];
    for (const row of rows)
      result.push(await remote(es, `${r.epoch}:${row.id}`));
    return result;
  };
  const documentsBefore = await documents();
  const esContainer = `${required('ES_PROJECT')}-elasticsearch-1`,
    rabbitContainer = `${required('RABBIT_PROJECT')}-rabbitmq-1`;
  await docker(['restart', '--time', '10', esContainer]);
  await waitFor(
    async () => {
      try {
        await new EsAdapter(es).validate(esTarget);
        await admin.request('GET', '/_security/_authenticate');
        assert.equal(
          String(
            object(
              object(await admin.request('GET', '/.security/_count'))[
                '_shards'
              ],
            )['failed'],
          ),
          '0',
        );
        return true;
      } catch {
        return false;
      }
    },
    Boolean,
    'Retained backfill Elasticsearch ready',
    90000,
    async () => {
      await es.close();
      await admin.close();
    },
  );
  await docker(['restart', '--time', '10', rabbitContainer]);
  await waitFor(
    async (signal) => {
      try {
        await validateTopology(metadata(), topology(), signal);
        return true;
      } catch {
        return false;
      }
    },
    Boolean,
    'Retained backfill RabbitMQ ready',
    30000,
  );
  assert.deepEqual(await ledger().target(), esTarget);
  assert.deepEqual(await rabbitLedger().target(), rabbitTarget);
  assert.deepEqual(await documents(), documentsBefore);
  assert.deepEqual(await snapshot(p, id), progressBefore);
  assert.deepEqual(await all(), stateBefore);
  const defs = (
    await s.query<{
      name: string;
      owner: string;
      definer: boolean;
      config: string[];
      public_execute: boolean;
    }>(
      "SELECT proname name,pg_get_userbyid(proowner) owner,prosecdef definer,proconfig config,EXISTS(SELECT FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') public_execute FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='source' AND proname IN ('backfill_identity','backfill_page','seal_backfill_fence')",
    )
  ).rows;
  assert.equal(defs.length, 3);
  for (const d of defs) {
    assert.equal(d.owner, 'source_owner');
    assert.equal(d.definer, true);
    assert.ok(d.config.includes('search_path=pg_catalog, pg_temp'));
    assert.equal(d.public_execute, false);
  }
  const identity = await new BackfillSource(cfg.source, cfg.binding).identity();
  evidence('BF16', {
    before,
    after,
    preserved: true,
    identity,
    definitions: defs,
    run: await snapshot(p, id),
    receiverRestart: {
      esContainer,
      rabbitContainer,
      esTarget,
      rabbitTarget,
      documentsBefore,
      unchanged: true,
    },
    databaseRestart:
      'independently required by enclosing retained-volume harness',
  });
});
