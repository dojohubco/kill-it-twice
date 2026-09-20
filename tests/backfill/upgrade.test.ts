import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { backfillUpgradeCases } from '../../scripts/required-backfill-cases.ts';
import {
  migrateBackfillSource,
  migrateBackfillPipeline,
} from '../../scripts/migrate-backfill.ts';
import { setup } from '../support/rabbit.ts';
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
  assert.deepEqual(await all(), before);
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
    after: before,
    preserved: true,
    identity,
    definitions: defs,
    run: await snapshot(p, id),
    receiverAndDatabaseRestart: 'required by enclosing retained-volume harness',
  });
});
