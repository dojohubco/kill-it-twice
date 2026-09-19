import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { bootstrapUpgradeCases } from '../../scripts/required-bootstrap-cases.ts';
import { migrateBootstrap } from '../../scripts/migrate-bootstrap.ts';
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
import { setup, drain } from '../support/rabbit.ts';
import { execute, request } from '../support/commands.ts';
import { required, evidence } from '../support/db.ts';
import { bootstrap, code, pipelineConfig } from '../support/bootstrap.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { esConfig, finish } from '../support/es.ts';
const entry = bootstrapUpgradeCases[0];
assert.ok(entry);
await test(entry.name, async (t) => {
  const { s, p, c } = await setup(t);
  await drain(p, c);
  const es = new EsTransport(esConfig());
  t.after(() => es.close());
  await finish(es, p);
  const snapshot = async () => ({
    source: await sourceSnapshot(s),
    capture: await captureSnapshot(s),
    pipeline: await pipelineSnapshot(p),
    es: await esSnapshot(p),
    rabbit: await rabbitSnapshot(p),
    consumer: await consumerSnapshot(c),
  });
  const before = await snapshot();
  assert.ok(
    (await s.query<Record<string, unknown>>('SELECT * FROM source.entities'))
      .rowCount,
  );
  await migrateBootstrap(s, required('SOURCE_BOOTSTRAP_PASSWORD'));
  const after = await snapshot();
  assert.deepEqual(after, before);
  const state = await bootstrap().status(required('SOURCE_EPOCH'));
  assert.equal(state.phase, 'active');
  assert.equal(state.bootstrap_key, null);
  assert.equal(
    (
      await s.query<Record<string, unknown>>(
        'SELECT * FROM source.baseline_revisions',
      )
    ).rowCount,
    0,
  );
  await assert.rejects(
    bootstrap().begin({
      epoch: required('SOURCE_EPOCH'),
      key: randomUUID(),
      version: 1,
      seed: 'forbidden',
      count: '1',
      chunkSize: 1,
    }),
    code('P7002'),
  );
  const entity = (
    await s.query<{ id: string; payload: string }>(
      'SELECT entity_id::text id,payload::text payload FROM source.entities WHERE NOT is_deleted ORDER BY entity_id LIMIT 1',
    )
  ).rows[0];
  assert.ok(entity);
  const command = request(
    required('SOURCE_EPOCH'),
    'update',
    entity.id,
    entity.payload,
  );
  const reply = await execute(command);
  assert.ok(reply.result.change_id);
  assert.deepEqual((await execute(command)).result, reply.result);
  await assert.rejects(
    bootstrap().activate(
      required('SOURCE_EPOCH'),
      randomUUID(),
      required('PIPELINE_ID'),
      pipelineConfig(),
    ),
    code('P7002'),
  );
  evidence('BS13', {
    before,
    after,
    state,
    ordinaryNoop: reply,
    baselineCount: 0,
  });
});
