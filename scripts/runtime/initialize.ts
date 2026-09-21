import assert from 'node:assert/strict';
import { readFile, writeFile, chown } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import type pg from 'pg';
import { EsTransport, object } from '../../src/es/transport.ts';
import {
  BrokerMetadata,
  validateTopology,
} from '../../src/rabbitmq/metadata.ts';
import { uuid } from '../../src/envelope.ts';
import { registerEs } from '../es-setup.ts';
import { prepareRabbit, setupTopology, bindRabbit } from '../rabbit-setup.ts';
import {
  prepare,
  applyFile,
  applyText,
  sourceFiles,
  pipelineFiles,
  consumerFiles,
} from './migrations.ts';
import {
  password,
  database,
  adminConfig,
  atomic,
  existing,
  safeFailure,
} from './private.ts';
import { configs, type BrokerSecrets, type Installation } from './configs.ts';
import { withCleanup } from '../support.ts';
let phase = 'preconditions';
const progress = (name: string) => {
  phase = name;
  console.log(JSON.stringify({ type: 'initialization', phase }));
};
async function roleSetup(c: pg.Client, roles: string[]) {
  const statements = await Promise.all(
    roles.map(
      async (r) => `ALTER ROLE ${r} LOGIN PASSWORD '${await password(r)}';`,
    ),
  );
  await applyText(c, 'runtime-credentials-v1', statements.join('\n'));
  const rows = (
    await c.query<{ rolname: string; safe: boolean }>(
      'SELECT rolname,rolcanlogin AND NOT(rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls) AS safe FROM pg_roles WHERE rolname=ANY($1)',
      [roles],
    )
  ).rows;
  assert.equal(rows.length, roles.length);
  assert.ok(rows.every((r) => r.safe));
}
async function ready(work: () => Promise<unknown>) {
  const until = performance.now() + 120000;
  for (;;) {
    try {
      await work();
      return;
    } catch {
      if (performance.now() >= until)
        throw new Error(`Readiness deadline: ${phase}`);
      await delay(500);
    }
  }
}
try {
  const retained = await existing('/private/initialized.json');
  const installationId = uuid(
    (await readFile('/private/installation-id', 'utf8')).trim(),
  );
  progress('source-migrations');
  const sourceEpoch = await database(await adminConfig('source'), async (c) => {
    if (retained)
      assert.ok(
        (
          await c.query<{ t: string | null }>(
            "SELECT to_regclass('source.source_identity') AS t",
          )
        ).rows[0]?.t,
        'Retained source disappeared',
      );
    await prepare(c, 'source');
    for (const file of sourceFiles) await applyFile(c, file);
    await roleSetup(c, [
      'source_command',
      'source_reader',
      'source_capture',
      'source_bootstrap',
      'source_backfill',
      'source_operator',
    ]);
    const value = uuid(
      (
        await c.query<{ source_epoch: string }>(
          'SELECT source_epoch FROM source.source_identity',
        )
      ).rows[0]?.source_epoch,
    );
    if (retained)
      assert.equal(
        retained['sourceEpoch'],
        value,
        'Retained source identity changed',
      );
    return value;
  });
  progress('pipeline-migrations');
  await database(await adminConfig('pipeline'), async (p) => {
    if (retained)
      assert.ok(
        (
          await p.query<{ t: string | null }>(
            "SELECT to_regclass('pipeline.source_binding') AS t",
          )
        ).rows[0]?.t,
        'Retained pipeline disappeared',
      );
    await prepare(p, 'pipeline');
    for (const file of pipelineFiles)
      await applyFile(p, file, async () => {
        if (file === 'pipeline/001-staging.sql')
          await p.query(
            'INSERT INTO pipeline.source_binding VALUES(true,$1,$2)',
            [sourceEpoch, 'pg18-jsonb-text/v1'],
          );
      });
    await roleSetup(p, [
      'pipeline_capture',
      'pipeline_es',
      'pipeline_rabbit',
      'pipeline_receipts',
      'pipeline_backfill',
      'pipeline_operator',
    ]);
    const binding = (
      await p.query<{ pipeline_id: string; source_epoch: string }>(
        'SELECT * FROM pipeline.source_binding',
      )
    ).rows[0];
    assert.ok(binding);
    assert.equal(binding.source_epoch, sourceEpoch);
    const pipelineId = uuid(binding.pipeline_id);
    if (retained)
      assert.equal(
        retained['pipelineId'],
        pipelineId,
        'Retained pipeline identity changed',
      );
    progress('receiver-readiness');
    const ca = await readFile('/es/server.crt', 'utf8');
    const setup = new EsTransport({
      node: 'https://elasticsearch:9200',
      username: 'kit_setup',
      password: await password('es_setup'),
      ca,
    });
    const broker = new BrokerMetadata({
      url: 'https://rabbitmq:15671',
      username: 'm4_setup',
      password: await password('rabbit_setup'),
      ca,
    });
    await withCleanup(
      async () => {
        await ready(async () => {
          assert.notEqual(
            object(await setup.request('GET', '/'))['cluster_uuid'],
            '_na_',
          );
        });
        await ready(() => broker.request('GET', '/api/overview'));
        progress('elasticsearch-registration');
        await registerEs(
          p,
          setup,
          pipelineId,
          sourceEpoch,
          async (_username, _generatedPassword, index) => {
            assert.match(index, /^kit-[a-f0-9-]+$/);
            const roles = `kit_runtime:\n  cluster: ['cluster:monitor/main']\n  indices:\n    - names: ['${index}']\n      privileges: ['indices:data/write/index', 'indices:data/write/bulk', 'read', 'view_index_metadata']\n`;
            if (retained)
              assert.equal(await readFile('/es/roles.yml', 'utf8'), roles);
            else {
              await writeFile('/es/roles.yml', roles, { mode: 0o600 });
              await chown('/es/roles.yml', 1000, 0);
            }
            const client = new EsTransport({
              node: 'https://elasticsearch:9200',
              username: 'kit_runtime',
              password: await password('es_runtime'),
              ca,
            });
            await withCleanup(
              () => ready(() => client.request('GET', `/${index}`)),
              () => client.close(),
            );
          },
        );
        progress('rabbit-registration');
        const topology = await prepareRabbit(p, pipelineId, sourceEpoch);
        const alreadyBound =
          (
            await p.query<{ ready: boolean }>(
              'SELECT registered_at IS NOT NULL AS ready FROM pipeline.rabbit_target',
            )
          ).rows[0]?.ready === true;
        const stored = await existing('/private/rabbit-credentials.json');
        let secrets: BrokerSecrets;
        if (stored) {
          for (const key of ['publisher', 'consumer', 'observer'])
            assert.match(String(stored[key]), /^[a-f0-9]{48}$/);
          secrets = {
            publisher: String(stored['publisher']),
            consumer: String(stored['consumer']),
            observer: String(stored['observer']),
          };
          await validateTopology(broker, topology);
        } else {
          assert.ok(
            !retained && !alreadyBound,
            'Missing retained broker credentials; refusing rotation',
          );
          secrets = await setupTopology(broker, topology, false, {
            host: 'rabbitmq',
            port: 5671,
            username: 'm4_setup',
            password: await password('rabbit_setup'),
            ca,
            vhost: topology.vhost,
          });
          await atomic('/private/rabbit-credentials.json', secrets, 0);
        }
        progress('consumer-migrations');
        const consumerExists =
          (
            await p.query(
              "SELECT 1 FROM pg_database WHERE datname='consumer_m4'",
            )
          ).rows.length !== 0;
        if (!consumerExists) {
          assert.ok(!retained, 'Retained consumer database disappeared');
          await p.query('CREATE DATABASE consumer_m4');
        }
        await database(await adminConfig('consumer'), async (c) => {
          if (retained)
            assert.ok(
              (
                await c.query<{ t: string | null }>(
                  "SELECT to_regclass('consumer.identity') AS t",
                )
              ).rows[0]?.t,
            );
          await prepare(c, 'consumer');
          for (const file of consumerFiles)
            await applyFile(c, file, async () => {
              if (file === 'consumer/001-consumer.sql')
                await c.query(
                  'INSERT INTO consumer.identity VALUES(true,$1,$2,$3,$4,$5)',
                  [
                    topology.consumerId,
                    sourceEpoch,
                    pipelineId,
                    topology.registrationId,
                    'pg18-jsonb-text/v1',
                  ],
                );
            });
          await roleSetup(c, [
            'consumer_runtime',
            'consumer_receipt_reader',
            'consumer_operator',
          ]);
          const row = (
            await c.query<{
              consumer_id: string;
              source_epoch: string;
              pipeline_id: string;
              registration_id: string;
            }>(
              'SELECT consumer_id,source_epoch,pipeline_id,registration_id FROM consumer.identity',
            )
          ).rows[0];
          assert.deepEqual(row, {
            consumer_id: topology.consumerId,
            source_epoch: sourceEpoch,
            pipeline_id: pipelineId,
            registration_id: topology.registrationId,
          });
        });
        await bindRabbit(p, topology);
        progress('restricted-configurations');
        const installation: Installation = {
          installationId,
          sourceEpoch,
          pipelineId,
          topology,
        };
        if (retained)
          assert.deepEqual(
            retained,
            installation,
            'Retained installation cannot be rebound',
          );
        await configs(installation, secrets);
        if (!retained)
          await atomic('/private/initialized.json', installation, 0);
        await atomic('/ready/initialized.json', { sourceEpoch, pipelineId });
        progress('ready-for-explicit-seed');
      },
      () => setup.close(),
    );
  });
} catch (error) {
  console.error(JSON.stringify({ ...safeFailure(error), phase }));
  process.exitCode = 1;
}
