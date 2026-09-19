import assert from 'node:assert/strict';
import { randomUUID, randomBytes } from 'node:crypto';
import type pg from 'pg';
import { readFile } from 'node:fs/promises';
import { EsTransport, object } from '../src/es/transport.ts';
import { configuration, exactJson } from '../src/es/projection.ts';
import { first } from './rows.ts';
import { withCleanup } from './support.ts';
export async function migrateEs(p: pg.Client, password: string) {
  assert.match(password, /^[a-f0-9]{48}$/);
  await p.query('BEGIN');
  try {
    await p.query(
      await readFile(
        new URL(
          '../migrations/pipeline/003-elasticsearch.sql',
          import.meta.url,
        ),
        'utf8',
      ),
    );
    await p.query(`ALTER ROLE pipeline_es LOGIN PASSWORD '${password}'`);
    assert.equal((await p.query('COMMIT')).command, 'COMMIT');
  } catch (primary) {
    return withCleanup(
      () =>
        Promise.reject(
          primary instanceof Error
            ? primary
            : new Error('Non-error failure', { cause: primary }),
        ),
      async () => {
        await p.query('ROLLBACK');
      },
    );
  }
}
export async function registerEs(
  p: pg.Client,
  setup: EsTransport,
  pipelineId: string,
  epoch: string,
  provisionRuntime: (
    username: string,
    password: string,
    index: string,
  ) => Promise<void>,
) {
  const info = object(await setup.request('GET', '/'));
  assert.equal(typeof info['cluster_uuid'], 'string');
  assert.notEqual(
    info['cluster_uuid'],
    '_na_',
    'Cannot register unavailable cluster identity',
  );
  const prior = (
    await p.query<{
      index_name: string;
      registration_id: string;
      configuration: unknown;
      cluster_uuid: string;
      index_uuid: string | null;
    }>('SELECT * FROM pipeline.es_target')
  ).rows;
  const registration = prior[0]?.registration_id ?? randomUUID();
  const index =
    prior[0]?.index_name ??
    `kit-${pipelineId}-${randomBytes(6).toString('hex')}`;
  const config = configuration(pipelineId, epoch, registration);
  if (prior.length) {
    assert.equal(prior.length, 1);
    assert.equal(prior[0]?.cluster_uuid, info['cluster_uuid']);
    assert.equal(exactJson(prior[0]?.configuration), exactJson(config));
  } else
    await p.query(
      `INSERT INTO pipeline.es_target(destination_id,generation,registration_id,pipeline_id,source_epoch,index_name,configuration,configuration_sha256,cluster_uuid) SELECT destination_id,generation,$1,$2,$3,$4,$5::jsonb,encode(sha256(convert_to(($5::jsonb)::text,'UTF8')),'hex'),$6 FROM pipeline.destinations WHERE kind='elasticsearch'`,
      [
        registration,
        pipelineId,
        epoch,
        index,
        JSON.stringify(config),
        info['cluster_uuid'],
      ],
    );
  let exists = false;
  try {
    await setup.request('GET', `/${index}`);
    exists = true;
  } catch (error) {
    if (
      !error ||
      typeof error !== 'object' ||
      !('statusCode' in error) ||
      error.statusCode !== 404
    )
      throw error;
  }
  if (!exists) {
    if (prior[0]?.index_uuid)
      throw new Error('Registered receiver missing; cannot recreate');
    await setup.request('PUT', `/${index}`, JSON.stringify(config));
  }
  const receiver = object(
    object(await setup.request('GET', `/${index}`))[index],
  );
  const settings = object(object(receiver['settings'])['index']);
  assert.equal(exactJson(receiver['mappings']), exactJson(config.mappings));
  assert.equal(
    exactJson({
      number_of_shards: settings['number_of_shards'],
      number_of_replicas: settings['number_of_replicas'],
      translog: settings['translog'],
      mapping: settings['mapping'],
    }),
    exactJson(config.settings),
  );
  assert.equal(typeof settings['uuid'], 'string');
  if (prior[0]?.index_uuid) assert.equal(settings['uuid'], prior[0].index_uuid);
  else {
    await p.query('BEGIN');
    try {
      await p.query(
        "UPDATE pipeline.es_target SET index_uuid=$1,registered_at=clock_timestamp(),mode='ready' WHERE index_name=$2 AND registered_at IS NULL",
        [settings['uuid'], index],
      );
      await p.query(
        "UPDATE pipeline.destinations SET state='bound',receiver_identity=$1 WHERE kind='elasticsearch' AND state='unbound'",
        [settings['uuid']],
      );
      assert.equal((await p.query('COMMIT')).command, 'COMMIT');
    } catch (primary) {
      return withCleanup(
        () =>
          Promise.reject(
            primary instanceof Error
              ? primary
              : new Error('Non-error failure', { cause: primary }),
          ),
        async () => {
          await p.query('ROLLBACK');
        },
      );
    }
  }
  const username = `worker-${registration}`,
    password = randomBytes(24).toString('hex');
  await provisionRuntime(username, password, index);
  const destination = first(
    (
      await p.query<{ destination_id: string }>(
        "SELECT destination_id FROM pipeline.destinations WHERE kind='elasticsearch'",
      )
    ).rows,
  );
  return {
    index,
    indexUuid: settings['uuid'],
    clusterUuid: info['cluster_uuid'],
    destinationId: destination.destination_id,
    username,
    password,
  };
}

export async function esSnapshot(p: pg.Client) {
  const rows: Record<string, string[]> = {};
  for (const table of ['es_target', 'es_attempts', 'es_dead_letters'])
    rows[table] = (
      await p.query<{ text: string }>(
        `SELECT row_to_json(t)::text text FROM pipeline.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.text);
  return rows;
}
