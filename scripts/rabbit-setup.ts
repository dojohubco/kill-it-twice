// Controlled setup and additive migrations; runtime receives none of these capabilities.
import assert from 'node:assert/strict';
import { randomUUID, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { AmqpSession, type AmqpConnection } from '../src/rabbitmq/session.ts';
import { withCleanup } from './support.ts';
import {
  BrokerMetadata,
  queueArguments,
  validateTopology,
  field,
  type Topology,
} from '../src/rabbitmq/metadata.ts';
import type { ConnectionConfig } from '../src/internal/transaction.ts';
export async function migrateRabbit(
  p: pg.Client,
  publisherPassword: string,
  observerPassword: string,
) {
  for (const x of [publisherPassword, observerPassword])
    assert.match(x, /^[a-f0-9]{48}$/);
  await p.query('BEGIN');
  try {
    await p.query(
      await readFile('migrations/pipeline/004-rabbitmq.sql', 'utf8'),
    );
    await p.query(
      `ALTER ROLE pipeline_rabbit LOGIN PASSWORD '${publisherPassword}'; ALTER ROLE pipeline_receipts LOGIN PASSWORD '${observerPassword}'`,
    );
    assert.equal((await p.query('COMMIT')).command, 'COMMIT');
  } catch (error) {
    return withCleanup(
      () =>
        Promise.reject(
          error instanceof Error
            ? error
            : new Error('Broker migration failed', { cause: error }),
        ),
      async () => {
        await p.query('ROLLBACK');
      },
    );
  }
}
export async function prepareRabbit(
  p: pg.Client,
  pipelineId: string,
  epoch: string,
): Promise<Topology> {
  const prior = (
    await p.query<Record<string, unknown>>(
      'SELECT * FROM pipeline.rabbit_target',
    )
  ).rows;
  if (prior.length) {
    const r = prior[0];
    assert.ok(r && prior.length === 1);
    assert.equal(r['pipeline_id'], pipelineId);
    assert.equal(r['source_epoch'], epoch);
    return {
      registrationId: field(r, 'registration_id'),
      consumerId: field(r, 'consumer_id'),
      pipelineId,
      epoch,
      vhost: field(r, 'vhost'),
      exchange: field(r, 'exchange'),
      queue: field(r, 'queue'),
      routingKey: field(r, 'routing_key'),
    };
  }
  const registrationId = randomUUID(),
    consumerId = randomUUID(),
    vhost = `kit-${registrationId}`;
  const t = {
    registrationId,
    consumerId,
    pipelineId,
    epoch,
    vhost,
    exchange: `${vhost}-events`,
    queue: `${vhost}-consumer`,
    routingKey: 'revision-v1',
  };
  const configuration = {
    ...t,
    queueArguments,
    maxMessageBytes: 131072,
    heartbeat: 15,
    wire: 'revision-v1',
  };
  await p.query(
    `INSERT INTO pipeline.rabbit_target(destination_id,generation,registration_id,consumer_id,pipeline_id,source_epoch,vhost,exchange,queue,routing_key,configuration,configuration_sha256) SELECT destination_id,generation,$1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,encode(sha256(convert_to(($9::jsonb)::text,'UTF8')),'hex') FROM pipeline.destinations WHERE kind='rabbitmq'`,
    [
      registrationId,
      consumerId,
      pipelineId,
      epoch,
      t.vhost,
      t.exchange,
      t.queue,
      t.routingKey,
      JSON.stringify(configuration),
    ],
  );
  return t;
}
export async function setupTopology(
  api: BrokerMetadata,
  t: Topology,
  previouslyBound: boolean,
  connection: AmqpConnection,
) {
  const v = encodeURIComponent(t.vhost);
  let exists = true;
  try {
    await api.request('GET', `/api/vhosts/${v}`);
  } catch (error) {
    if (error instanceof Error && error.message === 'Broker metadata HTTP 404')
      exists = false;
    else throw error;
  }
  if (!exists) {
    if (previouslyBound)
      throw new Error('Registered vhost disappeared; setup refuses recreation');
    await api.request('PUT', `/api/vhosts/${v}`, {});
  }
  // Setup has no runtime credentials. New vhosts grant its administrator deliberate access.
  await api.request('PUT', `/api/permissions/${v}/m4_setup`, {
    configure: '.*',
    write: '.*',
    read: '.*',
  });
  for (const [kind, name, definition] of [
    [
      'exchanges',
      t.exchange,
      {
        type: 'direct',
        durable: true,
        auto_delete: false,
        internal: false,
        arguments: {},
      },
    ],
    [
      'queues',
      t.queue,
      { durable: true, auto_delete: false, arguments: queueArguments },
    ],
  ] as const) {
    let found = true;
    try {
      await api.request('GET', `/api/${kind}/${v}/${name}`);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message === 'Broker metadata HTTP 404'
      )
        found = false;
      else throw error;
    }
    if (!found) {
      if (previouslyBound)
        throw new Error('Registered topology disappeared; no recreation');
      await api.request('PUT', `/api/${kind}/${v}/${name}`, definition);
    }
  }
  if (!previouslyBound)
    await api.request(
      'POST',
      `/api/bindings/${v}/e/${t.exchange}/q/${t.queue}`,
      { routing_key: t.routingKey, arguments: {} },
    );
  await validateTopology(api, t);
  if (!previouslyBound) {
    const session = new AmqpSession();
    await withCleanup(
      async () => {
        await session.open(connection);
        const channel = await session.channel(false);
        const queue = await channel.checkQueue(t.queue);
        assert.equal(
          queue.messageCount,
          0,
          'Unbound setup contains unapproved workload',
        );
        assert.equal(
          queue.consumerCount,
          0,
          'Unbound setup has an unexpected consumer',
        );
      },
      () => session.close(),
    );
  }
  const credentials: { publisher: string; consumer: string; observer: string } =
    {
      publisher: randomBytes(24).toString('hex'),
      consumer: randomBytes(24).toString('hex'),
      observer: randomBytes(24).toString('hex'),
    };
  for (const [name, write, read, tags] of [
    ['publisher', `^${t.exchange}$`, '^$', ''],
    ['consumer', '^$', `^${t.queue}$`, ''],
    ['observer', '^$', '^$', 'management'],
  ] as const) {
    const username = `${name}-${t.registrationId}`;
    await api.request('PUT', `/api/users/${username}`, {
      password: credentials[name],
      tags,
    });
    await api.request('PUT', `/api/permissions/${v}/${username}`, {
      configure: '^$',
      write,
      read,
    });
  }
  return credentials;
}
export async function initializeConsumer(
  p: pg.Client,
  config: ConnectionConfig,
  t: Topology,
  runtimePassword: string,
  readerPassword: string,
) {
  for (const x of [runtimePassword, readerPassword])
    assert.match(x, /^[a-f0-9]{48}$/);
  const exists =
    (await p.query("SELECT 1 FROM pg_database WHERE datname='consumer_m4'"))
      .rows.length > 0;
  if (!exists) await p.query('CREATE DATABASE consumer_m4');
  const c = new pg.Client({
    ...config,
    database: 'consumer_m4',
    connectionTimeoutMillis: 5000,
    query_timeout: 15000,
  });
  await withCleanup(
    async () => {
      await c.connect();
      const schema = (
        await c.query(
          "SELECT to_regclass('consumer.identity') IS NOT NULL AS exists",
        )
      ).rows as { exists: boolean }[];
      if (schema[0]?.exists) {
        const rows = (
          await c.query<Record<string, unknown>>(
            'SELECT consumer_id::text,source_epoch::text,pipeline_id::text,registration_id::text FROM consumer.identity',
          )
        ).rows;
        assert.deepEqual(rows, [
          {
            consumer_id: t.consumerId,
            source_epoch: t.epoch,
            pipeline_id: t.pipelineId,
            registration_id: t.registrationId,
          },
        ]);
        return;
      }
      await c.query('BEGIN');
      try {
        await c.query(
          await readFile('migrations/consumer/001-consumer.sql', 'utf8'),
        );
        await c.query(
          'INSERT INTO consumer.identity VALUES(true,$1,$2,$3,$4,$5)',
          [
            t.consumerId,
            t.epoch,
            t.pipelineId,
            t.registrationId,
            'pg18-jsonb-text/v1',
          ],
        );
        await c.query(
          `ALTER ROLE consumer_runtime LOGIN PASSWORD '${runtimePassword}'; ALTER ROLE consumer_receipt_reader LOGIN PASSWORD '${readerPassword}'`,
        );
        assert.equal((await c.query('COMMIT')).command, 'COMMIT');
      } catch (error) {
        return withCleanup(
          () =>
            Promise.reject(
              error instanceof Error
                ? error
                : new Error('Consumer migration failed', { cause: error }),
            ),
          async () => {
            await c.query('ROLLBACK');
          },
        );
      }
    },
    () => c.end(),
  );
}
export async function bindRabbit(p: pg.Client, t: Topology) {
  await p.query('BEGIN');
  try {
    await p.query(
      "UPDATE pipeline.rabbit_target SET registered_at=clock_timestamp(),mode='ready' WHERE registration_id=$1 AND registered_at IS NULL",
      [t.registrationId],
    );
    await p.query(
      "UPDATE pipeline.destinations SET state='bound',receiver_identity=$1 WHERE kind='rabbitmq' AND state='unbound'",
      [t.registrationId],
    );
    assert.equal((await p.query('COMMIT')).command, 'COMMIT');
  } catch (error) {
    return withCleanup(
      () =>
        Promise.reject(
          error instanceof Error
            ? error
            : new Error('Rabbit registration failed', { cause: error }),
        ),
      async () => {
        await p.query('ROLLBACK');
      },
    );
  }
}
export async function rabbitSnapshot(p: pg.Client) {
  const result: Record<string, string[]> = {};
  for (const table of [
    'rabbit_target',
    'rabbit_attempts',
    'delivery_intents',
    'consumer_observations',
  ])
    result[table] = (
      await p.query<{ text: string }>(
        `SELECT row_to_json(t)::text text FROM pipeline.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.text);
  return result;
}
export async function consumerSnapshot(c: pg.Client) {
  const result: Record<string, string[]> = {};
  for (const table of [
    'identity',
    'processed_events',
    'entity_projection',
    'mutation_effects',
    'entity_totals',
    'quarantine',
  ])
    result[table] = (
      await c.query<{ text: string }>(
        `SELECT row_to_json(t)::text text FROM consumer.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.text);
  return result;
}
