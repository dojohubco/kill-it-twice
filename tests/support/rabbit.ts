import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type { TestContext } from 'node:test';
import pg from 'pg';
import { required, connect, evidence } from './db.ts';
import { config, pconnect } from './staging.ts';
import { execute, request, assertReceipt } from './commands.ts';
import { drain as captureDrain } from './capture.ts';
import { RabbitLedger } from '../../src/rabbitmq/ledger.ts';
import { RabbitDelivery } from '../../src/rabbitmq/worker.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { ReceiptObserver } from '../../src/rabbitmq/receipts.ts';
import {
  BrokerMetadata,
  field,
  record,
  type Topology,
} from '../../src/rabbitmq/metadata.ts';
import type { SourceCommand, CommandReply } from '../../src/source.ts';
export function topology(): Topology {
  const raw: unknown = JSON.parse(required('RABBIT_TARGET'));
  const r = record(raw);
  return {
    registrationId: field(r, 'registrationId'),
    consumerId: field(r, 'consumerId'),
    epoch: field(r, 'epoch'),
    pipelineId: field(r, 'pipelineId'),
    vhost: field(r, 'vhost'),
    exchange: field(r, 'exchange'),
    queue: field(r, 'queue'),
    routingKey: field(r, 'routingKey'),
  };
}
export function amqpConfig(
  role: 'publisher' | 'consumer' | 'setup' = 'publisher',
) {
  const t = topology();
  return {
    host: '127.0.0.1',
    port: Number(required('RABBIT_PORT')),
    username: role === 'setup' ? 'm4_setup' : `${role}-${t.registrationId}`,
    password: required(
      role === 'setup'
        ? 'RABBIT_SETUP_PASSWORD'
        : role === 'publisher'
          ? 'RABBIT_PUBLISHER_PASSWORD'
          : 'RABBIT_CONSUMER_PASSWORD',
    ),
    ca: required('RABBIT_CA'),
    vhost: t.vhost,
  };
}
export function metadata(setup = false) {
  return new BrokerMetadata({
    url: required('RABBIT_API'),
    username: setup ? 'm4_setup' : `observer-${topology().registrationId}`,
    password: required(
      setup ? 'RABBIT_SETUP_PASSWORD' : 'RABBIT_OBSERVER_PASSWORD',
    ),
    ca: required('RABBIT_CA'),
  });
}
export function rabbitConfig(
  role: 'publisher' | 'observer' = 'publisher',
  label = 'rabbit',
) {
  return {
    ...config('pipelineAdmin', label),
    user: role === 'publisher' ? 'pipeline_rabbit' : 'pipeline_receipts',
    password: required(
      role === 'publisher'
        ? 'PIPELINE_RABBIT_PASSWORD'
        : 'PIPELINE_RECEIPTS_PASSWORD',
    ),
  };
}
export function consumerConfig(
  role: 'runtime' | 'reader' | 'admin' = 'runtime',
  label = 'consumer',
) {
  return {
    ...config('pipelineAdmin', label),
    database: 'consumer_m4',
    user:
      role === 'runtime'
        ? 'consumer_runtime'
        : role === 'reader'
          ? 'consumer_receipt_reader'
          : 'pipeline_admin',
    password: required(
      role === 'runtime'
        ? 'CONSUMER_PASSWORD'
        : role === 'reader'
          ? 'CONSUMER_READER_PASSWORD'
          : 'PIPELINE_ADMIN_PASSWORD',
    ),
  };
}
export const rabbitLedger = () => new RabbitLedger(rabbitConfig());
export const publisher = () =>
  new RabbitDelivery(rabbitLedger(), new Publisher(amqpConfig(), metadata()));
const consumerDb = () => new ConsumerDatabase(consumerConfig());
export const consumer = () =>
  new Consumer(consumerDb(), amqpConfig('consumer'), metadata(), topology());
export const observer = () =>
  new ReceiptObserver(
    rabbitConfig('observer'),
    new ConsumerDatabase(consumerConfig('reader')),
  );
export async function setup(t: TestContext) {
  const s = await connect('admin', t.name),
    p = await pconnect(t.name),
    c = new pg.Client({
      ...consumerConfig('admin', t.name),
      connectionTimeoutMillis: 5000,
      query_timeout: 15000,
    });
  await c.connect();
  t.after(() => s.end());
  t.after(() => p.end());
  t.after(() => c.end());
  return { s, p, c };
}
interface JournalEntry {
  command: SourceCommand;
  reply: CommandReply;
}
export const journal: JournalEntry[] = [];
export async function mutation(
  s: pg.Client,
  payload = '{"name":"M4 fixture","country":"GE","loyalty_points":7}',
  id: string | null = null,
  operation: SourceCommand['operation'] = 'create',
) {
  const command = request(
    required('SOURCE_EPOCH'),
    operation,
    id,
    operation === 'delete' ? null : payload,
  );
  evidence('M4-command-declaration', command); // Recorded before execution and either sink's result.
  const reply = await execute(command, 'm4-workload');
  await assertReceipt(s, command, reply);
  journal.push({ command, reply });
  const eventId = `${reply.result.source_epoch}:${reply.result.entity_id}:${reply.result.entity_version}`;
  evidence('M4-command-receipt', { command, reply, eventId });
  return { command, reply, eventId, id: reply.result.entity_id };
}
export async function drain(p: pg.Client, c: pg.Client) {
  await captureDrain('m4-capture');
  const pub = publisher(),
    con = consumer(),
    obs = observer();
  const end = performance.now() + 45000;
  const results = [];
  while (performance.now() < end) {
    results.push({
      publish: await pub.once(),
      consume: await con.once(undefined, 100),
      observe: await obs.once(),
    });
    const unresolved = (
      await p.query<{ n: string }>(
        "SELECT count(*)::text n FROM pipeline.delivery_intents WHERE kind='rabbitmq' AND state<>'satisfied' UNION ALL SELECT count(*)::text FROM pipeline.consumer_observations WHERE state<>'processed'",
      )
    ).rows;
    if (unresolved.every((r) => r.n === '0')) {
      evidence('M4-drain', {
        results,
        consumer: (await c.query('SELECT * FROM consumer.status()')).rows,
      });
      return;
    }
    await delay(100);
  }
  throw new Error('Rabbit/consumer drain deadline');
}
// Independent source export and literal flat envelope ordering; no production serializer/effect implementation.
export async function expected(s: pg.Client, id: string, version: string) {
  const rows = (
    await s.query<Record<string, unknown>>(
      `SELECT source_epoch::text,entity_id::text,entity_version::text,change_id::text,is_deleted,payload::text payload_json,to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') recorded_at FROM source.outbox WHERE entity_id=$1 AND entity_version=$2`,
      [id, version],
    )
  ).rows;
  assert.equal(rows.length, 1);
  const r = rows[0];
  assert.ok(r);
  const body = JSON.stringify({
    entity_id: r['entity_id'],
    entity_version: r['entity_version'],
    event_id: `${String(r['source_epoch'])}:${String(r['entity_id'])}:${String(r['entity_version'])}`,
    is_deleted: r['is_deleted'],
    kind: 'mutation',
    payload_encoding: 'pg18-jsonb-text/v1',
    payload_json: r['payload_json'],
    schema_version: 1,
    source_change_id: r['change_id'],
    source_epoch: r['source_epoch'],
    source_recorded_at: r['recorded_at'],
  });
  return {
    body,
    hash: createHash('sha256').update(body).digest('hex'),
    source: r,
  };
}
export async function intent(p: pg.Client, eventId: string) {
  const rows = (
    await p.query<Record<string, unknown>>(
      "SELECT event_id,state,claim_generation::text,owner_id::text,lease_until::text,clock_timestamp()::text observed_at,lease_until<=clock_timestamp() expired,rabbit_attempt_id::text,disposition,settled_at::text FROM pipeline.delivery_intents WHERE kind='rabbitmq' AND event_id=$1",
      [eventId],
    )
  ).rows;
  assert.equal(rows.length, 1);
  return record(rows[0]);
}
export async function effects(c: pg.Client, eventId: string) {
  return (
    await c.query<Record<string, unknown>>(
      "SELECT p.event_id,convert_from(p.body_bytes,'UTF8') body,p.content_sha256,p.entity_version::text,m.source_change_id::text,t.units::text FROM consumer.processed_events p LEFT JOIN consumer.mutation_effects m USING(event_id) JOIN consumer.entity_totals t ON t.source_epoch=p.source_epoch AND t.entity_id=p.entity_id WHERE p.event_id=$1",
      [eventId],
    )
  ).rows;
}
export async function rawPublish(
  wire: Buffer,
  eventId: string,
  overrides: import('amqplib').Options.Publish = {},
) {
  const session = new (
    await import('../../src/rabbitmq/session.ts')
  ).AmqpSession();
  await session.open(amqpConfig());
  try {
    const channel = await session.channel(true);
    const t = topology();
    const { properties } = await import('../../src/rabbitmq/protocol.ts');
    const { randomUUID } = await import('node:crypto');
    await new Promise<void>((resolve, reject) => {
      channel.publish(
        t.exchange,
        t.routingKey,
        wire,
        { ...properties(t, eventId, randomUUID()), ...overrides },
        (error: unknown) =>
          error
            ? reject(
                error instanceof Error
                  ? error
                  : new Error('Raw fixture publication failed'),
              )
            : resolve(),
      );
    });
  } finally {
    await session.close();
  }
}
export async function ledgerWire(p: pg.Client, eventId: string) {
  const rows = (
    await p.query<{ body: Buffer; hash: string }>(
      'SELECT body_bytes body,content_sha256 hash FROM pipeline.events WHERE event_id=$1',
      [eventId],
    )
  ).rows;
  assert.equal(rows.length, 1);
  const r = rows[0];
  assert.ok(r);
  return Buffer.from(
    `{"body":${r.body.toString('utf8')},"content_sha256":"${r.hash}"}`,
  );
}
