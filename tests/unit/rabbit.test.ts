import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomUUID } from 'node:crypto';
import { canonicalEvent } from '../../src/envelope.ts';
import {
  decodeWire,
  properties,
  retainedMetadata,
  validateMessage,
  PoisonMessage,
} from '../../src/rabbitmq/protocol.ts';
import type { ConsumeMessage } from 'amqplib';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { BrokerMetadata } from '../../src/rabbitmq/metadata.ts';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { RabbitDelivery } from '../../src/rabbitmq/worker.ts';
import { RabbitLedger } from '../../src/rabbitmq/ledger.ts';
const epoch = '11111111-1111-4111-8111-111111111111';
const t = {
  epoch,
  pipelineId: randomUUID(),
  registrationId: randomUUID(),
  consumerId: randomUUID(),
  vhost: 'fixture',
  exchange: 'fixture',
  queue: 'fixture',
  routingKey: 'revision-v1',
};
const e = canonicalEvent({
  schema_version: 1,
  source_epoch: epoch,
  entity_id: '9223372036854775807',
  entity_version: '9007199254740993',
  event_id: `${epoch}:9223372036854775807:9007199254740993`,
  source_change_id: '22222222-2222-4222-8222-222222222222',
  source_recorded_at: '2026-01-01T00:00:00.000001Z',
  kind: 'mutation',
  is_deleted: false,
  payload_encoding: 'pg18-jsonb-text/v1',
  payload_json: '{"huge": 9007199254740993, "loyalty_points": "not-a-number"}',
});
function message(): ConsumeMessage {
  return {
    content: e.wireBytes,
    fields: {
      consumerTag: 'one',
      deliveryTag: 1,
      redelivered: false,
      exchange: t.exchange,
      routingKey: t.routingKey,
    },
    properties: {
      ...properties(t, e.body.event_id, randomUUID()),
      deliveryMode: 2,
      clusterId: undefined,
      contentType: 'application/json',
      contentEncoding: 'utf-8',
      headers: { pipeline_id: t.pipelineId, registration_id: t.registrationId },
      priority: undefined,
      correlationId: randomUUID(),
      replyTo: undefined,
      expiration: undefined,
      messageId: e.body.event_id,
      timestamp: undefined,
      type: 'revision-v1',
      userId: undefined,
      appId: undefined,
    },
  };
}
await test('MQ-unit wire retains precise opaque source numbers and strict transport identity', () => {
  assert.equal(decodeWire(e.wireBytes).body.entity_version, '9007199254740993');
  assert.equal(decodeWire(e.wireBytes).body.payload_json, e.body.payload_json);
  assert.deepEqual(validateMessage(t, message()).wireBytes, e.wireBytes);
  for (const bytes of [
    Buffer.from([0xff]),
    Buffer.from(e.wireBytes.toString() + ' '),
    Buffer.from(
      e.wireBytes.toString().replace(e.contentSha256, '0'.repeat(64)),
    ),
  ])
    assert.throws(() => decodeWire(bytes), PoisonMessage);
  assert.throws(() => decodeWire(Buffer.alloc(131073)), /no ACK/);
  const m = message();
  m.properties.messageId = 'wrong';
  assert.throws(() => validateMessage(t, m), PoisonMessage);
  assert.throws(
    () => validateMessage({ ...t, epoch: randomUUID() }, message()),
    PoisonMessage,
  );
});
await test('MQ-unit quarantine identity excludes attempts and broker redelivery counters', () => {
  const a = message(),
    b = message();
  b.fields.redelivered = true;
  b.properties.headers = {
    ...b.properties.headers,
    'x-delivery-count': 25,
    'x-acquired-count': 26,
  };
  assert.deepEqual(retainedMetadata(a), retainedMetadata(b));
  b.properties.headers = {
    ...b.properties.headers,
    registration_id: randomUUID(),
  };
  assert.notDeepEqual(retainedMetadata(a), retainedMetadata(b));
  b.properties.messageId = 'x'.repeat(4097);
  assert.throws(() => retainedMetadata(b), /no ACK/);
});
await test('MQ-unit admission bounds reject before database or network work', async () => {
  const config = {
    host: '127.0.0.1',
    port: 1,
    username: 'unused',
    password: 'unused',
    ca: 'unused',
    vhost: 'fixture',
  };
  const sql = {
    host: '127.0.0.1',
    port: 1,
    database: 'unused',
    user: 'unused',
    password: 'unused',
    application_name: 'unit',
  };
  const meta = new BrokerMetadata({
    url: 'https://127.0.0.1:1',
    username: 'unused',
    password: 'unused',
    ca: 'unused',
  });
  const p = new Publisher(config, meta);
  await assert.rejects(p.publish(t, []), /bounds/);
  await assert.rejects(
    p.publish(
      t,
      Array.from({ length: 129 }, () => ({
        eventId: e.body.event_id,
        attemptId: randomUUID(),
        wire: e.wireBytes,
      })),
    ),
    /bounds/,
  );
  await assert.rejects(
    p.publish(t, [
      {
        eventId: e.body.event_id,
        attemptId: randomUUID(),
        wire: Buffer.alloc(4194305),
      },
    ]),
    /bounds/,
  );
  assert.throws(() => new Publisher(config, meta, 0));
  assert.throws(
    () => new RabbitDelivery(new RabbitLedger(sql), p, { count: 129 }),
  );
  await assert.rejects(
    new Consumer(new ConsumerDatabase(sql), config, meta, t).once(undefined, 0),
  );
});
