// Small real selected-version protocol probe; not the M4 acceptance inventory.
import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { startRabbit } from './rabbit-service.ts';
import { AmqpSession } from '../src/rabbitmq/session.ts';
import {
  BrokerMetadata,
  queueArguments,
  validateTopology,
  record,
} from '../src/rabbitmq/metadata.ts';
const run = `m4-protocol-${Date.now()}`;
const dir = `artifacts/m4/${run}`;
await mkdir(dir, { recursive: true });
const service = await startRabbit(run);
const sessions: AmqpSession[] = [];
try {
  const registrationId = randomUUID(),
    pipelineId = randomUUID(),
    epoch = randomUUID(),
    consumerId = randomUUID();
  const vhost = `kit-${registrationId}`,
    t = {
      registrationId,
      pipelineId,
      epoch,
      consumerId,
      vhost,
      exchange: `${vhost}-events`,
      queue: `${vhost}-consumer`,
      routingKey: 'revision-v1',
    };
  const base = `/api`,
    v = encodeURIComponent(vhost),
    pub = randomBytes(24).toString('hex'),
    con = randomBytes(24).toString('hex'),
    observer = randomBytes(24).toString('hex');
  await service.api.request('PUT', `${base}/vhosts/${v}`, {});
  for (const [name, password, configure, write, read, tags] of [
    ['m4_setup', service.password, '.*', '.*', '.*', 'administrator'],
    ['publisher', pub, '^$', `^${t.exchange}$`, '^$', ''],
    ['consumer', con, '^$', '^$', `^${t.queue}$`, ''],
    ['observer', observer, '^$', '^$', '^$', 'management'],
  ]) {
    await service.api.request('PUT', `${base}/users/${name}`, {
      password,
      tags,
    });
    await service.api.request('PUT', `${base}/permissions/${v}/${name}`, {
      configure,
      write,
      read,
    });
  }
  await service.api.request('PUT', `${base}/exchanges/${v}/${t.exchange}`, {
    type: 'direct',
    durable: true,
    auto_delete: false,
    internal: false,
    arguments: {},
  });
  await service.api.request('PUT', `${base}/queues/${v}/${t.queue}`, {
    durable: true,
    auto_delete: false,
    arguments: queueArguments,
  });
  await service.api.request(
    'POST',
    `${base}/bindings/${v}/e/${t.exchange}/q/${t.queue}`,
    { routing_key: t.routingKey, arguments: {} },
  );
  const metadata = new BrokerMetadata({
    ...service.config,
    username: 'observer',
    password: observer,
  });
  await validateTopology(metadata, t);
  const open = async (username: string, password: string) => {
    const s = new AmqpSession();
    sessions.push(s);
    await s.open({
      host: '127.0.0.1',
      port: service.amqpPort,
      username,
      password,
      ca: service.ca,
      vhost,
    });
    return s;
  };
  const ps = await open('publisher', pub),
    cs = await open('consumer', con);
  const p = await ps.channel(true, 1),
    c = await cs.channel(false);
  await p.checkExchange(t.exchange);
  await c.checkQueue(t.queue);
  let returned = false;
  p.on('return', () => {
    returned = true;
  });
  const payload = Buffer.from(
    '{"precision":9007199254740993,"decimal":1.0000000000000000001}',
  );
  let flow: boolean | undefined;
  await new Promise<void>((resolve, reject) => {
    flow = p.publish(
      t.exchange,
      t.routingKey,
      payload,
      { mandatory: true, persistent: true, correlationId: randomUUID() },
      (e: unknown) =>
        e
          ? reject(e instanceof Error ? e : new Error('Confirm failure'))
          : resolve(),
    );
  });
  assert.equal(returned, false);
  const message = await c.get(t.queue, { noAck: false });
  assert.ok(message);
  assert.deepEqual(message.content, payload);
  assert.equal(message.properties.deliveryMode, 2);
  c.ack(message, false);
  await new Promise<void>((resolve, reject) => {
    p.publish(
      t.exchange,
      'missing',
      payload,
      { mandatory: true, persistent: true },
      (e: unknown) =>
        e
          ? reject(e instanceof Error ? e : new Error('Confirm failure'))
          : resolve(),
    );
  });
  assert.equal(returned, true);
  const denied = await open('publisher', pub);
  const forbidden = await denied.channel(false);
  await assert.rejects(forbidden.assertQueue('unauthorized'));
  const queue = record(
    await metadata.request('GET', `/api/queues/${v}/${t.queue}`),
  );
  await writeFile(
    `${dir}/result.json`,
    JSON.stringify(
      {
        run,
        version: service.info['rabbitmq_version'],
        listeners: service.info['listeners'],
        topology: t,
        arguments: queue['arguments'],
        policy: queue['effective_policy_definition'],
        flow,
        confirmedBytes: payload.toString('utf8'),
        mandatoryReturned: true,
        deniedConfigure: true,
        passiveChecks: true,
        metadataObserverWithoutAMQPGrants: true,
      },
      null,
      2,
    ) + '\n',
  );
  console.log(`${dir}/result.json`);
} finally {
  for (const s of sessions) await s.close();
  await service.cleanup();
}
