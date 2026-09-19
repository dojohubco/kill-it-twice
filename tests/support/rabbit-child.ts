import { mock } from 'node:test';
import type { ConsumeMessage } from 'amqplib';
import { AmqpSession } from '../../src/rabbitmq/session.ts';
import { digest } from '../../src/rabbitmq/protocol.ts';
// Private IPC instrumentation around actual production owners and AMQP operations.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { RabbitLedger, type RabbitClaim } from '../../src/rabbitmq/ledger.ts';
import { RabbitDelivery } from '../../src/rabbitmq/worker.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { Consumer } from '../../src/rabbitmq/consumer.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import {
  BrokerMetadata,
  record,
  field,
  type Topology,
} from '../../src/rabbitmq/metadata.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import type { CanonicalEvent } from '../../src/envelope.ts';
import { deadline } from './fault-protocol.ts';
let boundary = '',
  reached = false,
  intentional = false,
  orphan = false,
  disableRenewal = false;
let claims: RabbitClaim[] = [];
let events: string[] = [];
const deliveries: unknown[] = [];
// eslint-disable-next-line @typescript-eslint/unbound-method -- private instrumentation preserves the original session receiver
const originalChannel = AmqpSession.prototype.channel;
const channelHook = mock.method(
  AmqpSession.prototype,
  'channel',
  async function (this: AmqpSession, confirm: false, highWaterMark?: number) {
    const channel = await Reflect.apply(originalChannel, this, [
      confirm,
      highWaterMark,
    ]);
    channel.on('delivery', (message: ConsumeMessage) => {
      assert.ok(deliveries.length < 64);
      const properties: unknown = message.properties;
      deliveries.push({
        channelId: this.id,
        fields: message.fields,
        properties,
        wireHash: digest(message.content),
        wireHex: message.content.toString('hex'),
      });
    });
    return channel;
  },
);
const stop = new AbortController();
process.once('disconnect', () => {
  if (!intentional) {
    orphan = true;
    stop.abort();
  }
});
for (const sig of ['SIGTERM', 'SIGINT']) process.once(sig, () => stop.abort());
async function barrier(name: string) {
  if (reached || boundary !== name) return;
  reached = true;
  const release = deadline(
    once(process, 'message', { signal: stop.signal }).then((v: unknown[]) => v),
    'Rabbit private barrier',
    25000,
  );
  void release.catch(() => undefined);
  assert.equal(typeof process.send, 'function');
  await new Promise<void>((resolve, reject) =>
    process.send?.(
      {
        type: 'rabbit-barrier',
        boundary: name,
        pid: process.pid,
        claims,
        events,
        deliveries,
      },
      (e) => (e ? reject(e) : resolve()),
    ),
  );
  assert.deepEqual((await release)[0], { type: 'release', boundary: name });
}
class FaultLedger extends RabbitLedger {
  override async claim(...args: Parameters<RabbitLedger['claim']>) {
    claims = await super.claim(...args);
    await barrier('rabbit.after_claim_commit.before_publish');
    return claims;
  }
  override async renew(...args: Parameters<RabbitLedger['renew']>) {
    return disableRenewal ? false : super.renew(...args);
  }
}
class FaultPublisher extends Publisher {
  override async publish(...args: Parameters<Publisher['publish']>) {
    const results = await super.publish(...args);
    // A lost lease deliberately skips ledger.settle. The real-confirm boundary
    // must still be observable before that production ownership decision.
    if (results.some((r) => r.outcome === 'confirmed'))
      await barrier('rabbit.after_confirm.before_local_commit');
    return results;
  }
}
class FaultDatabase extends ConsumerDatabase {
  override async process(t: Topology, items: readonly CanonicalEvent[]) {
    events = items.map((e) => e.body.event_id);
    const result = await this.transaction(async (tx) => {
      const rows = await tx.process(t, items);
      await barrier('consumer.before_db_commit');
      return rows;
    });
    await barrier('consumer.after_db_commit.before_ack');
    return result;
  }
  override async quarantine(
    ...args: Parameters<ConsumerDatabase['quarantine']>
  ) {
    const id = await super.quarantine(...args);
    await barrier('consumer.after_quarantine_commit.before_ack');
    return id;
  }
}
function connection(raw: unknown): ConnectionConfig {
  const r = record(raw);
  assert.equal(typeof r['port'], 'number');
  return {
    host: field(r, 'host'),
    port: Number(r['port']),
    database: field(r, 'database'),
    user: field(r, 'user'),
    password: field(r, 'password'),
    application_name: field(r, 'application_name'),
  };
}
try {
  const [raw]: unknown[] = await deadline(
    once(process, 'message', { signal: stop.signal }).then((v: unknown[]) => v),
    'Rabbit child start',
  );
  const input = record(raw);
  boundary = field(input, 'boundary');
  disableRenewal = input['disableRenewal'] === true;
  const a = record(input['amqp']),
    m = record(input['metadata']),
    t = record(input['target']);
  assert.equal(typeof a['port'], 'number');
  const amqp = {
    host: field(a, 'host'),
    port: Number(a['port']),
    username: field(a, 'username'),
    password: field(a, 'password'),
    ca: field(a, 'ca'),
    vhost: field(a, 'vhost'),
  };
  const metadata = new BrokerMetadata({
    url: field(m, 'url'),
    username: field(m, 'username'),
    password: field(m, 'password'),
    ca: field(m, 'ca'),
  });
  const target: Topology = {
    registrationId: field(t, 'registrationId'),
    consumerId: field(t, 'consumerId'),
    pipelineId: field(t, 'pipelineId'),
    epoch: field(t, 'epoch'),
    vhost: field(t, 'vhost'),
    exchange: field(t, 'exchange'),
    queue: field(t, 'queue'),
    routingKey: field(t, 'routingKey'),
  };
  let result: unknown;
  if (input['role'] === 'publisher') {
    const lease = input['leaseMs'];
    assert.equal(typeof lease, 'number');
    assert.ok(typeof lease === 'number');
    const worker = new RabbitDelivery(
      new FaultLedger(connection(input['sql'])),
      new FaultPublisher(amqp, metadata),
      { leaseMs: lease, renewalMs: Math.max(50, Math.floor(lease / 5)) },
      randomUUID(),
    );
    result = await worker.once();
    await barrier('rabbit.after_local_commit.before_success');
  } else {
    const worker = new Consumer(
      new FaultDatabase(connection(input['sql'])),
      amqp,
      metadata,
      target,
    );
    result = await worker.once(stop.signal, 10000);
  }
  if (orphan || stop.signal.aborted)
    throw new Error('Rabbit child lost parent or interrupted');
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(
      JSON.stringify({ type: 'rabbit-success', result }) + '\n',
      (e) => (e ? reject(e) : resolve()),
    ),
  );
} catch (error) {
  console.error(
    error instanceof Error ? error.message : 'Rabbit child failure',
  );
  process.exitCode = 1;
} finally {
  channelHook.mock.restore();
  intentional = true;
  if (process.connected) process.disconnect();
}
