import { AmqpSession, amqpFailure, type AmqpConnection } from './session.ts';
import {
  BrokerFailure,
  BrokerMetadata,
  validateTopology,
  type Topology,
} from './metadata.ts';
import { properties, decodeWire } from './protocol.ts';
import type { RabbitOutcome } from './ledger.ts';
export interface Publication {
  eventId: string;
  attemptId: string;
  wire: Buffer;
}
export interface PublicationResult {
  eventId: string;
  attemptId: string;
  channelId: string;
  outcome: RabbitOutcome;
  context: string;
  returned: boolean;
}
export class Publisher {
  readonly #connection: AmqpConnection;
  readonly #metadata: BrokerMetadata;
  readonly #deadline: number;
  readonly #highWaterMark: number;
  constructor(
    connection: AmqpConnection,
    metadata: BrokerMetadata,
    deadline = 10000,
    highWaterMark = 16,
  ) {
    if (!Number.isInteger(deadline) || deadline < 100 || deadline > 10000)
      throw new Error('Invalid confirm deadline');
    this.#connection = { ...connection };
    this.#metadata = metadata;
    this.#deadline = deadline;
    this.#highWaterMark = highWaterMark;
  }
  async publish(
    t: Topology,
    items: readonly Publication[],
  ): Promise<PublicationResult[]> {
    if (
      items.length < 1 ||
      items.length > 128 ||
      items.reduce((n, i) => n + i.wire.length, 0) > 4194304 ||
      new Set(items.map((i) => i.attemptId)).size !== items.length
    )
      throw new Error('Publication batch bounds/identity violated');
    for (const i of items) {
      const e = decodeWire(i.wire);
      if (e.body.event_id !== i.eventId || e.body.source_epoch !== t.epoch)
        throw new BrokerFailure(
          'integrity',
          'Publication ledger identity differs',
        );
    }
    await validateTopology(this.#metadata, t);
    if (this.#connection.vhost !== t.vhost)
      throw new BrokerFailure(
        'configuration',
        'AMQP registration vhost differs',
      );
    const session = new AmqpSession();
    const pending = new Map<
      string,
      { item: Publication; returned: boolean; sentAt: number }
    >();
    const results = new Map<string, PublicationResult>();
    let writable = true;
    try {
      await session.open(this.#connection);
      const channel = await session.channel(true, this.#highWaterMark);
      channel.on('drain', () => {
        writable = true;
        session.wake();
      });
      channel.on('return', (message) => {
        const attempt: unknown = message.properties.correlationId;
        const p =
          typeof attempt === 'string' ? pending.get(attempt) : undefined;
        if (!p || !p.item.wire.equals(message.content)) {
          session.retire(
            new BrokerFailure('integrity', 'Uncorrelated mandatory return'),
          );
          return;
        }
        p.returned = true;
      });
      await channel.checkExchange(t.exchange);
      let index = 0;
      const end = performance.now() + this.#deadline;
      while (results.size < items.length) {
        if (!session.alive) break;
        if (
          performance.now() >= end ||
          [...pending.values()].some(
            (p) => performance.now() - p.sentAt >= this.#deadline,
          )
        ) {
          session.retire(
            new BrokerFailure(
              'transient',
              'Publisher confirm deadline; remote outcome unresolved',
            ),
          );
          break;
        }
        while (
          index < items.length &&
          writable &&
          !session.blocked &&
          session.alive
        ) {
          const item = items[index++];
          if (!item) throw new Error('Missing publication');
          pending.set(item.attemptId, {
            item,
            returned: false,
            sentAt: performance.now(),
          });
          // false means this message is buffered. It is never sent a second time here.
          writable = channel.publish(
            t.exchange,
            t.routingKey,
            item.wire,
            properties(t, item.eventId, item.attemptId),
            (error: unknown) => {
              if (!session.alive) return;
              const p = pending.get(item.attemptId);
              if (!p) {
                session.retire(
                  new BrokerFailure('integrity', 'Unexpected confirm'),
                );
                return;
              }
              pending.delete(item.attemptId);
              results.set(item.attemptId, {
                eventId: item.eventId,
                attemptId: item.attemptId,
                channelId: session.id,
                outcome: p.returned
                  ? 'configuration'
                  : error
                    ? 'transient'
                    : 'confirmed',
                context: p.returned
                  ? 'Mandatory publication returned'
                  : error
                    ? 'Broker nack or unresolved channel outcome'
                    : 'Positive publisher confirm without mandatory return',
                returned: p.returned,
              });
              session.wake();
            },
          );
        }
        if (results.size < items.length)
          await session.changed(
            Math.max(1, Math.min(50, end - performance.now())),
          );
      }
      for (const item of items)
        if (!results.has(item.attemptId))
          results.set(item.attemptId, {
            eventId: item.eventId,
            attemptId: item.attemptId,
            channelId: session.id,
            outcome: session.failure?.classification ?? 'transient',
            context: session.failure?.message ?? 'Publication unresolved',
            returned: pending.get(item.attemptId)?.returned ?? false,
          });
      // A positive confirm is accepted only for the still-validated registered topology.
      try {
        await validateTopology(this.#metadata, t);
      } catch (error) {
        const failure = amqpFailure(error);
        for (const r of results.values())
          if (r.outcome === 'confirmed') {
            r.outcome = failure.classification;
            r.context =
              'Post-confirm receiver validation unavailable or mismatched';
          }
      }
      return items.map((i) => {
        const r = results.get(i.attemptId);
        if (!r) throw new Error('Missing publication result');
        return r;
      });
    } finally {
      await session.close();
    }
  }
}
