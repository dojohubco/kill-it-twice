// Private instrumentation only: delay the consume registration's return until real
// callbacks have queued a known cohort. The production selection/validation loop is unchanged.
import assert from 'node:assert/strict';
import { mock } from 'node:test';
import type { ConsumeMessage, Message, Options } from 'amqplib';
import { AmqpSession } from '../../src/rabbitmq/session.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { digest } from '../../src/rabbitmq/protocol.ts';
import type { CanonicalEvent } from '../../src/envelope.ts';
import type { Topology } from '../../src/rabbitmq/metadata.ts';
import { deadline } from './fault-protocol.ts';
interface DeliveryObservation {
  channelId: string;
  eventId: string;
  bytes: number;
  wireHash: string;
  tag: number;
  redelivered: boolean;
}
interface BatchObservation {
  events: string[];
  count: number;
  bodyBytes: number;
  wireBytes: number;
  oldSqlCharge: number;
  completion: string;
  sqlState?: string;
  statuses?: string[];
}
export interface BatchTrace {
  received: DeliveryObservation[];
  acknowledgements: { channelId: string; tag: number; multiple: boolean }[];
  batches: BatchObservation[];
}
export const batchTrace = (): BatchTrace => ({
  received: [],
  acknowledgements: [],
  batches: [],
});
export function installBatchTiming(trace: BatchTrace, minimums: number[]) {
  assert.ok(minimums.every((n) => Number.isInteger(n) && n >= 1 && n <= 64));
  const restores: (() => void)[] = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- test instrumentation preserves the original receiver
  const original = AmqpSession.prototype.channel;
  const hook = mock.method(
    AmqpSession.prototype,
    'channel',
    async function (this: AmqpSession, confirm: false, highWaterMark?: number) {
      assert.equal(confirm, false, 'Timing instrumentation is consumer-only');
      const channel = await Reflect.apply(original, this, [
        confirm,
        highWaterMark,
      ]);
      const channelId = this.id;
      const consume = channel.consume.bind(channel);
      const consumeHook = mock.method(
        channel,
        'consume',
        async function (
          _queue: string,
          listener: (message: ConsumeMessage | null) => void,
          options?: Options.Consume,
        ) {
          const minimum = minimums.shift() ?? 0;
          const queued = Promise.withResolvers<void>();
          let count = 0;
          const result = await consume(
            _queue,
            (message) => {
              if (message) {
                assert.ok(
                  trace.received.length < 256,
                  'Bounded private delivery observations',
                );
                const id: unknown = message.properties.messageId;
                assert.ok(typeof id === 'string');
                trace.received.push({
                  channelId,
                  eventId: id,
                  bytes: message.content.length,
                  wireHash: digest(message.content),
                  tag: message.fields.deliveryTag,
                  redelivered: message.fields.redelivered,
                });
              }
              listener(message); // Actual callback gets the same object, exactly once.
              if (message && ++count >= minimum) queued.resolve();
            },
            options,
          );
          if (minimum)
            await deadline(
              queued.promise,
              'Actual consumer cohort queued',
              10000,
            );
          return result;
        },
      );
      const ack = channel.ack.bind(channel);
      const ackHook = mock.method(
        channel,
        'ack',
        function (message: Message, multiple = false) {
          assert.ok(trace.acknowledgements.length < 256);
          trace.acknowledgements.push({
            channelId,
            tag: message.fields.deliveryTag,
            multiple,
          });
          ack(message, multiple); // Actual original-channel ACK; never a fabricated result.
        },
      );
      restores.push(
        () => consumeHook.mock.restore(),
        () => ackHook.mock.restore(),
      );
      return channel;
    },
  );
  return () => {
    for (const restore of restores) restore();
    hook.mock.restore();
  };
}
export class ObservedConsumerDatabase extends ConsumerDatabase {
  readonly trace: BatchTrace;
  readonly beforeCommit: (() => Promise<void>) | undefined;
  readonly afterCommit: (() => Promise<void>) | undefined;
  constructor(
    config: ConstructorParameters<typeof ConsumerDatabase>[0],
    trace: BatchTrace,
    beforeCommit?: () => Promise<void>,
    afterCommit?: () => Promise<void>,
  ) {
    super(config);
    this.trace = trace;
    this.beforeCommit = beforeCommit;
    this.afterCommit = afterCommit;
  }
  override async process(target: Topology, items: readonly CanonicalEvent[]) {
    assert.ok(this.trace.batches.length < 64);
    const observation: BatchObservation = {
      events: items.map((e) => e.body.event_id),
      count: items.length,
      bodyBytes: items.reduce((n, e) => n + e.bodyBytes.length, 0),
      wireBytes: items.reduce((n, e) => n + e.wireBytes.length, 0),
      oldSqlCharge: items.reduce((n, e) => n + e.bodyBytes.length + 108, 0),
      completion: 'attempted',
    };
    this.trace.batches.push(observation);
    try {
      // Same accepted transaction owner and real facade operation; hooks surround
      // real writes/COMMIT without exposing or replacing the private client.
      const rows = await this.transaction(async (tx) => {
        const rows = await tx.process(target, items);
        await this.beforeCommit?.();
        return rows;
      });
      observation.completion = 'confirmed_commit';
      observation.statuses = rows.map((r) => r.status);
      await this.afterCommit?.();
      return rows;
    } catch (error) {
      if (error instanceof TransactionError) {
        observation.completion = error.outcome;
        if (error.sqlState) observation.sqlState = error.sqlState;
      } else observation.completion = 'test_or_callback_failure';
      throw error;
    }
  }
}
