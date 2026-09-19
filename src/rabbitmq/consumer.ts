import { setTimeout as delay } from 'node:timers/promises';
import type { ConsumeMessage, Channel } from 'amqplib';
import { TransactionError } from '../internal/transaction.ts';
import type { CanonicalEvent } from '../envelope.ts';
import { retryDelay } from '../capture.ts';
import { ConsumerDatabase } from './consumer-db.ts';
import { AmqpSession, type AmqpConnection } from './session.ts';
import {
  BrokerMetadata,
  BrokerFailure,
  validateTopology,
  type Topology,
} from './metadata.ts';
import {
  PoisonMessage,
  retainedMetadata,
  validateMessage,
  transportCap,
} from './protocol.ts';
interface Retained {
  message: ConsumeMessage;
  metadata: Buffer;
  event: CanonicalEvent | undefined;
  poison: PoisonMessage | undefined;
}
export interface ConsumeResult {
  channelId: string;
  received: number;
  processed: string[];
  quarantined: string[];
  acknowledged: number;
  retired: boolean;
}
export class Consumer {
  readonly #db: ConsumerDatabase;
  readonly #connection: AmqpConnection;
  readonly #metadata: BrokerMetadata;
  readonly #target: Topology;
  #active = false;
  constructor(
    db: ConsumerDatabase,
    connection: AmqpConnection,
    metadata: BrokerMetadata,
    target: Topology,
  ) {
    this.#db = db;
    this.#connection = { ...connection };
    this.#metadata = metadata;
    this.#target = { ...target };
  }
  async once(
    signal = new AbortController().signal,
    idleMs = 1000,
  ): Promise<ConsumeResult> {
    if (this.#active) throw new Error('One active consumer batch per process');
    if (!Number.isInteger(idleMs) || idleMs < 25 || idleMs > 30000)
      throw new Error('Invalid consumer idle bound');
    this.#active = true;
    try {
      return await this.#run(signal, idleMs);
    } finally {
      this.#active = false;
    }
  }
  async #run(signal: AbortSignal, idleMs: number): Promise<ConsumeResult> {
    const identity = await this.#db.identity(),
      t = this.#target;
    if (
      identity.consumerId !== t.consumerId ||
      identity.pipelineId !== t.pipelineId ||
      identity.registrationId !== t.registrationId ||
      identity.epoch !== t.epoch ||
      identity.codec !== 'pg18-jsonb-text/v1' ||
      this.#connection.vhost !== t.vhost
    )
      throw new BrokerFailure(
        'configuration',
        'Consumer registration mismatch',
      );
    await validateTopology(this.#metadata, t);
    const session = new AmqpSession();
    const queue: ConsumeMessage[] = [];
    let bytes = 0;
    let firstAt: number | undefined;
    let intakeError: Error | undefined;
    const result: ConsumeResult = {
      channelId: session.id,
      received: 0,
      processed: [],
      quarantined: [],
      acknowledged: 0,
      retired: false,
    };
    try {
      await session.open(this.#connection);
      const channel = await session.channel(false);
      await channel.checkQueue(t.queue);
      await channel.prefetch(64, false);
      await channel.consume(
        t.queue,
        (message) => {
          if (!message) {
            session.retire(
              new BrokerFailure(
                'configuration',
                'Registered consumer queue cancelled',
              ),
            );
            return;
          }
          if (
            queue.length >= 64 ||
            message.content.length > transportCap ||
            bytes + message.content.length > 64 * transportCap
          ) {
            intakeError = new Error(
              'Consumer transport/prefetch retention bound exceeded; no ACK',
            );
            session.retire();
            return;
          }
          queue.push(message);
          bytes += message.content.length;
          firstAt ??= performance.now();
          session.wake();
        },
        { noAck: false, exclusive: false },
      );
      const end = performance.now() + idleMs;
      while (
        !signal.aborted &&
        session.alive &&
        (firstAt === undefined
          ? performance.now() < end
          : queue.length < 32 &&
            bytes < 1048576 &&
            performance.now() - firstAt < 25)
      )
        await session.changed(25);
      if (intakeError) throw intakeError;
      if (!session.alive)
        throw session.failure ?? new Error('Consumer session lost');
      if (signal.aborted || !queue.length) return result;
      const batch: ConsumeMessage[] = [];
      let batchBytes = 0;
      for (const m of queue) {
        if (batch.length >= 32 || batchBytes + m.content.length > 1048576)
          break;
        batch.push(m);
        batchBytes += m.content.length;
      }
      result.received = batch.length;
      const retained: Retained[] = batch.map((message) => {
        const metadata = retainedMetadata(message); // Overflow is not poison that can be truncated/ACKed.
        try {
          return {
            message,
            metadata,
            event: validateMessage(t, message),
            poison: undefined,
          };
        } catch (error) {
          if (!(error instanceof PoisonMessage)) throw error;
          return { message, metadata, event: undefined, poison: error };
        }
      });
      // Different content under one ID is never arbitrarily chosen as the batch winner.
      const byId = new Map<string, CanonicalEvent>();
      const conflicts = new Set<string>();
      for (const r of retained)
        if (r.event) {
          const old = byId.get(r.event.body.event_id);
          if (old && !old.wireBytes.equals(r.event.wireBytes))
            conflicts.add(r.event.body.event_id);
          byId.set(r.event.body.event_id, r.event);
        }
      for (const r of retained)
        if (r.event && conflicts.has(r.event.body.event_id)) {
          r.poison = new PoisonMessage(
            'Conflicting identities within consumer batch',
            'conflicting_content',
          );
          r.event = undefined;
        }
      const valid = retained.filter((r) => r.event !== undefined);
      try {
        const events = [
          ...new Map(
            valid.flatMap((r) =>
              r.event ? [[r.event.body.event_id, r.event] as const] : [],
            ),
          ).values(),
        ];
        if (events.length) {
          const rows = await this.#db.process(t, events);
          this.#checkResults(events, rows);
          for (const r of valid) {
            if (r.event) result.processed.push(r.event.body.event_id);
            this.#ack(channel, session, r.message, result);
          }
        }
      } catch (error) {
        // Only known rolled-back content/payload validation can be isolated. Availability,
        // SQL defects, deadlocks and ambiguous or committed-cleanup errors retire and redeliver.
        if (
          !(error instanceof TransactionError) ||
          error.outcome !== 'rolled_back' ||
          !['P6001', 'P6003'].includes(error.sqlState ?? '')
        )
          throw error;
        for (const r of valid) {
          if (!r.event)
            throw new Error('Missing retained event', { cause: error });
          try {
            const rows = await this.#db.process(t, [r.event]);
            this.#checkResults([r.event], rows);
            result.processed.push(r.event.body.event_id);
            this.#ack(channel, session, r.message, result);
          } catch (failure) {
            if (
              !(failure instanceof TransactionError) ||
              failure.outcome !== 'rolled_back' ||
              !['P6001', 'P6003'].includes(failure.sqlState ?? '')
            )
              throw failure;
            r.poison = new PoisonMessage(
              failure.sqlState === 'P6001'
                ? 'Conflicting retained consumer content'
                : 'Invalid database payload codec',
              failure.sqlState === 'P6001'
                ? 'conflicting_content'
                : 'validation',
            );
            r.event = undefined;
          }
        }
      }
      for (const r of retained)
        if (r.poison) {
          const id: unknown = r.message.properties.messageId;
          const claimed =
            typeof id === 'string' && Buffer.byteLength(id) <= 100 ? id : null;
          const q = await this.#db.quarantine(
            r.message.content,
            r.metadata,
            claimed,
            r.poison.classification,
            r.poison.message,
          );
          result.quarantined.push(q);
          this.#ack(channel, session, r.message, result);
        }
      result.retired = !session.alive;
      return result;
    } catch (error) {
      session.retire();
      throw error;
    } finally {
      await session.close();
    }
  }
  #checkResults(
    events: CanonicalEvent[],
    rows: { event_id: string; status: string }[],
  ) {
    if (
      rows.length !== events.length ||
      new Set(rows.map((r) => r.event_id)).size !== events.length ||
      rows.some(
        (r) =>
          !events.some((e) => e.body.event_id === r.event_id) ||
          !['processed', 'already_processed'].includes(r.status),
      )
    )
      throw new Error('Incomplete committed consumer result; no ACK');
  }
  #ack(
    channel: Channel,
    session: AmqpSession,
    message: ConsumeMessage,
    result: ConsumeResult,
  ) {
    if (!session.alive) {
      result.retired = true;
      return;
    }
    // This local channel and the retained delivery were created together. No reconnection
    // ever substitutes a channel, and no multiple=true ACK covers unfinished work.
    channel.ack(message, false);
    result.acknowledged++;
  }
  async follow(
    signal: AbortSignal,
    report: (
      result: ConsumeResult | { error: string; retryMs: number },
    ) => Promise<void>,
  ) {
    let failures = 0n;
    while (!signal.aborted) {
      try {
        await report(await this.once(signal));
        failures = 0n;
      } catch (error) {
        if (signal.aborted) return;
        if (
          error instanceof BrokerFailure &&
          error.classification !== 'transient'
        )
          throw error;
        const ms = retryDelay(String(++failures));
        await report({
          error: 'Consumer unavailable; unacknowledged work retained',
          retryMs: ms,
        });
        try {
          await delay(ms, undefined, { signal });
        } catch (failure) {
          if (!signal.aborted) throw failure;
        }
      }
    }
  }
}
