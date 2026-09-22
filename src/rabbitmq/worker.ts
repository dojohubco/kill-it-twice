import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { retryDelay } from '../capture.ts';
import { sinkChunks, sinkPollDelay } from '../internal/sink-batch.ts';
import {
  RabbitLedger,
  type RabbitClaim,
  type RabbitOutcome,
} from './ledger.ts';
import { Publisher, type PublicationResult } from './publisher.ts';
import { amqpFailure } from './session.ts';
import { BrokerFailure } from './metadata.ts';
export interface RabbitOptions {
  count: number;
  databaseBatchSize: number;
  leaseMs: number;
  renewalMs: number;
  idleMs: number;
}
export class RabbitDelivery {
  readonly workerId: string;
  readonly #ledger: RabbitLedger;
  readonly #publisher: Publisher;
  readonly #options: RabbitOptions;
  #active = false;
  constructor(
    ledger: RabbitLedger,
    publisher: Publisher,
    options: Partial<RabbitOptions> = {},
    workerId = randomUUID(),
  ) {
    this.#ledger = ledger;
    this.#publisher = publisher;
    this.workerId = workerId;
    this.#options = {
      count: 128,
      databaseBatchSize: 1,
      leaseMs: 30000,
      renewalMs: 5000,
      idleMs: 1000,
      ...options,
    };
    const o = this.#options;
    if (
      !Object.values(o).every(Number.isInteger) ||
      o.databaseBatchSize < 1 ||
      o.databaseBatchSize > 32 ||
      o.count < 1 ||
      o.count > 128 ||
      o.leaseMs < 300 ||
      o.leaseMs > 30000 ||
      o.renewalMs < 50 ||
      o.renewalMs * 3 > o.leaseMs ||
      o.idleMs < 50 ||
      o.idleMs > 30000
    )
      throw new Error('Invalid publisher bounds');
  }
  async once() {
    if (this.#active) throw new Error('One publisher batch in flight');
    this.#active = true;
    try {
      return await this.#once();
    } finally {
      this.#active = false;
    }
  }
  async #once() {
    const t = await this.#ledger.target();
    if (t.mode === 'blocked' || t.mode === 'preparing')
      throw new BrokerFailure(
        'configuration',
        'Broker target unavailable for publication',
      );
    const claims = await this.#ledger.claim(
      t,
      this.workerId,
      this.#options.count,
      this.#options.leaseMs,
    );
    const outcomes: {
      eventId: string;
      generation: string;
      outcome: RabbitOutcome;
      status: string;
    }[] = [];
    if (!claims.length)
      return {
        workerId: this.workerId,
        claimed: 0,
        outcomes,
        status: await this.#ledger.status(),
      };
    const active = new Map(claims.map((c) => [c.eventId, c]));
    const abort = new AbortController();
    let renewalFailure: unknown;
    const renewing = (async () => {
      while (!abort.signal.aborted) {
        try {
          await delay(this.#options.renewalMs, undefined, {
            signal: abort.signal,
          });
        } catch (error) {
          if (abort.signal.aborted) return;
          throw error;
        }
        for (const group of sinkChunks(
          [...active.values()],
          this.#options.databaseBatchSize,
        )) {
          if (abort.signal.aborted) return;
          if (this.#options.databaseBatchSize > 1) {
            const renewed = await this.#ledger.renewMany(
              t,
              group,
              this.workerId,
              this.#options.leaseMs,
            );
            for (const r of renewed) if (!r.renewed) active.delete(r.eventId);
          } else {
            for (const c of group)
              if (
                !(await this.#ledger.renew(
                  t,
                  c,
                  this.workerId,
                  this.#options.leaseMs,
                ))
              )
                active.delete(c.eventId);
          }
        }
      }
    })().catch((error: unknown) => {
      renewalFailure = error;
      active.clear();
    });
    let health:
      'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity' =
      'healthy';
    const settleGroup = async (
      items: readonly { claim: RabbitClaim; result: PublicationResult }[],
    ) => {
      for (const group of sinkChunks(items, this.#options.databaseBatchSize)) {
        if (renewalFailure)
          throw new Error('Publisher lease renewal failed', {
            cause: renewalFailure,
          });
        const current = group.filter((x) => active.has(x.claim.eventId));
        const statuses: { eventId: string; status: string | undefined }[] = [];
        if (current.length && this.#options.databaseBatchSize > 1) {
          statuses.push(
            ...(await this.#ledger.settleMany(
              t,
              this.workerId,
              current.map(({ claim, result: r }) => ({
                claim,
                outcome: r.outcome,
                channel: r.channelId || null,
                context: r.context,
                delay: retryDelay(claim.generation),
              })),
            )),
          );
        } else {
          for (const { claim: c, result: r } of current)
            statuses.push({
              eventId: c.eventId,
              status: await this.#ledger.settle(
                t,
                c,
                this.workerId,
                r.outcome,
                r.channelId || null,
                r.context,
                retryDelay(c.generation),
              ),
            });
        }
        for (const item of group) {
          const c = item.claim,
            r = item.result;
          const status = current.includes(item)
            ? statuses.find((s) => s.eventId === c.eventId)?.status
            : 'stale';
          if (status !== 'settled' && status !== 'stale')
            throw new Error('Invalid publisher settlement');
          active.delete(c.eventId);
          outcomes.push({
            eventId: c.eventId,
            generation: c.generation,
            outcome: r.outcome,
            status,
          });
          if (
            r.outcome !== 'confirmed' &&
            (health === 'healthy' || health === 'transient')
          )
            health = r.outcome;
        }
      }
    };
    try {
      const batches: RabbitClaim[][] = [];
      let batch: RabbitClaim[] = [];
      let size = 0;
      for (const c of claims) {
        const bytes = BigInt(c.bytes);
        if (bytes <= 0n || bytes > 65536n)
          throw new BrokerFailure(
            'integrity',
            'Unpublishable retained event size',
          );
        if (size + Number(bytes) > 4194304) {
          batches.push(batch);
          batch = [];
          size = 0;
        }
        batch.push(c);
        size += Number(bytes);
      }
      if (batch.length) batches.push(batch);
      for (const group of batches) {
        let results: PublicationResult[];
        try {
          const records = await this.#ledger.read(group.map((c) => c.eventId));
          if (
            records.length !== group.length ||
            new Set(records.map((r) => r.eventId)).size !== group.length ||
            records.some(
              (r) =>
                !group.some(
                  (c) => c.eventId === r.eventId && c.bytes === r.bytes,
                ),
            )
          )
            throw new BrokerFailure(
              'integrity',
              'Incomplete broker ledger read',
            );
          const items = group.map((c) => {
            const r = records.find((r) => r.eventId === c.eventId);
            if (!r) throw new Error('Missing wire');
            return { eventId: c.eventId, attemptId: c.attemptId, wire: r.wire };
          });
          results = await this.#publisher.publish(t, items);
          if (
            results.length !== group.length ||
            new Set(results.map((r) => r.attemptId)).size !== group.length ||
            results.some(
              (r) =>
                !group.some(
                  (c) => c.eventId === r.eventId && c.attemptId === r.attemptId,
                ),
            )
          )
            throw new BrokerFailure(
              'integrity',
              'Incomplete publication results',
            );
        } catch (error) {
          const failure = amqpFailure(error);
          results = group.map((c) => ({
            eventId: c.eventId,
            attemptId: c.attemptId,
            channelId: '',
            outcome: failure.classification,
            context: failure.message,
            returned: false,
          }));
        }
        const settlements = group.map((claim) => {
          const result = results.find((r) => r.attemptId === claim.attemptId);
          if (!result) throw new Error('Missing correlated result');
          return { claim, result };
        });
        await settleGroup(settlements);
      }
      const anchor = claims[0];
      if (!anchor) throw new Error('Missing admission anchor');
      await this.#ledger.admission(
        t,
        anchor,
        this.workerId,
        health,
        health === 'healthy' ? '' : 'Publication admission failure',
        retryDelay(anchor.generation),
      );
    } finally {
      abort.abort();
      await renewing;
    }
    return {
      workerId: this.workerId,
      claimed: claims.length,
      outcomes,
      status: await this.#ledger.status(),
    };
  }
  async follow(
    signal: AbortSignal,
    report: (
      result: Awaited<ReturnType<RabbitDelivery['once']>>,
    ) => Promise<void>,
  ) {
    while (!signal.aborted) {
      const value = await this.once();
      await report(value);
      try {
        await delay(
          sinkPollDelay(
            value.claimed,
            this.#options.databaseBatchSize,
            this.#options.idleMs,
          ),
          undefined,
          { signal },
        );
      } catch (error) {
        if (!signal.aborted) throw error;
      }
    }
  }
}
