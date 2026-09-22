import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { retryDelay } from '../capture.ts';
import { sinkChunks, sinkPollDelay } from '../internal/sink-batch.ts';
import type { EsSettlement } from './ledger.ts';
import { EsAdapter, classify, EsFailure } from './adapter.ts';
import { EsLedger, type EsClaim, type Target, type Outcome } from './ledger.ts';
import { bulkLine, type Projection } from './projection.ts';
export interface DeliveryOptions {
  count: number;
  databaseBatchSize: number;
  leaseMs: number;
  renewalMs: number;
  idleMs: number;
}
export interface DeliveryResult {
  workerId: string;
  claimed: number;
  outcomes: {
    eventId: string;
    generation: string;
    outcome: Outcome;
    status: string;
  }[];
  status: unknown;
}
export class Delivery {
  readonly workerId: string;
  readonly #ledger: EsLedger;
  readonly #adapter: EsAdapter;
  readonly #options: DeliveryOptions;
  #active = false;
  constructor(
    ledger: EsLedger,
    adapter: EsAdapter,
    options: Partial<DeliveryOptions> = {},
    workerId = randomUUID(),
  ) {
    this.#ledger = ledger;
    this.#adapter = adapter;
    this.workerId = workerId;
    this.#options = {
      count: 500,
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
      o.count > 500 ||
      o.leaseMs < 300 ||
      o.leaseMs > 30000 ||
      o.renewalMs < 50 ||
      o.renewalMs * 3 > o.leaseMs ||
      o.idleMs < 50 ||
      o.idleMs > 30000
    )
      throw new Error('Invalid delivery bounds');
  }
  async once(): Promise<DeliveryResult> {
    if (this.#active)
      throw new Error('One delivery batch at a time per worker');
    this.#active = true;
    try {
      return await this.#once();
    } finally {
      this.#active = false;
    }
  }
  async #once(): Promise<DeliveryResult> {
    const t = await this.#ledger.target();
    if (t.mode === 'blocked')
      throw new EsFailure(
        'configuration',
        'ES target blocked; controlled diagnosis required',
      );
    const claims = await this.#ledger.claim(
      t,
      this.workerId,
      this.#options.count,
      this.#options.leaseMs,
    );
    const result: DeliveryResult = {
      workerId: this.workerId,
      claimed: claims.length,
      outcomes: [],
      status: null,
    };
    if (!claims.length) {
      result.status = await this.#ledger.status();
      return result;
    }
    const active = new Map(claims.map((c) => [c.eventId, c]));
    const abort = new AbortController();
    let renewalFailure: unknown;
    const renew = async () => {
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
    };
    const renewing = renew().catch((e: unknown) => {
      renewalFailure = e;
      active.clear();
    });
    const settleGroup = async (items: readonly EsSettlement[]) => {
      for (const group of sinkChunks(items, this.#options.databaseBatchSize)) {
        if (renewalFailure)
          throw new Error('Lease renewal failed', { cause: renewalFailure });
        const current = group.filter((r) => active.has(r.claim.eventId));
        const results: { eventId: string; status: string | undefined }[] = [];
        if (current.length && this.#options.databaseBatchSize > 1) {
          results.push(
            ...(await this.#ledger.settleMany(t, this.workerId, current)),
          );
        } else {
          for (const r of current)
            results.push({
              eventId: r.claim.eventId,
              status: await this.#ledger.settle(
                t,
                r.claim,
                this.workerId,
                r.outcome,
                r.remote,
                r.witness,
                r.context,
                r.delay,
              ),
            });
        }
        for (const r of group) {
          const status = current.includes(r)
            ? results.find((v) => v.eventId === r.claim.eventId)?.status
            : 'stale';
          if (status !== 'settled' && status !== 'stale')
            throw new Error('Invalid ES settlement result');
          active.delete(r.claim.eventId);
          result.outcomes.push({
            eventId: r.claim.eventId,
            generation: r.claim.generation,
            outcome: r.outcome,
            status,
          });
        }
      }
    };
    const settle = (
      claim: EsClaim,
      outcome: Outcome,
      remote: string | null,
      witness: string | null,
      context: string,
    ) =>
      settleGroup([
        {
          claim,
          outcome,
          remote,
          witness,
          context,
          delay: retryDelay(claim.generation),
        },
      ]);
    let primary: unknown;
    let fatal: Error | undefined;
    try {
      await this.#adapter.validate(t);
      let batch: Projection[] = [];
      let size = 0;
      const send = async () => {
        if (!batch.length) return;
        const anchor = claims.find((c) => c.eventId === batch[0]?.eventId);
        if (!anchor)
          throw new EsFailure('integrity', 'Missing request attempt');
        // The request ID is a retained attempt UUID, so opaque receiver logs can
        // be correlated without introducing a second durable request queue.
        const raw = await this.#adapter.bulk(t, batch, anchor.attemptId);
        const resolved = [];
        const unresolved: unknown[] = [];
        for (const item of raw) {
          try {
            resolved.push(await this.#adapter.resolve(t, item, this.#ledger));
          } catch (error) {
            unresolved.push(error);
          }
        }
        // A completely validated response may contain an independently invalid conflict
        // witness. Preserve other confirmed items after the common target postflight.
        await this.#adapter.validate(t);
        const settlements: EsSettlement[] = resolved.map((item) => {
          const claim = claims.find(
            (c) => c.eventId === item.projection.eventId,
          );
          if (!claim)
            throw new EsFailure('integrity', 'Unsubmitted response identity');
          return {
            claim,
            outcome: item.outcome,
            remote: item.remote,
            witness: item.witness,
            context: `request=${anchor.attemptId}; ${item.context}`,
            delay: retryDelay(claim.generation),
          };
        });
        await settleGroup(settlements);
        if (unresolved.length) {
          const primary =
            unresolved.find(
              (e) => classify(e).classification !== 'transient',
            ) ?? unresolved[0];
          const failure = classify(primary);
          throw new EsFailure(failure.classification, failure.message, {
            cause: new AggregateError(
              unresolved,
              'Independent item resolution failures',
            ),
          });
        }
        if (resolved.some((r) => r.outcome === 'transient'))
          throw new EsFailure(
            'transient',
            'Some receiver items remain unresolved',
          );
        batch = [];
        size = 0;
      };
      const cached = new Map<string, Projection>();
      for (const [index, c] of claims.entries()) {
        if (!active.has(c.eventId)) {
          cached.delete(c.eventId);
          continue;
        }
        if (BigInt(c.bytes) + 256n > 262144n) {
          await settle(
            c,
            'oversized',
            null,
            null,
            'Projection exceeds per-operation bound',
          );
          continue;
        }
        // Flush BEFORE loading a record whose advertised upper bound cannot fit.
        if (
          batch.length &&
          BigInt(size) + BigInt(c.bytes) + 256n > 4n * 1024n * 1024n
        )
          await send();
        if (this.#options.databaseBatchSize > 1 && !cached.has(c.eventId)) {
          const ids = claims
            .slice(index, index + 16)
            .filter(
              (v) => active.has(v.eventId) && BigInt(v.bytes) + 256n <= 262144n,
            )
            .map((v) => v.eventId);
          for (const value of await this.#ledger.readBatch(ids))
            cached.set(value.eventId, value);
        }
        const p =
          this.#options.databaseBatchSize > 1
            ? cached.get(c.eventId)
            : await this.#ledger.read(c.eventId);
        if (!p)
          throw new EsFailure('integrity', 'Missing bounded projection read');
        cached.delete(c.eventId);
        const line = bulkLine(p);
        if (BigInt(p.bytes) !== BigInt(c.bytes))
          throw new EsFailure('integrity', 'Immutable projection size changed');
        batch.push(p);
        size += Buffer.byteLength(line);
      }
      await send();
      await this.#admission(t, claims, 'healthy', '');
    } catch (error) {
      primary = error;
      const failure = classify(error);
      const cleanup: unknown[] = [];
      for (const c of [...active.values()]) {
        try {
          await settle(c, failure.classification, null, null, failure.message);
        } catch (e) {
          cleanup.push(e);
        }
      }
      try {
        await this.#admission(
          t,
          claims,
          failure.classification,
          failure.message,
        );
      } catch (e) {
        cleanup.push(e);
      }
      if (cleanup.length)
        fatal = new AggregateError(
          [primary, ...cleanup],
          'Delivery primary and recovery failures',
        );
      if (!fatal && failure.classification !== 'transient')
        fatal = new Error(failure.message, { cause: error });
    } finally {
      abort.abort();
      await renewing;
    }
    if (renewalFailure)
      throw new AggregateError(
        primary === undefined ? [renewalFailure] : [primary, renewalFailure],
        'Delivery lease renewal failed; work remains fenced',
      );
    if (fatal) throw fatal;
    result.status = await this.#ledger.status();
    return result;
  }
  #admission(
    t: Target,
    claims: readonly EsClaim[],
    outcome: 'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity',
    reason: string,
  ) {
    const anchor = claims[0];
    if (!anchor) throw new Error('Missing claim admission');
    const probe = anchor.probeGeneration;
    return this.#ledger.admission(
      t,
      this.workerId,
      probe,
      anchor,
      outcome,
      reason,
      retryDelay((BigInt(t.failures) + 1n).toString()),
    );
  }
  async follow(
    signal: AbortSignal,
    report: (r: DeliveryResult) => Promise<void>,
  ) {
    while (!signal.aborted) {
      const value = await this.once();
      await report(value);
      if (signal.aborted) return;
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
      } catch (e) {
        if (signal.aborted) return;
        throw e;
      }
    }
  }
}
