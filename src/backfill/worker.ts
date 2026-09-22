import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import {
  OwnershipError,
  TransactionError,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { retryDelay } from '../capture.ts';
import { uuid } from '../envelope.ts';
import { BackfillSource } from './source.ts';
import { BackfillLedger } from './ledger.ts';
import type { Binding, Claim, Status } from './types.ts';
export interface BackfillConfig {
  source: ConnectionConfig;
  pipeline: ConnectionConfig;
  binding: Binding;
}
export interface BackfillOptions {
  leaseMs: number;
  renewalMs: number;
  idleMs: number;
}
export class BackfillFailure extends Error {
  readonly primary: unknown;
  readonly cleanup: unknown[];
  readonly fatal: boolean;
  constructor(primary: unknown, cleanup: unknown[], fatal: boolean) {
    super('Backfill work failed; durable progress remains authoritative', {
      cause: primary,
    });
    this.primary = primary;
    this.cleanup = cleanup;
    this.fatal = fatal;
  }
}
export class Backfill {
  readonly workerId: string;
  readonly options: Readonly<BackfillOptions>;
  readonly #config: BackfillConfig;
  #active = false;
  constructor(
    config: BackfillConfig,
    options: Partial<BackfillOptions> = {},
    workerId = randomUUID(),
  ) {
    this.#config = config;
    this.workerId = uuid(workerId);
    uuid(config.binding.pipelineId);
    uuid(config.binding.sourceEpoch);
    this.options = Object.freeze({
      leaseMs: 30000,
      renewalMs: 5000,
      idleMs: 1000,
      ...options,
    });
    const o = this.options;
    if (
      !Object.values(o).every(Number.isInteger) ||
      o.leaseMs < 300 ||
      o.leaseMs > 30000 ||
      o.renewalMs < 50 ||
      o.renewalMs * 3 > o.leaseMs ||
      o.idleMs < 50 ||
      o.idleMs > 30000
    )
      throw new Error('Invalid bounded backfill configuration');
  }
  #ledger(label = 'control') {
    return new BackfillLedger({
      ...this.#config.pipeline,
      application_name: `${this.#config.pipeline.application_name}:${label}`,
    });
  }
  #source() {
    return new BackfillSource(this.#config.source, this.#config.binding);
  }
  async start(run: string, ranges = 4) {
    const observation = await this.#source().identity();
    await this.#ledger().start(run, this.#config.binding, ranges, observation);
    return this.status(run);
  }
  status(run: string) {
    return this.#ledger().status(run);
  }
  async pause(run: string, paused: boolean) {
    await this.#ledger().pause(run, paused);
    return this.status(run);
  }
  async once(
    run: string,
    signal?: AbortSignal,
    observation: 'full' | 'admission' = 'full',
  ): Promise<{
    workerId: string;
    page?: Record<string, unknown>;
    status: Status;
  }> {
    if (this.#active)
      throw new OwnershipError('One bounded backfill operation per worker');
    this.#active = true;
    let claimed: Claim | null = null;
    const ledger = this.#ledger();
    try {
      const readStatus = () =>
        observation === 'admission' ? ledger.poll(run) : ledger.status(run);
      const before = await readStatus();
      if (
        signal?.aborted ||
        before.paused ||
        before.phase === 'complete' ||
        before.phase === 'complete_with_errors'
      )
        return { workerId: this.workerId, status: before };
      if (before.phase === 'sealing') {
        // No pipeline transaction is open while the source cut is created/recovered.
        const fence = await this.#source().fence(run);
        await ledger.attach(run, fence);
      }
      claimed = await ledger.claim(run, this.workerId, this.options.leaseMs);
      let pageResult: Record<string, unknown> | undefined;
      if (claimed) {
        const claim = claimed,
          stop = new AbortController(),
          renewal = this.#ledger('renew');
        let lost: unknown;
        const renew = (async () => {
          try {
            while (!stop.signal.aborted) {
              await delay(this.options.renewalMs, undefined, {
                signal: stop.signal,
              });
              if (!(await renewal.change(claim, 'renew', this.options.leaseMs)))
                throw new Error('Backfill lease lost during source read');
            }
          } catch (e) {
            if (!stop.signal.aborted) lost = e;
          }
        })();
        let page;
        try {
          page = await this.#source().page(
            claim.checkpoint,
            claim.upper,
            claim.range === 0 ? run : undefined,
          );
        } finally {
          stop.abort();
          await renew;
        }
        if (lost !== undefined)
          throw lost instanceof Error
            ? lost
            : new Error('Renewal failed', { cause: lost });
        if (page.blocked) {
          if (!(await ledger.change(claim, 'block', 1, page.blocked.reason)))
            throw new Error('Backfill ownership lost before blockage');
          claimed = null;
        } else {
          // Extend after draining the independent renewal owner. The bounded local page
          // transaction checks the database clock both after locks and at COMMIT.
          if (!(await ledger.change(claim, 'renew', this.options.leaseMs)))
            throw new Error('Backfill ownership lost before staging');
          const batchId = randomUUID();
          pageResult = await ledger.page({ claim, batchId, page });
          claimed = null;
        }
      }
      if (observation === 'admission') await ledger.advanceIfReady(run);
      else await ledger.advance(run);
      return {
        workerId: this.workerId,
        ...(pageResult === undefined ? {} : { page: pageResult }),
        status: await readStatus(),
      };
    } catch (primary) {
      const code =
        primary instanceof TransactionError ? primary.sqlState : undefined;
      const fatal =
        primary instanceof TypeError ||
        code === 'P8001' ||
        code === 'P8003' ||
        code === 'P3001' ||
        code === 'P3002' ||
        code === '22003';
      const cleanup: unknown[] = [];
      if (claimed) {
        try {
          await this.#ledger('recover').change(
            claimed,
            fatal ? 'block' : 'defer',
            retryDelay(claimed.generation),
            fatal ? 'integrity' : undefined,
          );
        } catch (e) {
          cleanup.push(e);
        }
      }
      throw new BackfillFailure(primary, cleanup, fatal);
    } finally {
      this.#active = false;
    }
  }
  async follow(
    run: string,
    report: (
      value: Awaited<ReturnType<Backfill['once']>> | BackfillFailure,
    ) => Promise<void>,
    signal: AbortSignal,
  ) {
    while (!signal.aborted) {
      try {
        const value = await this.once(run, signal);
        await report(value);
        if (
          value.status.phase === 'complete' ||
          value.status.phase === 'complete_with_errors'
        )
          return;
      } catch (e) {
        if (!(e instanceof BackfillFailure)) throw e;
        await report(e);
        if (e.fatal) throw e;
      }
      try {
        await delay(this.options.idleMs, undefined, { signal });
      } catch (e) {
        if (!signal.aborted) throw e;
      }
    }
  }
}
