import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import {
  SourceCapture,
  type CaptureBinding,
  type Claim,
  type CaptureState,
} from './source-capture.ts';
import { SourceReader } from './source-reader.ts';
import { Pipeline, IntegrityError, type StageResult } from './pipeline.ts';
import {
  OwnershipError,
  TransactionError,
  type ConnectionConfig,
} from './internal/transaction.ts';
import { limits } from './limits.ts';
import { uuid } from './envelope.ts';
export interface CaptureOptions {
  claimCount: number;
  leaseMs: number;
  renewalMs: number;
  idleMs: number;
}
const captureDefaults: Readonly<CaptureOptions> = Object.freeze({
  claimCount: 16,
  leaseMs: 30000,
  renewalMs: 5000,
  idleMs: 1000,
});
export interface CaptureConfig {
  source: ConnectionConfig;
  pipeline: ConnectionConfig;
  binding: CaptureBinding;
}
interface Acknowledgement {
  eventId: string;
  hash: string;
  generation: string;
  status: string;
  observedAt: string;
}
export interface CaptureResult {
  workerId: string;
  claimed: number;
  staged: StageResult[];
  acknowledgements: Acknowledgement[];
  blocked: string[];
  sourceState: CaptureState;
}
export class CaptureFailure extends Error {
  readonly primary: unknown;
  readonly cleanup: readonly unknown[];
  readonly fatal: boolean;
  constructor(primary: unknown, cleanup: readonly unknown[], fatal: boolean) {
    super(
      fatal
        ? 'Capture integrity/binding failure; stopped without acknowledging unconfirmed work'
        : 'Capture attempt failed; unconfirmed work remains recoverable',
      { cause: primary },
    );
    this.primary = primary;
    this.cleanup = cleanup;
    this.fatal = fatal;
  }
}
function stateCode(error: unknown): string | undefined {
  return error instanceof TransactionError
    ? error.sqlState
    : error instanceof IntegrityError
      ? error.code
      : undefined;
}
class CaptureIntegrity extends Error {}
export function retryDelay(generation: string): number {
  const attempt = BigInt(generation);
  if (attempt < 1n) throw new Error('Invalid retry generation');
  // Positive jitter even at the cap: choose the upper half of a bounded exponential window.
  const exponent = attempt > 6n ? 5 : Number(attempt - 1n);
  const ceiling = Math.min(30000, 1000 * 2 ** exponent * 2);
  const floor = Math.floor(ceiling / 2);
  return floor + randomInt(1, ceiling - floor + 1);
}
export function partitionClaims(claims: readonly Claim[]) {
  const blocked: Claim[] = [],
    batches: Claim[][] = [];
  let batch: Claim[] = [],
    bytes = 0n;
  for (const claim of claims) {
    const size = BigInt(claim.transferBytes);
    if (size > BigInt(limits.recordBytes)) {
      blocked.push(claim);
      continue;
    }
    if (size < 1n) throw new CaptureIntegrity('Invalid source size metadata');
    if (
      batch.length &&
      (bytes + size > BigInt(limits.batchBytes) ||
        batch.length === limits.records)
    ) {
      batches.push(batch);
      batch = [];
      bytes = 0n;
    }
    batch.push(claim);
    bytes += size;
  }
  if (batch.length) batches.push(batch);
  return { blocked, batches };
}
export class Capture {
  readonly workerId: string;
  readonly options: Readonly<CaptureOptions>;
  #config: CaptureConfig;
  #active = false;
  constructor(
    config: CaptureConfig,
    options: Partial<CaptureOptions> = {},
    workerId = randomUUID(),
  ) {
    this.#config = {
      source: { ...config.source },
      pipeline: { ...config.pipeline },
      binding: { ...config.binding },
    };
    uuid(config.binding.pipelineId);
    uuid(config.binding.sourceEpoch);
    this.workerId = uuid(workerId);
    this.options = Object.freeze({ ...captureDefaults, ...options });
    const o = this.options;
    if (
      !Object.values(o).every(Number.isInteger) ||
      o.claimCount < 1 ||
      o.claimCount > 16 ||
      o.leaseMs < 300 ||
      o.leaseMs > 30000 ||
      o.renewalMs < 50 ||
      o.renewalMs * 3 > o.leaseMs ||
      o.idleMs < 50 ||
      o.idleMs > 30000
    )
      throw new Error('Invalid bounded capture configuration');
  }
  #control(label: string) {
    return new SourceCapture(
      {
        ...this.#config.source,
        application_name: `${this.#config.source.application_name}:${label}`,
      },
      this.#config.binding,
    );
  }
  async captureOnce(signal?: AbortSignal): Promise<CaptureResult> {
    if (this.#active)
      throw new OwnershipError('One capture batch sequence per worker');
    this.#active = true;
    const control = this.#control('control');
    const remaining = new Map<string, Claim>();
    const id = (c: Claim) =>
      `${this.#config.binding.sourceEpoch}:${c.entityId}:${c.version}`;
    const staged: StageResult[] = [],
      acknowledgements: Acknowledgement[] = [],
      blocked: string[] = [];
    try {
      const before = await control.summary();
      if (before.missing !== '0')
        throw new CaptureIntegrity('Required capture work is missing');
      if (signal?.aborted)
        return {
          workerId: this.workerId,
          claimed: 0,
          staged,
          acknowledgements,
          blocked,
          sourceState: before,
        };
      const claims = await control.claim(
        this.workerId,
        this.options.claimCount,
        this.options.leaseMs,
      );
      for (const claim of claims) {
        if (remaining.has(id(claim)) || claim.ownerId !== this.workerId)
          throw new CaptureIntegrity('Invalid or duplicate claim reply');
        remaining.set(id(claim), claim);
      }
      const parts = partitionClaims(claims);
      for (const claim of parts.blocked) {
        await control.block(claim);
        remaining.delete(id(claim));
        blocked.push(id(claim));
      }
      for (const batch of parts.batches) {
        if (signal?.aborted) break;
        const stopRenewal = new AbortController();
        const renewalState: { failure?: { error: unknown } } = {};
        const requireRenewal = () => {
          if (renewalState.failure) throw renewalState.failure.error;
        };
        const renew = this.#control('renew');
        const renewal = (async () => {
          try {
            while (!stopRenewal.signal.aborted) {
              await delay(this.options.renewalMs, undefined, {
                signal: stopRenewal.signal,
              });
              for (const claim of remaining.values()) {
                if (stopRenewal.signal.aborted) return;
                await renew.renew(claim, this.options.leaseMs);
              }
            }
          } catch (error) {
            if (!(
              stopRenewal.signal.aborted &&
              error instanceof Error &&
              error.name === 'AbortError'
            ))
              renewalState.failure = { error };
          }
        })();
        let result: StageResult[], selected;
        try {
          selected = await new SourceReader(
            this.#config.source,
            this.#config.binding.sourceEpoch,
          ).outbox(batch);
          if (
            selected.notVisible.length ||
            selected.events.length !== batch.length
          )
            throw new CaptureIntegrity(
              'Claimed committed revision missing from bounded source read',
            );
          requireRenewal();
          // A already completed. B returns only after confirmed COMMIT AND normal owner completion.
          result = await new Pipeline(
            this.#config.pipeline,
            this.#config.binding.pipelineId,
          ).stage(selected.events);
        } finally {
          stopRenewal.abort();
          await renewal;
        }
        requireRenewal();
        const expected = selected.events.map((e) => e.body.event_id).sort();
        const actual = result.map((r) => r.eventId).sort();
        if (
          actual.length !== expected.length ||
          actual.some((value, i) => value !== expected[i]) ||
          new Set(actual).size !== actual.length
        )
          throw new CaptureIntegrity(
            'Pipeline result identity set differs from submitted batch',
          );
        staged.push(...result);
        for (const event of selected.events) {
          const claim = remaining.get(event.body.event_id);
          if (!claim) throw new CaptureIntegrity('Missing local claim');
          // C is separate from B, with a fresh row lock and source clock check.
          const ack = await control.acknowledge(claim, event.contentSha256);
          remaining.delete(event.body.event_id);
          acknowledgements.push({
            eventId: event.body.event_id,
            hash: event.contentSha256,
            generation: claim.generation,
            status: ack.status,
            observedAt: ack.observed_at,
          });
        }
      }
      // Graceful stop leaves non-admitted batches delayed and recoverable.
      for (const claim of remaining.values())
        await control.defer(claim, retryDelay(claim.generation));
      const sourceState = await control.summary();
      if (sourceState.missing !== '0')
        throw new CaptureIntegrity('Required capture work is missing');
      return {
        workerId: this.workerId,
        claimed: claims.length,
        staged,
        acknowledgements,
        blocked,
        sourceState,
      };
    } catch (primary) {
      const code = stateCode(primary);
      const fatal =
        primary instanceof CaptureIntegrity ||
        primary instanceof IntegrityError ||
        code === 'P4001' ||
        code === 'P4003' ||
        code === '22003';
      const cleanup: unknown[] = [];
      if (!fatal) {
        for (const claim of remaining.values()) {
          try {
            await this.#control('defer').defer(
              claim,
              retryDelay(claim.generation),
            );
          } catch (error) {
            cleanup.push(error);
            // A disconnected source cannot release the rest either. Leave those leases to expire.
            if (stateCode(error) !== 'P4002') break;
          }
        }
      }
      throw new CaptureFailure(primary, cleanup, fatal);
    } finally {
      this.#active = false;
    }
  }
  async follow(
    report: (value: CaptureResult | CaptureFailure) => void | Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    while (!signal.aborted) {
      try {
        await report(await this.captureOnce(signal));
      } catch (error) {
        if (!(error instanceof CaptureFailure)) throw error;
        try {
          await report(error);
        } catch (diagnostic) {
          throw new CaptureFailure(
            error.primary,
            [...error.cleanup, diagnostic],
            error.fatal,
          );
        }
        if (error.fatal) throw error;
      }
      try {
        await delay(this.options.idleMs, undefined, { signal });
      } catch (error) {
        if (!signal.aborted) throw error;
      }
    }
  }
}
