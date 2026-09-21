import { performance } from 'node:perf_hooks';
import {
  OperationsService,
  type Snapshot,
  type Observation,
} from './service.ts';
import { RecoveryService } from './recovery.ts';
import type { OperationsConfig } from './config.ts';
import { record } from './validation.ts';

const series = [
  'staged',
  'elasticsearch_satisfied',
  'rabbitmq_confirmed',
  'consumer_processed',
  'mutation_effects',
] as const;
type Series = (typeof series)[number];
export interface Rate {
  state: 'warming' | 'known' | 'unknown';
  per_second: string | null;
  reason: string | null;
  interval_ms: number | null;
}
export class RateSampler {
  readonly #samples = new Map<
    Series,
    { identity: string; total: bigint; at: bigint }
  >();
  sample(
    name: Series,
    identity: string,
    total: string | null,
    nowMs: number,
  ): Rate {
    if (
      total === null ||
      !/^(0|[1-9][0-9]*)$/.test(total) ||
      !Number.isFinite(nowMs) ||
      nowMs < 0 ||
      !Number.isSafeInteger(Math.floor(nowMs * 1000))
    ) {
      this.#samples.delete(name);
      return {
        state: 'unknown',
        per_second: null,
        reason: 'unavailable',
        interval_ms: null,
      };
    }
    const value = BigInt(total),
      at = BigInt(Math.floor(nowMs * 1000)),
      before = this.#samples.get(name);
    this.#samples.set(name, { identity, total: value, at });
    if (!before || before.identity !== identity || value < before.total)
      return {
        state: 'warming',
        per_second: null,
        reason: !before
          ? 'first_sample'
          : before.identity !== identity
            ? 'identity_changed'
            : 'counter_regressed',
        interval_ms: null,
      };
    const elapsed = at - before.at;
    if (elapsed <= 0n) {
      this.#samples.delete(name);
      return {
        state: 'unknown',
        per_second: null,
        reason: 'nonpositive_interval',
        interval_ms: null,
      };
    }
    const scaled = ((value - before.total) * 1000000000000n) / elapsed;
    return {
      state: 'known',
      per_second: `${scaled / 1000000n}.${(scaled % 1000000n).toString().padStart(6, '0')}`,
      reason: null,
      interval_ms: Number(elapsed) / 1000,
    };
  }
}
function total(value: unknown): string | null {
  return typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value)
    ? value
    : null;
}
function rows(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.map((v) => record(v));
}

/** Fixed-size last-known/rate memory. Never substitutes cached data for a current failed read. */
export class OperationalMonitor {
  readonly #service: OperationsService;
  readonly #recovery: RecoveryService;
  readonly #rate = new RateSampler();
  readonly #last = new Map<string, Observation>();
  readonly #clock: () => number;
  constructor(
    config: OperationsConfig,
    clock: () => number = () => performance.now(),
  ) {
    this.#service = new OperationsService(config);
    this.#recovery = new RecoveryService(config);
    this.#clock = clock;
  }
  async snapshot() {
    const [snapshot, overview, consumerBinding] = await Promise.all([
      this.#service.status(),
      this.#recovery.overview().then(
        (data) => ({ available: true as const, data }),
        () => ({ available: false as const, data: null }),
      ),
      this.#service.db.read('consumer', 'identity').then(
        (rows) => rows[0] ?? null,
        () => null,
      ),
    ]);
    let identity = 'unknown';
    if (overview.available) {
      const i = record(overview.data['identity']);
      identity = `${String(i['pipeline_id'])}:${String(i['source_epoch'])}`;
    }
    let consumerIdentity = 'unknown';
    const expected = snapshot.dependencies['pipeline']?.data?.['rabbit'];
    if (
      consumerBinding &&
      expected &&
      typeof expected === 'object' &&
      overview.available
    ) {
      const target = record(expected),
        binding = record(overview.data['identity']);
      if (
        consumerBinding['pipeline_id'] === binding['pipeline_id'] &&
        consumerBinding['source_epoch'] === binding['source_epoch'] &&
        consumerBinding['consumer_id'] === target['consumer_id'] &&
        consumerBinding['registration_id'] === target['registration_id']
      )
        consumerIdentity = `${identity}:${String(consumerBinding['consumer_id'])}:${String(consumerBinding['registration_id'])}`;
    }
    const rates = this.sample(snapshot, identity, consumerIdentity);
    const dependencies = Object.fromEntries(
      Object.entries(snapshot.dependencies).map(([name, current]) => {
        const previous = this.#last.get(name);
        if (current.freshness === 'fresh' && current.data !== null)
          this.#last.set(name, structuredClone(current));
        return [
          name,
          {
            ...current,
            data: current.freshness === 'unavailable' ? null : current.data,
            last_known:
              current.freshness === 'fresh'
                ? null
                : previous
                  ? { ...previous, freshness: 'stale' }
                  : null,
          },
        ];
      }),
    );
    return {
      ...snapshot,
      dependencies,
      worker_liveness: 'unknown_no_heartbeat_evidence',
      consumer_identity_validation:
        consumerIdentity === 'unknown' ? 'unknown_or_mismatched' : 'matched',
      throughput: rates,
      recovery: overview.available
        ? { freshness: 'fresh', data: overview.data }
        : { freshness: 'unavailable', data: null },
      consistency: 'independent_observations_not_atomic_global_state',
    };
  }
  private sample(
    snapshot: Snapshot,
    identity: string,
    consumerIdentity: string,
  ) {
    const p = snapshot.dependencies['pipeline'],
      c = snapshot.dependencies['consumer'];
    const freshPipeline =
      identity !== 'unknown' && p?.freshness === 'fresh' ? p.data : null;
    const freshConsumer =
      consumerIdentity !== 'unknown' && c?.freshness === 'fresh'
        ? c.data
        : null;
    const settled = rows(freshPipeline?.['settled']);
    const sum = (sink: string): string | null => {
      if (!freshPipeline || !Array.isArray(freshPipeline['settled']))
        return null;
      let n = 0n;
      for (const r of settled.filter(
        (r) => r['sink'] === sink && r['disposition'] !== 'dead_letter',
      )) {
        const value = total(r['count']);
        if (value === null) return null;
        n += BigInt(value);
      }
      return n.toString();
    };
    const values: Record<Series, string | null> = {
      staged: total(freshPipeline?.['staged']),
      elasticsearch_satisfied: sum('elasticsearch'),
      rabbitmq_confirmed: sum('rabbitmq'),
      consumer_processed: total(freshConsumer?.['processed']),
      mutation_effects: total(freshConsumer?.['effects']),
    };
    return Object.fromEntries(
      series.map((name) => [
        name,
        this.#rate.sample(
          name,
          name === 'consumer_processed' || name === 'mutation_effects'
            ? consumerIdentity
            : identity,
          values[name],
          this.#clock(),
        ),
      ]),
    );
  }
}
