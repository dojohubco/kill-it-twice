import {
  TransactionOwner,
  type ConnectionConfig,
} from './internal/transaction.ts';
import { uuid } from './envelope.ts';
import { positiveBigint } from './source.ts';
import { countBound } from './limits.ts';
export interface CaptureBinding {
  pipelineId: string;
  sourceEpoch: string;
}
export interface Claim {
  entityId: string;
  version: string;
  generation: string;
  ownerId: string;
  leaseUntil: string;
  observedAt: string;
  transferBytes: string;
}
export interface CaptureState {
  observed_at: string;
  pending_due: string;
  pending_delayed: string;
  leased_current: string;
  leased_expired: string;
  blocked: string;
  acknowledged: string;
  missing: string;
}
interface Transition {
  status: string;
  observed_at: string;
  lease_until: string | null;
}
function string(value: unknown): string {
  if (typeof value !== 'string')
    throw new Error('Invalid capture database reply');
  return value;
}
function count(value: unknown): string {
  const result = string(value);
  if (!/^(0|[1-9][0-9]*)$/.test(result))
    throw new Error('Invalid capture count');
  return result;
}
function row(rows: Record<string, unknown>[]) {
  const result = rows[0];
  if (rows.length !== 1 || !result)
    throw new Error('Missing capture database result');
  return result;
}
interface Control {
  claim(owner: string, maximum: number, leaseMs: number): Promise<Claim[]>;
  change(
    claim: Claim,
    action: string,
    duration: number | null,
    hash: string | null,
  ): Promise<Transition>;
  summary(): Promise<CaptureState>;
}
// Fixed operations only. Each public call completes and closes its owned source transaction.
export class SourceCapture {
  #owner: TransactionOwner<Control>;
  constructor(config: ConnectionConfig, binding: CaptureBinding) {
    const pipeline = uuid(binding.pipelineId),
      epoch = uuid(binding.sourceEpoch);
    this.#owner = new TransactionOwner(config, (client, operation) => ({
      claim: (owner, maximum, leaseMs) =>
        operation(async () => {
          countBound(maximum);
          const rows = (
            await client.query<Record<string, unknown>>(
              'SELECT * FROM source.capture_claim($1,$2,$3,$4,$5)',
              [pipeline, epoch, uuid(owner), maximum, leaseMs],
            )
          ).rows;
          if (rows.length > maximum) throw new Error('Claim count overflow');
          return rows.map((r) => ({
            entityId: positiveBigint(r['entity_id']),
            version: positiveBigint(r['entity_version']),
            generation: positiveBigint(r['generation']),
            ownerId: uuid(r['owner_id']),
            leaseUntil: string(r['lease_until']),
            observedAt: string(r['observed_at']),
            transferBytes: positiveBigint(r['transfer_bytes']),
          }));
        }),
      change: (claim, action, duration, hash) =>
        operation(async () => {
          const r = row(
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM source.capture_change($1,$2,$3,$4,$5,$6,$7,$8,$9)',
                [
                  pipeline,
                  epoch,
                  positiveBigint(claim.entityId),
                  positiveBigint(claim.version),
                  uuid(claim.ownerId),
                  positiveBigint(claim.generation),
                  action,
                  duration,
                  hash,
                ],
              )
            ).rows,
          );
          return {
            status: string(r['status']),
            observed_at: string(r['observed_at']),
            lease_until:
              r['lease_until'] === null ? null : string(r['lease_until']),
          };
        }),
      summary: () =>
        operation(async () => {
          const r = row(
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM source.capture_summary($1,$2)',
                [pipeline, epoch],
              )
            ).rows,
          );
          return {
            observed_at: string(r['observed_at']),
            pending_due: count(r['pending_due']),
            pending_delayed: count(r['pending_delayed']),
            leased_current: count(r['leased_current']),
            leased_expired: count(r['leased_expired']),
            blocked: count(r['blocked']),
            acknowledged: count(r['acknowledged']),
            missing: count(r['missing']),
          };
        }),
    }));
  }
  claim(owner: string, maximum: number, leaseMs: number) {
    return this.#owner.transaction((tx) => tx.claim(owner, maximum, leaseMs));
  }
  renew(claim: Claim, leaseMs: number) {
    return this.#owner.transaction((tx) =>
      tx.change(claim, 'renew', leaseMs, null),
    );
  }
  defer(claim: Claim, delayMs: number) {
    return this.#owner.transaction((tx) =>
      tx.change(claim, 'defer', delayMs, null),
    );
  }
  block(claim: Claim) {
    return this.#owner.transaction((tx) =>
      tx.change(claim, 'block', null, null),
    );
  }
  acknowledge(claim: Claim, hash: string) {
    return this.#owner.transaction((tx) => tx.change(claim, 'ack', null, hash));
  }
  summary() {
    return this.#owner.transaction((tx) => tx.summary());
  }
}
