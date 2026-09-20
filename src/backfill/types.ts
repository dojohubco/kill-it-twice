import { uuid, type CanonicalEvent } from '../envelope.ts';
import { positiveBigint } from '../source.ts';
export function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new TypeError('Expected backfill object');
  return value as Record<string, unknown>;
}
export function text(value: unknown): string {
  if (typeof value !== 'string') throw new TypeError('Expected backfill text');
  return value;
}
export function key(value: unknown): string {
  return value === '0' ? '0' : positiveBigint(value);
}
export function flag(value: unknown): boolean {
  if (typeof value !== 'boolean')
    throw new TypeError('Expected backfill boolean');
  return value;
}
export interface Binding {
  sourceEpoch: string;
  pipelineId: string;
}
export interface Claim {
  runId: string;
  range: number;
  owner: string;
  generation: string;
  checkpoint: string;
  upper: string;
  leaseUntil: string;
}
export function claim(value: unknown): Claim | null {
  if (value === null) return null;
  const r = object(value),
    n = r['range_no'];
  if (typeof n !== 'number' || !Number.isInteger(n) || n < 0 || n > 16)
    throw new TypeError('Invalid range identity');
  return {
    runId: uuid(r['run_id']),
    range: n,
    owner: uuid(r['owner_id']),
    generation: positiveBigint(r['generation']),
    checkpoint: key(r['checkpoint']),
    upper: key(r['upper_key']),
    leaseUntil: text(r['lease_until']),
  };
}
export interface Page {
  events: CanonicalEvent[];
  keys: string[];
  next: string;
  eof: boolean;
  observation: { observed_at: string; snapshot: string };
  blocked: {
    key: string;
    bytes: string;
    reason: 'oversized_record' | 'missing_revision';
  } | null;
}
export interface Status {
  runId: string;
  phase:
    | 'scanning'
    | 'sealing'
    | 'importing'
    | 'draining'
    | 'complete'
    | 'complete_with_errors';
  paused: boolean;
  effectivePaused: boolean;
  blocked: boolean;
  evidence: Record<string, unknown>;
}
export function status(value: unknown): Status {
  const r = object(value),
    p = r['phase'];
  if (
    p !== 'scanning' &&
    p !== 'sealing' &&
    p !== 'importing' &&
    p !== 'draining' &&
    p !== 'complete' &&
    p !== 'complete_with_errors'
  )
    throw new TypeError('Invalid backfill phase');
  return {
    runId: uuid(r['run_id']),
    phase: p,
    paused: flag(r['desired_paused']),
    effectivePaused: flag(r['effective_paused']),
    blocked: flag(r['blocked']),
    evidence: r,
  };
}
