import { uuid } from '../envelope.ts';
import { positiveBigint } from '../source.ts';
export class ControlError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string) {
    super(code);
    this.status = status;
    this.code = code;
  }
}
export function record(v: unknown): Record<string, unknown> {
  if (!v || typeof v !== 'object' || Array.isArray(v))
    throw new ControlError(400, 'invalid_request');
  return v as Record<string, unknown>;
}
export function shape(
  v: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  const r = record(v);
  if (Object.keys(r).some((k) => !keys.includes(k)))
    throw new ControlError(400, 'invalid_request');
  return r;
}
export function string(v: unknown, max = 128): string {
  if (
    typeof v !== 'string' ||
    v.length < 1 ||
    v.length > max ||
    [...v].some((c) => c.charCodeAt(0) < 32)
  )
    throw new ControlError(400, 'invalid_request');
  return v;
}
export function id(v: unknown) {
  try {
    return uuid(v);
  } catch {
    throw new ControlError(400, 'invalid_request');
  }
}
export function bigint(v: unknown) {
  try {
    return positiveBigint(v);
  } catch {
    throw new ControlError(400, 'invalid_request');
  }
}
export function integer(v: unknown, min: number, max: number): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v < min || v > max)
    throw new ControlError(400, 'invalid_request');
  return v;
}
export function eventId(v: unknown): string {
  const parts = string(v, 80).split(':');
  if (parts.length !== 3) throw new ControlError(400, 'invalid_request');
  return `${id(parts[0])}:${bigint(parts[1])}:${bigint(parts[2])}`;
}
export function page(v: unknown) {
  const r = shape(v, ['limit', 'cursor']);
  const limit =
    r['limit'] === undefined
      ? 25
      : integer(
          Number(
            /^[1-9][0-9]{0,2}$/.test(string(r['limit'], 3)) ? r['limit'] : NaN,
          ),
          1,
          100,
        );
  return { limit, cursor: r['cursor'] };
}
export function cursor(
  v: unknown,
  scope: string,
): Record<string, unknown> | null {
  if (v === undefined) return null;
  const s = string(v, 2048);
  if (!/^[A-Za-z0-9_-]+$/.test(s))
    throw new ControlError(400, 'invalid_request');
  try {
    const raw: unknown = JSON.parse(
      Buffer.from(s, 'base64url').toString('utf8'),
    );
    const r = record(raw);
    if (
      r['scope'] !== scope ||
      r['v'] !== 1 ||
      Buffer.from(JSON.stringify(r)).toString('base64url') !== s
    )
      throw new Error('cursor');
    return r;
  } catch {
    throw new ControlError(400, 'invalid_request');
  }
}
export function encodeCursor(
  scope: string,
  value: Record<string, unknown>,
): string {
  return Buffer.from(JSON.stringify({ v: 1, scope, ...value })).toString(
    'base64url',
  );
}
