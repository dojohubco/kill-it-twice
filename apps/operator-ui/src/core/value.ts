import { isLosslessNumber, stringify } from 'lossless-json';
export type Data = Record<string, unknown>;
export function record(v: unknown): Data {
  return v !== null &&
    typeof v === 'object' &&
    !Array.isArray(v) &&
    !isLosslessNumber(v)
    ? (v as Data)
    : {};
}
export function records(v: unknown): Data[] {
  return Array.isArray(v) ? v.map(record) : [];
}
export function value(v: unknown, fallback = '—'): string {
  if (typeof v === 'string') return v || fallback;
  if (isLosslessNumber(v)) return v.value;
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  if (typeof v === 'boolean') return v ? 'Yes' : 'No';
  return fallback;
}
function decimal(v: unknown): string | null {
  const s = value(v, '');
  return /^(0|[1-9][0-9]*)$/.test(s) ? s : null;
}
export function count(v: unknown): string {
  const s = decimal(v);
  return s === null ? '—' : BigInt(s).toLocaleString('en-US');
}
export function label(v: unknown): string {
  const s = value(v, 'Unknown');
  return s.replaceAll('_', ' ').replace(/^./, (x) => x.toUpperCase());
}
export function timestamp(v: unknown): string {
  const s = value(v, '');
  if (!s) return 'Not observed';
  const d = new Date(s);
  return Number.isNaN(d.valueOf())
    ? 'Not observed'
    : new Intl.DateTimeFormat('en-GB', {
        dateStyle: 'medium',
        timeStyle: 'medium',
      }).format(d);
}
export function jsonText(v: unknown): string {
  return stringify(v, undefined, 2) ?? 'null';
}
export function tone(v: unknown): 'success' | 'warning' | 'danger' | 'neutral' {
  const s = value(v, '');
  if (
    [
      'healthy',
      'ready',
      'satisfied',
      'processed',
      'acknowledged',
      'current',
      'connected',
      'complete',
      'passed',
    ].includes(s)
  )
    return 'success';
  if (
    [
      'unavailable',
      'blocked',
      'dead_letter',
      'failed',
      'disconnected',
      'quarantined',
      'missing',
    ].includes(s)
  )
    return 'danger';
  if (
    [
      'degraded',
      'stale',
      'retry_wait',
      'leased',
      'pending',
      'complete_with_errors',
      'pausing',
    ].includes(s)
  )
    return 'warning';
  return 'neutral';
}
export function age(v: unknown): string {
  const s = value(v, '');
  if (!s) return 'No pending work';
  const n = Number(s);
  if (!Number.isFinite(n) || n < 0) return 'Unknown';
  return n < 60
    ? `${Math.floor(n)}s`
    : n < 3600
      ? `${Math.floor(n / 60)}m ${Math.floor(n % 60)}s`
      : `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}m`;
}
export function sumRows(
  items: unknown,
  sink: string,
  states: readonly string[],
): string | null {
  if (!Array.isArray(items)) return null;
  let sum = 0n;
  for (const row of records(items)) {
    if (row['sink'] !== sink || !states.includes(value(row['state']))) continue;
    const n = decimal(row['count']);
    if (n === null) return null;
    sum += BigInt(n);
  }
  return sum.toString();
}

export function formText(data: FormData, key: string): string {
  const v = data.get(key);
  return typeof v === 'string' ? v : '';
}
