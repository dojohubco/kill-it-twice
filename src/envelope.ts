import { createHash } from 'node:crypto';
import { positiveBigint } from './source.ts';
import { byteBound } from './limits.ts';
export const codec = 'pg18-jsonb-text/v1';
interface Body {
  schema_version: 1;
  source_epoch: string;
  entity_id: string;
  entity_version: string;
  event_id: string;
  source_change_id: string | null;
  source_recorded_at: string;
  kind: 'mutation' | 'baseline';
  is_deleted: boolean;
  payload_encoding: typeof codec;
  payload_json: string | null;
}
export interface CanonicalEvent {
  body: Body;
  bodyBytes: Buffer;
  contentSha256: string;
  wireBytes: Buffer;
}
const keys = [
  'schema_version',
  'source_epoch',
  'entity_id',
  'entity_version',
  'event_id',
  'source_change_id',
  'source_recorded_at',
  'kind',
  'is_deleted',
  'payload_encoding',
  'payload_json',
].sort();
export function uuid(value: unknown): string {
  if (
    typeof value !== 'string' ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(
      value,
    )
  )
    throw new TypeError('Expected canonical lowercase UUID');
  return value;
}
function timestamp(value: unknown): string {
  if (
    typeof value !== 'string' ||
    !/^(?!0000)\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{6}Z$/.test(
      value,
    )
  )
    throw new TypeError('Expected canonical UTC microsecond timestamp');
  // Date is only a calendar validator; original six-digit precision remains untouched.
  const date = new Date(value);
  if (
    !Number.isFinite(date.getTime()) ||
    date.toISOString().slice(0, 19) !== value.slice(0, 19)
  )
    throw new TypeError('Invalid timestamp calendar date');
  return value;
}
function bodyValue(value: unknown): Body {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new TypeError('Expected envelope body');
  const row = value as Record<string, unknown>;
  if (
    Reflect.ownKeys(row).some((key) => typeof key !== 'string') ||
    Object.getOwnPropertyNames(row).sort().join(',') !== keys.join(',')
  )
    throw new TypeError('Unknown or missing body fields');
  for (const v of Object.values(row))
    if (typeof v === 'string' && !v.isWellFormed())
      throw new TypeError('Lone surrogate in envelope');
  const epoch = uuid(row['source_epoch']),
    id = positiveBigint(row['entity_id']),
    version = positiveBigint(row['entity_version']);
  if (
    row['schema_version'] !== 1 ||
    row['event_id'] !== `${epoch}:${id}:${version}` ||
    row['payload_encoding'] !== codec ||
    typeof row['is_deleted'] !== 'boolean'
  )
    throw new TypeError('Invalid envelope identity or encoding');
  const kind = row['kind'],
    change = row['source_change_id'];
  if (kind === 'mutation') uuid(change);
  else if (kind !== 'baseline' || change !== null)
    throw new TypeError('Invalid revision kind/change identity');
  const payload = row['payload_json'];
  if (row['is_deleted'] ? payload !== null : typeof payload !== 'string')
    throw new TypeError('Invalid deletion/payload state');
  if (
    !(change === null || typeof change === 'string') ||
    !(payload === null || typeof payload === 'string')
  )
    throw new TypeError('Invalid envelope text');
  return {
    schema_version: 1,
    source_epoch: epoch,
    entity_id: id,
    entity_version: version,
    event_id: `${epoch}:${id}:${version}`,
    source_change_id: change,
    source_recorded_at: timestamp(row['source_recorded_at']),
    kind,
    is_deleted: row['is_deleted'],
    payload_encoding: codec,
    payload_json: payload,
  };
}
// RFC 8785 restricted to this fixed flat body. No nested payload parsing or general number serializer.
export function canonicalEvent(input: unknown): CanonicalEvent {
  const body = bodyValue(input);
  const entries = Object.entries(body).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  );
  const text =
    '{' +
    entries
      .map(([key, value]) => `${JSON.stringify(key)}:${JSON.stringify(value)}`)
      .join(',') +
    '}';
  const bodyBytes = Buffer.from(text, 'utf8');
  const contentSha256 = createHash('sha256').update(bodyBytes).digest('hex');
  const wireBytes = Buffer.from(
    `{"body":${text},"content_sha256":"${contentSha256}"}`,
    'utf8',
  );
  byteBound([wireBytes.length]);
  return { body, bodyBytes, contentSha256, wireBytes };
}
export function validateEvent(bytes: Uint8Array, hash: string): CanonicalEvent {
  byteBound([bytes.byteLength]);
  const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  // Only the flat envelope is parsed. Its payload_json stays an opaque string.
  const parsed: unknown = JSON.parse(text);
  const event = canonicalEvent(parsed);
  if (!event.bodyBytes.equals(bytes) || hash !== event.contentSha256)
    throw new TypeError('Noncanonical body bytes or incorrect content hash');
  return event;
}
