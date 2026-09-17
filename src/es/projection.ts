import { parse, stringify, isLosslessNumber } from 'lossless-json';
import { object } from './transport.ts';
const searchMapping = {
  dynamic: 'strict',
  properties: {
    projection_schema: { type: 'keyword' },
    source_epoch: { type: 'keyword' },
    entity_id: { type: 'keyword' },
    entity_version: { type: 'keyword' },
    is_deleted: { type: 'boolean' },
    canonical_body_json: { type: 'keyword', index: false, doc_values: false },
    content_sha256: { type: 'keyword' },
    search_fields: {
      dynamic: 'strict',
      properties: {
        name: { type: 'text' },
        country: { type: 'keyword' },
        loyalty_points: {
          type: 'integer',
          coerce: false,
          ignore_malformed: false,
        },
      },
    },
  },
};
export function configuration(
  pipelineId: string,
  epoch: string,
  registrationId: string,
) {
  return {
    settings: {
      number_of_shards: '1',
      number_of_replicas: '0',
      translog: { durability: 'request' },
      mapping: { source: { mode: 'stored' } },
    },
    mappings: {
      ...searchMapping,
      _meta: {
        pipeline_id: pipelineId,
        source_epoch: epoch,
        registration_id: registrationId,
        projection_schema: 'search-v1',
      },
    },
  };
}
function sorted(value: unknown): unknown {
  if (isLosslessNumber(value) || value === null || typeof value !== 'object')
    return value;
  if (Array.isArray(value)) return value.map(sorted);
  return Object.fromEntries(
    Object.entries(object(value))
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => [k, sorted(v)]),
  );
}
export function exactJson(value: unknown): string {
  const text = stringify(sorted(value));
  if (typeof text !== 'string')
    throw new Error('Unserializable receiver evidence');
  return text;
}
export function sameProjection(actual: unknown, expected: string): boolean {
  const expectedValue: unknown = parse(expected);
  return exactJson(actual) === exactJson(expectedValue);
}
export interface Projection {
  eventId: string;
  documentId: string;
  version: string;
  json: string | null;
  bytes: string;
}
export function bulkLine(p: Projection): string {
  if (
    p.json === null ||
    !/^[1-9][0-9]*$/.test(p.version) ||
    BigInt(p.version) > 9223372036854775807n
  )
    throw new Error('Invalid or oversized projection');
  const line = `{"index":{"_id":${JSON.stringify(p.documentId)},"version":${p.version},"version_type":"external"}}\n${p.json}\n`;
  if (Buffer.byteLength(line) > 262144)
    throw new Error('Projected operation exceeds 256 KiB');
  return line;
}
