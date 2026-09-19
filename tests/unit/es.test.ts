import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parse } from 'lossless-json';
import { bulkLine, sameProjection } from '../../src/es/projection.ts';
import { version } from '../../src/es/transport.ts';
import {
  checkClusterIdentity,
  EsFailure,
  validateBulkResponse,
} from '../../src/es/adapter.ts';
const projection = {
  eventId: '00000000-0000-4000-8000-000000000001:7:9007199254740993',
  documentId: '00000000-0000-4000-8000-000000000001:7',
  version: '9007199254740993',
  json: '{"projection_schema":"search-v1","entity_id":"7","search_fields":{"loyalty_points":9007199254740993,"name":"Ω"}}',
  bytes: '142',
};
await test('ES unit unavailable startup identity admits no write and only that marker is transient', () => {
  checkClusterIdentity('retained-cluster', 'retained-cluster');
  for (const actual of ['_na_', 'different-cluster', '', null, undefined])
    assert.throws(
      () => checkClusterIdentity(actual, 'retained-cluster'),
      (error: unknown) =>
        error instanceof EsFailure &&
        error.classification ===
          (actual === '_na_' ? 'transient' : 'configuration'),
    );
});
await test('ES unit golden raw NDJSON preserves exact integer tokens and final newline', () => {
  const expected =
    '{"index":{"_id":"00000000-0000-4000-8000-000000000001:7","version":9007199254740993,"version_type":"external"}}\n{"projection_schema":"search-v1","entity_id":"7","search_fields":{"loyalty_points":9007199254740993,"name":"Ω"}}\n';
  assert.equal(bulkLine(projection), expected);
  for (const token of ['9007199254740993', '9223372036854775807'])
    assert.equal(version(parse(token)), token);
  for (const value of [
    Number(9007199254740993n),
    parse('9223372036854775808'),
    parse('0'),
    parse('1.1'),
    parse('1e3'),
    '1',
  ])
    assert.throws(() => version(value));
});
await test('ES unit projection comparison checks values types and all keys, never hash alone', () => {
  const input: unknown = parse(projection.json);
  assert.equal(sameProjection(input, projection.json), true);
  assert.equal(
    sameProjection(
      parse(projection.json.replace('9007199254740993', '9007199254740992')),
      projection.json,
    ),
    false,
  );
  assert.equal(
    sameProjection(
      parse(projection.json.replace('"Ω"', 'null')),
      projection.json,
    ),
    false,
  );
  assert.equal(
    sameProjection(
      parse(projection.json.replace('"Ω"', '"GE"')),
      projection.json,
    ),
    false,
  );
  assert.equal(
    sameProjection({ ...projection, extra: true }, projection.json),
    false,
  );
});
await test('ES unit malformed and contradictory bulk correspondence fails closed', () => {
  const reply: unknown = parse(
    '{"errors":false,"items":[{"index":{"_index":"kit-test","_id":"00000000-0000-4000-8000-000000000001:7","_version":9007199254740993,"status":201,"result":"created"}}]}',
  );
  assert.equal(
    validateBulkResponse(reply, [projection], 'kit-test')[0]?.remote,
    '9007199254740993',
  );
  for (const value of [
    {},
    [],
    null,
    { errors: false, items: [] },
    { errors: true, items: [] },
    parse('{"errors":false,"items":[{"delete":{}}]}'),
  ])
    assert.throws(() => validateBulkResponse(value, [projection], 'kit-test'));
  assert.throws(() =>
    validateBulkResponse(reply, [projection, projection], 'kit-test'),
  );
  assert.throws(() => validateBulkResponse(reply, [projection], 'wrong-index'));
});
