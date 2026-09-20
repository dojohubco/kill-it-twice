import test from 'node:test';
import assert from 'node:assert/strict';
import { parse } from 'lossless-json';
import { exactJsonReplacer } from '../../src/operations/serialization.ts';
void test('Operational JSON preserves exact receiver number tokens and string identifiers', () => {
  const wire =
    '{"entity_id":"9223372036854775807","receiver_version":"9007199254740993","search_fields":{"loyalty_points":42},"exact":9007199254740993}';
  assert.equal(JSON.stringify(parse(wire), exactJsonReplacer), wire);
});
