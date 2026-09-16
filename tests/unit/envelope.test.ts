import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { canonicalEvent, validateEvent } from '../../src/envelope.ts';
import { object } from '../../scripts/acceptance.ts';
// Checked-in vectors independently prepared using Python json(sort_keys, ensure_ascii=False,
// separators=(',',':')) and hashlib, for this flat, scalar-only envelope. No production serializer.
async function vectors() {
  const value: unknown = JSON.parse(
    await readFile(
      new URL('../fixtures/envelopes.json', import.meta.url),
      'utf8',
    ),
  );
  assert.ok(Array.isArray(value));
  return value.map((v: unknown) => object(v));
}
void test('S01-GOLDEN exact flat-envelope RFC8785 vectors', async () => {
  for (const vector of await vectors()) {
    const event = canonicalEvent(vector['body']);
    assert.equal(event.bodyBytes.toString('utf8'), vector['canonical']);
    assert.equal(event.contentSha256, vector['sha256']);
    assert.equal(event.wireBytes.toString('utf8'), vector['wire']);
    assert.deepEqual(
      validateEvent(event.bodyBytes, event.contentSha256),
      event,
    );
    assert.deepEqual(
      canonicalEvent(Object.fromEntries(Object.entries(event.body).reverse())),
      event,
    );
  }
});
void test('S01-REJECT malformed envelope bytes, identities, timestamps and Unicode', async () => {
  const vector = (await vectors())[0];
  assert.ok(vector);
  const b = object(vector['body']);
  const valid = canonicalEvent(b);
  for (const override of [
    { extra: 1 },
    { schema_version: null },
    { schema_version: 1.1 },
    { source_epoch: 'ABC' },
    { entity_id: 9007199254740992 },
    { entity_id: '01' },
    { entity_version: '9223372036854775808' },
    { event_id: 'other' },
    { source_recorded_at: '2026-02-30T12:34:56.123456Z' },
    { source_recorded_at: '2026-09-16T12:34:56.123Z' },
    { source_recorded_at: '0000-01-01T00:00:00.000000Z' },
    { kind: 'backfill' },
    { source_change_id: null },
    { kind: 'baseline' },
    { is_deleted: true },
    { is_deleted: null },
    { payload_json: null },
    { payload_json: '\ud800' },
    { payload_encoding: 'json/v2' },
  ])
    assert.throws(() => canonicalEvent({ ...b, ...override }));
  const missing = { ...b };
  delete missing['payload_json'];
  assert.throws(() => canonicalEvent(missing));
  assert.throws(() => validateEvent(Buffer.from([0xff]), valid.contentSha256));
  assert.throws(() =>
    validateEvent(
      Buffer.from(valid.bodyBytes.toString() + ' '),
      valid.contentSha256,
    ),
  );
  assert.throws(() =>
    validateEvent(Buffer.from(JSON.stringify(b)), valid.contentSha256),
  );
  assert.throws(() => validateEvent(valid.bodyBytes, '0'.repeat(64)));
  assert.throws(() =>
    validateEvent(
      Buffer.from(valid.bodyBytes.toString().replace('ქართული', '\\ud800')),
      valid.contentSha256,
    ),
  );
});
