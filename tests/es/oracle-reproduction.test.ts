import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stringify } from 'lossless-json';
import {
  EsTransport,
  object,
  version,
  exactInteger,
} from '../../src/es/transport.ts';
import { oracleReproductionCases } from '../../scripts/required-oracle-cases.ts';
import { first } from '../../scripts/rows.ts';
import { errorText } from '../../scripts/support.ts';
import { historicalM3Oracle } from '../support/es-historical-oracle.ts';
import {
  setup,
  create,
  drain,
  finish,
  remote,
  esConfig,
} from '../support/es.ts';
import { evidence } from '../support/db.ts';

await test(first(oracleReproductionCases).name, async (t) => {
  const { s, p, es, admin, target } = await setup(t);
  await drain();
  await finish(es, p);
  const valid = await create(
    '{"name":"retained-v1","country":"GE","loyalty_points":7,"precise":9007199254740993}',
  );
  await drain();
  await finish(es, p);
  const before = await remote(es, valid.documentId);
  assert.equal(version(before['_version']), '1');
  await admin.request('POST', `/${target.index}/_refresh`);
  await historicalM3Oracle(s, p, es);

  const rejected = await create(
    '{"name":"rejected-v2","loyalty_points":"not-a-number"}',
    valid.id,
    'update',
  );
  assert.equal(rejected.reply.result.entity_version, '2');
  await drain();
  class Recording extends EsTransport {
    readonly replies: unknown[] = [];
    override async request(...args: Parameters<EsTransport['request']>) {
      const reply = await super.request(...args);
      if (args[1].endsWith('/_bulk')) this.replies.push(reply);
      return reply;
    }
  }
  const recorded = new Recording(esConfig());
  t.after(() => recorded.close());
  await finish(recorded, p);
  assert.equal(recorded.replies.length, 1);
  const items = object(recorded.replies[0])['items'];
  assert.ok(Array.isArray(items));
  assert.equal(items.length, 1);
  const item = object(object(items[0])['index']);
  assert.equal(exactInteger(item['status']), '400');
  assert.equal(object(item['error'])['type'], 'document_parsing_exception');
  const errors = (
    await p.query<Record<string, unknown>>(
      'SELECT * FROM pipeline.es_dead_letters WHERE event_id=$1',
      [rejected.eventId],
    )
  ).rows;
  assert.equal(errors.length, 1);
  assert.match(
    String(first(errors)['context']),
    /search_fields.loyalty_points/,
  );
  const after = await remote(es, valid.documentId);
  assert.deepEqual(
    after,
    before,
    'Rejected version 2 must leave the complete version 1 document unchanged',
  );
  await admin.request('POST', `/${target.index}/_refresh`);
  const search = object(
    await es.request(
      'POST',
      `/${target.index}/_search`,
      JSON.stringify({ size: 1000, query: { match_all: {} } }),
    ),
  );
  const hits = object(search['hits'])['hits'];
  assert.ok(Array.isArray(hits));
  assert.ok(hits.some((h) => object(h)['_id'] === valid.documentId));
  let failure: unknown;
  try {
    await historicalM3Oracle(s, p, es);
  } catch (error) {
    failure = error;
  }
  assert.ok(
    failure instanceof assert.AssertionError,
    'The historical oracle must actually fail',
  );
  assert.equal(failure.operator, 'deepStrictEqual');
  assert.ok(Array.isArray(failure.actual) && Array.isArray(failure.expected));
  assert.ok(failure.actual.includes(valid.documentId));
  assert.ok(!failure.expected.includes(valid.documentId));
  evidence('OR00', {
    baseline: 'c3cb8f7c8398d8db4d158f2307f790244f4199e6',
    valid: { command: valid.command, eventId: valid.eventId },
    rejected: { command: rejected.command, eventId: rejected.eventId },
    before: stringify(before),
    after: stringify(after),
    actualBulk: stringify(recorded.replies[0]),
    deadLetters: errors,
    search: stringify(search),
    historicalOracleFailure: errorText(failure),
    actualIdentitySet: failure.actual,
    expectedIdentitySet: failure.expected,
    conclusion:
      'Receiver retained exact v1; historical oracle falsely excluded this entity after v2 rejection. No production data loss.',
  });
});
