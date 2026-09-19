import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parse, stringify } from 'lossless-json';
import { EsTransport, object, exactInteger } from '../../src/es/transport.ts';
import { oracleCases } from '../../scripts/required-oracle-cases.ts';
import { first } from '../../scripts/rows.ts';
import { evidence } from '../support/db.ts';
import {
  setup,
  create,
  drain,
  finish,
  remote,
  esConfig,
  oracle,
} from '../support/es.ts';
import {
  declareMappingRejection,
  checkExpectedOutcome,
  checkReceiver,
  checkSearchIdentities,
  type ExpectedRejection,
  type ExpectedReceiver,
} from '../support/es-expectations.ts';
const expectations: ExpectedRejection[] = [];
const name = (id: string) => first(oracleCases.filter((c) => c.id === id)).name;
let initial: Awaited<ReturnType<typeof create>> | undefined;
let invalidUpdate: Awaited<ReturnType<typeof create>> | undefined;
async function rejectActual(
  f: Awaited<ReturnType<typeof create>>,
  c: Awaited<ReturnType<typeof setup>>,
  fixture: string,
) {
  const declared = await declareMappingRejection(c.s, c.p, f, fixture);
  expectations.push(declared);
  await drain();
  class Recorded extends EsTransport {
    readonly responses: unknown[] = [];
    override async request(...args: Parameters<EsTransport['request']>) {
      const response = await super.request(...args);
      if (args[1].endsWith('/_bulk')) this.responses.push(response);
      return response;
    }
  }
  const recording = new Recorded(esConfig());
  let replies: unknown[];
  try {
    await finish(recording, c.p);
    replies = recording.responses;
  } finally {
    await recording.close();
  }
  assert.equal(replies.length, 1);
  const items = object(replies[0])['items'];
  assert.ok(Array.isArray(items));
  const rejected = items
    .map((v) => object(object(v)['index']))
    .filter((i) => i['_id'] === f.documentId);
  assert.equal(rejected.length, 1);
  const item = first(rejected);
  assert.equal(exactInteger(item['status']), '400');
  assert.equal(object(item['error'])['type'], declared.errorType);
  assert.ok(String(object(item['error'])['reason']).includes(declared.field));
  const diagnostic = (
    await c.p.query<Record<string, unknown>>(
      'SELECT * FROM pipeline.es_dead_letters WHERE event_id=$1',
      [f.eventId],
    )
  ).rows;
  assert.equal(diagnostic.length, 1);
  evidence(`${fixture}-actual-rejection`, {
    declared,
    actualItem: stringify(item),
    diagnostic,
  });
  await c.admin.request('POST', `/${c.target.index}/_refresh`);
  return { declared, diagnostic, item: stringify(item) };
}
async function reconcile(c: Awaited<ReturnType<typeof setup>>) {
  await c.admin.request('POST', `/${c.target.index}/_refresh`);
  return oracle(c.s, c.p, c.es, expectations);
}
async function retainedFailure(
  c: Awaited<ReturnType<typeof setup>>,
  eventId: string,
) {
  return {
    event: (
      await c.p.query(
        'SELECT row_to_json(e)::text text FROM pipeline.events e WHERE event_id=$1',
        [eventId],
      )
    ).rows,
    deadLetter: (
      await c.p.query(
        'SELECT row_to_json(d)::text text FROM pipeline.es_dead_letters d WHERE event_id=$1',
        [eventId],
      )
    ).rows,
  };
}
await test(name('O01'), async (t) => {
  const c = await setup(t);
  await drain();
  await finish(c.es, c.p);
  initial = await create(
    '{"name":"exact old live Ω","country":"GE","loyalty_points":7,"big":9007199254740993,"decimal":1.1234567890123456789}',
  );
  await drain();
  await finish(c.es, c.p);
  const before = await remote(c.es, initial.documentId);
  invalidUpdate = await create(
    '{"name":"invalid later","loyalty_points":"not-a-number"}',
    initial.id,
    'update',
  );
  const rejection = await rejectActual(invalidUpdate, c, 'O01');
  const after = await remote(c.es, initial.documentId);
  assert.deepEqual(after, before);
  const row = first(
    (await reconcile(c)).filter((r) => r.id === invalidUpdate?.eventId),
  );
  assert.equal(row.sourceRevision, '2');
  assert.equal(row.expectedReceiverRevision, '1');
  assert.equal(row.version, '1');
  assert.equal(row.convergence, 'EXPECTED DEGRADED');
  evidence('O01', {
    original: stringify(before),
    retained: stringify(after),
    rejection,
    row,
  });
});
await test(name('O02'), async (t) => {
  const c = await setup(t);
  const f = await create(
    '{"name":"initial rejection","loyalty_points":"not-a-number"}',
  );
  const rejection = await rejectActual(f, c, 'O02');
  const row = first((await reconcile(c)).filter((r) => r.id === f.eventId));
  assert.equal(row.expectedReceiverRevision, null);
  assert.equal(row.source, null);
  assert.equal(row.convergence, 'EXPECTED DEGRADED');
  evidence('O02', { rejection, row, requiredAbsence: true });
});
await test(name('O03'), async (t) => {
  assert.ok(initial && invalidUpdate);
  const c = await setup(t);
  const preserved = await retainedFailure(c, invalidUpdate.eventId);
  const f = await create(
    '{"name":"valid correction","loyalty_points":9}',
    initial.id,
    'update',
  );
  await drain();
  await finish(c.es, c.p);
  const row = first((await reconcile(c)).filter((r) => r.id === f.eventId));
  assert.equal(row.sourceRevision, '3');
  assert.equal(row.version, '3');
  assert.equal(row.convergence, 'CONVERGED');
  assert.deepEqual(await retainedFailure(c, invalidUpdate.eventId), preserved);
  evidence('O03', {
    row,
    preservedRejectedV2: preserved,
    receiver: stringify(await remote(c.es, f.documentId)),
  });
});
await test(name('O04'), async (t) => {
  const c = await setup(t);
  const live = await create();
  await drain();
  await finish(c.es, c.p);
  const tombstone = await create('{}', live.id, 'delete');
  await drain();
  await finish(c.es, c.p);
  const before = await remote(c.es, live.documentId);
  assert.equal(object(before['_source'])['is_deleted'], true);
  const bad = await create(
    '{"name":"bad restore","loyalty_points":"not-a-number"}',
    live.id,
    'restore',
  );
  const rejection = await rejectActual(bad, c, 'O04');
  assert.deepEqual(await remote(c.es, live.documentId), before);
  const degraded = first(
    (await reconcile(c)).filter((r) => r.id === bad.eventId),
  );
  assert.equal(degraded.expectedReceiverRevision, '2');
  assert.equal(degraded.convergence, 'EXPECTED DEGRADED');
  const failed = await retainedFailure(c, bad.eventId);
  // The rejected restore changed source lifecycle to live; its correction is an update.
  const corrected = await create(
    '{"name":"valid restored revision","loyalty_points":11}',
    live.id,
    'update',
  );
  await drain();
  await finish(c.es, c.p);
  const current = first(
    (await reconcile(c)).filter((r) => r.id === corrected.eventId),
  );
  assert.equal(current.version, '4');
  assert.equal(current.convergence, 'CONVERGED');
  assert.equal(object(current.source)['is_deleted'], false);
  assert.deepEqual(await retainedFailure(c, bad.eventId), failed);
  evidence('O04', {
    tombstone: tombstone.eventId,
    before: stringify(before),
    rejection,
    degraded,
    current,
    failed,
  });
});
await test(name('O05'), async (t) => {
  const c = await setup(t);
  assert.ok(initial);
  const before = await remote(c.es, initial.documentId);
  const model: ExpectedReceiver = {
    eventId: `${initial.documentId}:3`,
    documentId: initial.documentId,
    version: '3',
    source: object(before['_source']),
  };
  // Pure negative controls only: no fabricated status or diagnostic is written to a database.
  assert.throws(
    () =>
      checkExpectedOutcome(model.eventId, 'dead_letter', undefined, {
        event_id: model.eventId,
        error_class: 'mapping',
      }),
    /Undeclared/,
  );
  assert.throws(
    () =>
      checkReceiver(model.documentId, model, {
        _id: model.documentId,
        found: false,
      }),
    /Missing admissible/,
  );
  const declared = first(expectations);
  assert.throws(
    () =>
      checkExpectedOutcome(declared.eventId, 'satisfied', declared, undefined),
    /not actual failure evidence/,
  );
  await reconcile(c);
  assert.deepEqual(await remote(c.es, initial.documentId), before);
  evidence('O05', {
    kind: 'Pure snapshot negative controls; no manufactured real receiver rejection',
    rejected: [
      'unknown dead letter',
      'missing admissible document',
      'planned rejection without evidence',
    ],
    receiverUnchanged: true,
  });
});
await test(name('O06'), async (t) => {
  const c = await setup(t);
  const f = await create(
    '{"name":"retained negative-control live","loyalty_points":13}',
  );
  await drain();
  await finish(c.es, c.p);
  const before = await remote(c.es, f.documentId);
  const bad = await create(
    '{"name":"later rejected","loyalty_points":"not-a-number"}',
    f.id,
    'update',
  );
  const rejection = await rejectActual(bad, c, 'O06');
  await reconcile(c);
  const model: ExpectedReceiver = {
    eventId: f.eventId,
    documentId: f.documentId,
    version: '1',
    source: object(before['_source']),
  };
  const negative: string[] = [];
  for (const [label, doc] of [
    ['wrong version', { ...before, _version: parse('2') }],
    [
      'wrong projected field',
      {
        ...before,
        _source: { ...model.source, search_fields: { name: 'wrong' } },
      },
    ],
    [
      'wrong tombstone',
      { ...before, _source: { ...model.source, is_deleted: true } },
    ],
    [
      'copied hash with changed canonical content',
      { ...before, _source: { ...model.source, canonical_body_json: '{}' } },
    ],
  ] as const) {
    assert.throws(() => checkReceiver(f.documentId, model, doc));
    negative.push(label);
  }
  assert.throws(() =>
    checkSearchIdentities(
      [f.documentId],
      [{ _id: f.documentId }, { _id: 'owned-extra' }],
    ),
  );
  negative.push('extra identity');
  const restore = async (source: unknown) => {
    const reply = object(
      await c.admin.request(
        'POST',
        `/${c.target.index}/_bulk`,
        `{"index":{"_id":${JSON.stringify(f.documentId)},"version":1,"version_type":"external_gte"}}\n${stringify(source)}\n`,
      ),
    );
    assert.equal(
      reply['errors'],
      false,
      'Privileged negative-control restoration must succeed',
    );
  };
  // Privileged fault fixture only: preserve version 1 in the deletion tombstone so restoration is exact.
  await c.admin.request(
    'DELETE',
    `/${c.target.index}/_doc/${encodeURIComponent(f.documentId)}?version=1&version_type=external_gte`,
  );
  try {
    await assert.rejects(reconcile(c), /Missing admissible/);
    negative.push('actual missing retained old document');
  } finally {
    await restore(model.source);
  }
  await restore({
    ...model.source,
    search_fields: { name: 'receiver corruption', loyalty_points: 13 },
  });
  try {
    await assert.rejects(reconcile(c), /Receiver content mismatch/);
    negative.push('actual changed retained old document');
  } finally {
    await restore(model.source);
  }
  const row = first((await reconcile(c)).filter((r) => r.id === bad.eventId));
  assert.equal(row.version, '1');
  assert.equal(row.convergence, 'EXPECTED DEGRADED');
  assert.deepEqual(
    (await remote(c.es, f.documentId))['_source'],
    before['_source'],
  );
  evidence('O06', {
    rejection,
    negative,
    original: stringify(before),
    restored: stringify(await remote(c.es, f.documentId)),
    row,
    privilegedControlsOnly: true,
  });
});
