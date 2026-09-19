import { launchCapture } from '../support/capture-process.ts';
import { launchEs } from '../support/es-process.ts';
import { command, waitFor, withCleanup } from '../../scripts/support.ts';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { parse, stringify } from 'lossless-json';
import { setTimeout as delay } from 'node:timers/promises';
import { esCases } from '../../scripts/required-es-cases.ts';
import {
  EsTransport,
  object,
  exactInteger,
  version,
} from '../../src/es/transport.ts';
import { EsAdapter } from '../../src/es/adapter.ts';
import { bulkLine } from '../../src/es/projection.ts';
import { required, evidence, databaseWaitFor } from '../support/db.ts';
import { work } from '../support/capture.ts';
import { pipeline, reader } from '../support/staging.ts';
import {
  setup,
  drain,
  ledger,
  worker,
  create,
  staged,
  deliveries,
  finish,
  untouched,
  remote,
  oracle,
  esConfig,
} from '../support/es.ts';
import { first } from '../../scripts/rows.ts';
function name(id: string) {
  const c = esCases.find((c) => c.id === id);
  assert.ok(c);
  return c.name;
}

await test(name('ES01'), async (t) => {
  const { p, es, admin, target } = await setup(t);
  await new EsAdapter(es).validate(target);
  assert.equal(target.mode, 'ready');
  const index = `kit-protocol-${randomUUID()}`;
  await admin.request(
    'PUT',
    `/${index}`,
    JSON.stringify({
      settings: { number_of_shards: 1, number_of_replicas: 0 },
      mappings: {
        dynamic: 'strict',
        properties: {
          loyalty_points: {
            type: 'integer',
            coerce: false,
            ignore_malformed: false,
          },
        },
      },
    }),
  );
  const observations = [];
  try {
    for (const v of ['9007199254740993', '9223372036854775807'])
      for (const [sent, status] of [
        [v, '201'],
        [v, '409'],
        [(BigInt(v) - 1n).toString(), '409'],
      ] as const) {
        const body = `{"index":{"_id":"${v}","version":${sent},"version_type":"external"}}\n{"loyalty_points":42}\n`;
        const reply = object(
          await admin.request('POST', `/${index}/_bulk`, body),
        );
        assert.ok(Array.isArray(reply['items']));
        const item = object(object(reply['items'][0])['index']);
        assert.equal(exactInteger(item['status']), status);
        const get = object(await admin.request('GET', `/${index}/_doc/${v}`));
        assert.equal(version(get['_version']), v);
        observations.push({
          request: body,
          status,
          actualVersion: version(get['_version']),
        });
      }
  } finally {
    await admin.request('DELETE', `/${index}`);
  }
  const role = object(
    await admin.request('GET', `/_security/role/${required('ES_USERNAME')}`),
  );
  const d = await p.query(
    'SELECT kind,state,receiver_identity FROM pipeline.destinations ORDER BY kind',
  );
  evidence('ES01', {
    targetIds: {
      cluster: target.clusterUuid,
      index: target.indexUuid,
      destination: target.id,
    },
    configuration: target.configuration,
    server: await es.request('GET', '/'),
    role,
    destinations: d.rows,
    syntheticProtocolVersions: observations,
  });
});

await test(name('ES02'), async (t) => {
  const { p, s, es, admin } = await setup(t);
  const f = await staged(
    '{"name":"თორნიკე Ω","country":"GE","loyalty_points":42,"precise":9007199254740993,"decimal":0.12345678901234567890123456789,"array":[null,9007199254740993]}',
  );
  const before = await untouched(p);
  const results = await finish(es, p);
  assert.deepEqual(await untouched(p), before);
  const doc = await remote(es, f.documentId);
  const source = object(doc['_source']);
  assert.equal(source['entity_id'], f.id);
  assert.equal(source['entity_version'], '1');
  assert.equal(source['is_deleted'], false);
  assert.deepEqual(
    source['search_fields'],
    parse('{"country":"GE","loyalty_points":42,"name":"თორნიკე Ω"}'),
  );
  assert.match(String(source['canonical_body_json']), /9007199254740993/);
  assert.match(
    String(source['canonical_body_json']),
    /0.12345678901234567890123456789/,
  );
  await admin.request('POST', `/${required('ES_INDEX')}/_refresh`);
  const found = object(
    await es.request(
      'POST',
      `/${required('ES_INDEX')}/_search`,
      JSON.stringify({ query: { match: { 'search_fields.name': 'თორნიკე' } } }),
    ),
  );
  const foundHits = object(found['hits'])['hits'];
  assert.ok(Array.isArray(foundHits));
  assert.equal(foundHits.length, 1);
  const ack = await s.query<Record<string, unknown>>(
    'SELECT state,acknowledged_hash FROM source.capture_work WHERE entity_id=$1',
    [f.id],
  );
  assert.equal(first(ack.rows)['state'], 'acknowledged');
  evidence('ES02', {
    command: f.command.commandId,
    eventId: f.eventId,
    doc,
    ack: ack.rows,
    results,
  });
});

await test(name('ES03'), async (t) => {
  const { p, es, target } = await setup(t);
  const a = await create(),
    b = await create('{"name":"new","loyalty_points":55}', a.id, 'update'),
    c = await create('', a.id, 'delete'),
    d = await create(
      '{"name":"restored","loyalty_points":77}',
      a.id,
      'restore',
    );
  await drain();
  const l = ledger(),
    w = randomUUID(),
    adapter = new EsAdapter(es);
  const claims = await l.claim(target, w, 500, 30000);
  assert.equal(claims.length, 4);
  const seen = [];
  for (const f of [c, b, a, d]) {
    const claim = claims.find((c) => c.eventId === f.eventId);
    assert.ok(claim);
    const projection = await l.read(f.eventId);
    const raw = first(await adapter.bulk(target, [projection], randomUUID()));
    const outcome = await adapter.resolve(target, raw, l);
    await adapter.validate(target);
    assert.equal(
      await l.settle(
        target,
        claim,
        w,
        outcome.outcome,
        outcome.remote,
        outcome.witness,
        outcome.context,
        1,
      ),
      'settled',
    );
    const get = await remote(es, a.documentId);
    seen.push({
      eventId: f.eventId,
      outcome: outcome.outcome,
      remoteVersion: version(get['_version']),
      source: get['_source'],
    });
  }
  assert.deepEqual(
    seen.map((r) => r.remoteVersion),
    ['3', '3', '3', '4'],
  );
  assert.deepEqual(
    seen.map((r) => r.outcome),
    ['applied', 'superseded', 'superseded', 'applied'],
  );
  assert.equal(object(seen[0]?.source)['is_deleted'], true);
  assert.deepEqual(object(seen[0]?.source)['search_fields'], {});
  assert.equal(object(seen[3]?.source)['is_deleted'], false);
  const keep = await p.query<Record<string, unknown>>(
    "SELECT event_id,state FROM pipeline.delivery_intents WHERE kind='rabbitmq' AND event_id=ANY($1::text[]) ORDER BY event_id",
    [[a.eventId, b.eventId, c.eventId, d.eventId]],
  );
  assert.equal(keep.rows.length, 4);
  for (const r of keep.rows) assert.equal(r['state'], 'pending');
  evidence('ES03', { seen, retained: keep.rows });
});

await test(name('ES04'), async (t) => {
  const { p, es, admin, target } = await setup(t);
  const adapter = new EsAdapter(es),
    l = ledger();
  const f = await staged();
  const projection = await l.read(f.eventId);
  await adapter.bulk(target, [projection], randomUUID());
  const equal = await worker(es).once();
  assert.equal(equal.outcomes[0]?.outcome, 'already_applied');
  const cases = [];
  for (const corruption of [
    'copied_hash',
    'different_hash',
    'unknown_higher',
  ]) {
    const x = await staged();
    const independent = await staged();
    const expected = await l.read(x.eventId);
    assert.ok(expected.json);
    const bad = object(parse(expected.json));
    bad['search_fields'] = {
      name: 'forged',
      country: 'XX',
      loyalty_points: 77,
    };
    if (corruption === 'different_hash') bad['content_sha256'] = '0'.repeat(64);
    const json = stringify(bad);
    assert.ok(json);
    const v = corruption === 'unknown_higher' ? '2' : expected.version;
    await admin.request(
      'POST',
      `/${target.index}/_bulk`,
      `{"index":{"_id":${JSON.stringify(x.documentId)},"version":${v},"version_type":"external"}}\n${json}\n`,
    );
    await assert.rejects(worker(es).once(), /projection|witness/);
    const targetState = await l.target();
    assert.equal(targetState.mode, 'blocked');
    const state = await deliveries(p, [x.eventId]);
    assert.equal(state[0]?.['state'], 'retry_wait');
    const independentState = await deliveries(p, [independent.eventId]);
    assert.equal(independentState[0]?.['state'], 'satisfied');
    assert.equal(independentState[0]?.['disposition'], 'applied');
    cases.push({
      independentState,
      corruption,
      eventId: x.eventId,
      state,
      targetMode: targetState.mode,
    });
    // Isolated, explicitly privileged corruption control: restore only this owned document,
    // then reopen admission for the next case. This is not a runtime replay/rebind API.
    let repair = expected;
    if (corruption === 'unknown_higher') {
      const changed = await create(
        '{"name":"known repair","loyalty_points":10}',
        x.id,
        'update',
      );
      await drain();
      repair = await l.read(changed.eventId);
    }
    const restored = object(
      await admin.request(
        'POST',
        `/${target.index}/_bulk`,
        bulkLine(repair).replace(
          '"version_type":"external"',
          '"version_type":"external_gte"',
        ),
      ),
    );
    assert.equal(restored['errors'], false);
    await p.query("UPDATE pipeline.es_target SET mode='ready',reason=NULL");
    await finish(es, p);
  }
  evidence('ES04', { equal, cases });
});

await test(name('ES05'), async (t) => {
  const { p, es, s } = await setup(t);
  const fixtures: Awaited<ReturnType<typeof create>>[] = [];
  for (let i = 0; i < 500; i++)
    fixtures.push(
      await create(
        `{"name":"bulk-${i}","loyalty_points":${[17, 233, 499].includes(i) ? '"not-a-number"' : i}}`,
      ),
    );
  const capture = await drain('bulk-500-through-16-record-capture');
  assert.ok(capture.length >= 32);
  const before = await untouched(p);
  class RecordingTransport extends EsTransport {
    readonly replies: unknown[] = [];
    readonly bodies: string[] = [];
    readonly correlations: (string | undefined)[] = [];
    override async request(...args: Parameters<EsTransport['request']>) {
      const reply = await super.request(...args);
      if (args[1].endsWith('/_bulk')) {
        this.replies.push(reply);
        this.bodies.push(args[2] ?? '');
        this.correlations.push(args[3]);
      }
      return reply;
    }
  }
  const recording = new RecordingTransport(esConfig());
  t.after(() => recording.close());
  const result = await worker(recording).once();
  assert.equal(recording.replies.length, 1);
  assert.equal(recording.bodies[0]?.split('\n').length, 1001);
  evidence('ES05-actual-bulk', {
    response: stringify(recording.replies[0]),
    requestBytes: Buffer.byteLength(recording.bodies[0] ?? ''),
    requestAttemptId: recording.correlations[0],
    operations: 500,
  });

  assert.equal(result.claimed, 500);
  const requestAttempt = await p.query<{ event_id: string }>(
    'SELECT event_id FROM pipeline.es_attempts WHERE attempt_id=$1',
    [recording.correlations[0]],
  );
  assert.equal(requestAttempt.rows.length, 1);
  assert.ok(
    fixtures.some((f) => f.eventId === first(requestAttempt.rows).event_id),
  );
  assert.equal(
    result.outcomes.filter(
      (r) => r.outcome === 'applied' && r.status === 'settled',
    ).length,
    497,
  );
  assert.equal(
    result.outcomes.filter(
      (r) => r.outcome === 'mapping' && r.status === 'settled',
    ).length,
    3,
  );
  assert.deepEqual(await untouched(p), before);
  const mget = object(
    await es.request(
      'POST',
      `/${required('ES_INDEX')}/_mget`,
      JSON.stringify({ ids: fixtures.map((f) => f.documentId) }),
    ),
  );
  const docs = mget['docs'];
  assert.ok(Array.isArray(docs));
  assert.equal(docs.length, 500);
  const stored = (
    await p.query<{ event_id: string; body: string; hash: string }>(
      `SELECT event_id,convert_from(body_bytes,'UTF8') body,encode(sha256(body_bytes),'hex') hash FROM pipeline.events WHERE event_id=ANY($1::text[])`,
      [fixtures.map((f) => f.eventId)],
    )
  ).rows;
  for (let i = 0; i < 500; i++) {
    const doc = object(docs[i]),
      fixture = fixtures[i];
    assert.ok(fixture);
    assert.equal(doc['_id'], fixture.documentId);
    const valid = ![17, 233, 499].includes(i);
    assert.equal(doc['found'], valid);
    if (valid) {
      const source = object(doc['_source']),
        fields = object(source['search_fields']);
      assert.equal(version(doc['_version']), '1');
      assert.equal(fields['name'], `bulk-${i}`);
      assert.equal(exactInteger(fields['loyalty_points']), String(i));
      const e = stored.find((e) => e.event_id === fixture.eventId);
      assert.ok(e);
      assert.equal(source['canonical_body_json'], e.body);
      assert.equal(source['content_sha256'], e.hash);
    }
  }
  evidence('ES05-independent-mget', {
    appliedDocuments: 497,
    absentRejectedDocuments: 3,
    documents: stringify(docs),
  });
  const states = await deliveries(
    p,
    fixtures.map((f) => f.eventId),
  );
  assert.equal(states.length, 500);
  const failures = await p.query<Record<string, unknown>>(
    'SELECT * FROM pipeline.es_dead_letters WHERE event_id=ANY($1::text[]) ORDER BY event_id',
    [fixtures.map((f) => f.eventId)],
  );
  assert.equal(failures.rows.length, 3);
  for (const d of failures.rows)
    assert.match(String(d['context']), /document_parsing_exception/);
  const attemptsBefore = (
    await p.query('SELECT * FROM pipeline.es_attempts ORDER BY attempt_id')
  ).rows;
  assert.equal((await worker(es).once()).claimed, 0);
  assert.deepEqual(
    (await p.query('SELECT * FROM pipeline.es_attempts ORDER BY attempt_id'))
      .rows,
    attemptsBefore,
  );
  const bad = fixtures[17];
  assert.ok(bad);
  const repair = await create(
    '{"name":"repaired","loyalty_points":17}',
    bad.id,
    'update',
  );
  await drain();
  await finish(es, p);
  assert.equal(version((await remote(es, bad.documentId))['_version']), '2');
  assert.deepEqual(
    (
      await p.query(
        'SELECT * FROM pipeline.es_dead_letters WHERE event_id=ANY($1::text[]) ORDER BY event_id',
        [fixtures.map((f) => f.eventId)],
      )
    ).rows,
    failures.rows,
  );
  const ack = await s.query<Record<string, unknown>>(
    "SELECT count(*)::text count FROM source.capture_work WHERE entity_id=ANY($1::bigint[]) AND state='acknowledged'",
    [fixtures.map((f) => f.id)],
  );
  assert.equal(first(ack.rows)['count'], '501');
  evidence('ES05', {
    count: 500,
    sourceCaptureBatches: capture.length,
    result,
    states,
    actualReceiverErrors: failures.rows,
    repairEvent: repair.eventId,
  });
});

await test(name('ES06'), async (t) => {
  const { p, s, es, target } = await setup(t);
  const l = ledger();
  const rows = [];
  for (const state of [
    'pending',
    'leased',
    'retry_wait',
    'satisfied',
    'dead_letter',
  ]) {
    const f = await staged(
      state === 'dead_letter' ? '{"loyalty_points":"not-a-number"}' : undefined,
    );
    let claim;
    if (state === 'leased' || state === 'retry_wait') {
      const workerId = randomUUID();
      claim = first(await l.claim(target, workerId, 1, 1000));
      if (state === 'retry_wait')
        assert.equal(
          await l.settle(
            target,
            claim,
            workerId,
            'transient',
            null,
            null,
            'Controlled retry state',
            1,
          ),
          'settled',
        );
    } else if (state === 'satisfied' || state === 'dead_letter')
      await finish(es, p);
    assert.equal((await deliveries(p, [f.eventId]))[0]?.['state'], state);
    const before = (
      await p.query(
        'SELECT row_to_json(d)::text text FROM pipeline.delivery_intents d WHERE event_id=$1 ORDER BY kind',
        [f.eventId],
      )
    ).rows;
    const event = first(
      (await reader().outbox([{ entityId: f.id, version: '1' }])).events,
    );
    const stagedAgain = await pipeline().stage([event]);
    assert.equal(stagedAgain[0]?.status, 'already_staged');
    assert.deepEqual(
      (
        await p.query(
          'SELECT row_to_json(d)::text text FROM pipeline.delivery_intents d WHERE event_id=$1 ORDER BY kind',
          [f.eventId],
        )
      ).rows,
      before,
    );
    rows.push({ eventId: f.eventId, state, before, stagedAgain });
    await finish(es, p);
  }
  // Deferred obligation checks still reject incomplete sets. Owner-only negative setup rolls back.
  const id = rows[0]?.eventId;
  assert.ok(id);
  await p.query('BEGIN');
  try {
    await p.query(
      'ALTER TABLE pipeline.consumer_observations DISABLE TRIGGER immutable_rows',
    );
    await p.query(
      'DELETE FROM pipeline.consumer_observations WHERE event_id=$1',
      [id],
    );
    await assert.rejects(
      p.query('SELECT pipeline.assert_obligations($1)', [id]),
      { code: 'P3002' },
    );
  } finally {
    await p.query('ROLLBACK');
  }
  evidence('ES06', rows);
  // Actual capture restart must accept a settled ES intent without resetting it.
  const f = await create();
  const child = launchCapture(
    t,
    'ES06-capture-restart',
    'capture.after_pipeline_commit.before_source_ack',
  );
  const barrier = await child.barrier();
  assert.equal(first(await work(s, f.id))['state'], 'leased');
  await finish(es, p);
  const settled = await deliveries(p, [f.eventId]);
  assert.equal(first(settled)['state'], 'satisfied');
  const exit = await child.finish('kill');
  const clock = await databaseWaitFor(
    s,
    async () =>
      (
        await s.query<{
          expired: boolean;
          observed_at: string;
          lease_until: string;
        }>(
          'SELECT clock_timestamp()>lease_until expired,clock_timestamp()::text observed_at,lease_until::text FROM source.capture_work WHERE entity_id=$1',
          [f.id],
        )
      ).rows,
    (r) => first(r).expired,
    'capture lease expiry after ES satisfaction',
    10000,
  );
  const recovered = await drain('ES06-capture-restarted');
  assert.ok(
    recovered
      .flatMap((r) => r.staged)
      .some((r) => r.eventId === f.eventId && r.status === 'already_staged'),
  );
  assert.equal(first(await work(s, f.id))['state'], 'acknowledged');
  assert.deepEqual(await deliveries(p, [f.eventId]), settled);
  evidence('ES06-capture-restart', {
    eventId: f.eventId,
    barrier,
    exit,
    clock,
    settled,
    recovered,
    source: await work(s, f.id),
  });
});

await test(name('ES07'), async (t) => {
  const { p, es } = await setup(t);
  const records = [];
  for (const boundary of [
    'es.after_claim_commit.before_request',
    'es.after_remote_apply.before_local_commit',
    'es.after_local_commit.before_success',
  ])
    for (const kill of [true, false]) {
      const f = await staged();
      const child = launchEs(t, `${boundary}-${kill}`, boundary, false, 1500);
      const barrier = await child.barrier();
      const before = await deliveries(p, [f.eventId]);
      const claimed = barrier['claims'];
      assert.ok(Array.isArray(claimed));
      const token = object(claimed[0]);
      const backend = await p.query(
        'SELECT pid FROM pg_stat_activity WHERE pid=$1',
        [token['backendPid']],
      );
      assert.deepEqual(backend.rows, []);
      const claimXid = await p.query<{ xid: string }>(
        'SELECT xmin::text xid FROM pipeline.es_attempts WHERE attempt_id=$1',
        [token['attemptId']],
      );
      if (boundary !== 'es.after_local_commit.before_success')
        assert.equal(first(claimXid.rows).xid, token['transactionId']);

      const afterLocal = boundary === 'es.after_local_commit.before_success';
      assert.equal(before[0]?.['state'], afterLocal ? 'satisfied' : 'leased');
      let observed;
      if (boundary === 'es.after_claim_commit.before_request')
        await assert.rejects(remote(es, f.documentId));
      else {
        observed = await remote(es, f.documentId);
        assert.equal(version(observed['_version']), '1');
      }
      const exit = await child.finish(kill ? 'kill' : 'release');
      if (!kill) {
        const success = object(exit.output[0]);
        assert.equal(success['type'], 'es-success');
        assert.equal(success['claimed'], 1);
        assert.deepEqual(success['outcomes'], [
          {
            eventId: f.eventId,
            generation: token['generation'],
            outcome: 'applied',
            status: 'settled',
          },
        ]);
        assert.equal(
          first(await deliveries(p, [f.eventId]))['state'],
          'satisfied',
        );
      }
      await finish(es, p);
      const after = await deliveries(p, [f.eventId]);
      assert.equal(after[0]?.['state'], 'satisfied');
      if (afterLocal) assert.deepEqual(after, before);
      if (kill && boundary === 'es.after_remote_apply.before_local_commit')
        assert.equal(after[0]?.['disposition'], 'already_applied');
      const sessions = await p.query(
        "SELECT pid,application_name FROM pg_stat_activity WHERE usename='pipeline_es'",
      );
      assert.deepEqual(sessions.rows, []);
      records.push({
        eventId: f.eventId,
        boundary,
        kill,
        barrier,
        before,
        remote: observed,
        exit,
        after,
        sessions: sessions.rows,
      });
    }
  evidence('ES07', records);
});
async function toxic(
  type: 'timeout' | 'latency',
  attributes: Record<string, number>,
) {
  const r = await fetch(`${required('ES_PROXY_API')}/proxies/es/toxics`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: 'downstream',
      type,
      stream: 'downstream',
      toxicity: 1,
      attributes,
    }),
    signal: AbortSignal.timeout(5000),
  });
  assert.equal(r.status, 200);
}
async function removeToxic() {
  const r = await fetch(
    `${required('ES_PROXY_API')}/proxies/es/toxics/downstream`,
    { method: 'DELETE', signal: AbortSignal.timeout(5000) },
  );
  assert.equal(r.status, 204);
}
async function observedApplication(es: EsTransport, id: string) {
  return waitFor(
    async () => {
      try {
        return await remote(es, id);
      } catch {
        return undefined;
      }
    },
    (r) => r !== undefined,
    'independent realtime application',
    15000,
    () => es.close(),
  );
}

await test(name('ES08'), async (t) => {
  const { p, es, target } = await setup(t);
  const f = await staged();
  const proxy = new EsTransport(esConfig(false, true));
  t.after(() => proxy.close());
  // Complete identity preflight before withholding response. A private wrapper arms
  // the real downstream toxic immediately before the actual bulk HTTP call.
  class LossAdapter extends EsAdapter {
    override async bulk(...args: Parameters<EsAdapter['bulk']>) {
      await toxic('timeout', { timeout: 0 });
      return super.bulk(...args);
    }
  }
  const { Delivery } = await import('../../src/es/worker.ts');
  const w = new Delivery(ledger(), new LossAdapter(proxy));
  const running = w.once();
  void running.catch(() => undefined);
  let applied;
  let result;
  try {
    applied = await observedApplication(es, f.documentId);
    assert.ok(applied);
    assert.equal(version(applied['_version']), '1');
    const sessions = await p.query<Record<string, unknown>>(
      "SELECT pid,state,xact_start::text FROM pg_stat_activity WHERE usename='pipeline_es'",
    );
    for (const session of sessions.rows)
      assert.notEqual(session['state'], 'idle in transaction');
    evidence('ES08-no-open-network-transaction', sessions.rows);
    result = await running;
  } finally {
    await removeToxic();
  }
  const uncertain = await deliveries(p, [f.eventId]);
  assert.equal(uncertain[0]?.['state'], 'retry_wait');
  assert.equal(uncertain[0]?.['error_class'], 'transient');
  const recovery = await finish(es, p);
  const after = await deliveries(p, [f.eventId]);
  assert.equal(after[0]?.['disposition'], 'already_applied');
  await new EsAdapter(es).validate(target);
  evidence('ES08', {
    eventId: f.eventId,
    applied,
    result,
    uncertain,
    recovery,
    after,
  });
});

await test(name('ES09'), async (t) => {
  const { p, es } = await setup(t);
  const f = await staged();
  const child = launchEs(t, 'overlap', 'es.inflight', false, 1000, true);
  const barrier = await child.barrier();
  const remoteBefore = await observedApplication(es, f.documentId);
  assert.ok(remoteBefore);
  child.signal('SIGSTOP');
  let resumed = false;
  try {
    const clock = await databaseWaitFor(
      p,
      async () =>
        (
          await p.query<{
            expired: boolean;
            now: string;
            lease: string;
            generation: string;
          }>(
            "SELECT clock_timestamp()>lease_until expired,clock_timestamp()::text now,lease_until::text lease,claim_generation::text generation FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='elasticsearch'",
            [f.eventId],
          )
        ).rows,
      (r) => first(r).expired,
      'ES lease actually expired while process stopped',
    );
    const resultB = await worker(es).once();
    const b = await deliveries(p, [f.eventId]);
    const targetB = await ledger().target();
    assert.equal(b[0]?.['state'], 'satisfied');
    assert.equal(b[0]?.['claim_generation'], '2');
    assert.equal(b[0]?.['disposition'], 'already_applied');
    await removeToxic();
    child.signal('SIGCONT');
    resumed = true;
    const exit = await child.finish('release');
    assert.deepEqual(await deliveries(p, [f.eventId]), b);
    assert.deepEqual(await ledger().target(), targetB);
    evidence('ES09', {
      eventId: f.eventId,
      barrier,
      remoteBefore,
      clock,
      resultB,
      b,
      exit,
      after: await deliveries(p, [f.eventId]),
    });
  } finally {
    if (!resumed) {
      child.signal('SIGCONT');
      await removeToxic();
    }
  }
  const healthyFixture = await staged();
  const healthy = launchEs(
    t,
    'overlap-healthy',
    'es.inflight',
    false,
    1500,
    true,
  );
  const healthyBarrier = await healthy.barrier();
  await observedApplication(es, healthyFixture.documentId);
  await removeToxic();
  const healthyExit = await healthy.finish('release');
  const healthyResult = object(healthyExit.output[0]);
  assert.equal(healthyResult['type'], 'es-success');
  assert.equal(healthyResult['claimed'], 1);
  assert.deepEqual(healthyResult['outcomes'], [
    {
      eventId: healthyFixture.eventId,
      generation: '1',
      outcome: 'applied',
      status: 'settled',
    },
  ]);
  assert.equal(
    (await deliveries(p, [healthyFixture.eventId]))[0]?.['state'],
    'satisfied',
  );
  evidence('ES09-healthy', {
    eventId: healthyFixture.eventId,
    healthyBarrier,
    healthyExit,
  });
});

await test(name('ES10'), async (t) => {
  const { p, es } = await setup(t);
  await finish(es, p);
  const base = await staged();
  const docker = async (args: string[]) => {
    const r = await command('docker', args, process.env, 45000, true);
    assert.equal(r.code, 0, r.stderr);
    assert.deepEqual(r.cleanupErrors, []);
    return r;
  };
  const container = (
    await docker([
      'ps',
      '-q',
      '--filter',
      `label=com.docker.compose.project=${required('ES_PROJECT')}`,
      '--filter',
      'label=com.docker.compose.service=elasticsearch',
    ])
  ).stdout.trim();
  assert.match(container, /^[a-f0-9]+$/);
  await docker(['stop', '--time', '10', container]);
  const down = performance.now();
  const downAt = new Date().toISOString();
  const stop = new AbortController();
  const attempts: {
    at: string;
    elapsed: number;
    workerId: string;
    claimed: number;
  }[] = [];
  const source: string[] = [];
  const a = worker(es),
    b = worker(es);
  const report = (r: Awaited<ReturnType<typeof a.once>>) => {
    attempts.push({
      at: new Date().toISOString(),
      elapsed: performance.now() - down,
      workerId: r.workerId,
      claimed: r.claimed,
    });
    return Promise.resolve();
  };
  const following = Promise.all([
    a.follow(stop.signal, report),
    b.follow(stop.signal, report),
  ]);
  let failed: unknown;
  void following.catch((e: unknown) => {
    failed = e;
  });
  await withCleanup(
    async () => {
      let nextMutation = down;
      while (performance.now() < down + 60000) {
        if (failed) throw new Error('Follow worker failed', { cause: failed });
        if (performance.now() >= nextMutation) {
          source.push(
            (await staged('{"name":"during-outage","loyalty_points":12}'))
              .eventId,
          );
          nextMutation += 10000;
        }
        await delay(
          Math.min(500, Math.max(1, down + 60000 - performance.now())),
        );
      }
      const stoppedMs = performance.now() - down;
      assert.ok(stoppedMs >= 60000);
      assert.ok(source.length >= 5);
      const offline = await deliveries(p, [base.eventId, ...source]);
      for (const r of offline) assert.notEqual(r['state'], 'satisfied');
      const during = attempts.filter((r) => r.elapsed <= stoppedMs);
      assert.ok(during.reduce((n, r) => n + r.claimed, 0) <= 15);
      await docker(['start', container]);
      // The same two follow loops keep running through restart; no manual delivery,
      // credential change or admission reset is used to recover.
      await databaseWaitFor(
        p,
        async () => {
          if (failed)
            throw new Error('Follow recovery failed', { cause: failed });
          return deliveries(p, [base.eventId, ...source]);
        },
        (rows) => rows.every((r) => r['state'] === 'satisfied'),
        'automatic follow recovery after actual outage',
        150000,
      );
      evidence('ES10', {
        container,
        downAt,
        measuredStoppedMs: stoppedMs,
        sourceEvents: source,
        attempts,
        offline,
        after: await deliveries(p, [base.eventId, ...source]),
        automaticSameWorkerRecovery: true,
      });
    },
    async () => {
      stop.abort();
      await following;
    },
  );
});

await test(name('ES12'), async (t) => {
  const { p, es, admin, target } = await setup(t);
  const f = await staged();
  const l = ledger(),
    projection = await l.read(f.eventId);
  const { validateBulkResponse } = await import('../../src/es/adapter.ts');
  const good = object(
    parse(
      `{"errors":false,"items":[{"index":{"_id":${JSON.stringify(projection.documentId)},"_index":${JSON.stringify(target.index)},"status":201,"_version":1,"result":"created"}}]}`,
    ),
  );
  assert.equal(
    validateBulkResponse(good, [projection], target.index)[0]?.outcome,
    'applied',
  );
  for (const value of [
    {},
    { ...good, items: [] },
    { ...good, errors: true },
    {
      ...good,
      items: [...(good['items'] as unknown[]), ...(good['items'] as unknown[])],
    },
    {
      errors: false,
      items: [
        {
          index: {
            _id: 'wrong',
            _index: target.index,
            status: 201,
            _version: 1,
          },
        },
      ],
    },
  ])
    assert.throws(() =>
      validateBulkResponse(value, [projection], target.index),
    );
  assert.throws(() => version(Number(9007199254740993n)));
  assert.throws(() => version(parse('9223372036854775808')));
  assert.throws(() => bulkLine({ ...projection, json: 'x'.repeat(262145) }));
  await assert.rejects(
    new EsAdapter(es).bulk(
      target,
      Array.from({ length: 501 }, () => projection),
      randomUUID(),
    ),
    /count/,
  );
  await assert.rejects(
    es.request('POST', `/${target.index}/_bulk`, 'x'.repeat(4194305)),
    /4 MiB/,
  );
  const oversizedIndex = `kit-response-${randomUUID()}`;
  await admin.request(
    'PUT',
    `/${oversizedIndex}`,
    JSON.stringify({
      settings: { number_of_shards: 1, number_of_replicas: 0 },
      mappings: {
        dynamic: 'strict',
        properties: {
          text: { type: 'keyword', index: false, doc_values: false },
        },
      },
    }),
  );
  try {
    for (let page = 0; page < 2; page++) {
      const body = Array.from(
        { length: 45 },
        (_, i) =>
          `{"index":{"_id":"${page * 45 + i}"}}\n${JSON.stringify({ text: 'x'.repeat(60000) })}\n`,
      ).join('');
      assert.equal(
        object(await admin.request('POST', `/${oversizedIndex}/_bulk`, body))[
          'errors'
        ],
        false,
      );
    }
    await admin.request('POST', `/${oversizedIndex}/_refresh`);
    await assert.rejects(
      admin.request(
        'POST',
        `/${oversizedIndex}/_search`,
        JSON.stringify({ size: 100, query: { match_all: {} } }),
      ),
      /bigger|maximum|size|aborted/i,
    );
  } finally {
    await admin.request('DELETE', `/${oversizedIndex}`);
  }
  const sizes = await p.query<{ bytes: string; maximum: string }>(
    'SELECT octet_length(body_bytes)::text bytes,octet_length(pipeline.es_projection(event_id))::text maximum FROM pipeline.events WHERE event_id=$1',
    [f.eventId],
  );
  await finish(es, p);
  evidence('ES12', {
    supplementalParserFixtures: 5,
    countLimit: 500,
    requestLimit: 4194304,
    recordLimit: 262144,
    responseLimit: 4194304,
    sizes: sizes.rows,
  });
});

await test(name('ES13'), async (t) => {
  const { p, es, s } = await setup(t);
  const upgrade: unknown = JSON.parse(
    await readFile(`${required('M1_ARTIFACT_DIR')}/es-upgrade.json`, 'utf8'),
  );
  const record = object(upgrade);
  const before = object(record['before']),
    after = object(record['after']);
  for (const table of [
    'events',
    'consumer_observations',
    'source_binding',
    'destinations',
    'integrity_incidents',
  ])
    assert.deepEqual(before[table], after[table]);
  const old = before['delivery_intents'];
  assert.ok(Array.isArray(old));
  for (const value of old) {
    assert.equal(typeof value, 'string');
    const original = object(JSON.parse(String(value)));
    const current = first(
      (
        await p.query<Record<string, unknown>>(
          'SELECT event_id,kind,destination_id,state,created_at::text FROM pipeline.delivery_intents WHERE event_id=$1 AND kind=$2',
          [original['event_id'], original['kind']],
        )
      ).rows,
    );
    assert.equal(current['event_id'], original['event_id']);
    assert.equal(current['destination_id'], original['destination_id']);
    if (original['kind'] === 'rabbitmq')
      assert.equal(current['state'], 'pending');
  }
  const original = await untouched(p);
  await finish(es, p);
  assert.deepEqual(await untouched(p), original);
  // Synthetic operational counters, rolled back: even status JSON must avoid the
  // driver's ordinary JSON-number conversion for exact BIGINT metadata.
  await p.query('BEGIN');
  let exactStatus;
  try {
    await p.query(
      'UPDATE pipeline.es_target SET failures=9007199254740993,probe_generation=9223372036854775807',
    );
    exactStatus = object(
      first(
        (
          await p.query<{ value: unknown }>(
            'SELECT pipeline.es_status(1) value',
          )
        ).rows,
      ).value,
    );
    const exactTarget = object(exactStatus['target']);
    assert.equal(exactTarget['failures'], '9007199254740993');
    assert.equal(exactTarget['probe_generation'], '9223372036854775807');
    assert.equal(exactTarget['generation'], '1');
  } finally {
    await p.query('ROLLBACK');
  }
  const protectedSource = (
    await s.query(
      'SELECT source_epoch::text,command_id::text,result_entity_id::text,result_version::text FROM source.command_receipts ORDER BY command_id',
    )
  ).rows;
  assert.ok(protectedSource.length > 500);
  const runtime = new (await import('pg')).default.Client({
    ...(await import('../support/es.ts')).ledgerConfig('denials'),
  });
  await runtime.connect();
  t.after(() => runtime.end());
  for (const sql of [
    "UPDATE pipeline.events SET content_sha256=repeat('0',64)",
    "DELETE FROM pipeline.delivery_intents WHERE kind='rabbitmq'",
    "UPDATE pipeline.consumer_observations SET state='pending'",
    "UPDATE pipeline.es_target SET index_uuid='forged'",
    'SET ROLE pipeline_owner',
  ]) {
    await assert.rejects(runtime.query(sql), { code: '42501' });
  }
  evidence('ES13', {
    mode: old.length ? 'populated guarded M2C.1' : 'fresh',
    preservedOriginalRows: old.length,
    sourceReceipts: protectedSource.length,
    rabbitAndConsumerUnchanged: true,
    syntheticStatusCounters: exactStatus,
  });
});

await test(name('ES14'), async (t) => {
  const { s, p, es, admin, target } = await setup(t);
  await finish(es, p);
  const refresh = () => admin.request('POST', `/${target.index}/_refresh`);
  await refresh();
  const baseline = await oracle(s, p, es);
  const positive = baseline.find((r) => !r.unresolved);
  assert.ok(positive);
  const expected = await ledger().read(positive.id);
  assert.ok(expected.json);
  const negative = [];
  // Privileged negative controls operate only on the owned test receiver, outside runtime privileges.
  await admin.request(
    'DELETE',
    `/${target.index}/_doc/${encodeURIComponent(expected.documentId)}`,
  );
  await refresh();
  await assert.rejects(oracle(s, p, es));
  negative.push('missing detected');
  // A physical-delete tombstone retains its version. external_gte is used ONLY by
  // privileged corruption restoration, never by the production delivery adapter.
  await admin.request(
    'PUT',
    `/${target.index}/_settings`,
    JSON.stringify({ 'index.gc_deletes': '0ms' }),
  );
  // Replace corruption by a legitimate higher source revision instead of lowering remote history.
  const entityId = expected.documentId.split(':')[1];
  assert.ok(entityId);
  const higher = await create(
    '{"name":"negative-control-repair","loyalty_points":10}',
    entityId,
    'update',
  );
  await drain();
  const repair = await ledger().read(higher.eventId);
  assert.ok(repair.json);
  await admin.request(
    'POST',
    `/${target.index}/_bulk`,
    bulkLine(repair).replace(
      '"version_type":"external"',
      '"version_type":"external_gte"',
    ),
  );
  await finish(es, p);
  const extra = 'owned-extra';
  await admin.request('PUT', `/${target.index}/_doc/${extra}`, repair.json);
  await refresh();
  await assert.rejects(oracle(s, p, es));
  negative.push('extra detected');
  await admin.request('DELETE', `/${target.index}/_doc/${extra}`);
  const corrupted = object(parse(repair.json));
  corrupted['search_fields'] = { name: 'wrong' };
  const corruptJson = stringify(corrupted);
  assert.ok(corruptJson);
  await admin.request(
    'POST',
    `/${target.index}/_bulk`,
    bulkLine({ ...repair, json: corruptJson }).replace(
      '"version_type":"external"',
      '"version_type":"external_gte"',
    ),
  );
  await refresh();
  await assert.rejects(oracle(s, p, es));
  negative.push('corruption detected');
  await admin.request(
    'POST',
    `/${target.index}/_bulk`,
    bulkLine(repair).replace(
      '"version_type":"external"',
      '"version_type":"external_gte"',
    ),
  );
  await admin.request(
    'PUT',
    `/${target.index}/_settings`,
    JSON.stringify({ 'index.gc_deletes': '60s' }),
  );
  await refresh();
  const before = await oracle(s, p, es);
  const containers = await command(
    'docker',
    [
      'ps',
      '-q',
      '--filter',
      `label=com.docker.compose.project=${required('ES_PROJECT')}`,
      '--filter',
      'label=com.docker.compose.service=elasticsearch',
    ],
    process.env,
    10000,
    true,
  );
  assert.equal(containers.code, 0);
  const id = containers.stdout.trim();
  const restart = await command(
    'docker',
    ['restart', '--time', '10', id],
    process.env,
    45000,
    true,
  );
  assert.equal(restart.code, 0, restart.stderr);
  await waitFor(
    async () => {
      try {
        await new EsAdapter(es).validate(target);
        return true;
      } catch {
        return false;
      }
    },
    Boolean,
    'retained ES restart',
    90000,
    () => es.close(),
  );
  await refresh();
  assert.deepEqual(await oracle(s, p, es), before);
  evidence('ES14-restart', {
    container: id,
    clusterUuid: target.clusterUuid,
    indexUuid: target.indexUuid,
    negativeControls: negative,
    unchanged: true,
  });
});

// Last: controlled replacement intentionally leaves this owned target blocked. No
// attempt is made to adopt its new UUID. Earlier reconciliation/restart evidence is retained.
await test(name('ES11'), async (t) => {
  const { p, es, admin, target } = await setup(t);
  for (const [method, path, body] of [
    ['PUT', '/kit-forbidden-create', '{}'],
    ['DELETE', `/${target.index}`, undefined],
    ['PUT', `/${target.index}/_mapping`, '{"dynamic":false}'],
    ['PUT', '/_security/role/forged', '{}'],
    ['DELETE', `/${target.index}/_doc/forbidden-delete`, undefined],
  ] as const)
    await assert.rejects(es.request(method, path, body), (error: unknown) => {
      assert.ok(error && typeof error === 'object' && 'statusCode' in error);
      assert.equal(error.statusCode, 403);
      return true;
    });
  const protectedFixture = await staged();
  await finish(es, p);
  const protectedBefore = await remote(es, protectedFixture.documentId);
  const bulkDenials = [];
  for (const operation of ['delete', 'update']) {
    const request =
      JSON.stringify({ [operation]: { _id: protectedFixture.documentId } }) +
      '\n' +
      (operation === 'update' ? '{"doc":{}}\n' : '');
    const response = object(
      await es.request('POST', `/${target.index}/_bulk`, request),
    );
    assert.equal(response['errors'], true);
    assert.ok(Array.isArray(response['items']));
    assert.equal(response['items'].length, 1);
    const item = object(object(response['items'][0])[operation]);
    assert.equal(exactInteger(item['status']), '403');
    bulkDenials.push({ operation, response: stringify(response) });
  }
  assert.deepEqual(
    await remote(es, protectedFixture.documentId),
    protectedBefore,
  );
  const f = await staged();
  const bad = new EsTransport({
    ...esConfig(),
    password: 'invalid-controlled-password',
  });
  t.after(() => bad.close());
  await assert.rejects(worker(bad).once());
  assert.equal((await ledger().target()).mode, 'blocked');
  assert.equal((await deliveries(p, [f.eventId]))[0]?.['state'], 'retry_wait');
  const deadBefore = (
    await p.query('SELECT count(*)::text count FROM pipeline.es_dead_letters')
  ).rows;
  await p.query("UPDATE pipeline.es_target SET mode='ready',reason=NULL");
  await finish(es, p);
  const metadataFixture = await staged();
  const mappings = object(object(target.configuration)['mappings']);
  const originalMeta = object(mappings['_meta']);
  await admin.request(
    'PUT',
    `/${target.index}/_mapping`,
    JSON.stringify({ _meta: { ...originalMeta, projection_schema: 'forged' } }),
  );
  await assert.rejects(worker(es).once(), /configuration mismatch/);
  assert.equal((await ledger().target()).mode, 'blocked');
  assert.equal(
    first(await deliveries(p, [metadataFixture.eventId]))['state'],
    'retry_wait',
  );
  await admin.request(
    'PUT',
    `/${target.index}/_mapping`,
    JSON.stringify({ _meta: originalMeta }),
  );
  await p.query("UPDATE pipeline.es_target SET mode='ready',reason=NULL");
  await finish(es, p);
  const another = await staged();
  await admin.request('DELETE', `/${target.index}`);
  await admin.request(
    'PUT',
    `/${target.index}`,
    JSON.stringify(target.configuration),
  );
  const replacement = object(
    object(await admin.request('GET', `/${target.index}`))[target.index],
  );
  const newUuid = object(object(replacement['settings'])['index'])['uuid'];
  assert.notEqual(newUuid, target.indexUuid);
  await assert.rejects(worker(es).once(), /UUID/);
  assert.equal((await ledger().target()).mode, 'blocked');
  assert.equal(
    (await deliveries(p, [another.eventId]))[0]?.['state'],
    'retry_wait',
  );
  assert.deepEqual(
    (await p.query('SELECT count(*)::text count FROM pipeline.es_dead_letters'))
      .rows,
    deadBefore,
  );
  evidence('ES11', {
    bulkDenials,
    protectedDocumentId: protectedFixture.documentId,
    credentialFailureEvent: f.eventId,
    wrongMetadataEvent: metadataFixture.eventId,
    replacementEvent: another.eventId,
    originalUuid: target.indexUuid,
    replacementUuid: newUuid,
    targetMode: 'blocked',
    notAtomicRemoteFencing: true,
    deadLettersUnchanged: true,
  });
});
