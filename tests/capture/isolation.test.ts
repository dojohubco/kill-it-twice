import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { test } from 'node:test';
import pg from 'pg';
import { sourceSnapshot } from '../../scripts/migrate-staging.ts';
import { captureSnapshot } from '../support/capture-upgrade.ts';
import { object } from '../../scripts/acceptance.ts';
import { first } from '../../scripts/rows.ts';
import { setup, config } from '../support/staging.ts';
import { connect, evidence, required } from '../support/db.ts';
import { execute, request } from '../support/commands.ts';
import {
  transactionSnapshot,
  mutationAttempt,
  markerState,
} from '../support/isolation.ts';
import {
  captureConfig,
  control,
  drain,
  reconcile,
  stageClaim,
  work,
} from '../support/capture.ts';
async function protectedState(s: pg.Client, ids: string[]) {
  const result: Record<string, string[]> = {};
  for (const table of [
    'entities',
    'outbox',
    'capture_work',
    'command_receipts',
  ]) {
    const key = table === 'command_receipts' ? 'result_entity_id' : 'entity_id';
    result[table] = (
      await s.query<{ row: string }>(
        `SELECT row_to_json(t)::text AS row FROM source.${table} t WHERE ${key}=ANY($1::bigint[]) ORDER BY row_to_json(t)::text COLLATE "C"`,
        [ids],
      )
    ).rows.map((r) => r.row);
  }
  return result;
}
void test('R04 legacy mutation isolation matrix rejects real state changes and permits no-ops', async (t) => {
  const { s, epoch } = await setup(t);
  const a = await connect('writer', 'R04');
  t.after(() => a.end());
  const live = await execute(
    request(epoch, 'create', null, '{"legacy":"live","n":1}'),
  );
  const dead = await execute(
    request(epoch, 'create', null, '{"legacy":"deleted"}'),
  );
  await execute(request(epoch, 'delete', dead.result.entity_id, null));
  const ids = [live.result.entity_id, dead.result.entity_id],
    before = await protectedState(s, ids),
    attempts = [];
  for (const isolation of ['REPEATABLE READ', 'SERIALIZABLE'] as const) {
    for (const operation of [
      'create',
      'update',
      'delete',
      'restore',
    ] as const) {
      await a.query(`BEGIN ISOLATION LEVEL ${isolation}`);
      const transaction = await transactionSnapshot(a);
      assert.equal(transaction.role, 'source_writer');
      const id =
        operation === 'create'
          ? null
          : operation === 'restore'
            ? dead.result.entity_id
            : live.result.entity_id;
      const marker = randomUUID();
      const attempt = await mutationAttempt(
        a,
        operation,
        operation === 'delete'
          ? null
          : JSON.stringify({
              guardOperation: operation,
              isolationMarker: marker,
            }),
        id,
      );
      assert.equal(attempt.failure?.sqlState, '25001');
      assert.match(attempt.failure.where ?? '', /enqueue_capture\(\)/);
      assert.equal(attempt.completion, 'ROLLBACK');
      assert.deepEqual(await protectedState(s, ids), before);
      const absent = await markerState(s, marker);
      assert.deepEqual(absent.entities, []);
      assert.deepEqual(absent.outbox, []);
      assert.deepEqual(absent.work, []);
      attempts.push({ transaction, attempt, absent });
    }
  }
  const noops = [];
  for (const isolation of ['REPEATABLE READ', 'SERIALIZABLE'] as const) {
    await a.query(`BEGIN ISOLATION LEVEL ${isolation} READ ONLY`);
    const read = (
      await a.query<Record<string, unknown>>(
        "SELECT source_epoch::text,current_setting('transaction_isolation') AS isolation,current_setting('transaction_read_only') AS read_only,pg_current_snapshot()::text AS snapshot FROM source.source_identity",
      )
    ).rows;
    assert.equal(first(read)['read_only'], 'on');
    assert.equal((await a.query('COMMIT')).command, 'COMMIT');
    for (const [operation, id, payload] of [
      ['update', live.result.entity_id, '{"n":1,"legacy":"live"}'],
      ['delete', dead.result.entity_id, null],
    ] as const) {
      await a.query(`BEGIN ISOLATION LEVEL ${isolation}`);
      const transaction = await transactionSnapshot(a);
      const attempt = await mutationAttempt(a, operation, payload, id);
      assert.equal(attempt.failure, null);
      assert.equal(attempt.completion, 'COMMIT');
      assert.deepEqual(await protectedState(s, ids), before);
      noops.push({ read, transaction, attempt });
    }
  }
  evidence('R04', {
    ids,
    before,
    attempts,
    noops,
    after: await protectedState(s, ids),
  });
});
void test('R06 command receipts and owned capture transitions retain their guarantees', async (t) => {
  const { s, p, epoch } = await setup(t);
  await drain('R06-start');
  const c = await connect('command', 'R06-isolation');
  t.after(() => c.end());
  const commandRejections = [];
  for (const isolation of ['REPEATABLE READ', 'SERIALIZABLE'] as const) {
    await c.query(`BEGIN ISOLATION LEVEL ${isolation}`);
    const key = randomUUID();
    let sqlState;
    try {
      await c.query(
        "SELECT * FROM source.execute_command($1,$2,1,'create',NULL,'{}'::jsonb)",
        [epoch, key],
      );
    } catch (error) {
      assert.ok(error instanceof pg.DatabaseError);
      sqlState = error.code;
    }
    assert.equal(sqlState, '25001');
    assert.equal((await c.query('COMMIT')).command, 'ROLLBACK');
    assert.equal(
      first(
        (
          await s.query<{ n: string }>(
            'SELECT count(*)::text AS n FROM source.command_receipts WHERE command_id=$1',
            [key],
          )
        ).rows,
      ).n,
      '0',
    );
    commandRejections.push({
      isolation,
      key,
      sqlState,
      completion: 'ROLLBACK',
    });
  }
  const create = request(epoch, 'create', null, '{"n":9007199254740993}');
  const created = await execute(create);
  assert.equal((await execute(create)).replayed, true);
  const id = created.result.entity_id;
  const updated = await execute(request(epoch, 'update', id, '{"n":2}'));
  const noop = await execute(request(epoch, 'update', id, '{ "n" : 2.0 }'));
  assert.equal(noop.result.entity_version, updated.result.entity_version);
  const deleted = await execute(request(epoch, 'delete', id, null));
  const repeated = await execute(request(epoch, 'delete', id, null));
  assert.equal(repeated.result.entity_version, deleted.result.entity_version);
  const restore = request(epoch, 'restore', id, '{"n":3}');
  const restored = await execute(restore);
  assert.equal((await execute(restore)).replayed, true);
  const pending = await work(s, id);
  assert.equal(pending.length, 4);
  assert.ok(pending.every((r) => r['state'] === 'pending'));
  const ctl = control('R06'),
    claims = await ctl.claim(randomUUID(), 16, 30000);
  assert.equal(claims.length, 4);
  const leased = await work(s, id);
  assert.ok(leased.every((r) => r['state'] === 'leased'));
  const firstClaim = first(claims);
  await assert.rejects(
    ctl.renew({ ...firstClaim, ownerId: randomUUID() }, 30000),
    { sqlState: 'P4002' },
  );
  await assert.rejects(
    ctl.acknowledge({ ...firstClaim, generation: '999' }, '0'.repeat(64)),
    { sqlState: 'P4002' },
  );
  assert.deepEqual(await work(s, id), leased);
  const results = [];
  for (const claim of claims) {
    const staged = await stageClaim(claim, 'R06-stage');
    const ack = await ctl.acknowledge(claim, staged.event.contentSha256);
    results.push({
      claim,
      staged: staged.results,
      hash: staged.event.contentSha256,
      ack,
    });
  }
  const terminal = await work(s, id),
    firstResult = first(results);
  assert.equal(
    (await ctl.acknowledge(firstResult.claim, firstResult.hash)).status,
    'already_acknowledged',
  );
  await assert.rejects(
    ctl.acknowledge(
      { ...firstResult.claim, ownerId: randomUUID() },
      firstResult.hash,
    ),
    { sqlState: 'P4002' },
  );
  await assert.rejects(ctl.acknowledge(firstResult.claim, '0'.repeat(64)), {
    sqlState: 'P4003',
  });
  assert.deepEqual(await work(s, id), terminal);
  const receipts = (
    await s.query<{ row: string }>(
      'SELECT row_to_json(r)::text AS row FROM source.command_receipts r WHERE result_entity_id=$1 ORDER BY command_id',
      [id],
    )
  ).rows;
  assert.equal(receipts.length, 6);
  await reconcile(s, p, 'R06-reconciliation', [id]);
  evidence('R06', {
    commandRejections,
    created,
    updated,
    noop,
    deleted,
    repeated,
    restored,
    receipts,
    pending,
    leased,
    results,
    terminal,
  });
});
void test('R07 forward guard migration preserves all registered or unregistered data and identity', async (t) => {
  const { s } = await setup(t);
  const artifact = object(
    JSON.parse(
      await readFile(
        join(required('M1_ARTIFACT_DIR'), 'capture-upgrade.json'),
        'utf8',
      ),
    ),
  );
  const isolation = object(artifact['isolation']),
    migration = object(isolation['migration']);
  assert.deepEqual(migration['after'], migration['before']);
  assert.deepEqual(migration['catalogAfter'], migration['catalogBefore']);
  assert.doesNotMatch(
    String(migration['definitionBefore']),
    /transaction_isolation/,
  );
  assert.match(String(migration['definitionAfter']), /transaction_isolation/);
  const before = object(migration['before']),
    capture = object(before['capture']);
  assert.ok(Array.isArray(capture['capture_binding']));
  assert.equal(
    capture['capture_binding'].length,
    isolation['mode'] === 'registered' ? 1 : 0,
  );
  if (isolation['mode'] === 'registered') {
    const transfer = object(isolation['transfer']);
    assert.ok(Array.isArray(transfer['acknowledgements']));
    assert.ok(transfer['acknowledgements'].length >= 2);
    assert.ok(Array.isArray(capture['state_counts']));
    assert.ok(
      capture['state_counts'].some((v) =>
        String(v).startsWith('acknowledged:'),
      ),
    );
  }
  const current = (
    await s.query<Record<string, unknown>>(
      'SELECT pipeline_id::text,source_epoch::text,payload_encoding FROM source.capture_binding',
    )
  ).rows;
  assert.equal(first(current)['pipeline_id'], artifact['pipelineId']);
  evidence('R07', {
    mode: isolation['mode'],
    migration,
    transfer: isolation['transfer'],
    current,
  });
});
void test('R08 runtime roles cannot replace or bypass the common enqueue guard', async (t) => {
  const { s } = await setup(t);
  const observe = async () => ({
    source: await sourceSnapshot(s),
    capture: await captureSnapshot(s),
  });
  const before = await observe();
  const attempts = [];
  const writer = await connect('writer', 'R08-writer'),
    command = await connect('command', 'R08-command');
  t.after(() => writer.end());
  t.after(() => command.end());
  const reader = new pg.Client({
    ...config('reader', 'R08-reader'),
    query_timeout: 5000,
  });
  t.after(() => reader.end());
  await reader.connect();
  const capture = new pg.Client({
    ...captureConfig('R08-capture').source,
    query_timeout: 5000,
  });
  t.after(() => capture.end());
  await capture.connect();
  const statements = [
    'CREATE OR REPLACE FUNCTION source.enqueue_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$',
    'ALTER FUNCTION source.enqueue_capture() SECURITY INVOKER',
    'ALTER TABLE source.outbox DISABLE TRIGGER outbox_enqueue_capture',
    'ALTER TABLE source.entities DISABLE TRIGGER entities_capture',
    'SET ROLE source_owner',
    "SET session_replication_role='replica'",
    'UPDATE source.capture_binding SET pipeline_id=gen_random_uuid()',
    "UPDATE source.capture_work SET state='acknowledged'",
    'UPDATE source.entities SET entity_version=entity_version+1',
    'INSERT INTO source.outbox(entity_id) VALUES(1)',
    'SELECT source.enqueue_capture()',
  ];
  for (const client of [writer, command, reader, capture]) {
    const role = first(
      (await client.query<{ role: string }>('SELECT current_user AS role'))
        .rows,
    ).role;
    // The reader defaults to read-only; check privileges in READ COMMITTED READ WRITE too, so 25006 cannot falsely prove denial.
    await client.query('SET default_transaction_read_only=off');
    for (const sql of statements) {
      let sqlState;
      try {
        await client.query(sql);
      } catch (error) {
        assert.ok(error instanceof pg.DatabaseError);
        sqlState = error.code;
      }
      assert.equal(sqlState, '42501', `${role}: ${sql}`);
      attempts.push({ role, sql, sqlState });
    }
  }
  const catalog = (
    await s.query<
      Record<string, unknown>
    >(`SELECT pg_get_userbyid(p.proowner) AS owner,prosecdef,proconfig,pg_get_functiondef(p.oid) AS definition,
    (SELECT count(*)::text FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') AS public_execute
    FROM pg_proc p WHERE p.oid='source.enqueue_capture()'::regprocedure`)
  ).rows;
  const f = first(catalog);
  assert.equal(f['owner'], 'source_owner');
  assert.equal(f['prosecdef'], true);
  assert.equal(f['public_execute'], '0');
  assert.deepEqual(f['proconfig'], ['search_path=pg_catalog, pg_temp']);
  const definition = String(f['definition']);
  assert.ok(
    definition.indexOf('transaction_isolation') <
      definition.indexOf('INSERT INTO source.capture_work'),
  );
  assert.match(definition, /25001/);
  const summary = await control('R08').summary();
  assert.equal(summary.missing, '0');
  const after = await observe();
  assert.deepEqual(after, before);
  evidence('R08', { attempts, catalog, summary, before, after });
});
