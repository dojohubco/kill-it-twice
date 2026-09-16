import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import type pg from 'pg';
import { positiveBigint, SourceTransactionError } from '../../src/source.ts';
import { createEntity, mutateEntity } from '../support/sql.ts';
import { sourceOwner } from '../support/db.ts';
import { connect, evidence, databaseWaitFor } from '../support/db.ts';

let observer: pg.Client;
before(async () => {
  observer = await connect('admin', 'source-observer');
  const initial = (
    await observer.query(
      'SELECT (SELECT count(*)::text FROM source.entities) AS entities, (SELECT count(*)::text FROM source.outbox) AS outbox, source_epoch::text FROM source.source_identity',
    )
  ).rows[0];
  assert.equal(initial.entities, '0');
  assert.equal(initial.outbox, '0');
  evidence('T10-fresh-start', initial);
});
after(async () => {
  await observer?.end();
});

async function withWriter<T>(
  label: string,
  work: (writer: pg.Client) => Promise<T>,
): Promise<T> {
  const writer = await connect('writer', label);
  try {
    return await work(writer);
  } finally {
    await writer.end();
  }
}

// Independent SQL observations; deliberately does not import the helper's column mapping.
async function entity(id: string, client = observer) {
  return (
    await client.query(
      'SELECT entity_id::text, source_epoch::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload_json FROM source.entities WHERE entity_id=$1',
      [id],
    )
  ).rows;
}
async function revisions(id: string, client = observer) {
  return (
    await client.query(
      'SELECT allocation_id::text, source_epoch::text, entity_id::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload_json FROM source.outbox WHERE entity_id=$1 ORDER BY entity_version',
      [id],
    )
  ).rows;
}
async function sqlError(
  client: pg.Client,
  sql: string,
  code: string,
  params: unknown[] = [],
) {
  let observed = false;
  try {
    await client.query(sql, params);
  } catch (error) {
    observed = true;
    assert.equal((error as { code: string }).code, code, sql);
    evidence('SQL-rejection', {
      sql,
      expectedCode: code,
      actualCode: (error as { code: string }).code,
      message: (error as Error).message,
    });
  }
  assert.ok(observed, `SQL unexpectedly succeeded: ${sql}`);
}

test(
  'T01 mutation lifecycle, exact BIGINT boundaries, no-op, and two committed revisions',
  { timeout: 20_000 },
  async () => {
    {
      const owner = sourceOwner('T01');
      await observer.query(
        'ALTER SEQUENCE source.entities_entity_id_seq RESTART WITH 9007199254740993',
      );
      const floor = (
        await observer.query('SELECT clock_timestamp()::text AS time')
      ).rows[0].time;
      const created = await owner.transaction((c) =>
        c.create('{"label":"alpha","units":9007199254740993}'),
      );
      assert.equal(created.entity_id, '9007199254740993');
      assert.equal(created.entity_version, '1');
      assert.deepEqual((await entity(created.entity_id))[0], created);
      const updated = await owner.transaction((c) =>
        c.mutate(
          created.entity_id,
          'update',
          '{"label":"beta","units":9007199254740993}',
        ),
      );
      assert.equal(updated.entity_version, '2');
      const noOp = await owner.transaction((c) =>
        c.mutate(
          created.entity_id,
          'update',
          '{"units":9007199254740993, "label": "beta"}',
        ),
      );
      assert.deepEqual(noOp, updated);
      assert.equal((await revisions(created.entity_id)).length, 2);
      const deleted = await owner.transaction((c) =>
        c.mutate(created.entity_id, 'delete'),
      );
      assert.equal(deleted.entity_version, '3');
      assert.equal(deleted.is_deleted, true);
      assert.equal(deleted.payload_json, null);
      assert.deepEqual(
        await owner.transaction((c) => c.mutate(created.entity_id, 'delete')),
        deleted,
      );
      const restored = await owner.transaction((c) =>
        c.mutate(
          created.entity_id,
          'restore',
          '{"label":"restored","units":9007199254740993}',
        ),
      );
      assert.equal(restored.entity_id, created.entity_id);
      assert.equal(restored.entity_version, '4');
      assert.equal(restored.is_deleted, false);
      assert.deepEqual((await entity(created.entity_id))[0], restored);
      const history = await revisions(created.entity_id);
      assert.deepEqual(
        history.map((r) => r.entity_version),
        ['1', '2', '3', '4'],
      );
      assert.equal(new Set(history.map((r) => r.change_id)).size, 4);
      assert.equal(new Set(history.map((r) => r.source_epoch)).size, 1);
      const epoch = (
        await observer.query(
          'SELECT source_epoch::text FROM source.source_identity',
        )
      ).rows[0].source_epoch;
      for (const revision of history) {
        assert.equal(revision.source_epoch, epoch);
        assert.equal(revision.entity_id, '9007199254740993');
        assert.match(revision.change_id as string, /^[a-f0-9-]{36}$/);
      }
      const expected: [string, boolean, string | null][] = [
        ['1', false, '{"label":"alpha","units":9007199254740993}'],
        ['2', false, '{"label":"beta","units":9007199254740993}'],
        ['3', true, null],
        ['4', false, '{"label":"restored","units":9007199254740993}'],
      ];
      for (const [version, deleted, payload] of expected) {
        const result: pg.QueryResult<{
          payload_matches: boolean;
          deletion_matches: boolean;
          time_matches: boolean;
        }> = await observer.query(
          'SELECT payload IS NOT DISTINCT FROM $3::jsonb AS payload_matches, is_deleted=$4 AS deletion_matches, recorded_at BETWEEN $5::timestamptz AND clock_timestamp() AS time_matches FROM source.outbox WHERE entity_id=$1 AND entity_version=$2',
          [created.entity_id, version, payload, deleted, floor],
        );
        assert.deepEqual(result.rows, [
          { payload_matches: true, deletion_matches: true, time_matches: true },
        ]);
      }
      const highVersion = await observer.query(
        'SELECT source.next_version(9007199254740992) AS unsafe_number_boundary, source.next_version(9223372036854775806) AS maximum',
      );
      assert.deepEqual(highVersion.rows, [
        {
          unsafe_number_boundary: '9007199254740993',
          maximum: '9223372036854775807',
        },
      ]);
      await sqlError(
        observer,
        'SELECT source.next_version(9223372036854775807)',
        '22003',
      );
      await sqlError(observer, 'SELECT source.next_version(0)', '22003');
      await sqlError(observer, 'SELECT 9223372036854775808::bigint', '22003');
      const duplicates = await observer.query(
        'SELECT source_epoch, entity_id::text, entity_version::text FROM source.outbox GROUP BY source_epoch, entity_id, entity_version HAVING count(*) > 1',
      );
      assert.deepEqual(duplicates.rows, []);
      await sqlError(
        observer,
        'INSERT INTO source.outbox (source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload) SELECT source_epoch,entity_id,entity_version,gen_random_uuid(),recorded_at,is_deleted,payload FROM source.outbox WHERE entity_id=$1 AND entity_version=1',
        '23505',
        [created.entity_id],
      );
      const constraints = (
        await observer.query(
          "SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid IN ('source.entities'::regclass, 'source.outbox'::regclass) ORDER BY conname",
        )
      ).rows;
      assert.ok(
        constraints.some((c) => c.conname === 'outbox_revision_identity'),
      );
      const twice = await owner.transaction(async (c) => {
        const a = await c.mutate(
          created.entity_id,
          'update',
          '{"label":"fifth"}',
        );
        const b = await c.mutate(
          created.entity_id,
          'update',
          '{"label":"sixth"}',
        );
        return [a.entity_version, b.entity_version];
      });
      assert.deepEqual(twice, ['5', '6']);
      assert.deepEqual(
        (await revisions(created.entity_id)).map((r) => r.entity_version),
        ['1', '2', '3', '4', '5', '6'],
      );
      evidence('T01', {
        expected,
        lifecycle: history,
        final: await entity(created.entity_id),
        committedHistory: await revisions(created.entity_id),
        highVersion: highVersion.rows,
        constraints,
      });
    }
  },
);

test(
  'T02 rollback after executed single, multiple, and inserted source revisions',
  { timeout: 20_000 },
  async () => {
    {
      const owner = sourceOwner('T02');
      const original = await owner.transaction((c) =>
        c.create('{"state":"original"}'),
      );
      const beforeState = await entity(original.entity_id);
      const beforeEvents = await revisions(original.entity_id);
      for (const changes of [1, 2]) {
        await assert.rejects(
          owner.transaction(async (c) => {
            for (let n = 1; n <= changes; n++)
              await c.mutate(
                original.entity_id,
                'update',
                JSON.stringify({ state: `uncommitted-${n}` }),
              );
            const inTransaction = (await c.inspect(original.entity_id)).outbox;
            assert.deepEqual(
              inTransaction.map((r) => r.entity_version),
              changes === 1 ? ['1', '2'] : ['1', '2', '3'],
            );
            evidence('T02-reached-SQL', {
              changes,
              transaction: (await c.inspect(original.entity_id)).session,
              inTransaction,
            });
            throw new Error('intentional rollback after SQL');
          }),
          (error: unknown) =>
            error instanceof SourceTransactionError &&
            error.outcome === 'rolled_back' &&
            (error.cause as Error).message === 'intentional rollback after SQL',
        );
        assert.deepEqual(await entity(original.entity_id), beforeState);
        assert.deepEqual(await revisions(original.entity_id), beforeEvents);
      }
      let rolledBackId = '';
      await assert.rejects(
        owner.transaction(async (c) => {
          rolledBackId = (await c.create('{"state":"uncommitted-insert"}'))
            .entity_id;
          assert.equal((await c.inspect(rolledBackId)).outbox.length, 1);
          evidence('T02-insert-reached-SQL', {
            entity: (await c.inspect(rolledBackId)).entities,
            outbox: (await c.inspect(rolledBackId)).outbox,
          });
          throw new Error('intentional insert rollback');
        }),
        SourceTransactionError,
      );
      assert.deepEqual(await entity(rolledBackId), []);
      assert.deepEqual(await revisions(rolledBackId), []);
      evidence('T02', {
        beforeState,
        beforeEvents,
        afterState: await entity(original.entity_id),
        afterEvents: await revisions(original.entity_id),
        rolledBackId,
        rolledBackInsertRows: await entity(rolledBackId),
        rolledBackInsertEvents: await revisions(rolledBackId),
      });
    }
  },
);

test(
  'T03 real narrowly scoped outbox INSERT error rolls back the source mutation',
  { timeout: 20_000 },
  async () => {
    await withWriter('T03', async (writer) => {
      const original = await createEntity(
        writer,
        '{"state":"before-outbox-error"}',
      );
      const beforeState = await entity(original.entity_id);
      const beforeEvents = await revisions(original.entity_id);
      await observer.query(`CREATE FUNCTION source.test_reject_one_outbox() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN RAISE EXCEPTION 'T03 injected outbox INSERT failure' USING ERRCODE='P9001'; END; $$;
      CREATE TRIGGER test_reject_one_outbox BEFORE INSERT ON source.outbox FOR EACH ROW WHEN (NEW.entity_id=${positiveBigint(original.entity_id)} AND NEW.entity_version=2) EXECUTE FUNCTION source.test_reject_one_outbox();`);
      try {
        await sqlError(
          writer,
          'SELECT * FROM source.mutate_entity($1,\'update\',\'{"state":"must-rollback"}\'::jsonb)',
          'P9001',
          [original.entity_id],
        );
        assert.deepEqual(await entity(original.entity_id), beforeState);
        assert.deepEqual(await revisions(original.entity_id), beforeEvents);
        evidence('T03', {
          beforeState,
          afterState: await entity(original.entity_id),
          beforeEvents,
          afterEvents: await revisions(original.entity_id),
          sqlState: 'P9001',
        });
      } finally {
        await observer.query(
          'DROP TRIGGER test_reject_one_outbox ON source.outbox; DROP FUNCTION source.test_reject_one_outbox()',
        );
      }
      assert.equal(
        (
          await observer.query(
            "SELECT count(*)::text AS count FROM pg_trigger WHERE tgname='test_reject_one_outbox'",
          )
        ).rows[0].count,
        '0',
      );
      await mutateEntity(
        writer,
        original.entity_id,
        'update',
        '{"state":"after-cleanup"}',
      );
      assert.deepEqual(
        (await revisions(original.entity_id)).map((r) => r.entity_version),
        ['1', '2'],
      );
    });
  },
);

test(
  'T04 concurrent writers wait on an observed row lock and create successive revisions',
  { timeout: 25_000 },
  async () => {
    const a = await connect('writer', 'T04-A');
    const b = await connect('writer', 'T04-B');
    try {
      const original = await createEntity(a, '{"writer":"initial"}');
      const aPid = (await a.query('SELECT pg_backend_pid() AS pid')).rows[0]
        .pid;
      const bPid = (await b.query('SELECT pg_backend_pid() AS pid')).rows[0]
        .pid;
      await a.query('BEGIN');
      await b.query('BEGIN');
      await mutateEntity(a, original.entity_id, 'update', '{"writer":"A"}');
      const pendingB = mutateEntity(
        b,
        original.entity_id,
        'update',
        '{"writer":"B"}',
      ).then(
        (value) => ({ value, error: null }),
        (error: unknown) => ({ value: null, error }),
      );
      const lock = await databaseWaitFor(
        observer,
        async () =>
          (
            await observer.query(
              'SELECT pid, application_name, state, wait_event_type, wait_event, backend_xid::text, pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE pid=$1',
              [bPid],
            )
          ).rows,
        (rows) =>
          rows.length === 1 &&
          rows[0].wait_event_type === 'Lock' &&
          (rows[0].blockers as number[]).includes(aPid),
        'T04 B blocked by A',
      );
      evidence('T04-lock', { aPid, bPid, lock });
      assert.equal((await entity(original.entity_id))[0].entity_version, '1');
      await a.query('COMMIT');
      const completedB = await pendingB;
      if (completedB.error) throw completedB.error;
      assert.equal(completedB.value?.entity_version, '3');
      await b.query('COMMIT');
      const history = await revisions(original.entity_id);
      assert.deepEqual(
        history.map((r) => r.entity_version),
        ['1', '2', '3'],
      );
      assert.deepEqual(
        history.map((r) => r.payload_json),
        ['{"writer": "initial"}', '{"writer": "A"}', '{"writer": "B"}'],
      );
      assert.equal(
        (await entity(original.entity_id))[0].payload_json,
        '{"writer": "B"}',
      );
      assert.equal(new Set(history.map((r) => r.change_id)).size, 3);
      evidence('T04', { history, final: await entity(original.entity_id) });
    } finally {
      await Promise.allSettled([a.query('ROLLBACK'), b.query('ROLLBACK')]);
      await Promise.all([a.end(), b.end()]);
    }
  },
);

test(
  'T05 historical after-images stay unchanged and evidence cannot be edited or deleted',
  { timeout: 20_000 },
  async () => {
    await withWriter('T05', async (writer) => {
      const original = await createEntity(
        writer,
        '{"history":{"nested":[1,"first"]}}',
      );
      await mutateEntity(
        writer,
        original.entity_id,
        'update',
        '{"history":{"nested":[2,"second"]}}',
      );
      const recorded = await revisions(original.entity_id);
      assert.deepEqual(
        recorded.map((r) => r.payload_json),
        [
          '{"history": {"nested": [1, "first"]}}',
          '{"history": {"nested": [2, "second"]}}',
        ],
      );
      await mutateEntity(writer, original.entity_id, 'delete');
      await mutateEntity(
        writer,
        original.entity_id,
        'restore',
        '{"history":{"nested":[4,"restored"]}}',
      );
      assert.deepEqual(
        (await revisions(original.entity_id)).slice(0, 2),
        recorded,
      );
      for (const sql of [
        "UPDATE source.outbox SET payload='{}' WHERE entity_id=$1",
        'DELETE FROM source.outbox WHERE entity_id=$1',
      ])
        await sqlError(writer, sql, '42501', [original.entity_id]);
      await sqlError(
        observer,
        "UPDATE source.outbox SET payload='{}' WHERE entity_id=$1",
        '0A000',
        [original.entity_id],
      );
      await sqlError(
        observer,
        'DELETE FROM source.outbox WHERE entity_id=$1',
        '0A000',
        [original.entity_id],
      );
      assert.deepEqual(
        (await revisions(original.entity_id)).slice(0, 2),
        recorded,
      );
      evidence('T05', {
        expectedHistoricalFields: recorded,
        afterLaterMutations: await revisions(original.entity_id),
      });
    });
  },
);

test(
  'T06 actual runtime login, owners, grants, protected metadata and unsupported SQL rejection',
  { timeout: 25_000 },
  async () => {
    await withWriter('T06', async (writer) => {
      const identity = (
        await writer.query(
          "SELECT session_user, current_user, pg_backend_pid() AS pid, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls, pg_has_role(current_user, 'source_owner', 'MEMBER') AS owner_member FROM pg_roles WHERE rolname=current_user",
        )
      ).rows[0];
      assert.equal(identity.session_user, 'source_writer');
      assert.equal(identity.current_user, 'source_writer');
      for (const field of [
        'rolsuper',
        'rolcreaterole',
        'rolcreatedb',
        'rolreplication',
        'rolbypassrls',
        'owner_member',
      ])
        assert.equal(identity[field], false);
      const owners = (
        await observer.query(
          "SELECT c.relname, pg_get_userbyid(c.relowner) AS owner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='source' ORDER BY c.relname",
        )
      ).rows;
      assert.ok(owners.length >= 5);
      for (const object of owners) assert.equal(object.owner, 'source_owner');
      assert.equal(
        (
          await observer.query(
            "SELECT pg_get_userbyid(nspowner) AS owner FROM pg_namespace WHERE nspname='source'",
          )
        ).rows[0].owner,
        'source_owner',
      );
      const ownerRole = (
        await observer.query(
          "SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname='source_owner'",
        )
      ).rows[0];
      assert.deepEqual(ownerRole, { rolcanlogin: false, rolsuper: false });
      const functions = (
        await observer.query(
          "SELECT p.proname, pg_get_userbyid(p.proowner) AS owner, p.prosecdef, p.proconfig, has_function_privilege('source_writer',p.oid,'EXECUTE') AS writer_execute, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') AS public_execute FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='source' ORDER BY p.proname",
        )
      ).rows;
      assert.equal(functions.length, 6);
      for (const fn of functions) {
        assert.equal(fn.owner, 'source_owner');
        assert.deepEqual(fn.proconfig, ['search_path=pg_catalog, pg_temp']);
        assert.equal(fn.public_execute, false);
        assert.equal(
          fn.writer_execute,
          ['create_entity', 'mutate_entity'].includes(fn.proname as string),
        );
        assert.equal(
          fn.prosecdef,
          [
            'create_entity',
            'mutate_entity',
            'prepare_revision',
            'capture_revision',
          ].includes(fn.proname as string),
        );
      }
      const original = await createEntity(writer, '{"privileges":"original"}');
      const id = positiveBigint(original.entity_id);
      const denied = [
        `INSERT INTO source.entities(entity_id,payload) OVERRIDING SYSTEM VALUE VALUES (${id},'{}')`,
        "INSERT INTO source.entities(payload) VALUES ('{}')",
        ...[
          'source_epoch=gen_random_uuid()',
          'entity_version=999',
          'change_id=gen_random_uuid()',
          "recorded_at='2000-01-01'",
          "payload='{}'",
          'is_deleted=true',
        ].map(
          (set) => `UPDATE source.entities SET ${set} WHERE entity_id=${id}`,
        ),
        `DELETE FROM source.entities WHERE entity_id=${id}`,
        'TRUNCATE source.entities CASCADE',
        'TRUNCATE source.outbox',
        'INSERT INTO source.outbox(source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload) SELECT source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload FROM source.entities',
        'DELETE FROM source.outbox',
        "UPDATE source.outbox SET payload='{}'",
        'UPDATE source.source_identity SET source_epoch=gen_random_uuid()',
        'DELETE FROM source.source_identity',
        'ALTER TABLE source.entities DISABLE TRIGGER ALL',
        'DROP TRIGGER entities_capture ON source.entities',
        'ALTER TABLE source.outbox DISABLE TRIGGER ALL',
        'SET session_replication_role=replica',
        'SET ROLE source_owner',
        'SET ROLE m1_admin',
        'SET ROLE pg_write_all_data',
        'SET SESSION AUTHORIZATION source_owner',
        'ALTER ROLE source_writer SUPERUSER',
        "SELECT setval('source.entities_entity_id_seq',1)",
        "SELECT nextval('source.entities_entity_id_seq')",
        'ALTER SEQUENCE source.entities_entity_id_seq RESTART WITH 1',
        'SELECT source.next_version(1)',
        'SELECT source.prepare_revision()',
        'SELECT source.capture_revision()',
        'ALTER FUNCTION source.capture_revision() SECURITY INVOKER',
        'CREATE TABLE source.shadow(id int)',
        'CREATE TABLE public.shadow(id int)',
        'CREATE TEMP TABLE shadow(id int)',
        'CREATE SCHEMA shadow',
        'COPY source.entities(payload) FROM STDIN',
        `MERGE INTO source.entities e USING (VALUES (${id}::bigint)) AS v(id) ON e.entity_id=v.id WHEN MATCHED THEN UPDATE SET payload='{}'`,
        "INSERT INTO source.entities(payload) VALUES ('{}') ON CONFLICT(entity_id) DO UPDATE SET payload='{}'",
      ];
      for (const sql of denied) await sqlError(writer, sql, '42501');
      // PostgreSQL's generated-column check precedes its permission check here.
      // Review: the original 42501 expectation was wrong; retain exact rejection.
      await sqlError(
        writer,
        `UPDATE source.entities SET entity_id=1 WHERE entity_id=${id}`,
        '428C9',
      );
      const beforeSearchPath = await revisions(id);
      await writer.query('SET search_path=public,pg_temp');
      await mutateEntity(
        writer,
        id,
        'update',
        '{"privileges":"safe-fixed-path"}',
      );
      assert.deepEqual((await revisions(id)).slice(0, 1), beforeSearchPath);
      await sqlError(
        writer,
        "SELECT source.mutate_entity($1,'upsert','{}')",
        '22023',
        [id],
      );
      await sqlError(
        writer,
        "SELECT source.mutate_entity($1,'restore','{}')",
        '22023',
        [id],
      );
      await sqlError(
        writer,
        "SELECT source.mutate_entity($1,'delete','{}')",
        '22023',
        [id],
      );
      for (const badPayload of [null, 'null', '[]', '"scalar"'])
        await sqlError(
          writer,
          'SELECT source.create_entity($1::jsonb)',
          '23514',
          [badPayload],
        );
      await mutateEntity(writer, id, 'delete');
      await sqlError(
        writer,
        "SELECT source.mutate_entity($1,'update','{}')",
        '22023',
        [id],
      );
      await sqlError(
        writer,
        "SELECT source.mutate_entity($1,'restore',NULL)",
        '23514',
        [id],
      );
      assert.equal((await entity(id))[0].is_deleted, true);
      await mutateEntity(writer, id, 'restore', '{}');
      const final = (await entity(id))[0];
      assert.equal(final.entity_version, '4');
      assert.equal(final.is_deleted, false);
      await sqlError(
        observer,
        `DELETE FROM source.entities WHERE entity_id=${id}`,
        '0A000',
      );
      await sqlError(observer, 'TRUNCATE source.entities CASCADE', '0A000');
      const triggers = (
        await observer.query(
          "SELECT c.relname, t.tgname, t.tgenabled FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='source'::regnamespace AND NOT t.tgisinternal ORDER BY t.tgname",
        )
      ).rows;
      assert.equal(triggers.length, 6);
      for (const t of triggers) assert.equal(t.tgenabled, 'O');
      evidence('T06', {
        identity,
        ownerRole,
        owners,
        functions,
        deniedOperations: denied.length,
        final,
        triggers,
      });
    });
  },
);

test(
  'T07 known allocation-order risk: B commits first, late A remains queryable by identity',
  { timeout: 20_000 },
  async () => {
    const a = await connect('writer', 'T07-A');
    const b = await connect('writer', 'T07-B');
    try {
      await a.query('BEGIN');
      const aRow = await createEntity(a, '{"transaction":"A-late-commit"}');
      const aEvent = (await revisions(aRow.entity_id, a))[0];
      await b.query('BEGIN');
      const bRow = await createEntity(b, '{"transaction":"B-first-commit"}');
      const bEvent = (await revisions(bRow.entity_id, b))[0];
      assert.ok(
        BigInt(aEvent.allocation_id as string) <
          BigInt(bEvent.allocation_id as string),
      );
      await b.query('COMMIT');
      assert.deepEqual(await revisions(aRow.entity_id), []);
      assert.deepEqual(await entity(aRow.entity_id), []);
      assert.deepEqual(await revisions(bRow.entity_id), [bEvent]);
      evidence('T07-B-visible-A-uncommitted', {
        aEvent,
        bEvent,
        visibleA: await revisions(aRow.entity_id),
        visibleB: await revisions(bRow.entity_id),
      });
      await a.query('COMMIT');
      assert.deepEqual(await revisions(aRow.entity_id), [aEvent]);
      assert.deepEqual(await revisions(bRow.entity_id), [bEvent]);
      // Test-only characterization of a KNOWN unsafe watermark. No production cursor exists.
      const unsafe = (
        await observer.query(
          'SELECT entity_id::text, entity_version::text FROM source.outbox WHERE entity_id IN ($1,$2) AND allocation_id > $3',
          [aRow.entity_id, bRow.entity_id, bEvent.allocation_id],
        )
      ).rows;
      assert.deepEqual(unsafe, []);
      evidence('T07', {
        aEvent,
        bEvent,
        committedA: await revisions(aRow.entity_id),
        committedB: await revisions(bRow.entity_id),
        unsafeGreaterThanB: unsafe,
        conclusion:
          'allocation order is not commit order; no incremental completeness claim',
      });
    } finally {
      await Promise.allSettled([a.query('ROLLBACK'), b.query('ROLLBACK')]);
      await Promise.all([a.end(), b.end()]);
    }
  },
);

test('T10 source tests leave no runtime database sessions or instrumentation', async () => {
  const sessions = (
    await observer.query(
      "SELECT pid, application_name, state FROM pg_stat_activity WHERE usename='source_writer'",
    )
  ).rows;
  assert.deepEqual(sessions, []);
  const instrumentation = (
    await observer.query(
      "SELECT proname FROM pg_proc WHERE pronamespace='source'::regnamespace AND proname LIKE 'test_%'",
    )
  ).rows;
  assert.deepEqual(instrumentation, []);
  evidence('T10-source-cleanup', {
    sessions,
    instrumentation,
    note: 'service restart, owned-resource cleanup, and a fresh full run are checked by the orchestration and final acceptance',
  });
});
