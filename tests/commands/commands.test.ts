import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { before, after, test } from 'node:test';
import type pg from 'pg';
import { first } from '../../scripts/rows.ts';
import { withCleanup } from '../../scripts/support.ts';
import {
  SourceOwnershipError,
  type CommandReply,
  type SourceWork,
} from '../../src/source.ts';
import { connect, evidence, sourceOwner } from '../support/db.ts';
import {
  request,
  execute,
  epoch,
  state,
  assertReceipt,
  rejected,
  waitBlocked,
  sqlReject,
  counts,
} from '../support/commands.ts';
let observer: pg.Client;
let sourceEpoch: string;
before(async () => {
  observer = await connect('admin', 'commands-observer');
  sourceEpoch = await epoch(observer);
});
after(async () => {
  await observer?.end();
});

void test('C01 command lifecycle retains exact immutable results', async () => {
  // Disjoint high-ID range: independent of M1 suite execution order.
  await observer.query(
    'ALTER SEQUENCE source.entities_entity_id_seq RESTART WITH 9007199254741993',
  );
  const create = request(
    sourceEpoch,
    'create',
    null,
    '{"n":9007199254740993,"decimal":12345678901234567890.1234567890123456789}',
  );
  const created = await execute(create);
  assert.equal(created.result.entity_id, '9007199254741993');
  assert.match(created.result.payload_json ?? '', /9007199254740993/);
  assert.match(
    created.result.payload_json ?? '',
    /12345678901234567890\.1234567890123456789/,
  );
  const id = created.result.entity_id;
  const replies = [created];
  const commands = [
    create,
    request(sourceEpoch, 'update', id, '{"n":9007199254740994}'),
    request(sourceEpoch, 'delete', id, null),
    request(sourceEpoch, 'restore', id, '{"restored":true}'),
  ];
  for (const command of commands.slice(1)) replies.push(await execute(command));
  assert.deepEqual(
    replies.map((r) => r.result.entity_version),
    ['1', '2', '3', '4'],
  );
  assert.deepEqual(
    replies.map((r) => r.result.is_deleted),
    [false, false, true, false],
  );
  assert.equal(replies[2]?.result.payload_json, null);
  for (const [i, command] of commands.entries()) {
    const reply = replies[i];
    assert.ok(reply);
    assert.equal(reply.replayed, false);
    await assertReceipt(observer, command, reply);
  }
  const final = await state(observer, create, id);
  assert.deepEqual(
    final.outbox,
    replies.map((r) => r.result),
  );
  assert.deepEqual(final.entities, [first(replies.slice(-1)).result]);
  evidence('C01', { commands, replies, final });
});

void test('C02 normalized sequential replay executes once', async () => {
  const command = request(
    sourceEpoch,
    'create',
    null,
    '{"b":[1,2],"a":9007199254740993,"n":1.00}',
  );
  const original = await execute(command);
  const before = await state(observer, command, original.result.entity_id);
  const countsBefore = await counts(observer);
  const owner = sourceOwner('C02', 'command');
  let expired: SourceWork | undefined;
  for (let i = 0; i < 12; i++) {
    const replay = await owner.transaction(async (tx) => {
      expired = tx;
      return tx.command({
        ...command,
        payloadJson: ' { "n": 1, "a": 9007199254740993, "b": [1, 2] } ',
      });
    });
    assert.equal(replay.replayed, true);
    assert.deepEqual(replay.result, original.result);
  }
  assert.ok(expired);
  await assert.rejects(expired.command(command), SourceOwnershipError);
  assert.deepEqual(
    await state(observer, command, original.result.entity_id),
    before,
  );
  assert.deepEqual(await counts(observer), countsBefore);
  evidence('C02', {
    command,
    original,
    replays: 12,
    countsBefore,
    countsAfter: await counts(observer),
    before,
    after: await state(observer, command, original.result.entity_id),
  });
});

void test('C03 conflicting requests and wrong epoch cannot mutate', async () => {
  const created = await execute(request(sourceEpoch));
  const other = await execute(request(sourceEpoch));
  const command = request(
    sourceEpoch,
    'update',
    created.result.entity_id,
    '{"n":9007199254740993,"a":[1,2],"x":null}',
  );
  const original = await execute(command);
  const before = await state(observer, command, original.result.entity_id);
  const countsBefore = await counts(observer);
  const otherBefore = await state(observer, command, other.result.entity_id);
  const mismatches = [
    { ...command, operation: 'delete' as const, payloadJson: null },
    { ...command, entityId: other.result.entity_id },
    { ...command, payloadJson: '{"n":9007199254740994,"a":[1,2],"x":null}' },
    { ...command, payloadJson: '{"n":9007199254740993,"a":[2,1],"x":null}' },
    { ...command, payloadJson: '{"n":9007199254740993,"a":[1,2]}' },
    { ...command, contractVersion: 2 },
  ];
  for (const changed of mismatches) await rejected(execute(changed), 'P2001');
  await rejected(execute({ ...command, sourceEpoch: randomUUID() }), 'P2002');
  assert.deepEqual(
    await state(observer, command, original.result.entity_id),
    before,
  );
  assert.deepEqual(await counts(observer), countsBefore);
  assert.deepEqual(
    await state(observer, command, other.result.entity_id),
    otherBefore,
  );
  const invalid = request(sourceEpoch);
  for (const changed of [
    {
      ...invalid,
      operation: 'delete' as const,
      entityId: created.result.entity_id,
      payloadJson: '{}',
    },
    { ...invalid, entityId: created.result.entity_id },
    { ...invalid, operation: 'update' as const },
    { ...invalid, payloadJson: null },
    { ...invalid, payloadJson: 'null' },
    { ...invalid, payloadJson: '[]' },
    { ...invalid, contractVersion: 2 },
  ])
    await rejected(execute(changed), '22023');
  assert.deepEqual(await counts(observer), countsBefore);
  evidence('C03', {
    command,
    original,
    mismatchCount: mismatches.length,
    codes: ['P2001', 'P2002'],
    before,
    after: await state(observer, command, original.result.entity_id),
  });
});

void test('C04 historical replay precedes lifecycle evaluation', async () => {
  const create = request(sourceEpoch, 'create', null, '{"phase":"original"}');
  const original = await execute(create);
  const id = original.result.entity_id;
  const update = request(sourceEpoch, 'update', id, '{"phase":"updated"}');
  const updated = await execute(update);
  const deletion = request(sourceEpoch, 'delete', id, null);
  const deleted = await execute(deletion);
  assert.deepEqual((await execute(create)).result, original.result);
  assert.deepEqual((await execute(update)).result, updated.result);
  const restore = request(sourceEpoch, 'restore', id, '{"phase":"restored"}');
  const restored = await execute(restore);
  assert.deepEqual((await execute(restore)).result, restored.result);
  await execute(request(sourceEpoch, 'update', id, '{"phase":"later"}'));
  const before = await state(observer, restore, id);
  for (const [command, reply] of [
    [create, original],
    [update, updated],
    [deletion, deleted],
    [restore, restored],
  ] as const) {
    const replay = await execute(command);
    assert.equal(replay.replayed, true);
    assert.deepEqual(replay.result, reply.result);
    await assertReceipt(observer, command, replay);
  }
  assert.deepEqual(await state(observer, restore, id), before);
  evidence('C04', {
    create,
    update,
    deletion,
    restore,
    original,
    updated,
    deleted,
    restored,
    final: before,
  });
});

void test('C05 no-op commands retain receipts without revisions', async () => {
  const original = await execute(request(sourceEpoch));
  const id = original.result.entity_id;
  const noop = request(sourceEpoch, 'update', id, '{}');
  const unchanged = await execute(noop);
  assert.deepEqual(unchanged.result, original.result);
  assert.equal(unchanged.replayed, false);
  await execute(request(sourceEpoch, 'delete', id, null));
  const repeatedDelete = request(sourceEpoch, 'delete', id, null);
  const deleted = await execute(repeatedDelete);
  assert.equal(deleted.result.entity_version, '2');
  assert.equal(deleted.replayed, false);
  assert.equal((await state(observer, noop, id)).outbox.length, 2);
  await assertReceipt(observer, noop, unchanged);
  await assertReceipt(observer, repeatedDelete, deleted);
  await execute(request(sourceEpoch, 'restore', id, '{"later":true}'));
  const before = await state(observer, repeatedDelete, id);
  assert.deepEqual((await execute(noop)).result, unchanged.result);
  assert.deepEqual((await execute(repeatedDelete)).result, deleted.result);
  assert.deepEqual(await state(observer, repeatedDelete, id), before);
  evidence('C05', { noop, repeatedDelete, unchanged, deleted, final: before });
});

for (const scenario of [
  [
    'C06 concurrent duplicates wait and replay the committed winner',
    'C06',
    'same',
  ],
  [
    'C07 concurrent mismatch rejects after the winner commits',
    'C07',
    'mismatch',
  ],
  ['C08 waiting duplicate executes after winner rollback', 'C08', 'rollback'],
] as const) {
  void test(scenario[0], { timeout: 20_000 }, async () => {
    const [, id, mode] = scenario;
    const command = request(sourceEpoch, 'create', null, '{"winner":true}');
    const countsBefore = await counts(observer);
    const reached = Promise.withResolvers<CommandReply>();
    const release = Promise.withResolvers<void>();
    const stop = new Error('explicit reservation rollback');
    const winner = sourceOwner(`${id}-winner`, 'command').transaction(
      async (tx) => {
        const reply = await tx.command(command);
        reached.resolve(reply);
        await release.promise;
        if (mode === 'rollback') throw stop;
        return reply;
      },
    );
    // Observe rejections immediately, including during barrier cleanup.
    const winnerSettled = winner.then(
      (value) => ({ value }),
      (error: unknown) => {
        reached.reject(error);
        return { error };
      },
    );
    let waiting: Promise<PromiseSettledResult<CommandReply>[]> | undefined;
    try {
      const firstReply = await reached.promise;
      const competitor =
        mode === 'mismatch'
          ? { ...command, payloadJson: '{"winner":false}' }
          : command;
      const loser = execute(competitor, `${id}-waiter`);
      waiting = Promise.allSettled([loser]);
      const blocking = await waitBlocked(
        observer,
        `${id}-waiter`,
        `${id}-winner`,
      );
      const before = await state(
        observer,
        command,
        firstReply.result.entity_id,
      );
      assert.deepEqual(before, { receipts: [], entities: [], outbox: [] });
      release.resolve();
      const winnerResult = await winnerSettled;
      const loserResult = first(await waiting);
      if (mode === 'rollback') {
        assert.ok('error' in winnerResult);
        assert.ok(winnerResult.error instanceof Error);
        const failure = await rejected(
          Promise.reject(winnerResult.error),
          undefined,
        );
        assert.equal(failure.cause, stop);
        assert.equal(failure.completionTag, 'ROLLBACK');
        assert.equal(loserResult.status, 'fulfilled');
        if (loserResult.status !== 'fulfilled') throw new Error('unreachable');
        assert.equal(loserResult.value.replayed, false);
        assert.notEqual(
          loserResult.value.result.entity_id,
          firstReply.result.entity_id,
        );
        assert.deepEqual(
          (await state(observer, command, firstReply.result.entity_id))
            .entities,
          [],
        );
        assert.deepEqual(
          (await state(observer, command, firstReply.result.entity_id)).outbox,
          [],
        );
        await assertReceipt(observer, command, loserResult.value);
        assert.equal(
          (await state(observer, command, loserResult.value.result.entity_id))
            .outbox.length,
          1,
        );
      } else {
        assert.ok('value' in winnerResult);
        assert.deepEqual(winnerResult.value, firstReply);
        if (mode === 'same') {
          assert.equal(loserResult.status, 'fulfilled');
          if (loserResult.status !== 'fulfilled')
            throw new Error('unreachable');
          assert.equal(loserResult.value.replayed, true);
          assert.deepEqual(loserResult.value.result, firstReply.result);
        } else {
          assert.equal(loserResult.status, 'rejected');
          if (loserResult.status !== 'rejected') throw new Error('unreachable');
          assert.ok(loserResult.reason instanceof Error);
          await rejected(Promise.reject(loserResult.reason), 'P2001');
        }
        await assertReceipt(observer, command, firstReply);
        assert.equal(
          (await state(observer, command, firstReply.result.entity_id)).outbox
            .length,
          1,
        );
      }
      const countsAfter = await counts(observer);
      for (const field of ['entities', 'outbox', 'receipts'] as const)
        assert.equal(
          BigInt(countsAfter[field]),
          BigInt(countsBefore[field]) + 1n,
        );
      evidence(id, {
        countsBefore,
        countsAfter,
        command,
        competitor,
        blocking,
        before,
        firstReply,
        winnerOutcome: mode === 'rollback' ? 'rolled_back' : 'committed',
        loser:
          loserResult.status === 'fulfilled'
            ? loserResult.value
            : { sqlState: 'P2001' },
        after: await state(observer, command, firstReply.result.entity_id),
      });
    } finally {
      release.resolve();
      await winnerSettled;
      if (waiting) await waiting;
    }
  });
}
void test('C11 SQL failure atomically removes the command reservation', async () => {
  const created = await execute(request(sourceEpoch));
  const id = created.result.entity_id;
  const command = request(
    sourceEpoch,
    'update',
    id,
    '{"error_then_retry":true}',
  );
  const before = await state(observer, command, id);
  await observer.query(`CREATE FUNCTION source.test_command_outbox_failure() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN IF NEW.entity_id=${id} AND NEW.entity_version=2 THEN RAISE EXCEPTION 'M2A deliberate outbox failure' USING ERRCODE='P9002'; END IF; RETURN NEW; END $$;
    CREATE TRIGGER test_command_outbox_failure BEFORE INSERT ON source.outbox FOR EACH ROW EXECUTE FUNCTION source.test_command_outbox_failure()`);
  await withCleanup(
    async () => {
      await rejected(execute(command), 'P9002');
      assert.deepEqual(await state(observer, command, id), before);
      evidence('C11-rollback', {
        command,
        code: 'P9002',
        before,
        after: await state(observer, command, id),
      });
    },
    async () => {
      await observer.query(
        'DROP TRIGGER test_command_outbox_failure ON source.outbox; DROP FUNCTION source.test_command_outbox_failure()',
      );
    },
  );
  const retry = await execute(command);
  assert.equal(retry.replayed, false);
  assert.equal(retry.result.entity_version, '2');
  await assertReceipt(observer, command, retry);
  const invalid = request(sourceEpoch, 'restore', id, '{}');
  await rejected(execute(invalid), '22023');
  assert.equal((await state(observer, invalid, id)).receipts.length, 0);
  await execute(request(sourceEpoch, 'delete', id, null));
  const later = await execute(invalid);
  assert.equal(later.replayed, false);
  evidence('C11', {
    command,
    retry,
    invalidFailedThenLaterSucceeded: invalid,
    later,
    final: await state(observer, command, id),
  });
});

void test('C12 command privileges and receipt completion are enforced', async () => {
  const command = request(sourceEpoch);
  const original = await execute(command);
  const id = original.result.entity_id;
  const before = await state(observer, command, id);
  const runtime = await connect('command', 'C12-runtime');
  await withCleanup(
    async () => {
      const identity = first(
        (
          await runtime.query<
            Record<string, unknown>
          >(`SELECT session_user,current_user,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls,rolinherit,
      pg_has_role(current_user,'source_owner','MEMBER') AS owner_member,pg_has_role(current_user,'source_writer','MEMBER') AS writer_member FROM pg_roles WHERE rolname=current_user`)
        ).rows,
      );
      assert.equal(identity['session_user'], 'source_command');
      assert.equal(identity['current_user'], 'source_command');
      for (const [key, value] of Object.entries(identity))
        if (!['session_user', 'current_user'].includes(key))
          assert.equal(value, false, key);
      const denied = [
        'SELECT * FROM source.command_receipts',
        'SELECT * FROM source.entities',
        'SELECT * FROM source.outbox',
        'SELECT * FROM source.source_identity',
        'UPDATE source.command_receipts SET completed=true',
        'DELETE FROM source.command_receipts',
        'TRUNCATE source.command_receipts',
        `INSERT INTO source.command_receipts(source_epoch,command_id,contract_version,operation,request_payload) VALUES ('${sourceEpoch}','${randomUUID()}',1,'create','{}')`,
        "SELECT source.create_entity('{}')",
        `SELECT source.mutate_entity(${id},'delete',NULL)`,
        "INSERT INTO source.entities(payload) VALUES ('{}')",
        "UPDATE source.entities SET payload='{}'",
        'DELETE FROM source.entities',
        "UPDATE source.outbox SET payload='{}'",
        'TRUNCATE source.outbox',
        'ALTER TABLE source.entities DISABLE TRIGGER ALL',
        'ALTER TABLE source.command_receipts DISABLE TRIGGER ALL',
        'SET session_replication_role=replica',
        'SET ROLE source_owner',
        'SET ROLE source_writer',
        'SET ROLE m1_admin',
        'SELECT source.require_command_completion()',
        'SELECT source.guard_command_receipt()',
        "SELECT nextval('source.entities_entity_id_seq')",
        'CREATE TEMP TABLE command_shadow(id int)',
        'ALTER FUNCTION source.execute_command(uuid,uuid,integer,text,bigint,jsonb) SECURITY INVOKER',
      ];
      for (const sql of denied) await sqlReject(runtime, sql, '42501');
      const surface = (
        await observer.query<{
          proname: string;
          allowed: boolean;
          public_execute: boolean;
          volatility: string;
          owner: string;
        }>(`SELECT proname,has_function_privilege('source_command',p.oid,'EXECUTE') AS allowed,
      EXISTS(SELECT FROM aclexplode(p.proacl) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') AS public_execute,provolatile AS volatility,pg_get_userbyid(proowner) AS owner
      FROM pg_proc p WHERE pronamespace='source'::regnamespace ORDER BY proname`)
      ).rows;
      assert.deepEqual(
        surface.filter((f) => f.allowed).map((f) => f.proname),
        ['execute_command'],
      );
      for (const fn of surface) {
        assert.equal(fn.public_execute, false);
        assert.equal(fn.owner, 'source_owner');
      }
      assert.equal(
        surface.find((f) => f.proname === 'execute_command')?.volatility,
        'v',
      );
      await runtime.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
      await sqlReject(
        runtime,
        "SELECT source.execute_command($1,$2,1,'create',NULL,'{}')",
        '25001',
        [sourceEpoch, randomUUID()],
      );
      await runtime.query('ROLLBACK');
      await runtime.query(
        "SET search_path=public,pg_temp; SET TIME ZONE 'Pacific/Auckland'",
      );
      const raw = first(
        (
          await runtime.query<{ recorded_at: string; replayed: boolean }>(
            "SELECT recorded_at,replayed FROM source.execute_command($1,$2,1,'create',NULL,'{}')",
            [sourceEpoch, command.commandId],
          )
        ).rows,
      );
      assert.equal(raw.recorded_at, original.result.recorded_at);
      assert.equal(raw.replayed, true);
      for (const sql of [
        'UPDATE source.command_receipts SET result_version=result_version',
        'DELETE FROM source.command_receipts',
        'TRUNCATE source.command_receipts',
      ])
        await sqlReject(observer, sql, '0A000');
      const incomplete = request(sourceEpoch);
      await observer.query('BEGIN');
      await observer.query(
        "INSERT INTO source.command_receipts(source_epoch,command_id,contract_version,operation,request_payload) VALUES ($1,$2,1,'create','{}')",
        [sourceEpoch, incomplete.commandId],
      );
      await sqlReject(observer, 'COMMIT', 'P2003');
      await observer.query('ROLLBACK');
      assert.equal((await state(observer, incomplete, id)).receipts.length, 0);
      const constraint = first(
        (
          await observer.query<{
            tgdeferrable: boolean;
            tginitdeferred: boolean;
          }>(
            "SELECT tgdeferrable,tginitdeferred FROM pg_trigger WHERE tgname='command_receipts_complete'",
          )
        ).rows,
      );
      assert.deepEqual(constraint, {
        tgdeferrable: true,
        tginitdeferred: true,
      });
      assert.deepEqual(await state(observer, command, id), before);
      evidence('C12', {
        command,
        identity,
        deniedOperations: denied.length,
        surface,
        constraint,
        incompleteKey: incomplete.commandId,
        incompleteCommitSqlState: 'P2003',
        before,
        after: await state(observer, command, id),
      });
    },
    () => runtime.end(),
  );
});
