import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import test from 'node:test';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import { waitFor, CleanupFailure } from '../../scripts/support.ts';
import { connect, databaseWaitFor, evidence, required } from '../support/db.ts';
import {
  request,
  execute,
  epoch,
  state,
  assertReceipt,
  activity,
  counts,
} from '../support/commands.ts';
import { positiveBigint, type CommandReply } from '../../src/source.ts';

function readReply(value: unknown): CommandReply {
  const reply = object(value),
    row = object(reply['result']);
  assert.ok(
    typeof reply['replayed'] === 'boolean' &&
      typeof row['source_epoch'] === 'string' &&
      typeof row['change_id'] === 'string' &&
      typeof row['recorded_at'] === 'string' &&
      typeof row['is_deleted'] === 'boolean' &&
      (row['payload_json'] === null || typeof row['payload_json'] === 'string'),
  );
  return {
    replayed: reply['replayed'],
    result: {
      entity_id: positiveBigint(row['entity_id']),
      entity_version: positiveBigint(row['entity_version']),
      source_epoch: row['source_epoch'],
      change_id: row['change_id'],
      recorded_at: row['recorded_at'],
      is_deleted: row['is_deleted'],
      payload_json: row['payload_json'],
    },
  };
}
for (const [name, id, stage, mode] of [
  [
    'C09 real pre-COMMIT command SIGKILL permits same-key retry',
    'C09',
    'pre',
    'kill',
  ],
  [
    'C10 real post-COMMIT command SIGKILL recovers original result',
    'C10',
    'post',
    'kill',
  ],
  [
    'C09 healthy pre-COMMIT command release succeeds once',
    'C09-HEALTHY',
    'pre',
    'release',
  ],
  [
    'C10 healthy post-COMMIT command release succeeds once',
    'C10-HEALTHY',
    'post',
    'release',
  ],
] as const) {
  void test(name, { timeout: 25_000 }, async () => {
    const observer = await connect('admin', `${id}-observer`);
    const command = request(
      await epoch(observer),
      'create',
      null,
      `{"fault":"${id}","exact":9007199254740993}`,
    );
    const label = `${id}-child`;
    const child = fork(
      new URL('../support/command-child.ts', import.meta.url),
      [],
      {
        execArgv: [],
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
        env: { PATH: process.env['PATH'], TZ: 'UTC' },
      },
    );
    assert.ok(child.pid && child.stdout && child.stderr);
    const pid = child.pid;
    let telemetry: unknown,
      spawnError: Error | undefined,
      exit: { code: number | null; signal: NodeJS.Signals | null } | undefined,
      closed = false;
    let stdout = '',
      stderr = '',
      overflow = false;
    const capture = (stream: 'stdout' | 'stderr', chunk: string) => {
      if (stdout.length + stderr.length + chunk.length > 65536) {
        overflow = true;
        child.kill('SIGKILL');
        return;
      }
      if (stream === 'stdout') stdout += chunk;
      else stderr += chunk;
    };
    child.stdout
      .setEncoding('utf8')
      .on('data', (chunk: string) => capture('stdout', chunk));
    child.stderr
      .setEncoding('utf8')
      .on('data', (chunk: string) => capture('stderr', chunk));
    child.on('message', (message: unknown) => {
      telemetry = message;
    });
    child.on('error', (error) => {
      spawnError = error;
    });
    child.on('exit', (code, signal) => {
      exit = { code, signal };
    });
    child.on('close', () => {
      closed = true;
    });
    let primary: { error: unknown } | undefined;
    const cleanup: unknown[] = [];
    try {
      const countsBefore = await counts(observer);
      child.send({
        command,
        stage,
        port: Number(required('M1_PORT')),
        password: required('M2A_COMMAND_PASSWORD'),
        applicationName: `${required('M1_RUN_ID')}:${label}`,
      });
      const raw = await waitFor(
        () => {
          if (spawnError) throw spawnError;
          if (exit)
            throw new Error(
              `command child exited early: ${JSON.stringify(exit)} ${stderr}`,
            );
          return telemetry;
        },
        (value) => value !== undefined,
        'command barrier',
      );
      const barrier = object(raw);
      assert.equal(barrier['type'], 'command-barrier');
      assert.equal(barrier['pid'], pid);
      assert.equal(barrier['stage'], stage);
      const reply = readReply(barrier['reply']);
      assert.equal(reply.replayed, false);
      assert.equal(reply.result.entity_version, '1');
      assert.equal(stdout, '');
      assert.equal(overflow, false);
      const session = first(await activity(observer, label));
      assert.equal(session.usename, 'source_command');
      const locks = (
        await observer.query<{
          relation: string | null;
          mode: string;
          transactionid: string | null;
        }>(
          `SELECT relation::regclass::text,mode,transactionid::text FROM pg_locks WHERE pid=$1 AND granted ORDER BY relation,mode`,
          [session.pid],
        )
      ).rows;
      const before = await state(observer, command, reply.result.entity_id);
      if (stage === 'pre') {
        assert.equal(session.state, 'idle in transaction');
        assert.ok(session.backend_xid);
        assert.match(session.query, /source.execute_command/);
        for (const table of [
          'source.command_receipts',
          'source.entities',
          'source.outbox',
        ])
          assert.ok(
            locks.some(
              (lock) =>
                lock.relation === table && lock.mode === 'RowExclusiveLock',
            ),
          );
        assert.ok(
          locks.some(
            (lock) =>
              lock.transactionid === session.backend_xid &&
              lock.mode === 'ExclusiveLock',
          ),
        );
        assert.deepEqual(before, { receipts: [], entities: [], outbox: [] });
      } else {
        assert.equal(session.state, 'idle');
        assert.equal(session.backend_xid, null);
        assert.equal(session.query, 'COMMIT');
        await assertReceipt(observer, command, reply);
        assert.deepEqual(before.entities, [reply.result]);
        assert.deepEqual(before.outbox, [reply.result]);
      }
      evidence(`${id}-confirmed-barrier`, {
        command,
        pid,
        session,
        locks,
        reply,
        before,
        ordinarySuccessBytes: stdout.length,
      });
      if (mode === 'kill') assert.equal(child.kill('SIGKILL'), true);
      else child.send({ type: 'release', stage });
      await waitFor(() => closed, Boolean, 'command child reaped', 5000);
      assert.deepEqual(
        exit,
        mode === 'kill'
          ? { code: null, signal: 'SIGKILL' }
          : { code: 0, signal: null },
      );
      assert.equal(overflow, false);
      assert.equal(stderr, '');
      if (mode === 'kill') assert.equal(stdout, '');
      else {
        const lines = stdout.trim().split('\n');
        assert.equal(lines.length, 1);
        assert.deepEqual(JSON.parse(first(lines)), {
          type: 'caller-success',
          reply,
        });
      }
      const ended = await databaseWaitFor(
        observer,
        () => activity(observer, label),
        (rows) => rows.length === 0,
        'command session ended',
      );
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
      const after = await state(observer, command, reply.result.entity_id);
      if (mode === 'kill' && stage === 'pre')
        assert.deepEqual(after, { receipts: [], entities: [], outbox: [] });
      else {
        await assertReceipt(observer, command, reply);
        assert.deepEqual(after.entities, [reply.result]);
        assert.deepEqual(after.outbox, [reply.result]);
      }
      const recovered = await execute(command, `${id}-explicit-new-owner`);
      if (mode === 'kill' && stage === 'pre') {
        assert.equal(recovered.replayed, false);
        assert.notEqual(recovered.result.entity_id, reply.result.entity_id);
      } else {
        assert.equal(recovered.replayed, true);
        assert.deepEqual(recovered.result, reply.result);
      }
      await assertReceipt(observer, command, recovered);
      const countsAfter = await counts(observer);
      for (const field of ['entities', 'outbox', 'receipts'] as const)
        assert.equal(
          BigInt(countsAfter[field]),
          BigInt(countsBefore[field]) + 1n,
        );
      const afterRetry = await state(
        observer,
        command,
        recovered.result.entity_id,
      );
      assert.equal(afterRetry.receipts.length, 1);
      assert.deepEqual(afterRetry.entities, [recovered.result]);
      assert.deepEqual(afterRetry.outbox, [recovered.result]);
      assert.deepEqual(
        (await execute(command, `${id}-second-explicit-replay`)).result,
        recovered.result,
      );
      assert.deepEqual(
        await state(observer, command, recovered.result.entity_id),
        afterRetry,
      );
      evidence(mode === 'kill' ? `${id}-actual-signal` : id, {
        command,
        stage,
        pid,
        backendPid: session.pid,
        transactionId: session.backend_xid,
        entityId: reply.result.entity_id,
        entityVersion: reply.result.entity_version,
        changeId: reply.result.change_id,
        actualExit: exit,
        ordinarySuccessBytes: mode === 'kill' ? 0 : stdout.length,
        endedSession: ended,
        before,
        after,
        recovered,
        afterRetry,
      });
    } catch (error) {
      primary = { error };
    } finally {
      try {
        if (!exit) child.kill('SIGKILL');
        await waitFor(
          () => closed,
          Boolean,
          'owned command child cleanup',
          5000,
        );
        evidence(`${id}-cleanup`, { pid, exit, closed });
      } catch (error) {
        cleanup.push(error);
      }
      try {
        await observer.end();
      } catch (error) {
        cleanup.push(error);
      }
    }
    if (primary) {
      if (cleanup.length) throw new CleanupFailure(primary.error, cleanup);
      throw primary.error;
    }
    if (cleanup.length)
      throw new AggregateError(cleanup, 'command fault cleanup incomplete');
  });
}
