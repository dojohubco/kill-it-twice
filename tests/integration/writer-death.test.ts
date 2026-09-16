import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import test from 'node:test';
import type pg from 'pg';
import { createEntity } from '../support/sql.ts';
import { waitFor } from '../../scripts/support.ts';
import { connect, evidence, required, databaseWaitFor } from '../support/db.ts';
import type {
  BarrierName,
  BarrierTelemetry,
  StartWriter,
} from '../support/fault-protocol.ts';

interface Exit {
  code: number | null;
  signal: NodeJS.Signals | null;
}
const writers: {
  pid: number;
  exit?: Exit;
  closed: boolean;
  mode: 'kill' | 'release' | 'orphan';
}[] = [];

async function observeRows(client: pg.Client, entityId: string) {
  const entities = (
    await client.query(
      'SELECT entity_id::text, source_epoch::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload_json FROM source.entities WHERE entity_id=$1',
      [entityId],
    )
  ).rows;
  const outbox = (
    await client.query(
      'SELECT allocation_id::text, source_epoch::text, entity_id::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload_json FROM source.outbox WHERE entity_id=$1 ORDER BY entity_version',
      [entityId],
    )
  ).rows;
  return { entities, outbox };
}

async function killAtBarrier(
  name: BarrierName,
  label: string,
  observe: (client: pg.Client, barrier: BarrierTelemetry) => Promise<unknown>,
  verify: (
    client: pg.Client,
    barrier: BarrierTelemetry,
    before: unknown,
  ) => Promise<void>,
  mode: 'kill' | 'release' | 'orphan' = 'kill',
) {
  const observer = await connect('admin', `${label}-observer`);
  const child = fork(
    new URL('../support/writer-child.ts', import.meta.url),
    [],
    {
      execArgv: [],
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      env: { PATH: process.env.PATH, TZ: 'UTC' },
    },
  );
  assert.ok(child.pid);
  const tracked: {
    pid: number;
    exit?: Exit;
    closed: boolean;
    mode: 'kill' | 'release' | 'orphan';
  } = { pid: child.pid, closed: false, mode };
  writers.push(tracked);
  evidence(`${label}-spawn`, { writerPid: child.pid, barrier: name });
  let telemetry: BarrierTelemetry | undefined;
  let spawnError: Error | undefined;
  let stdout = '',
    stderr = '';
  child.stdout!.setEncoding('utf8').on('data', (chunk: string) => {
    stdout += chunk;
  });
  child.stderr!.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  child.on('message', (message: BarrierTelemetry) => {
    telemetry = message;
  });
  child.on('error', (error) => {
    spawnError = error;
  });
  child.on('exit', (code, signal) => {
    tracked.exit = { code, signal };
  });
  let stdoutClosed = false,
    stderrClosed = false;
  child.stdout!.once('close', () => {
    stdoutClosed = true;
  });
  child.stderr!.once('close', () => {
    stderrClosed = true;
  });
  child.on('close', () => {
    tracked.closed = true;
  });
  const streamsClosed = () => {
    // Node 24.19 does not emit ChildProcess close after parent-initiated IPC disconnect.
    // For that explicit path require independent exit, both pipe closes and IPC disconnect.
    if (
      mode === 'orphan' &&
      tracked.exit &&
      stdoutClosed &&
      stderrClosed &&
      !child.connected
    )
      tracked.closed = true;
    return tracked.closed;
  };
  const start: StartWriter = {
    type: 'start',
    runId: required('M1_RUN_ID'),
    barrier: name,
    payloadJson: JSON.stringify({ fault: label, runId: required('M1_RUN_ID') }),
    port: Number(required('M1_PORT')),
    password: required('M1_WRITER_PASSWORD'),
    applicationName: `${required('M1_RUN_ID')}:${label}-child`,
  };
  try {
    child.send(start);
    const barrier = await waitFor(
      async () => {
        if (spawnError) throw spawnError;
        if (tracked.exit)
          throw new Error(
            `Writer exited before barrier: ${JSON.stringify(tracked.exit)}; ${stderr}`,
          );
        return telemetry;
      },
      (value) => value !== undefined,
      `${label} private barrier`,
    );
    assert.ok(barrier);
    assert.equal(barrier.type, 'barrier');
    assert.equal(barrier.runId, required('M1_RUN_ID'));
    assert.equal(barrier.writerPid, child.pid);
    assert.equal(barrier.name, name);
    assert.equal(barrier.sessionUser, 'source_writer');
    assert.equal(barrier.effectiveUser, 'source_writer');
    assert.equal(barrier.entity.entity_version, '1');
    assert.equal(barrier.outbox.length, 1);
    assert.equal(barrier.outbox[0]?.entity_id, barrier.entity.entity_id);
    assert.equal(stdout, '');
    const before = await observe(observer, barrier);
    evidence(`${label}-confirmed-barrier`, {
      barrier,
      independentlyObserved: before,
      callerSuccessBytes: stdout.length,
    });
    let sent = false;
    if (mode === 'kill') {
      sent = child.kill('SIGKILL');
      assert.equal(sent, true);
    } else if (mode === 'release')
      child.send({ type: 'release', runId: barrier.runId, barrier: name });
    else child.disconnect();
    const exit = await waitFor(
      async () => tracked.exit,
      (value) => value !== undefined,
      `${label} ${mode} exit`,
      5_000,
    );
    await waitFor(
      async () => streamsClosed(),
      Boolean,
      `${label} child stdio closed`,
      5_000,
    );
    evidence(`${label}-completion`, { mode, exit, stdout, stderr });
    if (mode === 'kill') {
      assert.deepEqual(exit, { code: null, signal: 'SIGKILL' });
      assert.equal(stdout, '');
      assert.equal(stderr, '');
    } else if (mode === 'release') {
      assert.deepEqual(exit, { code: 0, signal: null });
      const lines = stdout.trim().split('\n');
      assert.equal(lines.length, 1);
      assert.deepEqual(JSON.parse(lines[0] ?? ''), {
        type: 'caller-success',
        entity: barrier.entity,
      });
      assert.equal(stderr, '');
    } else {
      assert.deepEqual(exit, { code: 72, signal: null });
      assert.equal(stdout, '');
    }
    const endedSession = await databaseWaitFor(
      observer,
      async () =>
        (
          await observer.query(
            'SELECT pid, state, backend_xid::text FROM pg_stat_activity WHERE pid=$1',
            [barrier.backendPid],
          )
        ).rows,
      (rows) => rows.length === 0,
      `${label} database session ended`,
    );
    if (mode === 'kill')
      evidence(`${label}-actual-signal`, {
        barrier: name,
        runId: barrier.runId,
        writerPid: child.pid,
        backendPid: barrier.backendPid,
        transactionId: barrier.transactionId,
        sourceEpoch: barrier.entity.source_epoch,
        entityId: barrier.entity.entity_id,
        entityVersion: barrier.entity.entity_version,
        changeId: barrier.entity.change_id,
        requestedSignal: 'SIGKILL',
        sent,
        actualExit: exit,
        endedSession,
        callerSuccessBytes: stdout.length,
        callerOutcome: 'unknown; no ordinary caller success response received',
      });
    await verify(observer, barrier, before);
  } finally {
    try {
      if (!tracked.exit) child.kill('SIGKILL');
      await waitFor(
        async () => streamsClosed(),
        Boolean,
        `${label} owned writer cleanup`,
        5_000,
      );
      evidence(`${label}-writer-cleanup`, tracked);
    } finally {
      await observer.end();
    }
  }
}

test(
  'T08 actual pre-COMMIT SIGKILL rolls back source and outbox, then a fresh writer progresses',
  { timeout: 25_000 },
  async () => {
    await killAtBarrier(
      'source.after_mutation.before_commit',
      'T08',
      async (observer, barrier) => {
        const session = (
          await observer.query(
            'SELECT pid, application_name, usename, state, backend_xid::text, xact_start IS NOT NULL AS has_transaction, query FROM pg_stat_activity WHERE pid=$1',
            [barrier.backendPid],
          )
        ).rows[0];
        assert.equal(session.application_name, barrier.applicationName);
        assert.equal(session.usename, 'source_writer');
        assert.equal(session.state, 'idle in transaction');
        assert.equal(session.has_transaction, true);
        assert.equal(session.backend_xid, barrier.transactionId);
        assert.match(session.query as string, /FROM source\.outbox/);
        const locks = (
          await observer.query(
            'SELECT locktype, mode, relation::regclass::text AS relation, transactionid::text, granted FROM pg_locks WHERE pid=$1 AND granted ORDER BY locktype, relation, mode',
            [barrier.backendPid],
          )
        ).rows;
        for (const relation of ['source.entities', 'source.outbox'])
          assert.ok(
            locks.some(
              (lock) =>
                lock.relation === relation && lock.mode === 'RowExclusiveLock',
            ),
          );
        assert.ok(
          locks.some(
            (lock) =>
              lock.transactionid === barrier.transactionId &&
              lock.mode === 'ExclusiveLock',
          ),
        );
        const visible = await observeRows(observer, barrier.entity.entity_id);
        assert.deepEqual(visible, { entities: [], outbox: [] });
        return { session, locks, visible };
      },
      async (observer, barrier) => {
        const afterKill = await observeRows(observer, barrier.entity.entity_id);
        assert.deepEqual(afterKill, { entities: [], outbox: [] });
        const freshWriter = await connect('writer', 'T08-fresh-progress');
        try {
          const fresh = await createEntity(
            freshWriter,
            '{"fault":"T08-fresh-progress"}',
          );
          assert.notEqual(fresh.entity_id, barrier.entity.entity_id);
          const progress = await observeRows(observer, fresh.entity_id);
          assert.equal(progress.entities.length, 1);
          assert.equal(progress.outbox.length, 1);
          assert.equal(
            progress.entities[0].payload_json,
            '{"fault": "T08-fresh-progress"}',
          );
          assert.equal(progress.entities[0].entity_version, '1');
          assert.equal(
            progress.outbox[0].change_id,
            progress.entities[0].change_id,
          );
          evidence('T08', { afterKill, freshWriter: progress });
        } finally {
          await freshWriter.end();
        }
      },
    );
  },
);

test(
  'T09 actual post-COMMIT SIGKILL retains both rows with caller outcome unknown',
  { timeout: 25_000 },
  async () => {
    await killAtBarrier(
      'source.after_commit.before_caller_success',
      'T09',
      async (observer, barrier) => {
        const session = (
          await observer.query(
            'SELECT pid, application_name, usename, state, backend_xid::text, xact_start IS NOT NULL AS has_transaction, query FROM pg_stat_activity WHERE pid=$1',
            [barrier.backendPid],
          )
        ).rows[0];
        assert.equal(session.application_name, barrier.applicationName);
        assert.equal(session.usename, 'source_writer');
        assert.equal(session.state, 'idle');
        assert.equal(session.has_transaction, false);
        assert.equal(session.backend_xid, null);
        assert.equal(session.query, 'COMMIT');
        const committed = await observeRows(observer, barrier.entity.entity_id);
        assert.deepEqual(committed.entities, [barrier.entity]);
        assert.deepEqual(committed.outbox, barrier.outbox);
        const committedEntity = committed.entities[0];
        assert.ok(committedEntity);
        assert.equal(
          committedEntity.payload_json,
          JSON.stringify({ fault: 'T09', runId: required('M1_RUN_ID') })
            .replaceAll(':', ': ')
            .replaceAll(',', ', '),
        );
        return { session, committed };
      },
      async (_observer, barrier, before) => {
        const reconnect = await connect('writer', 'T09-independent-reconnect');
        try {
          const persisted = await observeRows(
            reconnect,
            barrier.entity.entity_id,
          );
          assert.deepEqual(
            persisted,
            (before as { committed: unknown }).committed,
          );
          evidence('T09', {
            persistedAfterKillAndReconnect: persisted,
            callerOutcome:
              'unknown despite independently observed database success',
            mutationRepeated: false,
            commandIdempotencyImplemented: false,
          });
        } finally {
          await reconnect.end();
        }
      },
    );
  },
);

for (const barrierName of [
  'source.after_mutation.before_commit',
  'source.after_commit.before_caller_success',
] as const) {
  const stage = barrierName.includes('before_commit') ? 'PRE' : 'POST';
  test(`M11-HEALTHY-${stage} ordinary release exits zero once and commits both rows`, async () => {
    await killAtBarrier(
      barrierName,
      `M11-HEALTHY-${stage}`,
      async (observer, barrier) =>
        observeRows(observer, barrier.entity.entity_id),
      async (observer, barrier) => {
        const rows = await observeRows(observer, barrier.entity.entity_id);
        assert.deepEqual(rows.entities, [barrier.entity]);
        assert.deepEqual(rows.outbox, barrier.outbox);
        evidence(`M11-HEALTHY-${stage}`, rows);
      },
      'release',
    );
  });
  test(`M11-ORPHAN-${stage} parent loss fails and closes the owned session`, async () => {
    await killAtBarrier(
      barrierName,
      `M11-ORPHAN-${stage}`,
      async (observer, barrier) =>
        observeRows(observer, barrier.entity.entity_id),
      async (observer, barrier) => {
        const rows = await observeRows(observer, barrier.entity.entity_id);
        assert.deepEqual(
          rows,
          stage === 'PRE'
            ? { entities: [], outbox: [] }
            : { entities: [barrier.entity], outbox: barrier.outbox },
        );
        evidence(`M11-ORPHAN-${stage}`, rows);
      },
      'orphan',
    );
  });
}

test('T10 fault writers were reaped and their database sessions ended', async () => {
  assert.equal(writers.length, 6);
  assert.equal(writers.filter((writer) => writer.mode === 'kill').length, 2);
  for (const writer of writers) {
    assert.equal(writer.closed, true);
    assert.deepEqual(
      writer.exit,
      writer.mode === 'kill'
        ? { code: null, signal: 'SIGKILL' }
        : { code: writer.mode === 'release' ? 0 : 72, signal: null },
    );
    assert.throws(() => process.kill(writer.pid, 0), { code: 'ESRCH' });
  }
  const observer = await connect('admin', 'T10-fault-cleanup');
  try {
    const sessions = (
      await observer.query(
        "SELECT pid, application_name, state FROM pg_stat_activity WHERE usename='source_writer'",
      )
    ).rows;
    assert.deepEqual(sessions, []);
    evidence('T10-fault-cleanup', { writers, sessions });
  } finally {
    await observer.end();
  }
});
