import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { test, type TestContext } from 'node:test';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import { waitFor } from '../../scripts/support.ts';
import { databaseWaitFor, evidence, required } from '../support/db.ts';
import { activity, counts } from '../support/commands.ts';
import { setup, fixture, snapshot, oracle } from '../support/staging.ts';
import type { RevisionKey } from '../../src/source-reader.ts';
function launch(
  t: TestContext,
  label: string,
  key: RevisionKey,
  stage: 'pre' | 'post' | 'run',
) {
  const child = fork(
    new URL('../support/staging-child.ts', import.meta.url),
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
    closed = false,
    overflow = false,
    stdout = '',
    stderr = '';
  const capture = (stream: 'stdout' | 'stderr', chunk: string) => {
    if (
      Buffer.byteLength(stdout) +
        Buffer.byteLength(stderr) +
        Buffer.byteLength(chunk) >
      65536
    ) {
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
  t.after(async () => {
    if (!exit) child.kill('SIGKILL');
    await waitFor(() => closed, Boolean, 'owned staging child cleanup', 5000);
    evidence(`${label}-cleanup`, { pid, exit, closed });
  });
  child.send({
    key,
    stage,
    epoch: required('SOURCE_EPOCH'),
    sourcePort: Number(required('M1_PORT')),
    pipelinePort: Number(required('PIPELINE_PORT')),
    readerPassword: required('SOURCE_READER_PASSWORD'),
    stagerPassword: required('PIPELINE_STAGER_PASSWORD'),
    applicationName: `${required('M1_RUN_ID')}:${label}`,
  });
  return {
    pid,
    async barrier() {
      const raw = await waitFor(
        () => {
          if (spawnError) throw spawnError;
          if (exit)
            throw new Error(
              `Early staging exit: ${JSON.stringify(exit)} ${stderr}`,
            );
          return telemetry;
        },
        (value) => value !== undefined,
        'staging barrier',
      );
      const msg = object(raw);
      assert.equal(msg['pid'], pid);
      assert.equal(msg['stage'], stage);
      assert.equal(msg['type'], 'staging-barrier');
      assert.equal(stdout, '');
      assert.equal(stderr, '');
      assert.equal(overflow, false);
      return msg;
    },
    async finish(mode: 'kill' | 'release' | 'run') {
      if (mode === 'kill') assert.equal(child.kill('SIGKILL'), true);
      else if (mode === 'release') child.send({ type: 'release', stage });
      await waitFor(() => closed, Boolean, 'staging child reaped', 5000);
      assert.deepEqual(
        exit,
        mode === 'kill'
          ? { code: null, signal: 'SIGKILL' }
          : { code: 0, signal: null },
      );
      assert.equal(overflow, false);
      assert.equal(stderr, '');
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
      if (mode === 'kill') assert.equal(stdout, '');
      else assert.equal(stdout.trim().split('\n').length, 1);
      return {
        pid,
        exit,
        ordinarySuccessBytes: stdout.length,
        success: mode === 'kill' ? null : object(JSON.parse(stdout)),
      };
    },
  };
}
for (const [id, name, stage, mode] of [
  [
    'S08',
    'S08 real pre-COMMIT pipeline SIGKILL rolls back every obligation',
    'pre',
    'kill',
  ],
  [
    'S09',
    'S09 real post-COMMIT pipeline SIGKILL recovers from durable evidence',
    'post',
    'kill',
  ],
  [
    'S10-PRE',
    'S10-PRE healthy staging release reports committed success once',
    'pre',
    'release',
  ],
  [
    'S10-POST',
    'S10-POST healthy staging release reports committed success once',
    'post',
    'release',
  ],
] as const)
  void test(name, { timeout: 25000 }, async (t) => {
    const { s, p, epoch } = await setup(t);
    const f = await fixture(
      epoch,
      `{"fault":"${id}","precise":9007199254740993}`,
    );
    const sourceBefore = await counts(s);
    const label = `${id}-child`;
    const child = launch(t, label, f.key, stage);
    const barrier = await child.barrier();
    assert.deepEqual(barrier['results'], [
      { eventId: f.event.body.event_id, status: 'inserted' },
    ]);
    const session = first(await activity(p, label));
    assert.equal(session.usename, 'pipeline_stager');
    const locks = (
      await p.query<{
        relation: string | null;
        mode: string;
        transactionid: string | null;
      }>(
        'SELECT relation::regclass::text,mode,transactionid::text FROM pg_locks WHERE pid=$1 AND granted ORDER BY relation,mode',
        [session.pid],
      )
    ).rows;
    const before = await snapshot(p, [f.event.body.event_id]);
    if (stage === 'pre') {
      assert.equal(session.state, 'idle in transaction');
      assert.ok(session.backend_xid);
      assert.match(session.query, /pipeline.stage_event/);
      assert.deepEqual(before, {
        events: [],
        deliveries: [],
        observations: [],
      });
      for (const table of [
        'events',
        'delivery_intents',
        'consumer_observations',
      ])
        assert.ok(
          locks.some(
            (lock) =>
              lock.relation === `pipeline.${table}` &&
              lock.mode === 'RowExclusiveLock',
          ),
        );
      assert.ok(
        locks.some(
          (lock) =>
            lock.transactionid === session.backend_xid &&
            lock.mode === 'ExclusiveLock',
        ),
      );
    } else {
      assert.equal(session.state, 'idle');
      assert.equal(session.backend_xid, null);
      assert.equal(session.query, 'COMMIT');
      await oracle(s, p, f.event);
    }
    evidence(`${id}-confirmed-barrier`, {
      commandKey: f.command.commandId,
      revision: f.key,
      eventId: f.event.body.event_id,
      childPid: child.pid,
      session,
      locks,
      before,
      ordinarySuccessBytes: 0,
    });
    const actual = await child.finish(mode);
    const ended = await databaseWaitFor(
      p,
      () => activity(p, label),
      (rows) => rows.length === 0,
      'staging session ended',
    );
    const after = await snapshot(p, [f.event.body.event_id]);
    if (mode === 'kill' && stage === 'pre')
      assert.deepEqual(after, { events: [], deliveries: [], observations: [] });
    else await oracle(s, p, f.event);
    if (stage === 'post') assert.deepEqual(after, before);
    if (mode === 'release')
      assert.deepEqual(actual.success, {
        type: 'caller-success',
        results: [{ eventId: f.event.body.event_id, status: 'inserted' }],
      });
    // The retry is a new process and healthy source reader/pipeline owner, using the same immutable selection.
    const retry = launch(t, `${id}-retry`, f.key, 'run');
    const replay = await retry.finish('run');
    const expected =
      mode === 'kill' && stage === 'pre' ? 'inserted' : 'already_staged';
    assert.deepEqual(replay.success, {
      type: 'caller-success',
      results: [{ eventId: f.event.body.event_id, status: expected }],
    });
    const recovered = await oracle(s, p, f.event);
    if (expected === 'already_staged') assert.deepEqual(recovered, after);
    assert.deepEqual(await counts(s), sourceBefore);
    evidence(mode === 'kill' ? `${id}-actual-signal` : id, {
      commandKey: f.command.commandId,
      revision: f.key,
      eventId: f.event.body.event_id,
      session,
      actualExit: actual.exit,
      childPid: child.pid,
      ended,
      before,
      after,
      replay,
      recovered,
    });
  });
