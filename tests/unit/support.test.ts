import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import {
  command,
  redact,
  waitFor,
  CleanupFailure,
  errorCode,
} from '../../scripts/support.ts';

test('artifact redaction removes every temporary credential occurrence', () => {
  assert.equal(
    redact('secret-a secret-b secret-a', ['secret-a', 'secret-b', '']),
    '[REDACTED] [REDACTED] [REDACTED]',
  );
});

test('bounded observation waits for evidence and returns the accepted observation', async () => {
  let count = 0;
  assert.deepEqual(
    await waitFor(
      async () => ({ count: ++count }),
      (value) => value.count === 2,
      'unit observation',
      1_000,
    ),
    { count: 2 },
  );
});

test('missing boundary evidence fails with its last observation, never skips', async () => {
  await assert.rejects(
    waitFor(
      async () => 'not reached',
      () => false,
      'missing boundary',
      1,
    ),
    /Deadline waiting for missing boundary; last observation: "not reached"/,
  );
});

test('timed-out harness subprocess is killed and reported as a timeout', async () => {
  const result = await command(
    process.execPath,
    ['-e', 'setInterval(() => {}, 1000)'],
    process.env,
    100,
    true,
  );
  assert.equal(result.timedOut, true);
  assert.equal(result.code, null);
  assert.equal(result.signal, 'SIGKILL');
});

test('never-settling and late observations fail at a monotonic deadline with cleanup', async () => {
  let disposed = 0;
  let aborted = false;
  const start = performance.now();
  await assert.rejects(
    waitFor(
      (signal) => {
        signal.addEventListener('abort', () => {
          aborted = true;
        });
        return new Promise<never>(() => undefined);
      },
      () => true,
      'never',
      20,
      () => {
        disposed++;
        return Promise.resolve();
      },
    ),
    /Deadline waiting/,
  );
  assert.ok(aborted);
  assert.equal(disposed, 1);
  assert.ok(performance.now() - start < 1_000);
  // Event-loop blocking forces observation success before the deadline timer can fire.
  await assert.rejects(
    waitFor(
      () => {
        const end = performance.now() + 25;
        while (performance.now() < end) {
          /* controlled deadline fixture */
        }
        return Promise.resolve(true);
      },
      Boolean,
      'too late',
      5,
    ),
    /Deadline waiting/,
  );
  await assert.rejects(
    waitFor(
      () =>
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('late rejection')), 30),
        ),
      Boolean,
      'late rejection',
      5,
    ),
    /Deadline waiting/,
  );
  await new Promise((resolve) => setTimeout(resolve, 50));
});

test('observation cleanup failure retains the original deadline', async () => {
  await assert.rejects(
    waitFor(
      () => new Promise<never>(() => undefined),
      Boolean,
      'cleanup',
      5,
      () => Promise.reject(new Error('disposal failure')),
    ),
    (error: unknown) => {
      assert.ok(error instanceof CleanupFailure);
      assert.match(String(error.cause), /Deadline waiting/);
      assert.match(String(error.cleanupErrors[0]), /disposal failure/);
      return true;
    },
  );
});

test('bounded output fails explicitly and redacts secrets spanning writes and cutoff', async () => {
  const secret = 'split-secret-token';
  const result = await command(
    process.execPath,
    [
      '-e',
      'process.stdout.write("split-");setTimeout(()=>process.stdout.write("secret-token"),20)',
    ],
    process.env,
    2_000,
    true,
    { secrets: [secret] },
  );
  assert.equal(result.stdout, '[REDACTED]');
  assert.equal(result.code, 0);
  const overflow = await command(
    process.execPath,
    [
      '-e',
      'process.stdout.write("x".repeat(1020)+"split-secret-token"+"x".repeat(100000))',
    ],
    process.env,
    2_000,
    true,
    { maxOutputBytes: 1024, secrets: [secret] },
  );
  assert.ok(overflow.outputOverflow);
  assert.notEqual(overflow.code, 0);
  assert.ok(overflow.stdout.length < 1100);
  assert.ok(!overflow.stdout.includes('spli'));
});

test('timeout kills the real owned child and grandchild while unrelated sentinel survives', async () => {
  const sentinel = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
    stdio: 'ignore',
    detached: true,
  });
  const reaped = once(sentinel, 'exit');
  try {
    assert.ok(sentinel.pid);
    const result = await command(
      process.execPath,
      ['tests/support/group-child.ts'],
      process.env,
      750,
      true,
    );
    assert.equal(result.timedOut, true);
    assert.equal(result.signal, 'SIGKILL');
    assert.deepEqual(result.cleanupErrors, []);
    const pids: unknown = JSON.parse(result.stdout.trim());
    assert.ok(
      pids &&
        typeof pids === 'object' &&
        'child' in pids &&
        'grandchild' in pids,
    );
    for (const pid of [pids.child, pids.grandchild]) {
      assert.equal(typeof pid, 'number');
      assert.ok(typeof pid === 'number');
      await waitFor(
        async () => {
          try {
            const stat = await readFile(`/proc/${pid}/stat`, 'utf8');
            return stat.slice(
              stat.lastIndexOf(')') + 2,
              stat.lastIndexOf(')') + 3,
            );
          } catch (error) {
            if (errorCode(error) === 'ENOENT') return 'absent';
            throw error;
          }
        },
        (state) => state === 'absent' || state === 'Z',
        'owned process terminated',
        2_000,
      );
    }
    process.kill(sentinel.pid, 0);
    assert.equal(sentinel.exitCode, null);
    assert.equal(sentinel.signalCode, null);
  } finally {
    sentinel.kill('SIGKILL');
    await reaped;
  }
});
