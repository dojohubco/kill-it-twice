import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import type { TestContext } from 'node:test';
import { object } from '../../scripts/acceptance.ts';
import { waitFor } from '../../scripts/support.ts';
import { captureConfig } from './capture.ts';
import { evidence } from './db.ts';
export function launchCapture(
  t: TestContext,
  label: string,
  boundary = '',
  follow = false,
  leaseMs = 1500,
  quiesceRenewals = false,
) {
  const child = fork(new URL('./capture-child.ts', import.meta.url), [], {
    execArgv: [],
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { PATH: process.env['PATH'], TZ: 'UTC' },
  });
  assert.ok(child.pid && child.stdout && child.stderr);
  const pid = child.pid;
  let telemetry: unknown,
    exit: { code: number | null; signal: NodeJS.Signals | null } | undefined,
    closed = false,
    stdout = '',
    stderr = '',
    overflow = false,
    spawnError: Error | undefined;
  const chunks = (stream: 'stdout' | 'stderr', chunk: string) => {
    if (
      Buffer.byteLength(stdout) +
        Buffer.byteLength(stderr) +
        Buffer.byteLength(chunk) >
      524288
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
    .on('data', (c: string) => chunks('stdout', c));
  child.stderr
    .setEncoding('utf8')
    .on('data', (c: string) => chunks('stderr', c));
  child.on('message', (value: unknown) => {
    telemetry = value;
  });
  child.on('error', (e) => {
    spawnError = e;
  });
  child.on('exit', (code, signal) => {
    exit = { code, signal };
  });
  child.on('close', () => {
    closed = true;
  });
  t.after(async () => {
    if (!exit) {
      child.kill('SIGCONT');
      child.kill('SIGKILL');
    }
    await waitFor(() => closed, Boolean, 'capture child cleanup', 5000);
    evidence(`${label}-cleanup`, { pid, exit, closed, overflow });
  });
  child.send({
    config: captureConfig(label),
    boundary,
    follow,
    leaseMs,
    quiesceRenewals,
  });
  const output = () =>
    stdout.trim()
      ? stdout
          .trim()
          .split('\n')
          .filter(Boolean)
          .map((line) => object(JSON.parse(line)))
      : [];
  return {
    pid,
    output,
    async barrier() {
      const raw = await waitFor(
        () => {
          if (spawnError) throw spawnError;
          if (exit)
            throw new Error(
              `Early capture exit ${JSON.stringify(exit)} ${stderr}`,
            );
          return telemetry;
        },
        (v) => v !== undefined,
        'capture barrier',
        15000,
      );
      const msg = object(raw);
      assert.equal(msg['pid'], pid);
      assert.equal(msg['type'], 'capture-barrier');
      assert.equal(msg['boundary'], boundary);
      assert.equal(stdout, '');
      assert.equal(stderr, '');
      assert.equal(overflow, false);
      return msg;
    },
    signal(signal: NodeJS.Signals) {
      assert.equal(child.kill(signal), true);
    },
    release() {
      child.send({ type: 'release', boundary });
    },
    async finish(mode: 'kill' | 'release' | 'run' | 'term' | 'stale') {
      if (mode === 'kill') assert.equal(child.kill('SIGKILL'), true);
      else if (mode === 'release' || mode === 'stale')
        child.send({ type: 'release', boundary });
      else if (mode === 'term') assert.equal(child.kill('SIGTERM'), true);
      await waitFor(() => closed, Boolean, 'capture child exit', 10000);
      assert.deepEqual(
        exit,
        mode === 'kill'
          ? { code: null, signal: 'SIGKILL' }
          : { code: mode === 'stale' ? 1 : 0, signal: null },
      );
      assert.equal(overflow, false);
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
      if (mode === 'kill') {
        if (!follow) assert.equal(stdout, '');
        assert.equal(stderr, '');
      } else if (mode === 'stale') {
        assert.equal(stdout, '');
        assert.match(stderr, /Capture attempt failed/);
      } else {
        assert.equal(stderr, '');
        if (!follow) assert.equal(output().length, 1);
      }
      return {
        pid,
        exit,
        ordinarySuccessBytes: stdout.length,
        output: output(),
        stderr,
      };
    },
  };
}
