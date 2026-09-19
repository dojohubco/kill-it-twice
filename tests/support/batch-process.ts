import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import type { TestContext } from 'node:test';
import { object } from '../../scripts/acceptance.ts';
import { waitFor } from '../../scripts/support.ts';
import { amqpConfig, consumerConfig, topology } from './rabbit.ts';
import { required, evidence } from './db.ts';
export function launchBatch(
  t: TestContext,
  label: string,
  minimum = 32,
  pause = false,
) {
  const sql = consumerConfig('runtime', label);
  const child = fork(new URL('./batch-child.ts', import.meta.url), [], {
    execArgv: [],
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { PATH: process.env['PATH'], TZ: 'UTC' },
  });
  assert.ok(child.pid && child.stdout && child.stderr);
  const pid = child.pid;
  let exit: { code: number | null; signal: NodeJS.Signals | null } | undefined,
    closed = false,
    stdout = '',
    stderr = '',
    overflow = false;
  let spawnError: Error | undefined;
  child.on('error', (error) => {
    spawnError = error;
  });
  const messages: Record<string, unknown>[] = [];
  const chunk = (stream: 'stdout' | 'stderr', text: string) => {
    if (
      Buffer.byteLength(stdout) +
        Buffer.byteLength(stderr) +
        Buffer.byteLength(text) >
      131072
    ) {
      overflow = true;
      child.kill('SIGKILL');
      return;
    }
    if (stream === 'stdout') stdout += text;
    else stderr += text;
  };
  child.stdout
    .setEncoding('utf8')
    .on('data', (s: string) => chunk('stdout', s));
  child.stderr
    .setEncoding('utf8')
    .on('data', (s: string) => chunk('stderr', s));
  child.on('message', (value: unknown) => {
    assert.ok(messages.length < 8);
    messages.push(object(value));
  });
  child.on('exit', (code, signal) => {
    exit = { code, signal };
  });
  child.on('close', () => {
    closed = true;
  });
  t.after(async () => {
    if (!exit) child.kill('SIGKILL');
    await waitFor(() => closed, Boolean, 'Batch child cleanup', 5000);
    evidence(`${label}-cleanup`, { pid, exit, closed, overflow });
  });
  child.send({
    sql,
    amqp: amqpConfig('consumer'),
    metadata: {
      url: required('RABBIT_API'),
      username: `observer-${topology().registrationId}`,
      password: required('RABBIT_OBSERVER_PASSWORD'),
      ca: required('RABBIT_CA'),
    },
    target: topology(),
    minimum,
    pause,
  });
  return {
    pid,
    applicationName: sql.application_name,
    async barrier(phase: 'before_commit' | 'after_commit') {
      const found = await waitFor(
        () => {
          if (exit)
            throw new Error(
              `Early batch exit ${JSON.stringify(exit)} ${stderr}`,
            );
          return messages.find(
            (m) => m['type'] === 'batch-barrier' && m['phase'] === phase,
          );
        },
        (v) => v !== undefined,
        'Actual consumer batch barrier',
        20000,
      );
      const message = object(found);
      assert.equal(message['pid'], pid);
      assert.equal(stdout, '');
      assert.equal(stderr, '');
      return message;
    },
    release(phase: 'before_commit' | 'after_commit') {
      child.send({ type: 'release', phase });
    },
    async finish(mode: 'run' | 'failure' | 'kill') {
      if (mode === 'kill') assert.equal(child.kill('SIGKILL'), true);
      await waitFor(() => closed, Boolean, 'Consumer batch child exit', 15000);
      assert.deepEqual(
        exit,
        mode === 'kill'
          ? { code: null, signal: 'SIGKILL' }
          : { code: mode === 'failure' ? 1 : 0, signal: null },
      );
      assert.equal(spawnError, undefined);
      assert.equal(overflow, false);
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
      if (mode === 'run') {
        assert.equal(stderr, '');
        assert.equal(stdout.trim().split('\n').length, 1);
        assert.equal(object(JSON.parse(stdout))['type'], 'batch-success');
      } else {
        assert.equal(stdout, '');
        if (mode === 'failure') assert.match(stderr, /rolled_back P6002/);
        else assert.equal(stderr, '');
      }
      return {
        pid,
        exit,
        ordinarySuccessBytes: Buffer.byteLength(stdout),
        stdout,
        stderr,
        messages,
      };
    },
  };
}
