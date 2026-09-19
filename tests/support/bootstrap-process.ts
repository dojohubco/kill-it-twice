import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import type { TestContext } from 'node:test';
import { object } from '../../scripts/acceptance.ts';
import { waitFor } from '../../scripts/support.ts';
import { bootstrapConfig, pipelineConfig } from './bootstrap.ts';
import { required, evidence } from './db.ts';
export function launchBootstrap(
  t: TestContext,
  label: string,
  action: 'chunk' | 'activate',
  key: string,
  first = '1',
  database = 'source_m1',
) {
  const sql = { ...bootstrapConfig(label), database };
  const child = fork(new URL('./bootstrap-child.ts', import.meta.url), [], {
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
    await waitFor(() => closed, Boolean, 'Bootstrap child cleanup', 5000);
    evidence(`${label}-cleanup`, { pid, exit, closed, overflow });
  });
  child.send({
    sql,
    action,
    key,
    first,
    epoch: required('SOURCE_EPOCH'),
    pipeline: required('PIPELINE_ID'),
    pipelineConfig: pipelineConfig(label + '-pipeline'),
  });
  return {
    pid,
    applicationName: sql.application_name,
    async barrier(phase: 'before_commit' | 'after_commit') {
      const found = await waitFor(
        () => {
          if (exit)
            throw new Error(
              `Early bootstrap exit ${JSON.stringify(exit)} ${stderr}`,
            );
          return messages.find(
            (m) => m['type'] === 'bootstrap-barrier' && m['phase'] === phase,
          );
        },
        (v) => v !== undefined,
        'Actual bootstrap barrier',
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
      await waitFor(() => closed, Boolean, 'Bootstrap child exit', 15000);
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
        assert.equal(object(JSON.parse(stdout))['type'], 'bootstrap-success');
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
