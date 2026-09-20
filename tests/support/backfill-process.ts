import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import type { TestContext } from 'node:test';
import { waitFor } from '../../scripts/support.ts';
import { object } from '../../src/backfill/types.ts';
import { required, evidence } from './db.ts';
import { config } from './staging.ts';
export function backfillConfig(label = 'backfill') {
  return {
    source: {
      ...config('reader', label),
      user: 'source_backfill',
      password: required('SOURCE_BACKFILL_PASSWORD'),
    },
    pipeline: {
      ...config('stager', label),
      user: 'pipeline_backfill',
      password: required('PIPELINE_BACKFILL_PASSWORD'),
    },
    binding: {
      sourceEpoch: required('SOURCE_EPOCH'),
      pipelineId: required('PIPELINE_ID'),
    },
  };
}
export function launchBackfill(
  t: TestContext,
  label: string,
  run: string,
  boundary: string,
  options: {
    leaseMs?: number;
    renewalMs?: number;
    quiesce?: boolean;
    action?: 'once' | 'fence';
  } = {},
) {
  const cfg = backfillConfig(label),
    child = fork(new URL('./backfill-child.ts', import.meta.url), [], {
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
  const messages: Record<string, unknown>[] = [];
  let spawnError: unknown;
  child.on('error', (e) => {
    spawnError = e;
  });
  child.on('exit', (code, signal) => {
    exit = { code, signal };
  });
  child.on('close', () => {
    closed = true;
  });
  const chunk = (name: string, s: string) => {
    if (
      Buffer.byteLength(stdout) +
        Buffer.byteLength(stderr) +
        Buffer.byteLength(s) >
      524288
    ) {
      overflow = true;
      child.kill('SIGKILL');
      return;
    }
    if (name === 'stdout') stdout += s;
    else stderr += s;
  };
  child.stdout
    .setEncoding('utf8')
    .on('data', (s: string) => chunk('stdout', s));
  child.stderr
    .setEncoding('utf8')
    .on('data', (s: string) => chunk('stderr', s));
  child.on('message', (v: unknown) => {
    assert.ok(messages.length < 4);
    messages.push(object(v));
  });
  t.after(async () => {
    if (!closed) child.kill('SIGKILL');
    await waitFor(() => closed, Boolean, 'Backfill child cleanup', 10000);
    evidence(`${label}-cleanup`, { pid, exit, closed, overflow });
  });
  child.send({
    ...cfg,
    run,
    boundary,
    action: options.action ?? 'once',
    quiesce: options.quiesce ?? false,
    options: {
      leaseMs: options.leaseMs ?? 30000,
      renewalMs: options.renewalMs ?? 5000,
    },
  });
  return {
    pid,
    applicationName: cfg.pipeline.application_name,
    sourceApplicationName: cfg.source.application_name,
    async barrier() {
      const found = await waitFor(
        () => {
          if (closed)
            throw new Error(
              `Early backfill exit ${JSON.stringify(exit)} ${stderr}`,
            );
          return messages.find((m) => m['type'] === 'backfill-barrier');
        },
        (v) => v !== undefined,
        'Real backfill barrier',
        20000,
      );
      const value = object(found);
      assert.equal(value['pid'], pid);
      assert.equal(value['boundary'], boundary);
      assert.equal(stdout, '');
      assert.equal(stderr, '');
      return value;
    },
    release() {
      child.send({ type: 'release', boundary });
    },
    signal(signal: NodeJS.Signals) {
      assert.equal(child.kill(signal), true);
    },
    async finish(mode: 'run' | 'kill' | 'failure' = 'run') {
      if (mode === 'kill') assert.equal(child.kill('SIGKILL'), true);
      await waitFor(() => closed, Boolean, 'Backfill child exit', 15000);
      assert.deepEqual(
        exit,
        mode === 'kill'
          ? { code: null, signal: 'SIGKILL' }
          : { code: mode === 'failure' ? 1 : 0, signal: null },
      );
      assert.equal(overflow, false);
      assert.equal(spawnError, undefined);
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
      if (mode === 'run') {
        assert.equal(stderr, '');
        assert.equal(stdout.trim().split('\n').length, 1);
        assert.equal(object(JSON.parse(stdout))['type'], 'backfill-success');
      } else assert.equal(stdout, '');
      if (mode === 'kill') assert.equal(stderr, '');
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
