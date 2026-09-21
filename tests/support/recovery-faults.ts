import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fork, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type pg from 'pg';
import type { OperationsConfig } from '../../src/operations/config.ts';
import {
  RecoveryService,
  type RecoveryRequest,
} from '../../src/operations/recovery.ts';
import { record } from '../../src/operations/validation.ts';
import { create } from './es.ts';
import { required, evidence } from './db.ts';
async function close(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const end = once(child, 'exit', { signal: AbortSignal.timeout(10000) });
  child.kill('SIGKILL');
  await end;
}
export async function recoveryFaults(ctx: {
  db: { p: pg.Client };
  cfg: OperationsConfig;
  deliver: () => Promise<void>;
}) {
  const dir = await mkdtemp(join(tmpdir(), 'kit-recovery-fault-')),
    file = join(dir, 'config.json');
  await writeFile(file, JSON.stringify(ctx.cfg), { mode: 0o600 });
  try {
    for (const phase of ['pre', 'post'] as const)
      for (const kill of [true, false]) {
        const f = await create(
          JSON.stringify({
            name: `recovery fault ${phase} ${kill}`,
            loyalty_points: 'not-a-number',
          }),
        );
        await ctx.deliver();
        const d = (
          await ctx.db.p.query<{ attempt: string; target: string }>(
            "SELECT attempt_id::text attempt,destination_id::text target FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='elasticsearch'",
            [f.eventId],
          )
        ).rows[0];
        assert.ok(d);
        const request: RecoveryRequest = {
          request_id: randomUUID(),
          actor: 'fault-operator',
          reason: 'real admission transaction boundary',
          destination_id: d.target,
          generation: '1',
          selection: [{ event_id: f.eventId, attempt_id: d.attempt }],
        };
        const label = `${required('M1_RUN_ID')}:recovery:${phase}:${kill}`;
        const child = fork(resolve('tests/support/recovery-child.ts'), [], {
          env: { ...process.env, CONTROL_CONFIG_FILE: file },
          stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
        });
        let stdout = '',
          stderr = '';
        child.stdout?.on('data', (b: Buffer) => {
          stdout += b.toString();
          if (stdout.length > 65536) child.kill('SIGKILL');
        });
        child.stderr?.on('data', (b: Buffer) => {
          stderr += b.toString();
          if (stderr.length > 8192) child.kill('SIGKILL');
        });
        const exited = once(child, 'exit', {
          signal: AbortSignal.timeout(25000),
        });
        void exited.catch(() => undefined);
        try {
          const pending = once(child, 'message', {
            signal: AbortSignal.timeout(20000),
          });
          child.send({ phase, request, label });
          const messages: unknown[] = await pending;
          const barrier = record(messages[0]);
          assert.equal(barrier['type'], 'barrier');
          assert.equal(barrier['pid'], child.pid);
          const sessions = (
            await ctx.db.p.query<{
              pid: number;
              state: string;
              xid: string | null;
            }>(
              'SELECT pid,state,backend_xid::text xid FROM pg_stat_activity WHERE application_name=$1',
              [label],
            )
          ).rows;
          assert.equal(sessions.length, 1);
          assert.equal(
            sessions[0]?.state,
            phase === 'pre' ? 'idle in transaction' : 'idle',
          );
          const before = (
            await ctx.db.p.query<{ count: string }>(
              'SELECT count(*)::text count FROM pipeline.recovery_requests WHERE request_id=$1',
              [request.request_id],
            )
          ).rows[0]?.count;
          assert.equal(before, phase === 'pre' ? '0' : '1');
          assert.equal(stdout, '');
          if (kill) assert.equal(child.kill('SIGKILL'), true);
          else child.send({ type: 'release' });
          const result: unknown[] = await exited;
          assert.deepEqual(result, kill ? [null, 'SIGKILL'] : [0, null]);
          if (kill) assert.equal(stdout, '');
          else assert.equal(stdout.trim().split('\n').length, 1);
          assert.equal(stderr, '');
          const replay = await new RecoveryService(ctx.cfg).replay(request);
          assert.equal(
            replay['replayed'],
            phase === 'pre' && kill ? false : true,
          );
          const again = await new RecoveryService(ctx.cfg).replay(request);
          assert.equal(again['replayed'], true);
          const after = (
            await ctx.db.p.query<{ count: string }>(
              'SELECT count(*)::text count FROM pipeline.recovery_requests WHERE request_id=$1',
              [request.request_id],
            )
          ).rows[0]?.count;
          assert.equal(after, '1');
          assert.deepEqual(
            (
              await ctx.db.p.query(
                'SELECT pid FROM pg_stat_activity WHERE application_name=$1',
                [label],
              )
            ).rows,
            [],
          );
          evidence('RC06', {
            phase,
            kill,
            pid: child.pid,
            request_id: request.request_id,
            event_id: f.eventId,
            sessions,
            visibleBefore: before,
            actualExit: result,
            ordinarySuccessBytes: Buffer.byteLength(stdout),
            replay,
            repeat: again,
          });
        } finally {
          await close(child);
        }
      }
    await ctx.deliver();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
