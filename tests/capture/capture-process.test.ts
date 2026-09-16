import assert from 'node:assert/strict';
import { test } from 'node:test';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import { command, waitFor, withCleanup } from '../../scripts/support.ts';
import { setup, fixture, pconnect, snapshot } from '../support/staging.ts';
import { request, execute } from '../support/commands.ts';
import { databaseWaitFor, evidence, required } from '../support/db.ts';
import { drain, reconcile, work } from '../support/capture.ts';
import { launchCapture } from '../support/capture-process.ts';
async function docker(args: string[]) {
  const result = await command('docker', args, process.env, 20000, true);
  assert.equal(result.code, 0, JSON.stringify(result));
  assert.equal(result.timedOut, false);
  assert.equal(result.outputOverflow, false);
  assert.deepEqual(result.cleanupErrors, []);
  return result.stdout.trim();
}
void test(
  'IC12 actual pipeline outage persists delay and recovers automatically',
  { timeout: 45000 },
  async (t) => {
    const { s, p, epoch } = await setup(t);
    await drain();
    const f = await fixture(epoch);
    const child = launchCapture(
      t,
      'outage',
      'capture.before_pipeline_commit',
      true,
      5000,
    );
    const barrier = await child.barrier();
    const before = await snapshot(p, [f.event.body.event_id]);
    assert.equal(before.events.length, 0);
    const container = required('M2C_PIPELINE_CONTAINER');
    assert.match(container, /^[0-9a-f]{64}$/);
    assert.equal(
      await docker([
        'inspect',
        '--format',
        '{{index .Config.Labels "com.docker.compose.project"}}',
        container,
      ]),
      required('M1_RUN_ID'),
    );
    await p.end();
    await withCleanup(
      async () => {
        await docker(['stop', '--time', '1', container]);
        child.release();
        const deferred = await databaseWaitFor(
          s,
          async () =>
            (
              await s.query<Record<string, unknown>>(
                'SELECT state,generation::text,reason,clock_timestamp()::text AS observed_at,next_eligible_at::text, next_eligible_at>clock_timestamp() AS delayed,acknowledged_hash FROM source.capture_work WHERE entity_id=$1',
                [f.key.entityId],
              )
            ).rows,
          (rows) =>
            first(rows)['state'] === 'pending' &&
            first(rows)['delayed'] === true,
          'outage persisted transient delay',
        );
        assert.equal(first(deferred)['reason'], 'transient_failure');
        assert.equal(first(deferred)['acknowledged_hash'], null);
        await waitFor(
          () => child.output(),
          (rows) => rows.some((r) => r['type'] === 'capture-failure'),
          'outage caller observes failed attempt',
        );
        // Let the persisted source clock schedule admit another real attempt while the service remains down.
        const retry = await databaseWaitFor(
          s,
          async () => work(s, f.key.entityId),
          (rows) => BigInt(String(first(rows)['generation'])) >= 2n,
          'automatic retry after persisted eligibility',
          10000,
        );
        assert.ok(BigInt(String(first(retry)['generation'])) <= 3n);
        assert.notEqual(first(retry)['state'], 'acknowledged');
        evidence('IC12-outage', {
          container,
          barrier,
          before,
          deferred,
          retry,
          outputs: child.output(),
        });
      },
      async () => {
        await docker(['start', container]);
      },
    );
    await waitFor(
      () =>
        docker(['inspect', '--format', '{{.State.Health.Status}}', container]),
      (v) => v === 'healthy',
      'pipeline service restarted',
      15000,
    );
    const acknowledged = await databaseWaitFor(
      s,
      async () => work(s, f.key.entityId),
      (rows) => first(rows)['state'] === 'acknowledged',
      'automatic recovery acknowledgement',
      15000,
    );
    const actual = await child.finish('term');
    const restored = await pconnect('outage-restored');
    t.after(() => restored.end());
    await reconcile(s, restored, 'IC12', [f.key.entityId]);
    evidence('IC12-recovery', { acknowledged, actual });
  },
);
void test(
  'IC15 two capture processes reconcile finite workload kills and new work after idle',
  { timeout: 55000 },
  async (t) => {
    const { s, p, epoch } = await setup(t);
    await drain();
    const hot = await execute(request(epoch, 'create', null, '{"hot":0}'));
    const id = hot.result.entity_id;
    for (let i = 1; i <= 18; i++)
      await execute(
        request(epoch, 'update', id, `{"hot":${i},"precise":9007199254740993}`),
      );
    await execute(request(epoch, 'delete', id, null));
    await execute(request(epoch, 'restore', id, '{"hot":19}'));
    const firstWorker = launchCapture(
      t,
      'schedule-claim',
      'capture.after_claim_commit.before_stage',
      true,
    );
    const firstBarrier = await firstWorker.barrier();
    const survivor = launchCapture(t, 'schedule-survivor', '', true);
    const killed = [];
    killed.push({
      barrier: firstBarrier,
      actual: await firstWorker.finish('kill'),
    });
    for (const [label, boundary] of [
      ['schedule-pre', 'capture.before_pipeline_commit'],
      ['schedule-post', 'capture.after_pipeline_commit.before_source_ack'],
    ] as const) {
      // Precisely suspend the competing worker while admitting the target's next fixture.
      survivor.signal('SIGSTOP');
      await execute(request(epoch, 'update', id, `{"schedule":"${label}"}`));
      const target = launchCapture(t, label, boundary, true);
      const telemetry = await target.barrier();
      const claims = telemetry['claims'];
      assert.ok(Array.isArray(claims));
      const keys = claims.map((c) => {
        const r = object(c);
        return `${epoch}:${String(r['entityId'])}:${String(r['version'])}`;
      });
      const pipeline = await snapshot(p, keys);
      const source = await work(s, id);
      killed.push({ barrier: telemetry, actual: await target.finish('kill') });
      survivor.signal('SIGCONT');
      evidence(`${label}-state`, { pipeline, source, keys });
    }
    const second = launchCapture(t, 'schedule-second', '', true);
    await databaseWaitFor(
      s,
      async () => work(s, id),
      (rows) =>
        rows.length === 23 && rows.every((r) => r['state'] === 'acknowledged'),
      'finite workload quiescent',
      18000,
    );
    const atIdle = await s.query<{ at: string }>(
      'SELECT clock_timestamp()::text AS at',
    );
    await waitFor(
      () => [survivor.output(), second.output()],
      (outputs) =>
        outputs.every((rows) =>
          rows.some(
            (r) =>
              r['type'] === 'caller-success' &&
              object(r['result'])['claimed'] === 0,
          ),
        ),
      'both follow processes observed idle',
    );
    const afterIdle = await execute(
      request(epoch, 'update', id, '{"afterIdle":true}'),
    );
    await databaseWaitFor(
      s,
      async () => work(s, id),
      (rows) =>
        rows.length === 24 && rows.every((r) => r['state'] === 'acknowledged'),
      'new work after idle automatically captured',
      10000,
    );
    const results = await Promise.all([
      survivor.finish('term'),
      second.finish('term'),
    ]);
    await reconcile(s, p, 'IC15');
    evidence('IC15-schedule', {
      id,
      killed,
      atIdle: atIdle.rows,
      afterIdle: afterIdle.result,
      results,
    });
  },
);
