import assert from 'node:assert/strict';
import { test } from 'node:test';
import { first } from '../../scripts/rows.ts';
import { object } from '../../scripts/acceptance.ts';
import type { Claim } from '../../src/source-capture.ts';
import { setup, fixture, snapshot } from '../support/staging.ts';
import { activity } from '../support/commands.ts';
import { databaseWaitFor, evidence, required } from '../support/db.ts';
import { control, drain, expiry, work, reconcile } from '../support/capture.ts';
import { launchCapture } from '../support/capture-process.ts';
function claimFrom(message: Record<string, unknown>): Claim {
  const claims = message['claims'];
  assert.ok(Array.isArray(claims));
  const c = object(first(claims));
  const text = (key: string) => {
    const v = c[key];
    assert.equal(typeof v, 'string');
    return String(v);
  };
  return {
    entityId: text('entityId'),
    version: text('version'),
    generation: text('generation'),
    ownerId: text('ownerId'),
    leaseUntil: text('leaseUntil'),
    observedAt: text('observedAt'),
    transferBytes: text('transferBytes'),
  };
}
const boundaries = {
  claim: 'capture.after_claim_commit.before_stage',
  pre: 'capture.before_pipeline_commit',
  post: 'capture.after_pipeline_commit.before_source_ack',
  ack: 'capture.after_source_ack_commit.before_success',
};
for (const [id, name, stage, mode] of [
  [
    'IC07',
    'IC07 real kill after source claim commit recovers',
    'claim',
    'kill',
  ],
  [
    'IC08',
    'IC08 real kill before pipeline commit leaves no partial stage or acknowledgement',
    'pre',
    'kill',
  ],
  [
    'IC09',
    'IC09 real kill after pipeline commit restages before acknowledgement',
    'post',
    'kill',
  ],
  [
    'IC10',
    'IC10 real kill after source acknowledgement preserves the terminal receipt',
    'ack',
    'kill',
  ],
  [
    'IC11-CLAIM',
    'IC11-CLAIM healthy release returns one committed capture result',
    'claim',
    'release',
  ],
  [
    'IC11-PRE',
    'IC11-PRE healthy release returns one committed capture result',
    'pre',
    'release',
  ],
  [
    'IC11-POST',
    'IC11-POST healthy release returns one committed capture result',
    'post',
    'release',
  ],
  [
    'IC11-ACK',
    'IC11-ACK healthy release returns one committed capture result',
    'ack',
    'release',
  ],
] as const)
  void test(name, { timeout: 30000 }, async (t) => {
    const { s, p, epoch } = await setup(t);
    await drain();
    const f = await fixture(
      epoch,
      `{"fault":"${id}","exact":9007199254740993}`,
    );
    const label = `${id}-child`,
      child = launchCapture(t, label, boundaries[stage]);
    const telemetry = await child.barrier();
    const claim = claimFrom(telemetry);
    assert.equal(claim.entityId, f.key.entityId);
    const beforeSource = await work(s, f.key.entityId),
      before = await snapshot(p, [f.event.body.event_id]);
    assert.equal(
      first(beforeSource)['state'],
      stage === 'ack' ? 'acknowledged' : 'leased',
    );
    const sessions = await activity(p, label);
    const sourceSessions = (
      await s.query<Record<string, unknown>>(
        'SELECT pid,application_name,state,backend_xid::text,query FROM pg_stat_activity WHERE application_name LIKE $1',
        [`${required('M1_RUN_ID')}:${label}%`],
      )
    ).rows;
    const renewalName = `${required('M1_RUN_ID')}:${label}:renew`;
    const renewals = sourceSessions.filter(
      (r) => r['application_name'] === renewalName,
    );
    evidence(`${id}-source-session-observation`, {
      sourceSessions,
      renewalName,
      renewals,
    });
    assert.ok(
      sourceSessions
        .filter((r) => r['application_name'] !== renewalName)
        .every((r) => r['state'] !== 'idle in transaction'),
      'A and C never remain open while waiting for pipeline',
    );
    // Independent, short renewal transactions are required while B is outstanding.
    // Observe their actual completion instead of treating a scheduling snapshot
    // between renewal statements as an open claim/ACK transaction.
    if (renewals.length)
      await databaseWaitFor(
        s,
        async () =>
          (
            await s.query<Record<string, unknown>>(
              'SELECT pid,state,backend_xid::text FROM pg_stat_activity WHERE application_name=$1 AND pid=ANY($2::integer[])',
              [renewalName, renewals.map((r) => r['pid'])],
            )
          ).rows,
        (rows) => rows.length === 0,
        'observed renewal sessions finish while pipeline remains paused',
        5000,
      );
    let locks: unknown[] = [];
    if (stage === 'pre') {
      const session = first(sessions);
      assert.equal(session.state, 'idle in transaction');
      assert.ok(session.backend_xid);
      assert.equal(session.usename, 'pipeline_capture');
      locks = (
        await p.query(
          'SELECT relation::regclass::text,mode,transactionid::text FROM pg_locks WHERE pid=$1 AND granted ORDER BY relation,mode',
          [session.pid],
        )
      ).rows;
      for (const relation of [
        'events',
        'delivery_intents',
        'consumer_observations',
      ])
        assert.ok(
          locks.some((l) => {
            const lock = object(l);
            return (
              lock['relation'] === `pipeline.${relation}` &&
              lock['mode'] === 'RowExclusiveLock'
            );
          }),
        );
    }
    if (stage === 'claim' || stage === 'pre')
      assert.deepEqual(before, {
        events: [],
        deliveries: [],
        observations: [],
      });
    else {
      assert.equal(before.events.length, 1);
      assert.equal(before.deliveries.length, 2);
      assert.equal(before.observations.length, 1);
      if (stage === 'post') {
        const session = first(sessions);
        assert.equal(session.state, 'idle');
        assert.equal(session.backend_xid, null);
      }
    }
    evidence(`${id}-confirmed-barrier`, {
      telemetry,
      commandKey: f.command.commandId,
      eventId: f.event.body.event_id,
      source: beforeSource,
      pipeline: before,
      sessions,
      sourceSessions,
      locks,
      ordinarySuccessBytes: 0,
    });
    if (id === 'IC11-PRE') {
      const renewed = await databaseWaitFor(
        s,
        async () =>
          (
            await s.query<Record<string, unknown>>(
              'SELECT clock_timestamp()::text AS observed_at,lease_until::text,generation::text,owner_id::text,clock_timestamp()>$2::timestamptz AS past_original,lease_until>$2::timestamptz AS extended FROM source.capture_work WHERE entity_id=$1',
              [claim.entityId, claim.leaseUntil],
            )
          ).rows,
        (rows) =>
          first(rows)['past_original'] === true &&
          first(rows)['extended'] === true,
        'independent renewal during pipeline transaction',
      );
      assert.equal(first(renewed)['generation'], claim.generation);
      assert.equal(first(renewed)['owner_id'], claim.ownerId);
      evidence('IC11-PRE-renewal', { claim, renewed });
    }
    const actual = await child.finish(mode);
    const endedPipeline = await databaseWaitFor(
      p,
      () => activity(p, label),
      (rows) => rows.length === 0,
      'capture pipeline session ended',
    );
    const endedSource = await databaseWaitFor(
      s,
      async () =>
        (
          await s.query<{ pid: number }>(
            'SELECT pid FROM pg_stat_activity WHERE application_name LIKE $1',
            [`${required('M1_RUN_ID')}:${label}%`],
          )
        ).rows,
      (rows) => rows.length === 0,
      'capture source sessions ended',
    );
    const afterSource = await work(s, f.key.entityId),
      after = await snapshot(p, [f.event.body.event_id]);
    if (mode === 'kill') {
      assert.equal(
        first(afterSource)['state'],
        stage === 'ack' ? 'acknowledged' : 'leased',
      );
      if (stage === 'claim' || stage === 'pre')
        assert.deepEqual(after, {
          events: [],
          deliveries: [],
          observations: [],
        });
      else assert.deepEqual(after, before);
    }
    if (mode === 'kill' && stage !== 'ack') await expiry(s, claim);
    const retry = launchCapture(t, `${id}-retry`);
    const replay = await retry.finish('run');
    const result = object(first(replay.output)['result']);
    if (mode === 'kill' && stage !== 'ack')
      assert.deepEqual(result['staged'], [
        {
          eventId: f.event.body.event_id,
          status: stage === 'post' ? 'already_staged' : 'inserted',
        },
      ]);
    else {
      assert.equal(result['claimed'], 0);
      assert.deepEqual(result['staged'], []);
    }
    if (stage === 'ack') {
      const c = control('ack-repeat');
      const terminal = await work(s, f.key.entityId);
      assert.equal(
        (await c.acknowledge(claim, f.event.contentSha256)).status,
        'already_acknowledged',
      );
      await assert.rejects(
        c.acknowledge(
          { ...claim, generation: (BigInt(claim.generation) + 1n).toString() },
          f.event.contentSha256,
        ),
        { sqlState: 'P4002' },
      );
      await assert.rejects(c.acknowledge(claim, '0'.repeat(64)), {
        sqlState: 'P4003',
      });
      assert.deepEqual(await work(s, f.key.entityId), terminal);
    }
    const recovered = await reconcile(s, p, `${id}-reconciliation`, [
      f.key.entityId,
    ]);
    if (stage === 'post' || stage === 'ack')
      assert.deepEqual(await snapshot(p, [f.event.body.event_id]), before);
    evidence(mode === 'kill' ? `${id}-actual-signal` : id, {
      commandKey: f.command.commandId,
      eventId: f.event.body.event_id,
      childPid: child.pid,
      actualExit: actual.exit,
      ordinarySuccessBytes: actual.ordinarySuccessBytes,
      telemetry,
      sessions,
      sourceSessions,
      endedPipeline,
      endedSource,
      beforeSource,
      afterSource,
      before,
      after,
      replay,
      recovered,
    });
  });
void test(
  'IC06 delayed old worker cannot overwrite a newer terminal acknowledgement',
  { timeout: 25000 },
  async (t) => {
    const { s, p, epoch } = await setup(t);
    await drain();
    const f = await fixture(epoch);
    const a = launchCapture(t, 'delayed-a', boundaries.post, false, 900, true);
    const telemetry = await a.barrier();
    assert.equal(telemetry['renewalQuiescent'], true);
    const claim = claimFrom(telemetry);
    assert.equal(claim.entityId, f.key.entityId);
    a.signal('SIGSTOP');
    const stoppedRenewals = (
      await s.query<Record<string, unknown>>(
        'SELECT pid,state,backend_xid::text,query FROM pg_stat_activity WHERE application_name=$1',
        [`${required('M1_RUN_ID')}:delayed-a:renew`],
      )
    ).rows;
    assert.deepEqual(stoppedRenewals, []);
    const observed = await expiry(s, claim);
    const stagedBefore = await snapshot(p, [f.event.body.event_id]);
    // Explicit scheduling control: an expired row is still ineligible while
    // another transaction holds its lock. This is not a failed capture claim.
    await s.query('BEGIN');
    let lockedResult: unknown;
    try {
      await s.query(
        'SELECT work_id FROM source.capture_work WHERE entity_id=$1 AND entity_version=$2 FOR UPDATE',
        [claim.entityId, claim.version],
      );
      const blocked = launchCapture(t, 'locked-row-control');
      lockedResult = await blocked.finish('run');
      assert.equal(object(first(blocked.output())['result'])['claimed'], 0);
      assert.deepEqual(
        await snapshot(p, [f.event.body.event_id]),
        stagedBefore,
      );
    } finally {
      await s.query('ROLLBACK');
    }
    evidence('IC06-locked-row-control', { observed, lockedResult });
    const b = launchCapture(t, 'newer-b');
    const bResult = await b.finish('run');
    const terminal = await work(s, f.key.entityId);
    evidence('IC06-reclaim-observation', {
      telemetry,
      stoppedRenewals,
      observed,
      bResult,
      terminal,
    });
    assert.ok(
      BigInt(String(first(terminal)['generation'])) > BigInt(claim.generation),
    );
    a.signal('SIGCONT');
    const stale = await a.finish('stale');
    assert.deepEqual(await work(s, f.key.entityId), terminal);
    assert.deepEqual(await snapshot(p, [f.event.body.event_id]), stagedBefore);
    await reconcile(s, p, 'IC06', [f.key.entityId]);
    evidence('IC06-stale-worker', {
      telemetry,
      observed,
      bResult,
      terminal,
      stale,
      stagedBefore,
    });
  },
);
