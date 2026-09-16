import assert from 'node:assert/strict';
import pg from 'pg';
import { first } from '../../scripts/rows.ts';
import { randomUUID } from 'node:crypto';
import { withCleanup } from '../../scripts/support.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';

export async function transactionSnapshot(c: pg.Client) {
  return first(
    (
      await c.query<{
        role: string;
        isolation: string;
        snapshot: string;
        xid: string;
        pid: number;
        at: string;
        source_epoch: string;
      }>(`SELECT current_user AS role,current_setting('transaction_isolation') AS isolation,
    pg_current_snapshot()::text AS snapshot,pg_current_xact_id()::text AS xid,pg_backend_pid() AS pid,
    clock_timestamp()::text AS at,source_epoch::text FROM source.source_identity`)
    ).rows,
  );
}
export async function mutationAttempt(
  c: pg.Client,
  operation: 'create' | 'update' | 'delete' | 'restore',
  payload: string | null,
  id: string | null = null,
) {
  let result: Record<string, unknown>[] = [],
    failure;
  try {
    result = (
      await c.query<Record<string, unknown>>(
        operation === 'create'
          ? 'SELECT entity_id::text,entity_version::text,change_id::text,payload::text AS payload FROM source.create_entity($1::jsonb)'
          : 'SELECT entity_id::text,entity_version::text,change_id::text,payload::text AS payload FROM source.mutate_entity($2::bigint,$3,$1::jsonb)',
        operation === 'create' ? [payload] : [payload, id, operation],
      )
    ).rows;
  } catch (error) {
    assert.ok(error instanceof pg.DatabaseError);
    failure = {
      stage: 'mutation',
      sqlState: error.code,
      message: error.message,
      where: error.where,
    };
  }
  const commit = await c.query('COMMIT');
  return {
    operation,
    id,
    payload,
    result,
    failure: failure ?? null,
    completion: commit.command,
  };
}
export async function markerState(c: pg.Client, marker: string) {
  const values = [JSON.stringify({ isolationMarker: marker })];
  const entities = (
    await c.query<Record<string, unknown>>(
      'SELECT entity_id::text,source_epoch::text,entity_version::text,change_id::text,recorded_at::text,is_deleted,payload::text AS payload FROM source.entities WHERE payload @> $1::jsonb',
      values,
    )
  ).rows;
  const outbox = (
    await c.query<Record<string, unknown>>(
      'SELECT allocation_id::text,entity_id::text,source_epoch::text,entity_version::text,change_id::text,recorded_at::text,is_deleted,payload::text AS payload FROM source.outbox WHERE payload @> $1::jsonb',
      values,
    )
  ).rows;
  const work = (
    await c.query<Record<string, unknown>>(
      'SELECT w.work_id::text,w.entity_id::text,w.entity_version::text,w.state,w.generation::text,w.acknowledged_hash FROM source.capture_work w JOIN source.outbox o USING(source_epoch,entity_id,entity_version) WHERE o.payload @> $1::jsonb',
      values,
    )
  ).rows;
  const binding = (
    await c.query<Record<string, unknown>>(
      'SELECT pipeline_id::text,source_epoch::text,payload_encoding,registered_at::text,xmin::text AS xid FROM source.capture_binding',
    )
  ).rows;
  return { entities, outbox, work, binding };
}

// Private initialization probe: both old readers precede the existing lock-based registration callback.
export async function registrationSnapshots<T>(
  observer: pg.Client,
  config: ConnectionConfig,
  register: () => Promise<T>,
) {
  const rr = new pg.Client({
    ...config,
    application_name: `${config.application_name}:old-rr`,
    query_timeout: 12000,
    statement_timeout: 10000,
  });
  const rc = new pg.Client({
    ...config,
    application_name: `${config.application_name}:old-rc`,
    query_timeout: 12000,
    statement_timeout: 10000,
  });
  return withCleanup(
    async () => {
      await rr.connect();
      await rc.connect();
      const rejectedBefore = [];
      for (const isolation of ['REPEATABLE READ', 'SERIALIZABLE'] as const) {
        const marker = randomUUID();
        await rr.query(`BEGIN ISOLATION LEVEL ${isolation}`);
        const transaction = await transactionSnapshot(rr);
        const attempt = await mutationAttempt(
          rr,
          'create',
          JSON.stringify({ isolationMarker: marker }),
        );
        const state = await markerState(observer, marker);
        assert.equal(attempt.failure?.sqlState, '25001');
        assert.equal(attempt.completion, 'ROLLBACK');
        assert.deepEqual(state, {
          entities: [],
          outbox: [],
          work: [],
          binding: [],
        });
        rejectedBefore.push({ marker, transaction, attempt, state });
      }
      const preMarker = randomUUID();
      await rc.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const preTransaction = await transactionSnapshot(rc);
      const preAttempt = await mutationAttempt(
        rc,
        'create',
        JSON.stringify({ isolationMarker: preMarker }),
      );
      assert.equal(preAttempt.failure, null);
      assert.equal(preAttempt.completion, 'COMMIT');
      const preState = await markerState(observer, preMarker);
      assert.equal(preState.outbox.length, 1);
      assert.deepEqual(preState.work, []);
      assert.deepEqual(preState.binding, []);
      await rr.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
      const oldRR = await transactionSnapshot(rr);
      await rc.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const oldRC = await transactionSnapshot(rc);
      assert.equal(oldRR.role, 'source_writer');
      assert.equal(oldRC.role, 'source_writer');
      const locks = (
        await observer.query<Record<string, unknown>>(
          "SELECT pid,mode FROM pg_locks WHERE pid=ANY($1::integer[]) AND relation='source.outbox'::regclass",
          [[oldRR.pid, oldRC.pid]],
        )
      ).rows;
      assert.deepEqual(locks, []);
      const result = await register();
      const rrMarker = randomUUID(),
        rcMarker = randomUUID();
      const before = await markerState(observer, rrMarker);
      assert.equal(before.binding.length, 1);
      const bindingXid = first(before.binding)['xid'];
      assert.equal(typeof bindingXid, 'string');
      const visibility = first(
        (
          await observer.query<{ visible: boolean }>(
            'SELECT pg_visible_in_snapshot($1::xid8,$2::pg_snapshot) AS visible',
            [bindingXid, oldRR.snapshot],
          )
        ).rows,
      );
      assert.equal(visibility.visible, false);
      const rrAttempt = await mutationAttempt(
        rr,
        'create',
        JSON.stringify({ isolationMarker: rrMarker }),
      );
      const rrState = await markerState(observer, rrMarker);
      assert.equal(rrAttempt.failure?.sqlState, '25001');
      assert.equal(rrAttempt.completion, 'ROLLBACK');
      assert.deepEqual(rrState.entities, []);
      assert.deepEqual(rrState.outbox, []);
      assert.deepEqual(rrState.work, []);
      const rcAttempt = await mutationAttempt(
        rc,
        'create',
        JSON.stringify({ isolationMarker: rcMarker }),
      );
      const rcState = await markerState(observer, rcMarker);
      assert.equal(rcAttempt.failure, null);
      assert.equal(rcAttempt.completion, 'COMMIT');
      assert.equal(rcState.outbox.length, 1);
      assert.equal(rcState.work.length, 1);
      assert.equal(first(rcState.work)['state'], 'pending');
      const preInitialized = await markerState(observer, preMarker);
      assert.equal(preInitialized.work.length, 1);
      return {
        result,
        probe: {
          rejectedBefore,
          preMarker,
          preTransaction,
          preAttempt,
          preState,
          preInitialized,
          oldRR,
          oldRC,
          locks,
          before,
          visibility,
          rrMarker,
          rrAttempt,
          rrState,
          rcMarker,
          rcAttempt,
          rcState,
        },
      };
    },
    async () => {
      const results = await Promise.allSettled([rr.end(), rc.end()]);
      const failures = results
        .filter((r) => r.status === 'rejected')
        .map((r) => r.reason as unknown);
      if (failures.length)
        throw new AggregateError(
          failures,
          'Isolation probe session cleanup failed',
        );
    },
  );
}
