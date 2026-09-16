import assert from 'node:assert/strict';
import pg from 'pg';
import { first } from '../../scripts/rows.ts';

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
