import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { first } from '../../scripts/rows.ts';
import {
  SourceTransactionError,
  type SourceCommand,
  type SourceRow,
  type CommandReply,
} from '../../src/source.ts';
import { databaseWaitFor, required, sourceOwner } from './db.ts';

export function request(
  epoch: string,
  operation: SourceCommand['operation'] = 'create',
  entityId: string | null = null,
  payloadJson: string | null = '{}',
): SourceCommand {
  return {
    sourceEpoch: epoch,
    commandId: randomUUID(),
    contractVersion: 1,
    operation,
    entityId,
    payloadJson,
  };
}
export function execute(
  command: SourceCommand,
  label = 'command',
): Promise<CommandReply> {
  return sourceOwner(label, 'command').command(command);
}
export async function epoch(client: pg.Client): Promise<string> {
  return first(
    (
      await client.query<{ epoch: string }>(
        'SELECT source_epoch::text AS epoch FROM source.source_identity',
      )
    ).rows,
  ).epoch;
}
// Independent SQL mapping; no production normalization or numeric JSON parsing.
const resultColumns = `entity_id::text, source_epoch::text, entity_version::text, change_id::text,
  to_char(recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at,
  is_deleted, payload::text AS payload_json`;
export async function state(
  client: pg.Client,
  command: SourceCommand,
  id: string,
) {
  const receipts = (
    await client.query<Record<string, unknown>>(
      `SELECT xmin::text AS receipt_xid, source_epoch::text, command_id::text, contract_version,
    operation, target_id::text, request_payload::text, completed, result_entity_id::text, result_version::text,
    result_change_id::text, result_recorded_at::text, result_deleted, result_payload::text
    FROM source.command_receipts WHERE source_epoch=$1 AND command_id=$2`,
      [command.sourceEpoch, command.commandId],
    )
  ).rows;
  const entities = (
    await client.query<SourceRow>(
      `SELECT ${resultColumns} FROM source.entities WHERE entity_id=$1`,
      [id],
    )
  ).rows;
  const outbox = (
    await client.query<SourceRow>(
      `SELECT ${resultColumns} FROM source.outbox WHERE entity_id=$1 ORDER BY entity_version`,
      [id],
    )
  ).rows;
  return { receipts, entities, outbox };
}
export async function assertReceipt(
  client: pg.Client,
  command: SourceCommand,
  reply: CommandReply,
): Promise<void> {
  const observed = first(
    (
      await client.query<SourceRow & { completed: boolean; matches: boolean }>(
        `SELECT result_entity_id::text AS entity_id,
    source_epoch::text, result_version::text AS entity_version, result_change_id::text AS change_id,
    to_char(result_recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at,
    result_deleted AS is_deleted, result_payload::text AS payload_json, completed,
    ROW(contract_version,operation,target_id,request_payload) IS NOT DISTINCT FROM ROW($3::integer,$4::text,$5::bigint,$6::jsonb) AS matches
    FROM source.command_receipts WHERE source_epoch=$1 AND command_id=$2`,
        [
          command.sourceEpoch,
          command.commandId,
          command.contractVersion,
          command.operation,
          command.entityId,
          command.payloadJson,
        ],
      )
    ).rows,
  );
  const { completed, matches, ...snapshot } = observed;
  assert.equal(completed, true);
  assert.equal(matches, true);
  assert.deepEqual(snapshot, reply.result);
  assert.match(
    snapshot.recorded_at,
    /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/,
  );
  const revisions = (await state(client, command, reply.result.entity_id))
    .outbox;
  assert.deepEqual(
    revisions.find((row) => row.entity_version === reply.result.entity_version),
    snapshot,
  );
}
export async function rejected(
  attempt: Promise<unknown>,
  code: string | undefined,
): Promise<SourceTransactionError> {
  try {
    await attempt;
  } catch (error) {
    assert.ok(error instanceof SourceTransactionError);
    assert.equal(error.sqlState, code);
    assert.equal(error.outcome, 'rolled_back');
    if (code === 'P2001') assert.equal(error.kind, 'idempotency_conflict');
    if (code === 'P2002') assert.equal(error.kind, 'source_epoch_mismatch');
    return error;
  }
  throw new Error(`Expected source SQLSTATE ${code}`);
}
export interface Activity {
  pid: number;
  backend_xid: string | null;
  state: string;
  query: string;
  wait_event_type: string | null;
  blockers: number[];
  application_name: string;
  usename: string;
}
export async function activity(
  client: pg.Client,
  label: string,
): Promise<Activity[]> {
  return (
    await client.query<Activity>(
      `SELECT pid,backend_xid::text,state,query,wait_event_type,pg_blocking_pids(pid) AS blockers,application_name,usename
    FROM pg_stat_activity WHERE application_name=$1`,
      [`${required('M1_RUN_ID')}:${label}`],
    )
  ).rows;
}
export async function waitBlocked(
  client: pg.Client,
  waiter: string,
  holder: string,
) {
  const winner = first(await activity(client, holder));
  const rows = await databaseWaitFor(
    client,
    () => activity(client, waiter),
    (rows) => rows.length === 1 && first(rows).blockers.includes(winner.pid),
    `${waiter} waits on ${holder}`,
  );
  const loser = first(rows);
  assert.equal(loser.wait_event_type, 'Lock');
  assert.equal(loser.usename, 'source_command');
  return { winner, loser };
}
export async function sqlReject(
  client: pg.Client,
  sql: string,
  code: string,
  values: unknown[] = [],
): Promise<void> {
  await assert.rejects(
    client.query<Record<string, unknown>>(sql, values),
    (error: unknown) => {
      assert.ok(error instanceof Error && 'code' in error);
      assert.equal(error.code, code, sql);
      return true;
    },
  );
}

export async function counts(client: pg.Client) {
  return first(
    (
      await client.query<{
        entities: string;
        outbox: string;
        receipts: string;
      }>(
        'SELECT (SELECT count(*)::text FROM source.entities) AS entities, (SELECT count(*)::text FROM source.outbox) AS outbox, (SELECT count(*)::text FROM source.command_receipts) AS receipts',
      )
    ).rows,
  );
}
