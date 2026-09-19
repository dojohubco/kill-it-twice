import type pg from 'pg';
import {
  TransactionOwner,
  type ConnectionConfig,
  type Operation,
} from './internal/transaction.ts';
export {
  OwnershipError as SourceOwnershipError,
  TransactionError as SourceTransactionError,
} from './internal/transaction.ts';
export type { ConnectionConfig as SourceConfig } from './internal/transaction.ts';

const MAX_BIGINT = 9223372036854775807n;
export function positiveBigint(value: unknown): string {
  if (
    typeof value !== 'string' ||
    !/^[1-9][0-9]*$/.test(value) ||
    value.length > 19 ||
    BigInt(value) > MAX_BIGINT
  )
    throw new TypeError('Expected a positive signed BIGINT decimal string');
  return value;
}
export interface SourceRow {
  entity_id: string;
  source_epoch: string;
  entity_version: string;
  change_id: string | null;
  recorded_at: string;
  is_deleted: boolean;
  payload_json: string | null;
}
export interface Revision extends SourceRow {
  allocation_id: string;
}
export interface SourceCommand {
  sourceEpoch: string;
  commandId: string;
  contractVersion: number;
  operation: 'create' | 'update' | 'delete' | 'restore';
  entityId: string | null;
  payloadJson: string | null;
}
export interface CommandReply {
  result: SourceRow;
  replayed: boolean;
}
interface Session {
  pid: number;
  xid: string;
  session_user: string;
  current_user: string;
}
interface Inspection {
  session: Session;
  entities: SourceRow[];
  outbox: Revision[];
}
export interface SourceWork {
  command(request: SourceCommand): Promise<CommandReply>;
  create(payloadJson: string): Promise<SourceRow>;
  mutate(
    id: string,
    operation: 'update' | 'delete' | 'restore',
    payloadJson?: string | null,
  ): Promise<SourceRow>;
  inspect(id: string): Promise<Inspection>;
}
const columns =
  'e.entity_id::text, e.source_epoch::text, e.entity_version::text, e.change_id::text, e.recorded_at::text, e.is_deleted, e.payload::text AS payload_json';
function sourceRow(value: Record<string, unknown>): SourceRow {
  const {
    entity_id,
    source_epoch,
    entity_version,
    change_id,
    recorded_at,
    is_deleted,
    payload_json,
  } = value;
  if (
    typeof source_epoch !== 'string' ||
    !(change_id === null || typeof change_id === 'string') ||
    typeof recorded_at !== 'string' ||
    typeof is_deleted !== 'boolean' ||
    !(payload_json === null || typeof payload_json === 'string')
  )
    throw new TypeError('Invalid source row');
  return {
    entity_id: positiveBigint(entity_id),
    entity_version: positiveBigint(entity_version),
    source_epoch,
    change_id,
    recorded_at,
    is_deleted,
    payload_json,
  };
}
function one(result: pg.QueryResult<Record<string, unknown>>): SourceRow {
  const value = result.rows[0];
  if (result.rows.length !== 1 || !value)
    throw new Error('Expected one source entity');
  return sourceRow(value);
}

export class Source {
  #owner: TransactionOwner<SourceWork>;
  constructor(config: ConnectionConfig) {
    this.#owner = new TransactionOwner(config, sourceWork);
  }
  command(request: SourceCommand): Promise<CommandReply> {
    return this.transaction((tx) => tx.command(request));
  }
  transaction<T>(work: (tx: SourceWork) => Promise<T>): Promise<T> {
    return this.#owner.transaction(work);
  }
}
function sourceWork(client: pg.Client, operation: Operation): SourceWork {
  return Object.freeze({
    command: (request: SourceCommand) =>
      operation(async () => {
        if (
          typeof request.sourceEpoch !== 'string' ||
          typeof request.commandId !== 'string' ||
          !(
            request.payloadJson === null ||
            typeof request.payloadJson === 'string'
          )
        )
          throw new TypeError(
            'Command identity must be text and payload must be JSON text or SQL NULL',
          );
        const query = await client.query<Record<string, unknown>>(
          `SELECT entity_id::text, source_epoch::text, entity_version::text, change_id::text,
                  recorded_at, is_deleted, payload_json, replayed
           FROM source.execute_command($1::uuid,$2::uuid,$3::integer,$4::text,$5::bigint,$6::jsonb)`,
          [
            request.sourceEpoch,
            request.commandId,
            request.contractVersion,
            request.operation,
            request.entityId === null ? null : positiveBigint(request.entityId),
            request.payloadJson,
          ],
        );
        const row = query.rows[0];
        if (
          query.rows.length !== 1 ||
          !row ||
          typeof row['replayed'] !== 'boolean'
        )
          throw new TypeError('Expected one complete command reply');
        return { result: sourceRow(row), replayed: row['replayed'] };
      }),
    create: (payloadJson: string) =>
      operation(async () =>
        one(
          await client.query<Record<string, unknown>>(
            `SELECT ${columns} FROM source.create_entity($1::jsonb) AS e`,
            [payloadJson],
          ),
        ),
      ),
    mutate: (
      id: string,
      op: 'update' | 'delete' | 'restore',
      payload: string | null = null,
    ) =>
      operation(async () =>
        one(
          await client.query<Record<string, unknown>>(
            `SELECT ${columns} FROM source.mutate_entity($1::bigint,$2::text,$3::jsonb) AS e`,
            [positiveBigint(id), op, payload],
          ),
        ),
      ),
    inspect: (id: string) =>
      operation(async () => {
        const session = (
          await client.query<Record<string, unknown>>(
            'SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid, session_user, current_user',
          )
        ).rows[0];
        if (
          !session ||
          typeof session['pid'] !== 'number' ||
          typeof session['xid'] !== 'string' ||
          typeof session['session_user'] !== 'string' ||
          typeof session['current_user'] !== 'string'
        )
          throw new Error('Invalid session identity');
        const entities = (
          await client.query<Record<string, unknown>>(
            `SELECT ${columns} FROM source.entities e WHERE entity_id=$1`,
            [positiveBigint(id)],
          )
        ).rows.map(sourceRow);
        const outbox = (
          await client.query<Record<string, unknown>>(
            `SELECT e.allocation_id::text, ${columns} FROM source.outbox e WHERE entity_id=$1 ORDER BY entity_version`,
            [positiveBigint(id)],
          )
        ).rows.map((r) => ({
          ...sourceRow(r),
          allocation_id: positiveBigint(r['allocation_id']),
        }));
        return {
          session: {
            pid: session['pid'],
            xid: session['xid'],
            session_user: session['session_user'],
            current_user: session['current_user'],
          },
          entities,
          outbox,
        };
      }),
  });
}
