import pg from 'pg';

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
  change_id: string;
  recorded_at: string;
  is_deleted: boolean;
  payload_json: string | null;
}
export interface Revision extends SourceRow {
  allocation_id: string;
}
export interface Session {
  pid: number;
  xid: string;
  session_user: string;
  current_user: string;
}
export interface Inspection {
  session: Session;
  entities: SourceRow[];
  outbox: Revision[];
}
export interface SourceWork {
  create(payloadJson: string): Promise<SourceRow>;
  mutate(
    id: string,
    operation: 'update' | 'delete' | 'restore',
    payloadJson?: string | null,
  ): Promise<SourceRow>;
  inspect(id: string): Promise<Inspection>;
}
export interface SourceConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  application_name: string;
}
type Outcome = 'not_started' | 'rolled_back' | 'committed' | 'unknown';
export class SourceOwnershipError extends Error {}
export class SourceTransactionError extends Error {
  readonly outcome: Outcome;
  readonly cleanupErrors: readonly unknown[];
  readonly completionTag: string | undefined;
  readonly sqlState: string | undefined;
  readonly phase: string;
  constructor(
    cause: unknown,
    outcome: Outcome,
    phase: string,
    cleanupErrors: readonly unknown[] = [],
    completionTag?: string,
  ) {
    super(`Source transaction failed during ${phase}; outcome=${outcome}`, {
      cause,
    });
    this.outcome = outcome;
    this.phase = phase;
    this.cleanupErrors = cleanupErrors;
    this.completionTag = completionTag;
    this.sqlState = cause instanceof pg.DatabaseError ? cause.code : undefined;
  }
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
    typeof change_id !== 'string' ||
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

// One owner, one active transaction, one private session per transaction. No retries.
export class Source {
  #config: SourceConfig;
  #state: 'ready' | 'active' | 'poisoned' = 'ready';
  constructor(config: SourceConfig) {
    if (config instanceof pg.Client)
      throw new SourceOwnershipError(
        'Supply connection configuration, never an externally managed client',
      );
    this.#config = { ...config };
  }
  async transaction<T>(work: (tx: SourceWork) => Promise<T>): Promise<T> {
    if (this.#state !== 'ready')
      throw new SourceOwnershipError(
        `Source owner is ${this.#state}; nested/concurrent use is unsupported`,
      );
    this.#state = 'active';
    const client = new pg.Client({
      ...this.#config,
      connectionTimeoutMillis: 5_000,
      query_timeout: 15_000,
      statement_timeout: 12_000,
      idle_in_transaction_session_timeout: 30_000,
    });
    let primary: { error: unknown } | undefined;
    let phase = 'connect',
      outcome: Outcome = 'not_started',
      completionTag: string | undefined;
    let capabilityActive = false,
      pending: Promise<unknown> | undefined;
    let workFailed: { error: unknown } | undefined;
    let value: T | undefined;
    let began = false,
      commitAttempted = false;
    const cleanupErrors: unknown[] = [];
    client.on('error', (error: Error) => {
      workFailed ??= { error };
      this.#state = 'poisoned';
    });
    const operation = async <R>(action: () => Promise<R>): Promise<R> => {
      if (!capabilityActive)
        throw new SourceOwnershipError('Source work capability has completed');
      if (pending) {
        const error = new SourceOwnershipError(
          'Concurrent source operations are unsupported; await each operation',
        );
        workFailed ??= { error };
        throw error;
      }
      if (workFailed) throw workFailed.error;
      const task = action();
      pending = task;
      try {
        return await task;
      } catch (error) {
        workFailed ??= { error };
        throw error;
      } finally {
        pending = undefined;
      }
    };
    const tx: SourceWork = Object.freeze({
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
    try {
      await client.connect();
      phase = 'begin';
      if ((await client.query('BEGIN')).command !== 'BEGIN')
        throw new Error('BEGIN completion was not BEGIN');
      began = true;
      outcome = 'unknown';
      phase = 'work';
      capabilityActive = true;
      try {
        value = await work(tx);
      } finally {
        capabilityActive = false;
      }
      if (pending) {
        workFailed ??= {
          error: new SourceOwnershipError(
            'Unawaited source operation at callback completion',
          ),
        };
        await pending.catch(() => undefined);
      }
      if (workFailed) throw workFailed.error;
      phase = 'commit';
      commitAttempted = true;
      completionTag = (await client.query('COMMIT')).command;
      if (completionTag === 'ROLLBACK') {
        outcome = 'rolled_back';
        throw new Error('COMMIT completed as ROLLBACK');
      }
      if (completionTag !== 'COMMIT')
        throw new Error(`Unexpected COMMIT completion: ${completionTag}`);
      outcome = 'committed';
    } catch (error) {
      primary = workFailed ?? { error };
      capabilityActive = false;
      if (pending)
        await pending.catch((failure: unknown) => {
          cleanupErrors.push(failure);
        });
      if (began && outcome !== 'rolled_back') {
        try {
          const tag = (await client.query('ROLLBACK')).command;
          if (tag !== 'ROLLBACK')
            throw new Error(`Unexpected ROLLBACK completion: ${tag}`);
          if (!commitAttempted) {
            outcome = 'rolled_back';
            completionTag = tag;
          }
        } catch (failure) {
          cleanupErrors.push(failure);
        }
      }
    } finally {
      capabilityActive = false;
      try {
        await client.end();
      } catch (error) {
        if (!primary) {
          primary = { error };
          phase = 'close';
        } else cleanupErrors.push(error);
        this.#state = 'poisoned';
      }
      if (outcome === 'unknown' || cleanupErrors.length)
        this.#state = 'poisoned';
      if (this.#state === 'active') this.#state = 'ready';
    }
    if (primary)
      throw new SourceTransactionError(
        primary.error,
        outcome,
        phase,
        cleanupErrors,
        completionTag,
      );
    // A successful callback may intentionally return undefined; do not conflate it with failure.
    return value as T;
  }
}
