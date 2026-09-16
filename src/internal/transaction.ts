// Internal lifecycle only. Public domain facades construct fixed capabilities; callers never receive this client.
import pg from 'pg';
export interface ConnectionConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  application_name: string;
}
type Outcome = 'not_started' | 'rolled_back' | 'committed' | 'unknown';
export class OwnershipError extends Error {}
export class TransactionError extends Error {
  readonly outcome: Outcome;
  readonly cleanupErrors: readonly unknown[];
  readonly completionTag: string | undefined;
  readonly sqlState: string | undefined;
  readonly phase: string;
  readonly kind:
    'idempotency_conflict' | 'source_epoch_mismatch' | 'transaction_failure';
  constructor(
    cause: unknown,
    outcome: Outcome,
    phase: string,
    cleanupErrors: readonly unknown[] = [],
    completionTag?: string,
  ) {
    super(`Owned transaction failed during ${phase}; outcome=${outcome}`, {
      cause,
    });
    this.outcome = outcome;
    this.phase = phase;
    this.cleanupErrors = cleanupErrors;
    this.completionTag = completionTag;
    this.sqlState = cause instanceof pg.DatabaseError ? cause.code : undefined;
    this.kind =
      this.sqlState === 'P2001'
        ? 'idempotency_conflict'
        : this.sqlState === 'P2002'
          ? 'source_epoch_mismatch'
          : 'transaction_failure';
  }
}
export type Operation = <R>(action: () => Promise<R>) => Promise<R>;
// One owner, one active transaction, one private session per transaction. No retries.
export class TransactionOwner<Work> {
  #config: ConnectionConfig;
  #state: 'ready' | 'active' | 'poisoned' = 'ready';
  #build: (client: pg.Client, operation: Operation) => Work;
  constructor(
    config: ConnectionConfig,
    build: (client: pg.Client, operation: Operation) => Work,
  ) {
    this.#build = build;
    if (config instanceof pg.Client)
      throw new OwnershipError(
        'Supply connection configuration, never an externally managed client',
      );
    this.#config = { ...config };
  }
  async transaction<T>(work: (tx: Work) => Promise<T>): Promise<T> {
    if (this.#state !== 'ready')
      throw new OwnershipError(
        `Transaction owner is ${this.#state}; nested/concurrent use is unsupported`,
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
        throw new OwnershipError('Work capability has completed');
      if (pending) {
        const error = new OwnershipError(
          'Concurrent managed operations are unsupported; await each operation',
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

    try {
      await client.connect();
      phase = 'begin';
      if (
        (await client.query('BEGIN ISOLATION LEVEL READ COMMITTED')).command !==
        'BEGIN'
      )
        throw new Error('BEGIN completion was not BEGIN');
      began = true;
      outcome = 'unknown';
      phase = 'work';
      capabilityActive = true;
      try {
        value = await work(this.#build(client, operation));
      } finally {
        capabilityActive = false;
      }
      if (pending) {
        workFailed ??= {
          error: new OwnershipError(
            'Unawaited managed operation at callback completion',
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
            throw new Error(`Unexpected ROLLBACK completion: ${tag}`, {
              cause: error,
            });
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
      throw new TransactionError(
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
