import type pg from 'pg';

export const MAX_BIGINT = 9223372036854775807n;

export function positiveBigint(value: unknown): string {
  if (typeof value !== 'string' || !/^[1-9][0-9]*$/.test(value) || value.length > 19 || BigInt(value) > MAX_BIGINT) {
    throw new TypeError('Expected a positive signed BIGINT decimal string');
  }
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

const columns = 'e.entity_id::text, e.source_epoch::text, e.entity_version::text, e.change_id::text, e.recorded_at::text, e.is_deleted, e.payload::text AS payload_json';

function row(result: pg.QueryResult<SourceRow>): SourceRow {
  if (result.rows.length !== 1) throw new Error('Expected one source entity');
  const value = result.rows[0]!;
  positiveBigint(value.entity_id);
  positiveBigint(value.entity_version);
  return value;
}

// Payload is SQL JSON text, not an invented canonical envelope. No automatic retries.
export async function createEntity(client: pg.Client, payloadJson: string): Promise<SourceRow> {
  return row(await client.query<SourceRow>(`SELECT ${columns} FROM source.create_entity($1::jsonb) AS e`, [payloadJson]));
}

export async function mutateEntity(client: pg.Client, id: string, operation: 'update' | 'delete' | 'restore', payloadJson: string | null = null): Promise<SourceRow> {
  return row(await client.query<SourceRow>(`SELECT ${columns} FROM source.mutate_entity($1::bigint, $2::text, $3::jsonb) AS e`, [positiveBigint(id), operation, payloadJson]));
}

export class SourceTransactionError extends Error {
  readonly outcome: 'rolled_back' | 'unknown';
  constructor(cause: unknown, outcome: 'rolled_back' | 'unknown') {
    super(`Source transaction failed; outcome=${outcome}`, { cause });
    this.outcome = outcome;
  }
}

export async function transaction<T>(client: pg.Client, work: (client: pg.Client) => Promise<T>): Promise<T> {
  await client.query('BEGIN');
  let commitAttempted = false;
  try {
    const value = await work(client);
    commitAttempted = true;
    await client.query('COMMIT');
    return value;
  } catch (cause) {
    let rolledBack = false;
    try { await client.query('ROLLBACK'); rolledBack = true; } catch { /* Disconnected: outcome remains unknown. */ }
    throw new SourceTransactionError(cause, !commitAttempted && rolledBack ? 'rolled_back' : 'unknown');
  }
}
