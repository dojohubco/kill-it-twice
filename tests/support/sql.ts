// Low-level SQL fixtures only; managed application work uses Source.
import type pg from 'pg';
import { positiveBigint } from '../../src/source.ts';
import type { SourceRow } from '../../src/source.ts';

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

