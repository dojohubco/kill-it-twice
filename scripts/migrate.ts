import { readFile } from 'node:fs/promises';
import type pg from 'pg';

export async function migrateSource(client: pg.Client, temporaryWriterPassword: string): Promise<void> {
  // This value is generated locally, never accepted from source data or logged.
  if (!/^[a-f0-9]{48}$/.test(temporaryWriterPassword)) throw new Error('Invalid temporary password format');
  await client.query('BEGIN');
  try {
    await client.query(await readFile(new URL('../migrations/001-source.sql', import.meta.url), 'utf8'));
    await client.query(`ALTER ROLE source_writer LOGIN PASSWORD '${temporaryWriterPassword}'`);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}
