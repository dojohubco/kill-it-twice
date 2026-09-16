import { readFile } from 'node:fs/promises';
import type pg from 'pg';
import { CleanupFailure } from './support.ts';

export async function migrateSource(
  client: pg.Client,
  temporaryWriterPassword: string,
  temporaryCommandPassword?: string,
): Promise<void> {
  // This value is generated locally, never accepted from source data or logged.
  if (
    !/^[a-f0-9]{48}$/.test(temporaryWriterPassword) ||
    (temporaryCommandPassword !== undefined &&
      !/^[a-f0-9]{48}$/.test(temporaryCommandPassword))
  )
    throw new Error('Invalid temporary password format');
  try {
    if ((await client.query('BEGIN')).command !== 'BEGIN')
      throw new Error('Migration BEGIN not confirmed');
    await client.query(
      await readFile(
        new URL('../migrations/001-source.sql', import.meta.url),
        'utf8',
      ),
    );
    await client.query(
      `ALTER ROLE source_writer LOGIN PASSWORD '${temporaryWriterPassword}'`,
    );
    if (temporaryCommandPassword !== undefined) {
      await client.query(
        await readFile(
          new URL('../migrations/002-source-commands.sql', import.meta.url),
          'utf8',
        ),
      );
      await client.query(
        `ALTER ROLE source_command LOGIN PASSWORD '${temporaryCommandPassword}'`,
      );
    }
    if ((await client.query('COMMIT')).command !== 'COMMIT')
      throw new Error('Migration COMMIT not confirmed');
  } catch (error) {
    try {
      if ((await client.query('ROLLBACK')).command !== 'ROLLBACK')
        throw new Error('Migration ROLLBACK not confirmed', { cause: error });
    } catch (cleanup) {
      throw new CleanupFailure(error, [cleanup]);
    }
    throw error;
  }
}
