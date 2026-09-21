import { readFile } from 'node:fs/promises';
import type pg from 'pg';
import { CleanupFailure } from './support.ts';
export async function migrateOperations(
  client: pg.Client,
  store: 'source' | 'pipeline' | 'consumer',
  password: string,
  includeRecovery = true,
) {
  if (!/^[a-f0-9]{48}$/.test(password))
    throw new Error('Invalid private migration credential');
  const files = {
    source: '008-source-operations.sql',
    pipeline: 'pipeline/007-operations.sql',
    consumer: 'consumer/003-operations.sql',
  };
  try {
    await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    await client.query(
      await readFile(
        new URL(`../migrations/${files[store]}`, import.meta.url),
        'utf8',
      ),
    );
    if (store === 'pipeline' && includeRecovery) {
      await client.query(
        await readFile(
          new URL(
            '../migrations/pipeline/008-recovery-controls.sql',
            import.meta.url,
          ),
          'utf8',
        ),
      );
    }
    await client.query(
      `ALTER ROLE ${store}_operator LOGIN PASSWORD '${password}'`,
    );
    if ((await client.query('COMMIT')).command !== 'COMMIT')
      throw new Error('Migration COMMIT not confirmed');
  } catch (primary) {
    try {
      await client.query('ROLLBACK');
    } catch (cleanup) {
      throw new CleanupFailure(primary, [cleanup]);
    }
    throw primary;
  }
}
