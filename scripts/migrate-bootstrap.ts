import { readFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
import type pg from 'pg';
import { CleanupFailure } from './support.ts';
export async function migrateBootstrap(
  client: pg.Client,
  password: string,
  capacity = false,
) {
  assert.match(password, /^[a-f0-9]{48}$/);
  try {
    assert.equal(
      (await client.query('BEGIN ISOLATION LEVEL READ COMMITTED')).command,
      'BEGIN',
    );
    await client.query(
      await readFile(
        new URL('../migrations/006-source-baselines.sql', import.meta.url),
        'utf8',
      ),
    );
    if (capacity) {
      for (const file of [
        '009-source-validation-indexes.sql',
        '010-source-chunk-validation.sql',
      ])
        await client.query(
          await readFile(
            new URL('../migrations/' + file, import.meta.url),
            'utf8',
          ),
        );
    }
    await client.query(
      `ALTER ROLE source_bootstrap LOGIN PASSWORD '${password}'`,
    );
    assert.equal((await client.query('COMMIT')).command, 'COMMIT');
  } catch (primary) {
    try {
      assert.equal((await client.query('ROLLBACK')).command, 'ROLLBACK');
    } catch (cleanup) {
      throw new CleanupFailure(primary, [cleanup]);
    }
    throw primary;
  }
}
