import { readFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
import type pg from 'pg';
import { CleanupFailure } from './support.ts';
async function apply(
  c: pg.Client,
  file: string,
  role: string,
  password: string,
) {
  assert.match(password, /^[a-f0-9]{48}$/);
  try {
    assert.equal(
      (await c.query('BEGIN ISOLATION LEVEL READ COMMITTED')).command,
      'BEGIN',
    );
    await c.query(
      await readFile(new URL(`../migrations/${file}`, import.meta.url), 'utf8'),
    );
    await c.query(`ALTER ROLE ${role} LOGIN PASSWORD '${password}'`);
    assert.equal((await c.query('COMMIT')).command, 'COMMIT');
  } catch (primary) {
    try {
      assert.equal((await c.query('ROLLBACK')).command, 'ROLLBACK');
    } catch (cleanup) {
      throw new CleanupFailure(primary, [cleanup]);
    }
    throw primary;
  }
}
export function migrateBackfillSource(c: pg.Client, password: string) {
  return apply(c, '007-source-backfill-fence.sql', 'source_backfill', password);
}
export function migrateBackfillPipeline(c: pg.Client, password: string) {
  return apply(c, 'pipeline/006-backfill.sql', 'pipeline_backfill', password);
}
