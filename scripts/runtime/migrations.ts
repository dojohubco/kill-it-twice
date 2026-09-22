import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type pg from 'pg';
import { withCleanup } from '../support.ts';
export async function prepare(
  client: pg.Client,
  schema: 'source' | 'pipeline' | 'consumer',
) {
  await client.query('SELECT pg_advisory_lock(1769235801)');
  const row = (
    await client.query<{ domain: string | null; journal: string | null }>(
      "SELECT to_regnamespace($1)::text domain,to_regclass('runtime_setup.migrations')::text journal",
      [schema],
    )
  ).rows[0];
  assert.ok(row);
  assert.ok(
    !row.domain || row.journal,
    'Existing unjournaled installation cannot be adopted automatically',
  );
  await client.query(
    'CREATE SCHEMA IF NOT EXISTS runtime_setup; REVOKE ALL ON SCHEMA runtime_setup FROM PUBLIC',
  );
  await client.query(
    'CREATE TABLE IF NOT EXISTS runtime_setup.migrations(name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())',
  );
}
export async function applyText(
  client: pg.Client,
  name: string,
  text: string,
  after: () => Promise<void> = async () => {},
) {
  const sha = createHash('sha256').update(text).digest('hex');
  const prior = (
    await client.query<{ sha256: string }>(
      'SELECT sha256 FROM runtime_setup.migrations WHERE name=$1',
      [name],
    )
  ).rows[0];
  if (prior) {
    assert.equal(prior.sha256, sha, `Changed applied migration: ${name}`);
    return;
  }
  assert.equal((await client.query('BEGIN')).command, 'BEGIN');
  try {
    await client.query(text);
    await client.query('RESET ROLE');
    await after();
    await client.query(
      'INSERT INTO runtime_setup.migrations(name,sha256) VALUES($1,$2)',
      [name, sha],
    );
    assert.equal((await client.query('COMMIT')).command, 'COMMIT');
  } catch (error) {
    await withCleanup(
      () =>
        Promise.reject(
          error instanceof Error ? error : new Error('Migration failed'),
        ),
      async () => {
        await client.query('ROLLBACK');
      },
    );
  }
}
export async function applyFile(
  client: pg.Client,
  file: string,
  after?: () => Promise<void>,
) {
  return applyText(
    client,
    file,
    await readFile(`migrations/${file}`, 'utf8'),
    after,
  );
}
export const sourceFiles = [
  '001-source.sql',
  '002-source-commands.sql',
  '003-source-reader.sql',
  '004-source-capture.sql',
  '005-capture-mutation-isolation.sql',
  '006-source-baselines.sql',
  '007-source-backfill-fence.sql',
  '008-source-operations.sql',
  '009-source-validation-indexes.sql',
  '010-source-chunk-validation.sql',
] as const;
export const pipelineFiles = [
  'pipeline/001-staging.sql',
  'pipeline/002-capture-instance.sql',
  'pipeline/003-elasticsearch.sql',
  'pipeline/004-rabbitmq.sql',
  'pipeline/005-rabbit-probe.sql',
  'pipeline/006-backfill.sql',
  'pipeline/007-operations.sql',
  'pipeline/008-recovery-controls.sql',
  'pipeline/009-runtime-discovery.sql',
  'pipeline/010-capacity-polling.sql',
  'pipeline/011-terminal-admission.sql',
  'pipeline/012-constraint-backed-counts.sql',
] as const;
export const consumerFiles = [
  'consumer/001-consumer.sql',
  'consumer/002-batch-byte-accounting.sql',
  'consumer/003-operations.sql',
  'consumer/004-bounded-receipt-reads.sql',
] as const;
