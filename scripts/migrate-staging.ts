import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import type pg from 'pg';
import { CleanupFailure } from './support.ts';
async function migration(
  client: pg.Client,
  file: string,
  setup: () => Promise<void>,
) {
  try {
    assert.equal((await client.query('BEGIN')).command, 'BEGIN');
    await client.query(
      await readFile(new URL(`../migrations/${file}`, import.meta.url), 'utf8'),
    );
    await setup();
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
export async function migrateReader(client: pg.Client, password: string) {
  assert.match(password, /^[a-f0-9]{48}$/);
  await migration(client, '003-source-reader.sql', async () => {
    await client.query(`ALTER ROLE source_reader LOGIN PASSWORD '${password}'`);
  });
}
export async function migratePipeline(
  client: pg.Client,
  password: string,
  epoch: string,
) {
  assert.match(password, /^[a-f0-9]{48}$/);
  await migration(client, 'pipeline/001-staging.sql', async () => {
    await client.query(
      'INSERT INTO pipeline.source_binding VALUES (true,$1,$2)',
      [epoch, 'pg18-jsonb-text/v1'],
    );
    await client.query(
      `ALTER ROLE pipeline_stager LOGIN PASSWORD '${password}'`,
    );
  });
}
export async function sourceSnapshot(client: pg.Client) {
  const rows: Record<string, unknown[]> = {};
  for (const table of [
    'source_identity',
    'entities',
    'outbox',
    'command_receipts',
  ])
    rows[table] = (
      await client.query<{ text: string }>(
        `SELECT row_to_json(t)::text AS text FROM source.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((row) => row.text);
  return rows;
}
export async function pipelineSnapshot(client: pg.Client) {
  const sessions = (
    await client.query(
      "SELECT pid FROM pg_stat_activity WHERE usename IN ('pipeline_stager','pipeline_capture')",
    )
  ).rows;
  assert.deepEqual(sessions, [], 'Leaked pipeline runtime session');
  const rows: Record<string, unknown[]> = {};
  for (const table of [
    'source_binding',
    'destinations',
    'events',
    'delivery_intents',
    'consumer_observations',
    'integrity_incidents',
  ])
    rows[table] = (
      await client.query<{ text: string }>(
        `SELECT row_to_json(t)::text AS text FROM pipeline.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((row) => row.text);
  return rows;
}

export async function migrateCaptureSource(
  client: pg.Client,
  password: string,
) {
  assert.match(password, /^[a-f0-9]{48}$/);
  await migration(client, '004-source-capture.sql', async () => {
    await client.query(
      `ALTER ROLE source_capture LOGIN PASSWORD '${password}'`,
    );
  });
}
export async function migrateCapturePipeline(
  client: pg.Client,
  password: string,
) {
  assert.match(password, /^[a-f0-9]{48}$/);
  await migration(client, 'pipeline/002-capture-instance.sql', async () => {
    await client.query(
      `ALTER ROLE pipeline_capture LOGIN PASSWORD '${password}'`,
    );
  });
}
// Controlled initialization only; runtime roles cannot call register_capture.
// Pipeline migration/identity must commit first. A crash here cannot permit ACK without source binding.
export async function registerCapture(
  client: pg.Client,
  pipelineId: string,
  epoch: string,
) {
  try {
    assert.equal(
      (await client.query('BEGIN ISOLATION LEVEL READ COMMITTED')).command,
      'BEGIN',
    );
    await client.query('SELECT source.register_capture($1,$2,$3)', [
      pipelineId,
      epoch,
      'pg18-jsonb-text/v1',
    ]);
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
