// Harness-only controlled initialization and independent upgrade observations.
import assert from 'node:assert/strict';
import { withCleanup } from '../../scripts/support.ts';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { first } from '../../scripts/rows.ts';
import {
  migrateCapturePipeline,
  migrateCaptureSource,
  pipelineSnapshot,
  sourceSnapshot,
  registerCapture,
} from '../../scripts/migrate-staging.ts';
import { Pipeline, pipelineIdentity } from '../../src/pipeline.ts';
import { SourceReader } from '../../src/source-reader.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import { databaseWaitFor } from './db.ts';
export async function captureSnapshot(c: pg.Client) {
  const rows: Record<string, string[]> = {};
  for (const table of ['capture_binding', 'capture_work'])
    rows[table] = (
      await c.query<{ text: string }>(
        `SELECT row_to_json(t)::text AS text FROM source.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.text);
  rows['state_counts'] = (
    await c.query<{ text: string }>(
      "SELECT state || ':' || count(*)::text AS text FROM source.capture_work GROUP BY state ORDER BY state",
    )
  ).rows.map((r) => r.text);
  return rows;
}
interface Setup {
  source: ConnectionConfig;
  pipeline: ConnectionConfig;
  commandPassword: string;
  stagerPassword: string;
  capturePassword: string;
  pipelineCapturePassword: string;
  epoch: string;
  upgrade: boolean;
}
export async function initializeCaptureFixture(
  s: pg.Client,
  p: pg.Client,
  input: Setup,
) {
  const keys = (
    await s.query<{ entityId: string; version: string }>(
      'SELECT entity_id::text AS "entityId",entity_version::text AS version FROM source.outbox ORDER BY allocation_id LIMIT 16',
    )
  ).rows;
  if (input.upgrade) {
    assert.equal(keys.length, 2);
    const events = (
      await new SourceReader(input.source, input.epoch).outbox(keys)
    ).events;
    await new Pipeline({
      ...input.pipeline,
      user: 'pipeline_stager',
      password: input.stagerPassword,
    }).stage(events);
  } else assert.equal(keys.length, 0);
  const sourceBefore = await sourceSnapshot(s),
    pipelineBefore = await pipelineSnapshot(p);
  await migrateCapturePipeline(p, input.pipelineCapturePassword);
  const identity = await pipelineIdentity({
    ...input.pipeline,
    user: 'pipeline_capture',
    password: input.pipelineCapturePassword,
  });
  assert.equal(identity.sourceEpoch, input.epoch);
  await migrateCaptureSource(s, input.capturePassword);
  const sourceAfterMigration = await sourceSnapshot(s);
  assert.deepEqual(sourceAfterMigration, sourceBefore);
  const pipelineAfterMigration = await pipelineSnapshot(p);
  for (const key of Object.keys(pipelineBefore).filter(
    (k) => k !== 'source_binding',
  ))
    assert.deepEqual(pipelineAfterMigration[key], pipelineBefore[key]);
  // Incomplete setup has an identity but no binding, claims or ACK opportunity.
  const runtime = new pg.Client({
    ...input.source,
    user: 'source_capture',
    password: input.capturePassword,
  });
  await runtime.connect();
  try {
    await assert.rejects(
      runtime.query('SELECT * FROM source.capture_claim($1,$2,$3,16,30000)', [
        identity.pipelineId,
        input.epoch,
        randomUUID(),
      ]),
      { code: 'P4001' },
    );
  } finally {
    await runtime.end();
  }
  const a = new pg.Client({
    ...input.source,
    user: 'source_command',
    password: input.commandPassword,
    application_name: `${input.source.application_name}:before-register`,
    query_timeout: 12000,
  });
  const registration = new pg.Client({
    ...input.source,
    application_name: `${input.source.application_name}:register`,
    query_timeout: 12000,
  });
  const b = new pg.Client({
    ...input.source,
    user: 'source_command',
    password: input.commandPassword,
    application_name: `${input.source.application_name}:during-register`,
    query_timeout: 12000,
  });
  const commandA = randomUUID(),
    commandB = randomUUID();
  return withCleanup(
    async () => {
      await a.connect();
      await registration.connect();
      await b.connect();
      const aPid = first(
        (await a.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows,
      ).pid;
      const rPid = first(
        (
          await registration.query<{ pid: number }>(
            'SELECT pg_backend_pid() AS pid',
          )
        ).rows,
      ).pid;
      const bPid = first(
        (await b.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows,
      ).pid;
      await a.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const createdA = first(
        (
          await a.query<{ entity_id: string }>(
            'SELECT entity_id::text FROM source.execute_command($1,$2,1,\'create\',NULL,\'{"registration":"before"}\'::jsonb)',
            [input.epoch, commandA],
          )
        ).rows,
      );
      await registration.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const registering = registration.query(
        'SELECT source.register_capture($1,$2,$3)',
        [identity.pipelineId, input.epoch, identity.codec],
      );
      void registering.catch(() => undefined);
      const waitA = await databaseWaitFor(
        s,
        async () =>
          (
            await s.query<{ blockers: number[] }>(
              'SELECT pg_blocking_pids($1) AS blockers',
              [rPid],
            )
          ).rows,
        (rows) => first(rows).blockers.includes(aPid),
        'registration waits for committing source writer',
      );
      await a.query('COMMIT');
      await registering;
      const writingB = b.query<{ entity_id: string }>(
        'SELECT entity_id::text FROM source.execute_command($1,$2,1,\'create\',NULL,\'{"registration":"during"}\'::jsonb)',
        [input.epoch, commandB],
      );
      void writingB.catch(() => undefined);
      const waitB = await databaseWaitFor(
        s,
        async () =>
          (
            await s.query<{ blockers: number[] }>(
              'SELECT pg_blocking_pids($1) AS blockers',
              [bPid],
            )
          ).rows,
        (rows) => first(rows).blockers.includes(rPid),
        'source writer waits for registration commit',
      );
      await registration.query('COMMIT');
      const createdB = first((await writingB).rows);
      const initialized = await captureSnapshot(s);
      // Identical initialization is repeatable, including after interruption between databases.
      await registerCapture(s, identity.pipelineId, input.epoch);
      assert.deepEqual(await captureSnapshot(s), initialized);
      const rows = (
        await s.query<Record<string, unknown>>(
          `SELECT o.source_epoch::text,o.entity_id::text,o.entity_version::text,o.allocation_id::text,w.work_id::text,w.state,w.generation::text,w.pipeline_id::text FROM source.outbox o LEFT JOIN source.capture_work w USING(source_epoch,entity_id,entity_version) ORDER BY o.allocation_id`,
        )
      ).rows;
      assert.equal(rows.length, keys.length + 2);
      for (const row of rows) {
        assert.equal(row['state'], 'pending');
        assert.equal(row['generation'], '0');
        assert.equal(row['pipeline_id'], identity.pipelineId);
      }
      return {
        pipelineId: identity.pipelineId,
        identity,
        mode: input.upgrade ? 'populated M2B' : 'fresh',
        sourceBefore,
        sourceAfterMigration,
        pipelineBefore,
        pipelineAfterMigration,
        registration: {
          commandA,
          commandB,
          aPid,
          rPid,
          bPid,
          waitA,
          waitB,
          createdA,
          createdB,
          rows,
          initialized,
          repeatedUnchanged: true,
        },
        incompleteSetupRejected: 'P4001',
      };
    },
    async () => {
      // Closing an owned connection rolls back an open transaction; try every close and retain failures.
      const closed = await Promise.allSettled([
        a.end(),
        registration.end(),
        b.end(),
      ]);
      const failures = closed
        .filter((r) => r.status === 'rejected')
        .map((r) => r.reason as unknown);
      if (failures.length)
        throw new AggregateError(
          failures,
          'Capture initialization connection cleanup failed',
        );
    },
  );
}
