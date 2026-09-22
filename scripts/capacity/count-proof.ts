// Isolated database regression: schema-negative controls always roll back.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import pg from 'pg';
import { database, adminConfig, safeFailure } from '../runtime/private.ts';
import { withCleanup } from '../support.ts';
try {
  const migration = await readFile(
    'migrations/pipeline/007-operations.sql',
    'utf8',
  );
  const start = migration.indexOf(
    'CREATE OR REPLACE FUNCTION pipeline.backfill_counts(',
  );
  const end = migration.indexOf('\n$$;', start) + 4;
  assert.ok(start >= 0 && end > start);
  const original = migration
    .slice(start, end)
    .replace('pipeline.backfill_counts(', 'pg_temp.reference_counts(');
  const result = await database(
    await adminConfig('pipeline'),
    async (client) => {
      await client.query('SET search_path=pg_catalog,pg_temp');
      await client.query(original);
      const runs = (
        await client.query<{ run_id: string }>(
          'SELECT run_id FROM pipeline.backfill_runs ORDER BY run_id LIMIT 16',
        )
      ).rows;
      assert.ok(runs.length > 0);
      const comparison = [];
      for (const run of runs) {
        const rows = (
          await client.query<{ reference: unknown; observed: unknown }>(
            'SELECT pg_temp.reference_counts($1) AS reference,pipeline.backfill_counts($1) AS observed',
            [run.run_id],
          )
        ).rows;
        assert.equal(rows.length, 1);
        const row = rows[0];
        assert.ok(row);
        assert.deepEqual(row.observed, row.reference);
        comparison.push({ run_id: run.run_id, ...row });
      }
      const history = async () =>
        (
          await client.query<{ evidence: string }>(
            'SELECT row_to_json(e)::text evidence FROM pipeline.events e ORDER BY event_id LIMIT 4097',
          )
        ).rows;
      const before = await history();
      assert.ok(
        before.length > 0 && before.length <= 4096,
        'Small isolated fixture only',
      );
      const controls: { name: string; rejected: string }[] = [];
      for (const [name, alter] of [
        ['missing', ''],
        [
          'unvalidated',
          'ADD CONSTRAINT event_body_consistency CHECK (pipeline.valid_body(events) IS TRUE) NOT VALID',
        ],
        [
          'wrong_definition',
          'ADD CONSTRAINT event_body_consistency CHECK (entity_id>0)',
        ],
      ] as const) {
        await client.query('BEGIN');
        await withCleanup(
          async () => {
            await client.query(
              'ALTER TABLE pipeline.events DROP CONSTRAINT event_body_consistency',
            );
            if (alter)
              await client.query('ALTER TABLE pipeline.events ' + alter);
            await assert.rejects(
              client.query('SELECT pipeline.backfill_counts($1)', [
                runs[0]?.run_id,
              ]),
              (e: unknown) =>
                e instanceof pg.DatabaseError && e.code === 'P8003',
            );
            controls.push({ name, rejected: 'P8003' });
          },
          async () => {
            const r = await client.query('ROLLBACK');
            assert.equal(r.command, 'ROLLBACK');
          },
        );
      }
      const row = (
        await client.query<{ body: Buffer; hash: string }>(
          'SELECT body_bytes body,content_sha256 hash FROM pipeline.events ORDER BY event_id LIMIT 1',
        )
      ).rows[0];
      assert.ok(row);
      const body: unknown = JSON.parse(row.body.toString('utf8'));
      assert.ok(body && typeof body === 'object' && !Array.isArray(body));
      const changed = {
        ...body,
        entity_id: '9223372036854775806',
        schema_version: 2,
      } as Record<string, unknown>;
      changed['event_id'] =
        `${String(changed['source_epoch'])}:9223372036854775806:${String(changed['entity_version'])}`;
      const bytes = Buffer.from(JSON.stringify(changed));
      const digest = createHash('sha256').update(bytes).digest('hex');
      await client.query('BEGIN');
      await withCleanup(
        async () => {
          await assert.rejects(
            client.query(
              `INSERT INTO pipeline.events(event_id,source_epoch,entity_id,entity_version,source_change_id,source_recorded_at,kind,is_deleted,body_bytes,content_sha256) SELECT $1,source_epoch,9223372036854775806,entity_version,source_change_id,source_recorded_at,kind,is_deleted,$2,$3 FROM pipeline.events ORDER BY event_id LIMIT 1`,
              [changed['event_id'], bytes, digest],
            ),
            (e: unknown) =>
              e instanceof pg.DatabaseError &&
              e.code === '23514' &&
              e.constraint === 'event_body_consistency',
          );
        },
        async () => {
          const r = await client.query('ROLLBACK');
          assert.equal(r.command, 'ROLLBACK');
        },
      );
      assert.deepEqual(
        await history(),
        before,
        'No event or byte is rewritten by constraint-backed counting or negative fixtures',
      );
      const constraint = (
        await client.query<{
          convalidated: boolean;
          conenforced: boolean;
          definition: string;
        }>(
          "SELECT convalidated,conenforced,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='pipeline.events'::regclass AND conname='event_body_consistency'",
        )
      ).rows;
      assert.equal(constraint.length, 1);
      assert.ok(constraint[0]?.convalidated && constraint[0].conenforced);
      return {
        comparison,
        controls,
        malformed_body_rejected: '23514/event_body_consistency',
        unchanged_events: before.length,
        constraint,
      };
    },
  );
  console.log(
    JSON.stringify({
      status: 'PASS',
      scope:
        'Actual constraint enforcement and old/new aggregate equivalence on an isolated fixture',
      ...result,
    }),
  );
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
