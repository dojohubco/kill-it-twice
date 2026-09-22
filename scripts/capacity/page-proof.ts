// Isolated real old-to-new page-bound upgrade; workers are stopped by the owner.
import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import type { Client } from 'pg';
import { BackfillSource } from '../../src/backfill/source.ts';
import { BackfillLedger } from '../../src/backfill/ledger.ts';
import { Source } from '../../src/source.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import {
  database,
  adminConfig,
  sql,
  password,
  safeFailure,
} from '../runtime/private.ts';
import { prepare, applyFile } from '../runtime/migrations.ts';
async function snapshot(c: Client, domain: 'source' | 'pipeline') {
  const tables = (
    await c.query<{ name: string }>(
      "SELECT c.relname name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' ORDER BY c.relname",
      [domain],
    )
  ).rows;
  const result: Record<string, string[]> = {};
  for (const { name } of tables) {
    assert.match(name, /^[a-z_]+$/);
    result[name] = (
      await c.query<{ value: string }>(
        `SELECT row_to_json(x)::text value FROM ${domain}.${name} x ORDER BY row_to_json(x)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.value);
  }
  return result;
}
async function signature(c: Client, fn: string) {
  return (
    await c.query<Record<string, unknown>>(
      'SELECT oid,proowner,proacl,proconfig,prosecdef,provolatile FROM pg_proc WHERE oid=$1::regprocedure',
      [fn],
    )
  ).rows;
}
try {
  const chunks: Buffer[] = [];
  let inputBytes = 0;
  for await (const chunk of process.stdin as AsyncIterable<Buffer>) {
    inputBytes += chunk.length;
    assert.ok(inputBytes <= 512 * 1024, 'Bounded declared workload');
    chunks.push(chunk);
  }
  const raw: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  assert.ok(Array.isArray(raw) && raw.length === 6);
  const commands = raw.map((value: unknown) => {
    assert.ok(value && typeof value === 'object');
    const r = value as Record<string, unknown>;
    assert.equal(r['operation'], 'create');
    assert.equal(r['entity_id'], null);
    assert.equal(typeof r['command_id'], 'string');
    assert.equal(typeof r['payload_json'], 'string');
    return {
      command_id: String(r['command_id']),
      operation: 'create' as const,
      entity_id: null,
      payload_json: String(r['payload_json']),
    };
  });
  const binding = await database(
    await adminConfig('pipeline'),
    async (c) =>
      (
        await c.query<{ source_epoch: string; pipeline_id: string }>(
          'SELECT source_epoch,pipeline_id FROM pipeline.source_binding',
        )
      ).rows[0],
  );
  assert.ok(binding);
  const run = await database(
    await adminConfig('source'),
    async (c) =>
      (
        await c.query<{ bootstrap_key: string }>(
          'SELECT bootstrap_key FROM source.bootstrap_manifest',
        )
      ).rows[0]?.bootstrap_key,
  );
  assert.ok(run);
  const sourceConfig = sql(
    'source_backfill',
    await password('source_backfill'),
  );
  const pipelineConfig = sql(
    'pipeline_backfill',
    await password('pipeline_backfill'),
  );
  const identity = {
    sourceEpoch: binding.source_epoch,
    pipelineId: binding.pipeline_id,
  };
  const oldSource = new BackfillSource(sourceConfig, identity),
    oldLedger = new BackfillLedger(pipelineConfig);
  const oldClaim = await oldLedger.claim(run, randomUUID(), 30000);
  assert.ok(oldClaim);
  const oldPage = await oldSource.page(oldClaim.checkpoint, oldClaim.upper);
  assert.equal(oldPage.events.length, 16);
  const first = await oldLedger.page({
    claim: oldClaim,
    batchId: randomUUID(),
    page: oldPage,
  });
  const upgrades = [];
  for (const [domain, file, fn] of [
    [
      'source',
      '012-backfill-page-bounds.sql',
      'source.backfill_page(uuid,uuid,bigint,bigint,integer,uuid)',
    ],
    [
      'pipeline',
      'pipeline/014-backfill-page-bounds.sql',
      'pipeline.backfill_page(uuid,integer,uuid,bigint,uuid,bigint,bigint,boolean,jsonb,jsonb)',
    ],
  ] as const) {
    upgrades.push(
      await database(await adminConfig(domain), async (c) => {
        const before = await snapshot(c, domain),
          meta = await signature(c, fn);
        await prepare(c, domain);
        await applyFile(c, file);
        assert.deepEqual(await snapshot(c, domain), before);
        assert.deepEqual(await signature(c, fn), meta);
        return {
          domain,
          unchanged_tables: Object.keys(before).length,
          rows_sha256: createHash('sha256')
            .update(JSON.stringify(before))
            .digest('hex'),
          unchanged_function: meta,
        };
      }),
    );
  }
  const reader = new BackfillSource(sourceConfig, identity, 64),
    ledger = new BackfillLedger(pipelineConfig, 64);
  const rest = await ledger.claim(run, randomUUID(), 30000);
  assert.ok(rest);
  const restPage = await reader.page(rest.checkpoint, rest.upper);
  assert.equal(restPage.events.length, 48);
  await ledger.page({ claim: rest, batchId: randomUUID(), page: restPage });
  const claim = await ledger.claim(run, randomUUID(), 30000);
  assert.ok(claim);
  const page = await reader.page(claim.checkpoint, claim.upper);
  assert.equal(page.events.length, 64);
  assert.ok(page.events.reduce((n, e) => n + e.wireBytes.length, 0) <= 262144);
  const small = await oldSource.page(claim.checkpoint, claim.upper);
  assert.equal(small.events.length, 16);
  assert.deepEqual(
    small.events.map((e) => e.wireBytes.toString('hex')),
    page.events.slice(0, 16).map((e) => e.wireBytes.toString('hex')),
  );
  const request = { claim, batchId: randomUUID(), page };
  await assert.rejects(() => oldLedger.page(request));
  const abort = new Error('Controlled local page rollback after all page SQL');
  await database(await adminConfig('pipeline'), async (c) => {
    const before = await snapshot(c, 'pipeline');
    await assert.rejects(
      () =>
        ledger.transaction(async (tx) => {
          await tx.page(request);
          throw abort;
        }),
      (error: unknown) =>
        error instanceof TransactionError &&
        error.outcome === 'rolled_back' &&
        error.cause === abort,
    );
    assert.deepEqual(await snapshot(c, 'pipeline'), before);
  });
  const committed = await ledger.page(request),
    replayed = await ledger.page(request);
  assert.equal(committed['replayed'], false);
  assert.equal(replayed['replayed'], true);
  assert.equal(committed['created_at'], replayed['created_at']);
  assert.deepEqual(committed['items'], replayed['items']);
  const tx = await database(
    await adminConfig('pipeline'),
    async (c) =>
      (
        await c.query<{ tx: string }>(
          `SELECT xmin::text tx FROM pipeline.events WHERE event_id=ANY($1) UNION SELECT xmin::text FROM pipeline.backfill_batches WHERE batch_id=$2 UNION SELECT xmin::text FROM pipeline.backfill_members WHERE first_batch=$2`,
          [page.events.map((e) => e.body.event_id), request.batchId],
        )
      ).rows,
  );
  assert.equal(
    tx.length,
    1,
    'The entire admitted page shares one committed transaction',
  );
  const rejected: string[] = [];
  await database(sourceConfig, async (c) => {
    await assert.rejects(
      () =>
        c.query('SELECT source.backfill_page($1,$2,0,257,65,NULL)', [
          identity.sourceEpoch,
          identity.pipelineId,
        ]),
      (e: unknown) => e instanceof Error && 'code' in e && e.code === 'P8003',
    );
    rejected.push('source-65');
    await assert.rejects(
      () =>
        c.query(
          'ALTER FUNCTION source.backfill_page(uuid,uuid,bigint,bigint,integer,uuid) COST 1',
        ),
      (e: unknown) => e instanceof Error && 'code' in e && e.code === '42501',
    );
    rejected.push('source-runtime-ddl');
  });
  const inputs = page.events.map((e, i) => ({
    key: page.keys[i],
    body: e.bodyBytes.toString('hex'),
    hash: e.contentSha256,
  }));
  await database(pipelineConfig, async (c) => {
    await assert.rejects(
      () =>
        c.query(
          'SELECT pipeline.backfill_page($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
          [
            run,
            claim.range,
            claim.owner,
            claim.generation,
            randomUUID(),
            claim.checkpoint,
            page.next,
            page.eof,
            JSON.stringify([...inputs, inputs[0]]),
            JSON.stringify(page.observation),
          ],
        ),
      (e: unknown) =>
        e instanceof Error &&
        'code' in e &&
        e.code === 'P8003' &&
        /Invalid page bound/.test(e.message),
    );
    rejected.push('pipeline-65');
    await assert.rejects(
      () =>
        c.query(
          'ALTER TABLE pipeline.backfill_batches DROP CONSTRAINT backfill_batches_items_check',
        ),
      (e: unknown) => e instanceof Error && 'code' in e && e.code === '42501',
    );
    rejected.push('pipeline-runtime-ddl');
  });
  const writer = new Source(
    sql('source_command', await password('source_command')),
  );
  const created: string[] = [];
  for (const command of commands) {
    const result = await writer.command({
      sourceEpoch: identity.sourceEpoch,
      commandId: command.command_id,
      contractVersion: 1,
      operation: 'create',
      entityId: null,
      payloadJson: command.payload_json,
    });
    created.push(result.result.entity_id);
  }
  const last = created.at(-1);
  assert.ok(last);
  const prefix = await reader.page('257', last);
  assert.ok(
    prefix.events.length > 0 &&
      prefix.events.length < 6 &&
      !prefix.eof &&
      !prefix.blocked,
  );
  const tail = await reader.page(prefix.next, last);
  assert.ok(tail.eof);
  assert.equal(prefix.events.length + tail.events.length, 6);
  assert.deepEqual(
    [...prefix.events, ...tail.events].map((e) => e.body.entity_id),
    created,
  );
  const result = {
    status: 'PASS',
    scope:
      'Actual bounded page transaction, populated function upgrade and prefix checks; not a scale benchmark',
    upgrades,
    old_page_records: oldPage.events.length,
    old_batch_id: first['batch_id'],
    page_records: page.events.length,
    wire_bytes: page.events.reduce((n, e) => n + e.wireBytes.length, 0),
    committed_transaction: tx[0]?.tx,
    batch_id: request.batchId,
    replay_same_content: true,
    controlled_rollback: true,
    rejected,
    byte_prefix: prefix.events.length,
    byte_tail: tail.events.length,
    command_ids: commands.map((c) => c.command_id),
  };
  const output = JSON.stringify(result);
  assert.ok(Buffer.byteLength(output) <= 512 * 1024);
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(output + '\n', (e) => (e ? reject(e) : resolve())),
  );
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
