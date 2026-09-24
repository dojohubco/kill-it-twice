// Real restricted-role and immutable-receipt checks in the verifier-owned runtime.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { adminConfig, database, password, sql } from './private.ts';
const checks: string[] = [];
for (const role of ['pipeline_capture', 'pipeline_backfill']) {
  await database(sql(role, await password(role)), async (client) => {
    const current = await client.query<{ value: Record<string, unknown> }>(
      'SELECT pipeline.read_polling() value',
    );
    assert.ok(current.rows[0]?.value['revision']);
    for (const query of [
      'UPDATE pipeline.polling_settings SET capture_poll_ms=50',
      'SELECT * FROM pipeline.polling_requests',
      `SELECT pipeline.set_polling('${randomUUID()}','${randomUUID()}',1,50,50)`,
    ])
      await assert.rejects(
        client.query(query),
        (e: unknown) =>
          !!e && typeof e === 'object' && 'code' in e && e.code === '42501',
      );
  });
  checks.push(role + ': read settings allowed, writes and history denied');
}
await database(await adminConfig('pipeline'), async (client) => {
  const before = await client.query(
    'SELECT * FROM pipeline.polling_requests ORDER BY request_id',
  );
  assert.ok(before.rows.length >= 3);
  await assert.rejects(
    client.query("UPDATE pipeline.polling_requests SET actor='local-operator'"),
    (e: unknown) =>
      !!e && typeof e === 'object' && 'code' in e && e.code === 'P3002',
  );
  await assert.rejects(
    client.query('DELETE FROM pipeline.polling_requests'),
    (e: unknown) =>
      !!e && typeof e === 'object' && 'code' in e && e.code === 'P3002',
  );
  await assert.rejects(
    client.query('TRUNCATE pipeline.polling_requests'),
    (e: unknown) =>
      !!e && typeof e === 'object' && 'code' in e && e.code === 'P3002',
  );
  assert.deepEqual(
    (
      await client.query(
        'SELECT * FROM pipeline.polling_requests ORDER BY request_id',
      )
    ).rows,
    before.rows,
  );
  const current = (
    await client.query<{ value: unknown }>(
      'SELECT pipeline.read_polling() value',
    )
  ).rows[0]?.value;
  for (const args of [
    [randomUUID(), randomUUID(), 0, 1000, 1000],
    [randomUUID(), randomUUID(), 1, 49, 1000],
    [randomUUID(), randomUUID(), 1, 1000, 30001],
    [randomUUID(), randomUUID(), 1, null, 1000],
  ]) {
    await assert.rejects(
      client.query('SELECT pipeline.set_polling($1,$2,$3,$4,$5)', args),
      (e: unknown) =>
        !!e && typeof e === 'object' && 'code' in e && e.code === '22023',
    );
  }
  assert.deepEqual(
    (
      await client.query<{ value: unknown }>(
        'SELECT pipeline.read_polling() value',
      )
    ).rows[0]?.value,
    current,
  );
});
checks.push(
  'immutable receipts: update/delete/truncate rejected, retained exactly',
);
checks.push('SQL input bounds: rejected without changing settings');
console.log(JSON.stringify({ status: 'PASS', checks }));
