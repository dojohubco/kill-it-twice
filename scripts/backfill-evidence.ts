import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import type pg from 'pg';
import { object } from './acceptance.ts';
import {
  backfillCases,
  backfillUpgradeCases,
  backfillEmptyCases,
} from './required-backfill-cases.ts';
export async function backfillSnapshot(
  c: pg.Client,
  side: 'source' | 'pipeline',
) {
  const rows: Record<string, string[]> = {};
  for (const table of side === 'source'
    ? ['backfill_fences', 'backfill_fence_members']
    : [
        'backfill_runs',
        'backfill_ranges',
        'backfill_batches',
        'backfill_members',
      ])
    rows[table] = (
      await c.query<{ r: string }>(
        `SELECT row_to_json(t)::text r FROM ${side}.${table} t ORDER BY row_to_json(t)::text COLLATE "C"`,
      )
    ).rows.map((r) => r.r);
  return rows;
}
export async function checkBackfillEvidence(
  path: string,
  upgrade: boolean,
  empty: boolean,
) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  for (const r of rows) assert.equal(r['runId'], basename(dirname(path)));
  const one = (id: string) => {
    const r = rows.filter((r) => r['test'] === id);
    assert.equal(r.length, 1, `Missing/duplicate ${id} evidence`);
    return object(r[0]?.['data']);
  };
  for (const c of empty
    ? backfillEmptyCases
    : upgrade
      ? backfillUpgradeCases
      : backfillCases)
    one(c.id);
  if (empty) {
    const r = one('BF01E');
    assert.equal(object(r['completed'])['phase'], 'complete');
    assert.equal(
      object(object(r['populatedLegacy'])['completed'])['phase'],
      'complete',
    );
    return { empty: true, populatedLegacy: true, requiredEvents: [0, 1] };
  }
  if (upgrade) {
    const r = one('BF16');
    assert.deepEqual(r['before'], r['after']);
    assert.equal(r['preserved'], true);
    return { upgrade: true, preserved: true };
  }
  const faults = [];
  for (const id of ['BF03', 'BF04', 'BF11', 'BF11A', 'BF12']) {
    const r = one(id),
      exit = object(r['exit']);
    assert.deepEqual(exit['exit'], { code: null, signal: 'SIGKILL' });
    assert.equal(exit['ordinarySuccessBytes'], 0);
    assert.equal(exit['sessionGone'], true);
    assert.equal(object(r['barrier'])['pid'], exit['pid']);
    faults.push({ id, pid: exit['pid'], exit: exit['exit'] });
  }
  for (const id of ['BF03H', 'BF04H', 'BF11H', 'BF11AH', 'BF12H']) {
    const e = object(one(id)['exit']);
    assert.deepEqual(e['exit'], { code: 0, signal: null });
    assert.ok(Number(e['ordinarySuccessBytes']) > 0);
    assert.equal(e['sessionGone'], true);
  }
  assert.equal(object(one('BF14')['completed'])['phase'], 'complete');
  assert.equal(
    object(one('BF13')['completed'])['phase'],
    'complete_with_errors',
  );
  const negative = one('BF13')['negative'];
  assert.ok(Array.isArray(negative));
  const quarantine = negative
    .map(object)
    .find((n) => n['mode'] === 'quarantined_classification');
  assert.ok(quarantine);
  assert.equal(object(quarantine['counts'])['es_errors'], '0');
  assert.equal(object(quarantine['counts'])['consumer_errors'], '1');
  assert.equal(object(one('BF17')['blocked'])['sealed_at'], null);
  const ids = one('BF15')['requiredIds'];
  assert.ok(Array.isArray(ids) && ids.length >= 257);
  return {
    faults,
    healthy: ['BF03H', 'BF04H', 'BF11H', 'BF11AH', 'BF12H'],
    requiredEvents: ids.length,
    negativeControls: one('BF15')['negativeControls'],
    terminalErrors: 'explicit complete_with_errors',
    oversized: 'blocked and unsealed',
  };
}
