import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import { object } from './acceptance.ts';
import {
  bootstrapCases,
  bootstrapUpgradeCases,
} from './required-bootstrap-cases.ts';
export async function checkBootstrapEvidence(path: string, upgrade: boolean) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  for (const row of rows) assert.equal(row['runId'], basename(dirname(path)));
  const one = (id: string) => {
    const found = rows.filter((row) => row['test'] === id);
    assert.equal(found.length, 1, `Missing/duplicate ${id} evidence`);
    return object(found[0]?.['data']);
  };
  for (const item of upgrade ? bootstrapUpgradeCases : bootstrapCases)
    one(item.id);
  if (upgrade) {
    const r = one('BS13');
    assert.deepEqual(r['before'], r['after']);
    assert.equal(r['baselineCount'], 0);
    assert.equal(object(r['state'])['phase'], 'active');
    return { upgrade: true, preserved: true, baselineCount: 0 };
  }
  const faults = [];
  for (const id of ['BS03', 'BS04', 'BS06', 'BS06A']) {
    const r = one(id),
      child = object(r['exit']);
    assert.deepEqual(child['exit'], { code: null, signal: 'SIGKILL' });
    assert.equal(child['ordinarySuccessBytes'], 0);
    assert.equal(child['sessionGone'], true);
    assert.equal(object(r['barrier'])['pid'], child['pid']);
    assert.equal(object(r['tx'])['state'], 'idle in transaction');
    faults.push({
      id,
      pid: child['pid'],
      exit: child['exit'],
      transaction: object(r['tx'])['backend_xid'],
    });
  }
  for (const id of ['BS03H', 'BS04H', 'BS06H', 'BS06AH']) {
    const r = one(id),
      child = object(r['exit']);
    assert.deepEqual(child['exit'], { code: 0, signal: null });
    assert.ok(Number(child['ordinarySuccessBytes']) > 0);
    assert.equal(child['sessionGone'], true);
  }
  const last = one('BS14');
  const baseline = object(last['snapshot'])['baseline_revisions'];
  assert.ok(Array.isArray(baseline));
  assert.equal(baseline.length, 257);
  const manifest = object(last['snapshot'])['bootstrap_manifest'];
  assert.ok(Array.isArray(manifest) && typeof manifest[0] === 'string');
  return {
    seedManifest: object(JSON.parse(manifest[0])),
    upgrade: false,
    baselineCount: 257,
    selectedBaselineEvents: last['selected'],
    faults,
    healthy: ['BS03H', 'BS04H', 'BS06H', 'BS06AH'],
    negativeControls: last['negativeControls'],
    sourceDerived: true,
  };
}
