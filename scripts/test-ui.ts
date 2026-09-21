import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { command } from './support.ts';
const required = [
  'U01',
  'U02',
  'U03',
  'U04',
  'U05',
  'U06',
  'U07',
  'U08',
  'U09',
  'U10',
];
const directory = resolve(
  'artifacts/ui',
  `browser-${new Date().toISOString().replaceAll(/[^0-9]/g, '')}-${randomUUID().slice(0, 8)}`,
);
await mkdir(directory, { recursive: true });
const result = await command(
  process.execPath,
  ['--test', '--test-reporter=spec', 'tests/ui/interface.test.ts'],
  { ...process.env, UI_EVIDENCE_DIR: directory },
  180000,
  true,
  { maxOutputBytes: 4 * 1024 * 1024 },
);
await writeFile(resolve(directory, 'stdout.log'), result.stdout);
await writeFile(resolve(directory, 'stderr.log'), result.stderr);
process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
assert.equal(
  result.code,
  0,
  'Browser test command failed; inspect ' + directory,
);
assert.equal(result.timedOut, false);
assert.equal(result.outputOverflow, false);
assert.deepEqual(result.cleanupErrors, []);
const evidence: unknown = JSON.parse(
  await readFile(resolve(directory, 'evidence.json'), 'utf8'),
);
assert.ok(evidence && typeof evidence === 'object' && 'results' in evidence);
assert.ok(Array.isArray(evidence.results));
const rows: unknown[] = evidence.results;
const ids: string[] = [];
for (const r of rows) {
  assert.ok(r && typeof r === 'object' && 'id' in r && 'status' in r);
  assert.equal(r.status, 'PASS');
  assert.ok(typeof r.id === 'string');
  ids.push(r.id);
}
assert.deepEqual(
  ids,
  required,
  'Every independently required UI case must execute exactly once.',
);
console.log(
  'PASS: operator UI browser cases with isolated API fixtures; evidence ' +
    directory,
);
