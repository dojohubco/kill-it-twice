import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { command } from './support.ts';
import { object } from './acceptance.ts';
const directory = resolve(
  'artifacts/ui',
  `live-${new Date().toISOString().replaceAll(/[^0-9]/g, '')}-${randomUUID().slice(0, 8)}`,
);
await mkdir(directory, { recursive: true });
const startedAt = new Date().toISOString();
const result = await command(
  process.execPath,
  ['scripts/m1.ts', 'm6'],
  { ...process.env, UI_LIVE_CHECK: '1' },
  1800000,
  true,
  { maxOutputBytes: 4 * 1024 * 1024 },
);
await writeFile(join(directory, 'stdout.log'), result.stdout);
await writeFile(join(directory, 'stderr.log'), result.stderr);
const summary: Record<string, unknown> = {
  scope:
    'Actual browser against the real isolated operational API and service fixture, plus unchanged operational requirements',
  startedAt,
  finishedAt: new Date().toISOString(),
  exit: result.code,
  signal: result.signal,
  timedOut: result.timedOut,
  outputOverflow: result.outputOverflow,
};
try {
  assert.equal(result.code, 0);
  assert.equal(result.timedOut, false);
  assert.equal(result.outputOverflow, false);
  assert.deepEqual(result.cleanupErrors, []);
  const matches = [...result.stdout.matchAll(/^PASS: (.+\/run\.json)$/gm)];
  assert.equal(matches.length, 1);
  const runPath = matches[0]?.[1];
  assert.ok(runPath);
  const manifest = object(JSON.parse(await readFile(runPath, 'utf8')));
  assert.equal(manifest['status'], 'PASS');
  assert.equal(object(manifest['cleanup'])['status'], 'PASS');
  const live = object(
    JSON.parse(await readFile(join(dirname(runPath), 'ui-live.json'), 'utf8')),
  );
  assert.equal(live['mutations'], 0);
  assert.deepEqual(live['page_errors'], []);
  assert.ok(typeof live['inspected_entity_id'] === 'string');
  summary['status'] = 'PASS';
  summary['manifest'] = runPath;
  summary['live'] = live;
  summary['head'] = manifest['head'];
  summary['developmental'] = manifest['developmental'];
  console.log('PASS: live read-only operator interface; evidence ' + directory);
} catch (error) {
  summary['status'] = 'FAIL';
  summary['error'] = error instanceof Error ? error.message : 'Unknown error';
  process.exitCode = 1;
  console.error(result.stdout);
  console.error(result.stderr);
} finally {
  await writeFile(
    join(directory, 'evidence.json'),
    JSON.stringify(summary, null, 2) + '\n',
  );
}
