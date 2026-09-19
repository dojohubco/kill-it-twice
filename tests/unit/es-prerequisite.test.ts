import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, writeFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  observePrerequisite,
  requirePrerequisite,
} from '../../scripts/es-prerequisite.ts';
import { verifyM3 } from '../../scripts/verify-m3.ts';
import {
  sanitizeStageEvidence,
  MissingEvidenceError,
} from '../../scripts/stage-evidence.ts';
import { Diagnostics } from '../../scripts/finalization.ts';
await test('P01 below-threshold gate records observation and starts no profiles or services', async () => {
  const calls: string[] = [];
  await assert.rejects(
    verifyM3({
      observe: () =>
        observePrerequisite(
          'unit-complete-gate',
          () => Promise.resolve('1048575\n'),
          'linux',
        ),
      record: (observation) => {
        calls.push('record');
        assert.equal(observation.required, '1048576');
        assert.equal(observation.observed, '1048575');
        assert.equal(observation.platform, 'linux');
        assert.equal(observation.phase, 'unit-complete-gate');
        return Promise.resolve();
      },
      execute: () => {
        calls.push('EXPENSIVE PROFILE OR SERVICE');
        return Promise.resolve();
      },
    }),
    /required=1048576 observed=1048575.*phase=unit-complete-gate/,
  );
  assert.deepEqual(calls, ['record']);
});
await test('P02 threshold and higher observations pass unchanged with fixed profile ordering', async () => {
  for (const observed of ['1048576', '2097152']) {
    const calls: string[] = [];
    await verifyM3({
      observe: () =>
        observePrerequisite('unit', () => Promise.resolve(observed), 'linux'),
      record: (r) => {
        assert.equal(r.observed, observed);
        calls.push('record');
        return Promise.resolve();
      },
      execute: (args) => {
        calls.push(args.join(' '));
        return Promise.resolve();
      },
    });
    assert.deepEqual(calls, [
      'record',
      'make verify-m2c1',
      'npm run test:integration:m3',
      'npm run test:integration:m3 -- --upgrade',
      'npm run test:integration:m31',
    ]);
  }
});
await test('P02 unreadable malformed and unsupported observations fail without claiming zero', async () => {
  for (const read of [
    () => Promise.reject(new Error('unavailable')),
    () => Promise.resolve('garbage'),
  ]) {
    const r = await observePrerequisite('unit', read, 'linux');
    assert.equal(r.observed, null);
    assert.throws(() => requirePrerequisite(r));
  }
  assert.throws(() =>
    requirePrerequisite({
      required: '1048576',
      observed: '2097152',
      platform: 'darwin',
      phase: 'unit',
      observedAt: 'synthetic',
      status: 'FAIL',
      reason: 'unsupported',
      assumption: 'local Linux',
    }),
  );
});
await test('P03 missing pre-test reports are NOT RUN while available diagnostics are sanitized', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'kit-phase-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const diagnostics = new Diagnostics((s) => s);
  diagnostics.fail(new Error('preflight failed'));
  for (const report of ['tests.xml', 'tests.json']) {
    assert.equal(
      await sanitizeStageEvidence(join(dir, report), false, (s) => s),
      'NOT RUN',
    );
    await assert.rejects(stat(join(dir, report)), { code: 'ENOENT' });
  }
  const actual = join(dir, 'setup.log');
  await writeFile(actual, 'fixture-private-value');
  assert.equal(
    await sanitizeStageEvidence(actual, false, (s) =>
      s.replace('fixture-private-value', '[REDACTED]'),
    ),
    'AVAILABLE',
  );
  assert.equal(await readFile(actual, 'utf8'), '[REDACTED]');
  assert.match(diagnostics.primary ?? '', /preflight failed/);
  assert.equal(diagnostics.cleanup.length, 0);
  assert.equal(diagnostics.ok, false);
});
await test('P04 attempted missing evidence fails separately from real cleanup and unsafe files are removed', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'kit-phase-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const diagnostics = new Diagnostics((s) => s);
  diagnostics.fail(new Error('runner primary'));
  await assert.rejects(
    sanitizeStageEvidence(join(dir, 'tests.json'), true, (s) => s),
    MissingEvidenceError,
  );
  assert.equal(diagnostics.cleanup.length, 0);
  const unsafe = join(dir, 'tests.xml');
  await writeFile(unsafe, 'fixture-private-value');
  await diagnostics.finalize('sanitize actual report', () =>
    sanitizeStageEvidence(unsafe, true, () => {
      throw new Error('sanitizer failed');
    }),
  );
  await assert.rejects(stat(unsafe), { code: 'ENOENT' });
  await diagnostics.finalize('owned resource cleanup', () =>
    Promise.reject(new Error('cleanup failed')),
  );
  assert.match(diagnostics.primary ?? '', /runner primary/);
  assert.deepEqual(
    diagnostics.cleanup.map((r) => r.step),
    ['sanitize actual report', 'owned resource cleanup'],
  );
  assert.equal(diagnostics.ok, false);
});
