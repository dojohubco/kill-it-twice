import assert from 'node:assert/strict';
import test from 'node:test';
import { checkAcceptance } from '../../scripts/acceptance.ts';
import { requiredCases } from '../../scripts/required-cases.ts';
import { Diagnostics } from '../../scripts/finalization.ts';
const runner = {
  code: 0,
  signal: null,
  timedOut: false,
  outputOverflow: false,
};
const fixture = () => ({
  format: 1,
  results: requiredCases.map((c) => ({
    name: c.name,
    file: c.file,
    status: 'pass',
    skip: false,
    todo: false,
    nesting: 0,
  })),
  summary: {
    success: true,
    counts: {
      tests: requiredCases.length,
      passed: requiredCases.length,
      cancelled: 0,
      skipped: 0,
      todo: 0,
      suites: 0,
    },
  },
});

test('acceptance requires the independent complete inventory', () => {
  assert.equal(
    checkAcceptance(JSON.stringify(fixture()), runner).passed,
    requiredCases.length,
  );
});
for (const kind of [
  'missing kill',
  'skip',
  'todo',
  'cancelled',
  'duplicate',
  'empty',
  'malformed',
  'nonzero',
] as const) {
  test(`acceptance rejects ${kind} evidence`, () => {
    const report = fixture();
    const first = report.results[0];
    assert.ok(first);
    if (kind === 'missing kill')
      report.results = report.results.filter((r) => !r.name.startsWith('T08 '));
    if (kind === 'skip') first.skip = true;
    if (kind === 'todo') first.todo = true;
    if (kind === 'cancelled') first.status = 'cancelled';
    if (kind === 'duplicate') report.results.push(first);
    if (kind === 'empty') report.results = [];
    assert.throws(() =>
      checkAcceptance(
        kind === 'malformed' ? '{broken' : JSON.stringify(report),
        kind === 'nonzero' ? { ...runner, code: 1 } : runner,
      ),
    );
  });
}

test('finalization preserves primary failure and attempts all cleanup steps', async () => {
  const diagnostics = new Diagnostics((text) => text);
  diagnostics.fail(new Error('runner failed'));
  for (const step of [
    'rollback',
    'process cleanup',
    'XML sanitization',
    'artifact finalization',
  ])
    await diagnostics.finalize(step, () =>
      Promise.reject(new Error(`${step} failed`)),
    );
  assert.match(diagnostics.primary ?? '', /runner failed/);
  assert.equal(diagnostics.cleanup.length, 4);
  assert.equal(diagnostics.ok, false);
  const success = new Diagnostics((text) => text);
  await success.finalize('artifact', () =>
    Promise.reject(new Error('write failed')),
  );
  assert.equal(success.ok, false);
});
