import assert from 'node:assert/strict';
import { requiredCases } from './required-cases.ts';

export interface CaseResult {
  name: string;
  file: string;
  status: string;
  skip: boolean;
  todo: boolean;
  nesting: number;
}
export function object(value: unknown): Record<string, unknown> {
  assert.ok(
    value !== null && typeof value === 'object' && !Array.isArray(value),
    'Expected an object',
  );
  return value as Record<string, unknown>;
}
export function checkAcceptance(
  text: string,
  runner: {
    code: number | null;
    signal: string | null;
    timedOut: boolean;
    outputOverflow: boolean;
  },
) {
  assert.equal(runner.code, 0, 'Runner exit was nonzero');
  assert.equal(runner.signal, null);
  assert.equal(runner.timedOut, false);
  assert.equal(runner.outputOverflow, false);
  const report = object(JSON.parse(text));
  assert.equal(report['format'], 1, 'Unknown structured report format');
  assert.ok(
    Array.isArray(report['results']) && report['results'].length > 0,
    'Empty results',
  );
  const results: CaseResult[] = report['results'].map((raw: unknown) => {
    const r = object(raw);
    assert.ok(
      typeof r['name'] === 'string' &&
        typeof r['file'] === 'string' &&
        typeof r['status'] === 'string' &&
        typeof r['skip'] === 'boolean' &&
        typeof r['todo'] === 'boolean' &&
        typeof r['nesting'] === 'number',
      'Malformed case result',
    );
    return {
      name: r['name'],
      file: r['file'],
      status: r['status'],
      skip: r['skip'],
      todo: r['todo'],
      nesting: r['nesting'],
    };
  });
  assert.equal(
    new Set(requiredCases.map((entry) => entry.id)).size,
    requiredCases.length,
    'Duplicate inventory ID',
  );
  const seen = new Set<string>();
  for (const result of results) {
    const key = `${result.file}:${result.name}`;
    assert.ok(!seen.has(key), `Duplicate result: ${key}`);
    seen.add(key);
    assert.ok(
      requiredCases.some(
        (required) =>
          required.name === result.name && required.file === result.file,
      ),
      `Unexpected case: ${key}`,
    );
    assert.equal(result.status, 'pass', `Failed or cancelled: ${key}`);
    assert.equal(result.skip, false, `Skipped: ${key}`);
    assert.equal(result.todo, false, `Todo: ${key}`);
    assert.equal(result.nesting, 0, `Unexpected nested case: ${key}`);
  }
  const cases = requiredCases.map((required) => {
    assert.ok(
      results.some((r) => r.name === required.name && r.file === required.file),
      `Missing required case: ${required.id}`,
    );
    return { id: required.id, status: 'PASS' };
  });
  const summary = object(report['summary']);
  const counts = object(summary['counts']);
  assert.equal(summary['success'], true, 'Runner summary not successful');
  assert.equal(counts['tests'], results.length);
  assert.equal(counts['passed'], results.length);
  for (const field of ['cancelled', 'skipped', 'todo', 'suites'])
    assert.equal(counts[field], 0, `Nonzero ${field}`);
  return {
    cases,
    passed: results.length,
    failed: 0,
    skipped: 0,
    cancelled: 0,
    todo: 0,
  };
}
