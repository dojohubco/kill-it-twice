// Pure report-shape/ownership checks; actual process peaks require a real capacity run.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { test } from 'node:test';
void test('capacity resource reports retain byte units, ownership and explicit failure', async () => {
  const result = await promisify(execFile)(
    'python3',
    ['-B', 'tests/capacity/resources_test.py'],
    // Includes interpreter startup and the complete Python suite, not a service deadline.
    { timeout: 30000, maxBuffer: 65536 },
  );
  assert.match(result.stderr, /Ran 6 tests/);
  assert.match(result.stderr, /\bOK\b/);
});
