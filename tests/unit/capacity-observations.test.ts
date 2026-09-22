// Pure Python sampler fixtures; this does not create a service or use the network.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { test } from 'node:test';
void test('capacity sampler distinguishes fresh, stale, unavailable and malformed observations', async () => {
  const result = await promisify(execFile)(
    'python3',
    ['-B', 'tests/capacity/observations_test.py'],
    { timeout: 5000, maxBuffer: 65536 },
  );
  assert.match(result.stderr, /Ran 7 tests/);
  assert.match(result.stderr, /\bOK\b/);
});
