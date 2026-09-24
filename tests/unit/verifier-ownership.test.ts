// Refusal checks protect foreign resources; real lifecycle acceptance is separate.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { test } from 'node:test';
void test('verifier refuses demo cleanup, foreign resources and wrong Compose projects', async () => {
  const result = await promisify(execFile)(
    'python3',
    ['-B', 'tests/final/runtime_test.py'],
    { timeout: 5000, maxBuffer: 65536 },
  );
  assert.match(result.stderr, /Ran 4 tests/);
  assert.match(result.stderr, /\bOK\b/);
});
