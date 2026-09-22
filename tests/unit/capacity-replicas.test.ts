// Pure configuration/admission checks; no broker or database is provisioned here.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { test } from 'node:test';
void test('capacity replica options are bounded and invalid CLI inputs create no fixture', async () => {
  const result = await promisify(execFile)(
    'python3',
    ['-B', 'tests/capacity/replicas_test.py'],
    { timeout: 15000, maxBuffer: 65536 },
  );
  assert.match(result.stderr, /Ran 6 tests/);
  assert.match(result.stderr, /\bOK\b/);
});
