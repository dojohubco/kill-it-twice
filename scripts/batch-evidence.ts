import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import { object } from './acceptance.ts';
import { batchCases, batchReproductionCases } from './required-batch-cases.ts';
export async function checkBatchEvidence(path: string, reproduction: boolean) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((l) => object(JSON.parse(l)));
  for (const r of rows) assert.equal(r['runId'], basename(dirname(path)));
  const one = (id: string) => {
    const found = rows.filter((r) => r['test'] === id);
    assert.equal(found.length, 1, `Missing/duplicate ${id} evidence`);
    return object(found[0]?.['data']);
  };
  for (const c of reproduction ? batchReproductionCases : batchCases) one(c.id);
  if (reproduction) {
    const b = one('B01'),
      sizes = object(b['sizes']);
    assert.equal(b['sourceDerived'], true);
    assert.equal(sizes['wireBytes'], 1048576);
    assert.equal(sizes['oldSqlCharge'], 1049056);
    assert.equal(object(b['error'])['sqlState'], 'P6002');
    assert.equal(b['rollback'], 'ROLLBACK');
    assert.equal(b['consumerUnchanged'], true);
    assert.equal(b['quarantineUnchanged'], true);
    const attempts = b['attempts'];
    assert.ok(Array.isArray(attempts));
    assert.equal(attempts.length, 2);
    for (const a of attempts) {
      const e = object(object(a)['exit']);
      assert.deepEqual(e['exit'], { code: 1, signal: null });
      assert.equal(e['ordinarySuccessBytes'], 0);
      const messages = e['messages'];
      assert.ok(Array.isArray(messages));
      const failure = object(
        messages.find((m) => object(m)['type'] === 'batch-failure'),
      );
      const trace = object(failure['trace']);
      assert.deepEqual(trace['acknowledgements'], []);
      const batches = trace['batches'];
      assert.ok(Array.isArray(batches) && batches.length === 1);
      assert.equal(object(batches[0])['wireBytes'], 1048576);
      assert.equal(object(batches[0])['sqlState'], 'P6002');
      assert.equal(object(batches[0])['completion'], 'rolled_back');
      assert.equal(object(object(a)['queue'])['messages_ready'], 32);
    }
    return {
      intent: b['intent'],
      sourceDerived: true,
      count: 32,
      wireBytes: 1048576,
      oldSqlCharge: 1049056,
      sqlState: 'P6002',
      consumerAttempts: 2,
      ordinarySuccess: 0,
      acknowledgements: 0,
    };
  }
  const killed = one('B06'),
    exit = object(killed['exit']);
  assert.deepEqual(exit['exit'], { code: null, signal: 'SIGKILL' });
  assert.equal(exit['ordinarySuccessBytes'], 0);
  assert.equal(exit['pid'], object(killed['committed'])['pid']);
  for (const id of ['B02', 'B06H']) {
    const b = one(id);
    assert.equal(object(b['sizes'])['wireBytes'], 1048576);
    assert.deepEqual(object(b['exit'])['exit'], { code: 0, signal: null });
  }
  assert.equal(object(one('B03')['sizes'])['wireBytes'], 1048577);
  assert.equal(one('B05')['originalBytes'], 1048576);
  assert.equal(one('B05')['uniqueBytes'], 524288);
  assert.equal(one('B07')['runtimeDenied'], 4);
  assert.equal(one('B08')['sameConsumerObject'], true);
  return {
    caseIds: batchCases.map((c) => c.id),
    wireBoundary: 1048576,
    distinct: 32,
    overBoundary: 1048577,
    duplicateOriginalBytes: 1048576,
    duplicateUniqueBytes: 524288,
    upgrade: one('B07')['upgrade'],
    fault: { pid: exit['pid'], exit: exit['exit'], eventIds: killed['events'] },
    healthy: ['B02', 'B06H'],
    contentCompared: true,
  };
}
