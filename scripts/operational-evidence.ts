import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { object } from './acceptance.ts';
import { operationalCases } from './required-operational-cases.ts';
export async function checkOperationalEvidence(path: string, upgrade: boolean) {
  const entries = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  const get = (id: string) => {
    const entry = entries.find((r) => r['test'] === id);
    assert.ok(entry, `Missing ${id} operational evidence`);
    return object(entry['data']);
  };
  if (upgrade) {
    const r = get('OP17');
    assert.equal(object(r['status'])['phase'], 'complete_with_errors');
    return {
      upgrade: true,
      retainedFailure: r['oldFailure'],
      historicalPhase: 'complete_with_errors',
    };
  }
  for (const required of operationalCases) get(required.id);
  const faults = entries
    .filter((r) => r['test'] === 'OP-process-death')
    .map((r) => object(r['data']));
  assert.equal(faults.length, 4);
  assert.deepEqual(
    faults.map((f) => f['operation']).sort(),
    ['backfill_start', 'backfill_pause', 'es_replay', 'source_change'].sort(),
  );
  for (const f of faults) {
    assert.ok(Array.isArray(f['exit']));
    assert.equal(f['exit'][1], 'SIGKILL');
    assert.equal(object(f['barrier'])['pid'], f['pid']);
  }
  assert.equal(object(get('OP07')['current'])['state'], 'satisfied');
  assert.equal(object(get('OP08')['current'])['state'], 'dead_letter');
  const history = get('OP08')['failures'];
  assert.ok(Array.isArray(history));
  assert.equal(history.length, 2);
  const network = get('OP13');
  assert.equal(
    object(object(network['disconnected'])['elasticsearch'])['state'],
    'disconnected',
  );
  assert.equal(
    object(object(network['reconnected'])['rabbitmq'])['state'],
    'connected',
  );
  return {
    upgrade: false,
    faults: faults.map((f) => ({
      operation: f['operation'],
      pid: f['pid'],
      exit: f['exit'],
    })),
    replayEvent: get('OP07')['event'],
    repeatedFailureEvent: get('OP08')['badEvent'],
    network: {
      disconnected: network['disconnected'],
      reconnected: network['reconnected'],
    },
    backendOnly: true,
  };
}
