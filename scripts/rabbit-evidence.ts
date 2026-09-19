import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import { object } from './acceptance.ts';
export async function checkRabbitEvidence(path: string) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((l) => object(JSON.parse(l)));
  assert.ok(rows.length);
  for (const row of rows)
    assert.equal(
      row['runId'],
      basename(dirname(path)),
      'Evidence belongs to another run',
    );
  const matching = (name: string) =>
    rows.filter((r) => r['test'] === name).map((r) => object(r['data']));
  const one = (name: string) => {
    const r = matching(name);
    assert.equal(r.length, 1, `Missing/duplicate broker evidence ${name}`);
    return object(r[0]);
  };
  const faults = [];
  for (const [name, count] of [
    ['MQ03-boundary', 3],
    ['MQ06-boundary', 2],
  ] as const) {
    const found = matching(name);
    assert.equal(found.length, count);
    for (const r of found) {
      const killed = object(r['killed']);
      assert.deepEqual(killed['exit'], { code: null, signal: 'SIGKILL' });
      assert.equal(killed['ordinarySuccessBytes'], 0);
      assert.equal(killed['pid'], object(r['reached'])['pid']);
      faults.push({
        boundary: r['boundary'],
        eventId: r['eventId'],
        pid: killed['pid'],
        exit: killed['exit'],
      });
    }
  }
  const repeated = one('MQ07');
  const crashes = repeated['crashes'];
  assert.ok(Array.isArray(crashes));
  assert.equal(crashes.length, 25);
  let wire: unknown, correlation: unknown;
  for (const [i, value] of crashes.entries()) {
    const r = object(value),
      k = object(r['killed']),
      b = object(r['reached']);
    assert.deepEqual(k['exit'], { code: null, signal: 'SIGKILL' });
    assert.equal(k['ordinarySuccessBytes'], 0);
    assert.equal(k['pid'], b['pid']);
    const deliveries = b['deliveries'];
    assert.ok(Array.isArray(deliveries));
    assert.equal(deliveries.length, 1);
    const d = object(deliveries[0]),
      props = object(d['properties']);
    if (i === 0) {
      wire = d['wireHex'];
      correlation = props['correlationId'];
      assert.ok(typeof wire === 'string' && /^[a-f0-9]+$/.test(wire));
      assert.ok(
        typeof correlation === 'string' && /^[a-f0-9-]{36}$/.test(correlation),
      );
    }
    assert.equal(d['wireHex'], wire);
    assert.equal(props['correlationId'], correlation);
    assert.equal(object(props['headers'])['x-delivery-count'] ?? 0, i);
  }
  const quarantine = one('MQ12');
  const quarantineKill = object(quarantine['killed']);
  assert.deepEqual(quarantineKill['exit'], { code: null, signal: 'SIGKILL' });
  assert.equal(quarantineKill['ordinarySuccessBytes'], 0);
  assert.equal(quarantineKill['pid'], object(quarantine['reached'])['pid']);
  faults.push({
    boundary: 'consumer.after_quarantine_commit.before_ack',
    eventId: 'quarantine-crash',
    pid: quarantineKill['pid'],
    exit: quarantineKill['exit'],
  });
  const healthy = [];
  for (let i = 1; i <= 6; i++) {
    const r = one(`MQH0${i}`);
    const exit = object(r['released']);
    assert.deepEqual(exit['exit'], { code: 0, signal: null });
    const output = exit['output'];
    assert.ok(Array.isArray(output) && output.length === 1);
    healthy.push({ id: `MQH0${i}`, pid: exit['pid'], exit: exit['exit'] });
  }
  const unknown = one('MQ04'),
    outage = one('MQ10'),
    reconcile = one('MQ14-reconciliation');
  one('MQ14-negative-controls');
  one('MQ15');
  one('MQ16');
  one('MQ08');
  one('MQ09-publisher');
  one('MQ09-consumer');
  const returned = one('MQ05-production-return')['returned'];
  assert.ok(Array.isArray(returned));
  assert.equal(returned.length, 1);
  assert.equal(object(returned[0])['returned'], true);
  assert.equal(object(returned[0])['outcome'], 'configuration');
  one('MQ12');
  one('MQ13');
  const effects = object(reconcile['snapshot'])['effects'];
  assert.ok(Array.isArray(effects));
  const mutations = reconcile['declaredMutations'];
  assert.ok(Array.isArray(mutations));
  assert.equal(effects.length, mutations.length);
  return {
    faults,
    healthy,
    singlePublicationCrashes: crashes.map((v) => {
      const r = object(v);
      return {
        iteration: r['iteration'],
        ...object(r['killed']),
        output: undefined,
      };
    }),
    declaredMutationCount: mutations.length,
    consumerEffects: effects.length,
    unknownConfirmEvent: unknown['eventId'],
    brokerStoppedMs: outage['brokerStoppedMs'],
    declaredEsRejections: reconcile['expectedRejections'],
  };
}
