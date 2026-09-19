import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { object } from './acceptance.ts';
// Independent evidence inventory for sub-schedules inside the aggregate ES cases.
export async function checkEsEvidence(path: string) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  const one = (test: string): unknown => {
    const found = rows.filter((r) => r['test'] === test);
    assert.equal(
      found.length,
      1,
      `Missing/duplicate required ES observation ${test}`,
    );
    return found[0]?.['data'];
  };
  const deaths = one('ES07');
  assert.ok(Array.isArray(deaths));
  assert.equal(deaths.length, 6);
  const faults: Record<string, unknown>[] = [];
  const deathRecords: Record<string, unknown>[] = deaths.map((v: unknown) =>
    object(v),
  );
  for (const boundary of [
    'es.after_claim_commit.before_request',
    'es.after_remote_apply.before_local_commit',
    'es.after_local_commit.before_success',
  ])
    for (const kill of [true, false]) {
      const matches: Record<string, unknown>[] = deathRecords.filter(
        (r) => r['boundary'] === boundary && r['kill'] === kill,
      );
      assert.equal(matches.length, 1);
      const r = object(matches[0]);
      const exit = object(r['exit']);
      const processExit = object(exit['exit']);
      assert.equal(processExit['signal'], kill ? 'SIGKILL' : null);
      assert.equal(processExit['code'], kill ? null : 0);
      assert.equal(exit['pid'], object(r['barrier'])['pid']);
      if (kill) assert.equal(exit['ordinarySuccessBytes'], 0);
      else {
        assert.ok(Array.isArray(exit['output']));
        assert.equal(exit['output'].length, 1);
        const success = object(exit['output'][0]);
        assert.equal(success['type'], 'es-success');
        assert.equal(success['claimed'], 1);
        const claims = object(r['barrier'])['claims'];
        assert.ok(Array.isArray(claims));
        assert.equal(claims.length, 1);
        assert.deepEqual(success['outcomes'], [
          {
            eventId: r['eventId'],
            generation: object(claims[0])['generation'],
            outcome: 'applied',
            status: 'settled',
          },
        ]);
      }
      faults.push({
        boundary,
        kill,
        pid: exit['pid'],
        exit: processExit,
        eventId: r['eventId'],
      });
    }
  const outage = object(one('ES10'));
  assert.equal(outage['automaticSameWorkerRecovery'], true);
  assert.ok(
    typeof outage['measuredStoppedMs'] === 'number' &&
      outage['measuredStoppedMs'] >= 60000,
  );
  const bulk = object(one('ES05-actual-bulk'));
  assert.equal(bulk['operations'], 500);
  const documents = object(one('ES05-independent-mget'));
  assert.equal(documents['appliedDocuments'], 497);
  assert.equal(documents['absentRejectedDocuments'], 3);
  const captureRestart = object(one('ES06-capture-restart'));
  assert.equal(
    object(object(captureRestart['exit'])['exit'])['signal'],
    'SIGKILL',
  );
  one('ES08');
  one('ES08-no-open-network-transaction');
  one('ES09');
  one('ES09-healthy');
  one('ES11');
  one('ES12');
  one('ES13');
  const restart = object(one('ES14-restart'));
  assert.equal(restart['unchanged'], true);
  const expectations = object(one('O07'));
  assert.equal(expectations['declaredBeforeDelivery'], true);
  assert.equal(expectations['originalBulkExpectedApplied'], 497);
  assert.equal(expectations['originalBulkExpectedRejected'], 3);
  assert.equal(expectations['historyAndReceiverChecked'], true);
  const declarations = expectations['expectedRejections'];
  assert.ok(Array.isArray(declarations));
  assert.equal(
    declarations.filter((d) => object(d)['fixture'] === 'ES05').length,
    3,
  );
  assert.equal(
    declarations.filter((d) => object(d)['fixture'] === 'ES06').length,
    1,
  );
  return {
    reconciliation: expectations,
    faults,
    actualBulk: {
      operations: 500,
      applied: 497,
      mappingFailures: 3,
      requestBytes: bulk['requestBytes'],
    },
    measuredOutageMs: outage['measuredStoppedMs'],
    automaticSameWorkerRecovery: true,
    retainedReceiverRestart: true,
  };
}

// Required independent evidence for the isolated M3.1 receiver/oracle profile.
export async function checkOracleEvidence(path: string) {
  const rows = (await readFile(path, 'utf8'))
    .trim()
    .split('\n')
    .map((line) => object(JSON.parse(line)));
  const cases: Record<string, unknown> = {};
  for (const id of ['O01', 'O02', 'O03', 'O04', 'O05', 'O06']) {
    const matches = rows.filter((r) => r['test'] === id);
    assert.equal(matches.length, 1, `Missing/duplicate oracle evidence ${id}`);
    cases[id] = object(matches[0])['data'];
  }
  const old = object(object(cases['O01'])['row']);
  assert.equal(old['sourceRevision'], '2');
  assert.equal(old['version'], '1');
  assert.equal(old['convergence'], 'EXPECTED DEGRADED');
  const absent = object(object(cases['O02'])['row']);
  assert.equal(absent['source'], null);
  assert.equal(absent['expectedReceiverRevision'], null);
  const corrected = object(object(cases['O03'])['row']);
  assert.equal(corrected['version'], '3');
  assert.equal(corrected['convergence'], 'CONVERGED');
  const tombstone = object(object(cases['O04'])['degraded']);
  assert.equal(tombstone['version'], '2');
  assert.equal(object(tombstone['source'])['is_deleted'], true);
  assert.equal(object(object(cases['O04'])['current'])['version'], '4');
  const controls = object(cases['O06'])['negative'];
  assert.ok(Array.isArray(controls));
  assert.ok(controls.includes('actual missing retained old document'));
  assert.ok(controls.includes('actual changed retained old document'));
  return {
    requiredCaseIds: Object.keys(cases),
    retainedOldVersion: old['version'],
    unresolvedSourceVersion: old['sourceRevision'],
    correctedVersion: corrected['version'],
    retainedTombstoneVersion: tombstone['version'],
    negativeControls: controls,
    declaredRejections: rows
      .filter(
        (r) => r['test'] === 'expected-rejection-declared-before-delivery',
      )
      .map((r) => r['data']),
  };
}
