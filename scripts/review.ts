// Reproducible local handoff capture and bundle generation; never publishes anything.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { cp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { command } from './support.ts';
import { object } from './acceptance.ts';

const m2c1 = process.argv.includes('--m2c1');
const m2c = m2c1 || process.argv.includes('--m2c');
const m2b = process.argv.includes('--m2b');
const m2a = process.argv.includes('--m2a');
const milestone = m2c1
  ? 'M2C.1'
  : m2c
    ? 'M2C'
    : m2b
      ? 'M2B'
      : m2a
        ? 'M2A'
        : 'M1.1';
const profile = m2c1
  ? 'm2c1'
  : m2c
    ? 'm2c'
    : m2b
      ? 'm2b'
      : m2a
        ? 'm2a'
        : 'm1.1';
const baseline = m2c1
  ? '6a908f46d3c690023d204b0fdb785e53d471d207'
  : m2c
    ? '642925bf66fc0755f4a845bab2a55350957475c4'
    : m2b
      ? '735585c3ddb45fa9c2239943102dbc45b37ce710'
      : m2a
        ? 'e5813a4bcd7857ba816ac6ad4537cc7a52f86963'
        : 'f8c2e061822fbb97edf831f23b9611570ccd9ef6';
async function checked(executable: string, args: string[]) {
  const result = await command(executable, args, process.env, 30_000, true);
  assert.ok(
    result.code === 0 &&
      !result.signal &&
      !result.timedOut &&
      !result.outputOverflow &&
      result.cleanupErrors.length === 0,
    `${executable}: ${result.stderr}`,
  );
  return result.stdout;
}
const head = (await checked('git', ['rev-parse', 'HEAD'])).trim();
assert.equal(
  (await checked('git', ['status', '--porcelain'])).trim(),
  '',
  'Handoff capture requires a clean working tree',
);
const mode = process.argv[2];
if (mode === 'capture') {
  const directory = resolve(
    `artifacts/${profile}`,
    `acceptance-${new Date().toISOString().replace(/\D/g, '')}-${randomBytes(4).toString('hex')}`,
  );
  await mkdir(directory, { recursive: true });
  console.log(directory);
  const commands = [
    ['npm', 'ci', '--no-audit', '--no-fund'],
    ['npm', 'ls', '--depth=0'],
    ...[
      'format:check',
      'lint',
      'typecheck',
      'knip',
      'validate:compose',
      'validate:workflow',
      'test:unit',
    ].map((name) => ['npm', 'run', name]),
    ['make', 'quality'],
    ['npm', 'run', 'test:integration:m1'],
    ...(m2a || m2b || m2c ? [['npm', 'run', 'test:integration:m2a']] : []),
    ...(m2b || m2c
      ? [
          ['npm', 'run', 'test:integration:m2b'],
          ['npm', 'run', 'test:integration:m2b', '--', '--upgrade'],
        ]
      : []),
    ...(m2c
      ? [
          ['npm', 'run', 'test:integration:m2c'],
          ['npm', 'run', 'test:integration:m2c', '--', '--upgrade'],
        ]
      : []),
    ...(m2c1
      ? [
          ['npm', 'run', 'test:integration:m2c1'],
          ['npm', 'run', 'test:integration:m2c1', '--', '--upgrade'],
        ]
      : []),
    [
      'make',
      m2c1
        ? 'verify-m2c1'
        : m2c
          ? 'verify-m2c'
          : m2b
            ? 'verify-m2b'
            : m2a
              ? 'verify-m2a'
              : 'verify-m1',
    ],
    [
      'make',
      m2c1
        ? 'verify-m2c1'
        : m2c
          ? 'verify-m2c'
          : m2b
            ? 'verify-m2b'
            : m2a
              ? 'verify-m2a'
              : 'verify-m1',
    ],
    ['make', 'verify'],
  ];
  const results: Record<string, unknown>[] = [];
  const runIds: string[] = [];
  let failed = false;
  for (const [index, args] of commands.entries()) {
    const executable = args[0];
    assert.ok(executable);
    const startedAt = new Date().toISOString();
    const result = await command(
      executable,
      args.slice(1),
      process.env,
      300_000,
      true,
    );
    const stem = String(index + 1).padStart(2, '0');
    await writeFile(join(directory, `${stem}.stdout.log`), result.stdout);
    await writeFile(join(directory, `${stem}.stderr.log`), result.stderr);
    const expectedExit = args.join(' ') === 'make verify' ? 2 : 0;
    const passed =
      result.code === expectedExit &&
      !result.signal &&
      !result.timedOut &&
      !result.outputOverflow &&
      result.cleanupErrors.length === 0;
    failed ||= !passed;
    results.push({
      command: args,
      startedAt,
      finishedAt: new Date().toISOString(),
      code: result.code,
      signal: result.signal,
      timedOut: result.timedOut,
      outputOverflow: result.outputOverflow,
      cleanupErrors: result.cleanupErrors,
      expectedExit,
      passed,
      stdout: `${stem}.stdout.log`,
      stderr: `${stem}.stderr.log`,
    });
    for (const match of result.stdout.matchAll(
      /(?:M1|M2A|M2B|M2C|M2C1) isolated run ((?:m1|m2a|m2b|m2c|m2c1)-[0-9]+-[a-f0-9]+)/g,
    )) {
      if (match[1]) runIds.push(match[1]);
    }
    await writeFile(
      join(directory, 'commands.json'),
      JSON.stringify(results, null, 2) + '\n',
    );
    console.log(
      `${args.join(' ')}: exit ${result.code}; ${passed ? 'expected' : 'FAIL'}`,
    );
  }
  const runs: Record<string, unknown>[] = [];
  for (const runId of runIds) {
    const isGuardedRun = runId.startsWith('m2c1-');
    const isCaptureRun = isGuardedRun || runId.startsWith('m2c-');
    const isCommandRun = runId.startsWith('m2a-');
    const isStagingRun = runId.startsWith('m2b-');
    const path = join(
      isGuardedRun
        ? 'artifacts/m2c1'
        : isCaptureRun
          ? 'artifacts/m2c'
          : isStagingRun
            ? 'artifacts/m2b'
            : isCommandRun
              ? 'artifacts/m2a'
              : 'artifacts/m1',
      runId,
    );
    const manifest = object(
      JSON.parse(await readFile(join(path, 'run.json'), 'utf8')),
    );
    assert.equal(manifest['head'], head);
    assert.equal(manifest['gitStatus'], '');
    assert.equal(manifest['developmental'], false);
    const localSummary = object(
      JSON.parse(await readFile(join(path, 'summary.json'), 'utf8')),
    );
    assert.equal(
      localSummary['milestone'],
      isGuardedRun
        ? 'M2C.1'
        : isCaptureRun
          ? 'M2C'
          : isStagingRun
            ? 'M2B'
            : isCommandRun
              ? 'M2A'
              : 'M1.1',
      'Run summary must identify the actual acceptance profile',
    );
    assert.equal(localSummary['runId'], runId);
    assert.equal(localSummary['head'], head);
    assert.deepEqual(localSummary['acceptance'], manifest['acceptance']);
    failed ||= manifest['status'] !== 'PASS';
    const sql = (await readFile(join(path, 'sql-evidence.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => object(JSON.parse(line)));
    const signals = sql
      .filter((line) =>
        [
          'T08-actual-signal',
          'T09-actual-signal',
          'C09-actual-signal',
          'C10-actual-signal',
          'S08-actual-signal',
          'S09-actual-signal',
          'IC07-actual-signal',
          'IC08-actual-signal',
          'IC09-actual-signal',
          'IC10-actual-signal',
        ].includes(String(line['test'])),
      )
      .map((line) => {
        const data = object(line['data']);
        assert.deepEqual(data['actualExit'], { code: null, signal: 'SIGKILL' });
        if (isCaptureRun) {
          assert.deepEqual(data['endedPipeline'], []);
          assert.deepEqual(data['endedSource'], []);
          assert.equal(data['ordinarySuccessBytes'], 0);
          const telemetry = object(data['telemetry']);
          return {
            case: line['test'],
            eventId: data['eventId'],
            commandKey: data['commandKey'],
            pid: data['childPid'],
            actualExit: data['actualExit'],
            ordinarySuccessBytes: 0,
            endedPipeline: [],
            endedSource: [],
            boundary: telemetry['boundary'],
            sourcePid: telemetry['sourcePid'],
            pipelinePid: telemetry['pipelinePid'],
            claims: telemetry['claims'],
            pipelineSessions: data['sessions'],
            sourceSessions: data['sourceSessions'],
            beforeSource: data['beforeSource'],
            afterSource: data['afterSource'],
            replay: object(data['replay'])['output'],
          };
        }
        if (isStagingRun) {
          assert.deepEqual(data['ended'], []);
          const barrier = sql.find(
            (row) =>
              row['test'] ===
              String(line['test']).replace(
                '-actual-signal',
                '-confirmed-barrier',
              ),
          );
          assert.ok(barrier);
          assert.equal(object(barrier['data'])['ordinarySuccessBytes'], 0);
          const session = object(data['session']);
          return {
            case: line['test'],
            commandKey: data['commandKey'],
            revision: data['revision'],
            eventId: data['eventId'],
            pid: data['childPid'],
            backendPid: session['pid'],
            transactionId: session['backend_xid'],
            actualExit: data['actualExit'],
            ordinarySuccessBytes: 0,
            endedSession: [],
            replay: object(data['replay'])['success'],
          };
        }
        assert.deepEqual(data['endedSession'], []);
        const isCommand = String(line['test']).startsWith('C');
        assert.equal(
          data[isCommand ? 'ordinarySuccessBytes' : 'callerSuccessBytes'],
          0,
        );
        return isCommand
          ? {
              case: line['test'],
              command: data['command'],
              writerPid: data['pid'],
              backendPid: data['backendPid'],
              transactionId: data['transactionId'],
              entityId: data['entityId'],
              entityVersion: data['entityVersion'],
              changeId: data['changeId'],
              actualExit: data['actualExit'],
              ordinarySuccessBytes: 0,
              endedSession: [],
              recovered: data['recovered'],
            }
          : data;
      });
    assert.equal(signals.length, isCommandRun || isCaptureRun ? 4 : 2);
    const restart = object(
      JSON.parse(await readFile(join(path, 'restart-evidence.json'), 'utf8')),
    );
    assert.deepEqual(restart['beforeRestart'], restart['afterRestart']);
    if (isStagingRun || isCaptureRun)
      assert.deepEqual(restart['pipelineBefore'], restart['pipelineAfter']);
    let captureEvidence;
    if (isCaptureRun) {
      const restartSource = object(restart['beforeRestart']);
      const persistedCapture = object(restartSource['capture']);
      const states = persistedCapture['state_counts'];
      assert.ok(Array.isArray(states));
      const counts = new Map<string, string>();
      for (const item of states) {
        assert.equal(typeof item, 'string');
        const [state, count] = String(item).split(':');
        assert.ok(
          (state === 'acknowledged' || state === 'blocked') &&
            count &&
            /^[0-9]+$/.test(count),
        );
        counts.set(state, count);
      }
      captureEvidence = {
        pipelineId: manifest['pipelineId'],
        acknowledged: counts.get('acknowledged') ?? '0',
        blocked: counts.get('blocked') ?? '0',
        sqlEvidenceSha256: createHash('sha256')
          .update(await readFile(join(path, 'sql-evidence.jsonl')))
          .digest('hex'),
        upgradeSha256: createHash('sha256')
          .update(await readFile(join(path, 'capture-upgrade.json')))
          .digest('hex'),
        observations: sql.filter((row) =>
          ['IC05-clock', 'IC11-PRE-renewal', 'IC13-identity'].includes(
            String(row['test']),
          ),
        ),
        reconciliationCases: sql
          .filter(
            (row) =>
              String(row['test']).endsWith('-reconciliation') ||
              ['IC02', 'IC03', 'IC15', 'IC16'].includes(String(row['test'])),
          )
          .map((row) => row['test']),
      };
    }
    let isolationEvidence;
    if (isGuardedRun) {
      const data = (id: string) => {
        const row = sql.find((r) => r['test'] === id);
        assert.ok(row, `Missing isolation SQL evidence: ${id}`);
        return object(row['data']);
      };
      const r04 = data('R04'),
        r07 = data('R07'),
        r08 = data('R08');
      const migration = object(r07['migration']);
      assert.deepEqual(migration['before'], migration['after']);
      assert.deepEqual(migration['catalogBefore'], migration['catalogAfter']);
      assert.ok(
        Array.isArray(r04['attempts']) && Array.isArray(r08['attempts']),
      );
      const registered = r07['mode'] === 'registered';
      const r01 = registered ? undefined : data('R01');
      const probe = r01 ? object(r01['probe']) : undefined;
      isolationEvidence = {
        mode: r07['mode'],
        oldSnapshot: r01?.['old'],
        registrationTransaction: r01?.['registration'],
        outboxLocksBeforeRegistration: probe?.['locks'],
        bindingVisibleInOldSnapshot: probe?.['visibility'],
        oldWriterOutcome: probe?.['rrAttempt'],
        readCommittedOutcome: probe?.['rcAttempt'],
        oldWriterPersistedState: r01?.['state'],
        missing: r01
          ? object(object(r01['capture'])['sourceState'])['missing']
          : undefined,
        rejectedOperations: r04['attempts'].map((raw: unknown) => {
          const row = object(raw),
            attempt = object(row['attempt']);
          return {
            transaction: row['transaction'],
            operation: attempt['operation'],
            failure: attempt['failure'],
            completion: attempt['completion'],
          };
        }),
        runtimeDenials: r08['attempts'].length,
        snapshotBeforeSha256: createHash('sha256')
          .update(JSON.stringify(migration['before']))
          .digest('hex'),
        snapshotAfterSha256: createHash('sha256')
          .update(JSON.stringify(migration['after']))
          .digest('hex'),
        catalogUnchanged: true,
        requiredCases: registered
          ? ['R04', 'R06', 'R07', 'R08']
          : ['R01', 'R02', 'R03', 'R04', 'R05', 'R06', 'R07', 'R08'],
      };
    }
    runs.push({
      runId,
      head,
      contentSha256: manifest['contentSha256'],
      status: manifest['status'],
      initialState: manifest['initialState'],
      node: manifest['node'],
      npm: manifest['npm'],
      docker: manifest['docker'],
      compose: manifest['compose'],
      imageReference: manifest['imageReference'],
      profile: manifest['profile'],
      migrationMode: manifest['migrationMode'],
      pipelineImage: manifest['pipelineImage'],
      captureEvidence,
      isolationEvidence,
      pipelinePostgres: manifest['pipelinePostgres'],
      acceptance: manifest['acceptance'],
      signals,
      cleanup: manifest['cleanup'],
      postgres: manifest['postgres'],
      epoch: object(restart['beforeRestart'])['epoch'],
      localArtifacts: path,
    });
  }
  assert.equal(
    runIds.length,
    m2c1 ? 24 : m2c ? 18 : m2b ? 12 : m2a ? 4 : 3,
    'Expected selected standalone integrations plus two fresh acceptance runs',
  );
  assert.equal(
    new Set(runIds).size,
    m2c1 ? 24 : m2c ? 18 : m2b ? 12 : m2a ? 4 : 3,
  );
  assert.equal(
    new Set(runs.map((run) => JSON.stringify(run['epoch']))).size,
    m2c1 ? 24 : m2c ? 18 : m2b ? 12 : m2a ? 4 : 3,
  );
  assert.equal((await checked('git', ['status', '--porcelain'])).trim(), '');
  await writeFile(
    join(directory, 'summary.json'),
    JSON.stringify(
      {
        format: 1,
        milestone,
        baseline,
        testedCode: head,
        status: failed ? 'FAIL' : 'PASS',
        clean: true,
        counterexample: m2c1
          ? object(
              JSON.parse(
                await readFile('docs/evidence/M2C.1-reproduction.json', 'utf8'),
              ),
            )
          : undefined,
        commands: results,
        runs,
        remoteCI: 'NOT RUN',
        fullGates: 'G1-G5 NOT IMPLEMENTED; make verify expected nonzero',
        evidenceLocation: 'Local artifacts only; paths are not public URLs',
      },
      null,
      2,
    ) + '\n',
  );
  process.exitCode = failed ? 1 : 0;
} else if (mode === 'summary') {
  const capture = process.argv[3];
  assert.ok(capture, 'Supply the acceptance capture directory');
  const data = object(
    JSON.parse(await readFile(join(capture, 'summary.json'), 'utf8')),
  );
  const compact =
    m2b || m2c
      ? {
          ...data,
          commands: Array.isArray(data['commands'])
            ? data['commands'].map((raw: unknown) => {
                const c = object(raw);
                return {
                  command: c['command'],
                  code: c['code'],
                  passed: c['passed'],
                };
              })
            : data['commands'],
          runs: Array.isArray(data['runs'])
            ? data['runs'].map((raw: unknown) => {
                const r = object(raw);
                return {
                  runId: r['runId'],
                  profile: r['profile'],
                  migrationMode: r['migrationMode'],
                  contentSha256: r['contentSha256'],
                  status: r['status'],
                  acceptance: r['acceptance'],
                  signals: r['signals'],
                  captureEvidence: r['captureEvidence'],
                  isolationEvidence: r['isolationEvidence'],
                  cleanup: r['cleanup'],
                  localArtifacts: r['localArtifacts'],
                };
              })
            : data['runs'],
        }
      : data;
  await writeFile(
    `docs/evidence/${milestone}-summary.json`,
    JSON.stringify(compact, null, 2) + '\n',
  );
  console.log(`docs/evidence/${milestone}-summary.json`);
} else if (mode === 'bundle') {
  const capture = process.argv[3];
  assert.ok(capture, 'Supply the acceptance capture directory');
  const acceptance = object(
    JSON.parse(await readFile(join(capture, 'summary.json'), 'utf8')),
  );
  assert.equal(acceptance['status'], 'PASS');
  const directory = resolve(
    'artifacts/review',
    `${profile}-final-${head.slice(0, 12)}`,
  );
  await mkdir(resolve('artifacts/review'), { recursive: true });
  await mkdir(directory, { recursive: false });
  await checked('git', [
    'archive',
    '--format=tar',
    `--output=${join(directory, 'tracked.tar')}`,
    head,
  ]);
  // Full review diffs are file-backed artifacts, not subprocess diagnostic output.
  // Keep command()'s bounded capture and overflow failure unchanged.
  await checked('git', [
    'diff',
    '--binary',
    `--output=${join(directory, 'since-reviewed.patch')}`,
    baseline,
    head,
  ]);
  await checked('git', [
    'diff',
    '--binary',
    `--output=${join(directory, 'since-spec.patch')}`,
    '251cb5a5b095c204dc99985b3eae1e21aac3c6ce',
    head,
  ]);
  await writeFile(
    join(directory, 'commits.txt'),
    await checked('git', [
      'log',
      '--reverse',
      '--format=fuller',
      `${baseline}..${head}`,
    ]),
  );
  await writeFile(
    join(directory, 'changed-files.txt'),
    await checked('git', ['diff', '--name-status', baseline, head]),
  );
  await cp(`artifacts/${profile}`, join(directory, profile), {
    recursive: true,
  });
  if (m2b || m2c)
    await cp('artifacts/m2a', join(directory, 'm2a'), { recursive: true });
  if (m2c)
    await cp('artifacts/m2b', join(directory, 'm2b'), { recursive: true });
  if (m2c1) {
    for (const extra of ['m2c', 'm2c.1', 'm2c1-repro'])
      await cp(`artifacts/${extra}`, join(directory, extra), {
        recursive: true,
      });
  }
  // Retain historical and developmental failures as well as the final acceptance runs.
  await cp('artifacts/m1', join(directory, 'm1'), { recursive: true });
  await writeFile(
    join(directory, 'manifest.json'),
    JSON.stringify(
      {
        baseline,
        testedCode: acceptance['testedCode'],
        finalDocumentationHead: head,
        clean: true,
        capture,
        remoteCI: 'NOT RUN',
        fullGates: 'NOT IMPLEMENTED',
      },
      null,
      2,
    ) + '\n',
  );
  const checksums: string[] = [];
  async function hashFiles(path: string, relative = ''): Promise<void> {
    for (const entry of (await readdir(path, { withFileTypes: true })).sort(
      (a, b) => a.name.localeCompare(b.name),
    )) {
      const file = join(path, entry.name);
      const name = join(relative, entry.name);
      if (entry.isDirectory()) await hashFiles(file, name);
      else {
        const digest = createHash('sha256')
          .update(await readFile(file))
          .digest('hex');
        checksums.push(`${digest}  ${name}`);
      }
    }
  }
  await hashFiles(directory);
  await writeFile(join(directory, 'SHA256SUMS'), checksums.join('\n') + '\n');
  console.log(directory);
} else {
  throw new Error('Expected capture, summary or bundle');
}
