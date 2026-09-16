// Reproducible local handoff capture and bundle generation; never publishes anything.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { cp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { command } from './support.ts';
import { object } from './acceptance.ts';

const m2b = process.argv.includes('--m2b');
const m2a = process.argv.includes('--m2a');
const milestone = m2b ? 'M2B' : m2a ? 'M2A' : 'M1.1';
const profile = m2b ? 'm2b' : m2a ? 'm2a' : 'm1.1';
const baseline = m2b
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
    ...(m2a || m2b ? [['npm', 'run', 'test:integration:m2a']] : []),
    ...(m2b
      ? [
          ['npm', 'run', 'test:integration:m2b'],
          ['npm', 'run', 'test:integration:m2b', '--', '--upgrade'],
        ]
      : []),
    ['make', m2b ? 'verify-m2b' : m2a ? 'verify-m2a' : 'verify-m1'],
    ['make', m2b ? 'verify-m2b' : m2a ? 'verify-m2a' : 'verify-m1'],
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
      /(?:M1|M2A|M2B) isolated run ((?:m1|m2a|m2b)-[0-9]+-[a-f0-9]+)/g,
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
    const isCommandRun = runId.startsWith('m2a-');
    const isStagingRun = runId.startsWith('m2b-');
    const path = join(
      isStagingRun
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
        ].includes(String(line['test'])),
      )
      .map((line) => {
        const data = object(line['data']);
        assert.deepEqual(data['actualExit'], { code: null, signal: 'SIGKILL' });
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
    assert.equal(signals.length, isCommandRun ? 4 : 2);
    const restart = object(
      JSON.parse(await readFile(join(path, 'restart-evidence.json'), 'utf8')),
    );
    assert.deepEqual(restart['beforeRestart'], restart['afterRestart']);
    if (isStagingRun)
      assert.deepEqual(restart['pipelineBefore'], restart['pipelineAfter']);
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
    m2b ? 12 : m2a ? 4 : 3,
    'Expected selected standalone integrations plus two fresh acceptance runs',
  );
  assert.equal(new Set(runIds).size, m2b ? 12 : m2a ? 4 : 3);
  assert.equal(
    new Set(runs.map((run) => JSON.stringify(run['epoch']))).size,
    m2b ? 12 : m2a ? 4 : 3,
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
  const compact = m2b
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
  await writeFile(
    join(directory, 'since-reviewed.patch'),
    await checked('git', ['diff', '--binary', baseline, head]),
  );
  await writeFile(
    join(directory, 'since-spec.patch'),
    await checked('git', [
      'diff',
      '--binary',
      '251cb5a5b095c204dc99985b3eae1e21aac3c6ce',
      head,
    ]),
  );
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
  if (m2b)
    await cp('artifacts/m2a', join(directory, 'm2a'), { recursive: true });
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
