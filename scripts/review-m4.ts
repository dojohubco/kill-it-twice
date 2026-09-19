// Local, reproducible M4 handoff. Full earlier profiles run once per complete gate.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile, cp, readdir, rm } from 'node:fs/promises';
import { join, resolve, basename, dirname } from 'node:path';
import { command, errorText } from './support.ts';
import { object } from './acceptance.ts';
const mode = process.argv[2];
const milestone = 'M4';
const baseline = '47e14920d8fa1b1bab154efe253ba85736b28c6f';
const artifactRoot = 'artifacts/m4';
const remoteCi =
  'M4 NOT RUN remotely. Prior M3.1 run 35445223962 observed successful; no M4 push or dispatch authorized.';
async function git(args: string[]) {
  const r = await command('git', args, process.env, 30000, true);
  assert.equal(r.code, 0, r.stderr);
  assert.equal(r.outputOverflow, false);
  assert.deepEqual(r.cleanupErrors, []);
  return r.stdout.trim();
}
const hash = (bytes: Uint8Array | string) =>
  createHash('sha256').update(bytes).digest('hex');
async function files(directory: string): Promise<string[]> {
  const result: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) result.push(...(await files(path)));
    else if (entry.isFile()) result.push(path);
    else throw new Error(`Unsupported bundle entry ${path}`);
  }
  return result.sort();
}
if (mode === 'capture') {
  assert.equal(
    await git(['status', '--porcelain']),
    '',
    'Final capture requires clean tracked and untracked code',
  );
  const head = await git(['rev-parse', 'HEAD']);
  const directory = resolve(
    `${artifactRoot}/review-${new Date().toISOString().replace(/[^0-9]/g, '')}`,
  );
  await mkdir(directory, { recursive: true });
  const manifest: Record<string, unknown> = {
    format: 1,
    baseline,
    testedCode: head,
    startedAt: new Date().toISOString(),
    status: 'RUNNING',
    commands: [],
    runs: [],
    remoteCi,
  };
  const commands: unknown[] = [];
  const runs: string[] = [];
  const save = () =>
    writeFile(
      join(directory, 'capture.json'),
      JSON.stringify({ ...manifest, commands, runs }, null, 2) + '\n',
    );
  console.log(`${milestone} capture: ${directory}`);
  await save();
  try {
    const paths = (await git(['ls-files', '-z'])).split('\0').filter(Boolean);
    const inputs = [];
    for (const path of paths)
      inputs.push({ path, sha256: hash(await readFile(path)) });
    await writeFile(
      join(directory, 'inputs.json'),
      JSON.stringify(inputs, null, 2) + '\n',
    );
    manifest['inputSha256'] = hash(JSON.stringify(inputs));
    for (const [name, executable, args, expected] of [
      ['npm-ci', 'npm', ['ci', '--no-audit', '--no-fund'], 0],
      ['verify-m4-first', 'make', ['verify-m4'], 0],
      ['verify-m4-repeat', 'make', ['verify-m4'], 0],
      ['full-verify', 'make', ['verify'], 2],
    ] as const) {
      const startedAt = new Date().toISOString();
      const r = await command(
        executable,
        [...args],
        process.env,
        3000000,
        true,
      );
      await writeFile(join(directory, `${name}.stdout.log`), r.stdout);
      await writeFile(join(directory, `${name}.stderr.log`), r.stderr);
      commands.push({
        name,
        executable,
        args,
        expected,
        code: r.code,
        signal: r.signal,
        timedOut: r.timedOut,
        outputOverflow: r.outputOverflow,
        cleanupErrors: r.cleanupErrors.map((e) => errorText(e)),
        startedAt,
        finishedAt: new Date().toISOString(),
        stdoutSha256: hash(r.stdout),
        stderrSha256: hash(r.stderr),
      });
      for (const match of r.stdout.matchAll(/^PASS: (.+\/run\.json)$/gm))
        if (match[1]) runs.push(match[1]);
      await save();
      assert.equal(r.code, expected, `${name} failed; see ${directory}`);
      assert.equal(r.signal, null);
      assert.equal(r.timedOut, false);
      assert.equal(r.outputOverflow, false);
      assert.deepEqual(r.cleanupErrors, []);
      assert.equal(await git(['status', '--porcelain']), '');
      assert.equal(await git(['rev-parse', 'HEAD']), head);
      console.log(`${name}: exit ${r.code}`);
    }
    assert.equal(
      runs.length,
      26,
      'Thirteen explicit profiles per gate, two complete gates',
    );
    assert.equal(
      new Set(runs).size,
      26,
      'Each profile uses fresh owned resources',
    );
    const executedProfiles: string[] = [];
    for (const path of runs) {
      const run = object(JSON.parse(await readFile(path, 'utf8')));
      executedProfiles.push(
        `${String(run['profile'])}:${String(run['migrationMode'])}`,
      );
      assert.equal(run['head'], head);
      assert.equal(run['developmental'], false);
      assert.equal(run['status'], 'PASS');
      assert.equal(object(run['cleanup'])['status'], 'PASS');
    }
    const expectedProfiles = [
      'm1:fresh',
      'm2a:fresh',
      'm2b:fresh',
      'm2b:populated M2A upgrade',
      'm2c:fresh',
      'm2c:populated M2B upgrade',
      'm2c1:fresh',
      'm2c1:populated registered M2C upgrade',
      'm3:fresh',
      'm3:populated guarded M2C.1 upgrade',
      'm3-oracle:fresh',
      'm4:fresh',
      'm4:populated M3.1 upgrade',
    ];
    assert.deepEqual(executedProfiles, [
      ...expectedProfiles,
      ...expectedProfiles,
    ]);
    manifest['status'] = 'PASS';
  } catch (error) {
    manifest['status'] = 'FAIL';
    manifest['error'] = errorText(error);
    process.exitCode = 1;
  } finally {
    manifest['finishedAt'] = new Date().toISOString();
    await save();
  }
  console.log(`${String(manifest['status'])}: ${directory}/capture.json`);
} else if (mode === 'summary' || mode === 'bundle') {
  const input = process.argv[3];
  assert.ok(input, 'Supply local capture directory');
  const directory = resolve(input);
  const captured = object(
    JSON.parse(await readFile(join(directory, 'capture.json'), 'utf8')),
  );
  assert.equal(captured['status'], 'PASS');
  assert.equal(captured['baseline'], baseline);
  const runs = captured['runs'];
  assert.ok(Array.isArray(runs));
  const profiles = [];
  for (const path of runs) {
    assert.equal(typeof path, 'string');
    const r = object(JSON.parse(await readFile(String(path), 'utf8')));
    profiles.push({
      runId: r['runId'],
      profile: r['profile'],
      mode: r['migrationMode'],
      status: r['status'],
      counts: Object.fromEntries(
        Object.entries(object(r['acceptance'])).filter(
          ([key]) => key !== 'cases',
        ),
      ),
      caseIds: (object(r['acceptance'])['cases'] as unknown[]).map(
        (c) => object(c)['id'],
      ),
      cleanup: r['cleanup'],
      elasticsearchCleanup: r['elasticsearchCleanup'],
      esEvidence: r['esEvidence']
        ? {
            actualBulk: object(r['esEvidence'])['actualBulk'],
            measuredOutageMs: object(r['esEvidence'])['measuredOutageMs'],
            faults: (object(r['esEvidence'])['faults'] as unknown[]).length,
          }
        : undefined,
      oracleEvidence: r['oracleEvidence'],
      rabbitEvidence: r['rabbitEvidence']
        ? {
            declaredMutationCount: object(r['rabbitEvidence'])[
              'declaredMutationCount'
            ],
            consumerEffects: object(r['rabbitEvidence'])['consumerEffects'],
            singlePublicationCrashCount: (
              object(r['rabbitEvidence'])[
                'singlePublicationCrashes'
              ] as unknown[]
            ).length,
            brokerStoppedMs: object(r['rabbitEvidence'])['brokerStoppedMs'],
            unknownConfirmEvent: object(r['rabbitEvidence'])[
              'unknownConfirmEvent'
            ],
          }
        : undefined,
      rabbitCleanup: r['rabbitCleanup'],
      prerequisite: r['prerequisite'],
      testExecution: r['testExecution'],
      evidenceErrors: r['evidenceErrors'],
      localRun: String(path),
      manifestSha256: hash(await readFile(String(path))),
    });
  }
  const summary = {
    format: 1,
    milestone,
    baseline,
    testedCode: captured['testedCode'],
    inputSha256: captured['inputSha256'],
    status: captured['status'],
    commands: captured['commands'],
    profiles,
    remoteCi,
    evidenceLocation: 'Local paths only; bundle is not published',
    incomplete: ['backfill', 'UI', 'full G1-G5'],
  };
  await writeFile(
    join(directory, 'summary.json'),
    JSON.stringify(summary, null, 2) + '\n',
  );
  if (mode === 'summary') {
    await writeFile(
      `docs/evidence/${milestone}-summary.json`,
      JSON.stringify(summary, null, 2) + '\n',
    );
    console.log(`Wrote compact tracked ${milestone} summary`);
  } else {
    assert.equal(
      await git(['status', '--porcelain']),
      '',
      'Bundle requires committed documentation',
    );
    const finalHead = await git(['rev-parse', 'HEAD']);
    const bundle = join(directory, 'bundle');
    await rm(bundle, { recursive: true, force: true });
    await mkdir(bundle, { recursive: true });
    for (const file of (await files(directory)).filter(
      (p) => !p.startsWith(bundle + '/'),
    ))
      if (dirname(file) === directory && /\.(log|json)$/.test(file))
        await cp(file, join(bundle, basename(file)));
    const baselineDir = 'artifacts/m4/baseline-47e1492';
    await cp(baselineDir, join(bundle, 'baseline'), { recursive: true });
    const baselineLog = await readFile(
      join(baselineDir, 'verify-m3.log'),
      'utf8',
    );
    for (const match of baselineLog.matchAll(/^PASS: (.+\/run\.json)$/gm))
      if (match[1])
        await cp(
          dirname(match[1]),
          join(bundle, 'baseline-runs', basename(dirname(match[1]))),
          { recursive: true },
        );
    const development = join(bundle, 'development');
    await mkdir(development, { recursive: true });
    for (const entry of await readdir(artifactRoot, { withFileTypes: true })) {
      const path = join(artifactRoot, entry.name);
      if (entry.isFile() && /\.(log|json)$/.test(entry.name))
        await cp(path, join(development, entry.name));
      else if (
        entry.isDirectory() &&
        /^(m4-2026|m4-protocol-|hosted-)/.test(entry.name) &&
        !runs.some((p) => dirname(String(p)) === resolve(path))
      )
        await cp(path, join(development, entry.name), { recursive: true });
    }
    for (const path of runs) {
      const dir = dirname(String(path));
      await cp(dir, join(bundle, 'runs', basename(dir)), { recursive: true });
    }
    await writeFile(
      join(bundle, 'chronology.txt'),
      await git([
        'log',
        '--reverse',
        '--format=%H %aI %an <%ae>%n%B',
        `${baseline}..${finalHead}`,
      ]),
    );
    await writeFile(
      join(bundle, 'changed-files.txt'),
      await git(['diff', '--name-status', baseline, finalHead]),
    );
    const diff = await command(
      'git',
      ['diff', '--binary', baseline, finalHead],
      process.env,
      30000,
      true,
      { maxOutputBytes: 16 * 1024 * 1024 },
    );
    assert.equal(diff.code, 0);
    assert.equal(diff.outputOverflow, false);
    await writeFile(join(bundle, 'full.diff'), diff.stdout);
    for (const [label, ref] of [
      ['tested-code', captured['testedCode']],
      ['final-documentation', finalHead],
    ]) {
      assert.equal(typeof ref, 'string');
      const r = await command(
        'git',
        [
          'archive',
          '--format=tar.gz',
          `--output=${join(bundle, `${String(label)}.tar.gz`)}`,
          String(ref),
        ],
        process.env,
        30000,
        true,
      );
      assert.equal(r.code, 0);
    }
    await writeFile(
      join(bundle, 'identity.json'),
      JSON.stringify(
        {
          baseline,
          testedCode: captured['testedCode'],
          finalDocumentationHead: finalHead,
          clean: true,
          remoteCi,
        },
        null,
        2,
      ) + '\n',
    );
    const sums = [];
    for (const path of await files(bundle))
      sums.push(
        `${hash(await readFile(path))}  ${path.slice(bundle.length + 1)}`,
      );
    await writeFile(join(bundle, 'SHA256SUMS'), sums.join('\n') + '\n');
    const archive = join(
      directory,
      `${milestone.toLowerCase()}-review-${finalHead.slice(0, 12)}.tar.gz`,
    );
    const packed = await command(
      'tar',
      ['-czf', archive, '-C', directory, 'bundle'],
      process.env,
      120000,
      true,
    );
    assert.equal(packed.code, 0);
    const digest = hash(await readFile(archive));
    await writeFile(`${archive}.sha256`, `${digest}  ${basename(archive)}\n`);
    console.log(`${archive}\nSHA-256 ${digest}`);
  }
} else throw new Error('Select capture, summary or bundle');
