import { first } from './rows.ts';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import pg from 'pg';
import { command, redact, withCleanup } from './support.ts';
import { migrateSource } from './migrate.ts';
import { checkAcceptance } from './acceptance.ts';
import { Diagnostics } from './finalization.ts';
import { CleanupFailure } from './support.ts';

const runId = `m1-${new Date().toISOString().replace(/[^0-9]/g, '')}-${randomBytes(4).toString('hex')}`;
const artifactDir = resolve('artifacts/m1', runId);
await mkdir(artifactDir, { recursive: true });
let temporaryDir: string | undefined;
const adminPassword = randomBytes(24).toString('hex');
const writerPassword = randomBytes(24).toString('hex');
const secrets = [adminPassword, writerPassword];
const env = { ...process.env, M1_PASSWORD_FILE: '' };
const compose = ['compose', '-p', runId, '-f', resolve('compose.m1.yaml')];
const manifest: Record<string, unknown> = {
  runId,
  artifactDir,
  startedAt: new Date().toISOString(),
  status: 'RUNNING',
  commands: [],
};
let sequence = 0;
let interrupted: NodeJS.Signals | undefined;
let cleaningUp = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const)
  process.once(signal, () => {
    interrupted = signal;
    console.error(
      `${signal}: finishing the bounded active command, then cleaning up owned resources`,
    );
  });
const clean = (text: string) => redact(text, secrets);
const diagnostics = new Diagnostics(clean);
async function record(
  name: string,
  executable: string,
  args: string[],
  runEnv = env,
  timeout = 90_000,
) {
  // Docker's CLI can launch a Compose plugin child too. Every command gets its
  // own group so a timeout cannot leave that command's descendants running.
  const result = await command(executable, args, runEnv, timeout, true, {
    secrets,
  });
  const base = `${String(++sequence).padStart(2, '0')}-${name}`;
  const logFailuresBefore = diagnostics.cleanup.length;
  for (const stream of ['stdout', 'stderr'] as const) {
    await diagnostics.finalize(`${base} ${stream}`, () =>
      writeFile(
        join(artifactDir, `${base}.${stream}.log`),
        clean(result[stream]),
      ),
    );
  }
  (manifest['commands'] as unknown[]).push({
    executable,
    args,
    code: result.code,
    signal: result.signal,
    timedOut: result.timedOut,
    outputOverflow: result.outputOverflow,
    cleanupErrors: result.cleanupErrors,
    stdout: `${base}.stdout.log`,
    stderr: `${base}.stderr.log`,
  });
  if (interrupted && !cleaningUp)
    throw new Error(`Run interrupted by ${interrupted}`);
  if (
    result.code !== 0 ||
    result.signal ||
    result.timedOut ||
    result.outputOverflow ||
    result.cleanupErrors.length
  )
    throw new Error(
      `${name} failed: ${JSON.stringify({ code: result.code, signal: result.signal, timedOut: result.timedOut })}\n${clean(result.stdout).slice(-8_000)}\n${clean(result.stderr)}`,
    );
  if (diagnostics.cleanup.length !== logFailuresBefore)
    throw new Error(`${name}: command evidence could not be retained`);
  return result.stdout.trim();
}
let started = false;
try {
  temporaryDir = await mkdtemp(join(tmpdir(), `${runId}-`));
  env.M1_PASSWORD_FILE = join(temporaryDir, 'postgres-password');
  await writeFile(env.M1_PASSWORD_FILE, adminPassword, { mode: 0o600 });
  assert.equal(
    process.versions.node,
    (await readFile('.node-version', 'utf8')).trim(),
  );
  manifest['head'] = await record('head', 'git', ['rev-parse', 'HEAD']);
  manifest['gitStatus'] = await record('git-status', 'git', [
    'status',
    '--porcelain',
  ]);
  manifest['developmental'] = manifest['gitStatus'] !== '';
  const paths = (
    await record('tracked-inputs', 'git', [
      'ls-files',
      '--cached',
      '--others',
      '--exclude-standard',
      '-z',
    ])
  )
    .split('\0')
    .filter(Boolean)
    .sort();
  const inputs = await Promise.all(
    paths.map(async (path) => ({
      path,
      sha256: createHash('sha256')
        .update(await readFile(path))
        .digest('hex'),
    })),
  );
  manifest['contentSha256'] = createHash('sha256')
    .update(JSON.stringify(inputs))
    .digest('hex');
  await writeFile(
    join(artifactDir, 'inputs.json'),
    JSON.stringify(inputs, null, 2),
  );
  await writeFile(
    join(artifactDir, 'worktree.patch'),
    await record('worktree-diff', 'git', ['diff', 'HEAD', '--binary']),
  );
  manifest['node'] = process.version;
  manifest['npm'] = await record('npm-version', 'npm', ['--version']);
  manifest['docker'] = await record('docker-version', 'docker', [
    'version',
    '--format',
    '{{.Server.Version}}',
  ]);
  manifest['compose'] = await record('compose-version', 'docker', [
    'compose',
    'version',
    '--short',
  ]);
  console.log(`M1 isolated run ${runId}; evidence: ${artifactDir}`);
  started = true;
  await record(
    'up',
    'docker',
    [...compose, 'up', '-d', '--wait', '--wait-timeout', '60'],
    env,
    120_000,
  );
  const containerId = await record('container', 'docker', [
    ...compose,
    'ps',
    '-q',
    'source',
  ]);
  manifest['containerId'] = containerId;
  manifest['image'] = JSON.parse(
    await record('image', 'docker', [
      'inspect',
      '--format',
      '{{json .Image}}',
      containerId,
    ]),
  );
  manifest['imageReference'] = await record('image-ref', 'docker', [
    'inspect',
    '--format',
    '{{.Config.Image}}',
    containerId,
  ]);
  const address = await record('port', 'docker', [
    ...compose,
    'port',
    'source',
    '5432',
  ]);
  assert.match(address, /^127\.0\.0\.1:\d+$/);
  let port = Number(address.split(':')[1]);
  manifest['port'] = port;
  const admin = new pg.Client({
    host: '127.0.0.1',
    port,
    database: 'source_m1',
    user: 'm1_admin',
    password: adminPassword,
    application_name: `${runId}:setup`,
    connectionTimeoutMillis: 5_000,
    query_timeout: 10_000,
  });
  try {
    await admin.connect();
    const settings = first(
      (
        await admin.query<{
          version: string;
          server_version: string;
          fsync: string;
          synchronous_commit: string;
          full_page_writes: string;
        }>(
          "SELECT version(), current_setting('server_version') AS server_version, current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes",
        )
      ).rows,
    );
    assert.match(settings.server_version, /^18\.6(?:\s|$)/);
    for (const name of [
      'fsync',
      'synchronous_commit',
      'full_page_writes',
    ] as const)
      assert.equal(settings[name], 'on');
    manifest['postgres'] = settings;
    await migrateSource(admin, writerPassword);
    manifest['migration'] = '001-source.sql committed';
  } catch (error) {
    try {
      await admin.end();
    } catch (cleanup) {
      throw new CleanupFailure(error, [cleanup]);
    }
    throw error;
  }
  await admin.end();
  const testEnv = {
    ...env,
    M1_RUN_ID: runId,
    M1_ARTIFACT_DIR: artifactDir,
    M1_PORT: String(port),
    M1_ADMIN_PASSWORD: adminPassword,
    M1_WRITER_PASSWORD: writerPassword,
  };
  const output = await record(
    'tests',
    process.execPath,
    [
      '--test',
      '--test-concurrency=1',
      '--test-timeout=60000',
      '--test-reporter=./scripts/test-reporter.ts',
      `--test-reporter-destination=${join(artifactDir, 'tests.json')}`,
      '--test-reporter=junit',
      `--test-reporter-destination=${join(artifactDir, 'tests.xml')}`,
      'tests/integration/*.test.ts',
    ],
    testEnv,
    180_000,
  );
  if (output) console.log(output);
  manifest['acceptance'] = checkAcceptance(
    await readFile(join(artifactDir, 'tests.json'), 'utf8'),
    { code: 0, signal: null, timedOut: false, outputOverflow: false },
  );
  async function retainedSnapshot() {
    const connection = new pg.Client({
      host: '127.0.0.1',
      port,
      database: 'source_m1',
      user: 'm1_admin',
      password: adminPassword,
      application_name: `${runId}:restart-observer`,
      connectionTimeoutMillis: 5_000,
      query_timeout: 10_000,
    });
    return withCleanup(
      async () => {
        await connection.connect();
        const sessions = (
          await connection.query<{
            pid: number;
            application_name: string;
            state: string;
          }>(
            "SELECT pid, application_name, state FROM pg_stat_activity WHERE usename='source_writer'",
          )
        ).rows;
        assert.deepEqual(sessions, [], 'runtime writer session leaked');
        const epoch = (
          await connection.query<{ source_epoch: string }>(
            'SELECT source_epoch::text FROM source.source_identity',
          )
        ).rows;
        const entities = (
          await connection.query<Record<string, unknown>>(
            'SELECT entity_id::text, source_epoch::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload FROM source.entities ORDER BY entity_id',
          )
        ).rows;
        const outbox = (
          await connection.query<Record<string, unknown>>(
            'SELECT allocation_id::text, entity_id::text, source_epoch::text, entity_version::text, change_id::text, recorded_at::text, is_deleted, payload::text AS payload FROM source.outbox ORDER BY allocation_id',
          )
        ).rows;
        return { epoch, entities, outbox, sessions };
      },
      () => connection.end(),
    );
  }
  const beforeRestart = await retainedSnapshot();
  await record('restart', 'docker', [
    ...compose,
    'restart',
    '--timeout',
    '10',
    'source',
  ]);
  await record('restart-ready', 'docker', [
    ...compose,
    'up',
    '-d',
    '--wait',
    '--wait-timeout',
    '60',
    'source',
  ]);
  const restartedAddress = await record('restart-port', 'docker', [
    ...compose,
    'port',
    'source',
    '5432',
  ]);
  assert.match(restartedAddress, /^127\.0\.0\.1:\d+$/);
  port = Number(restartedAddress.split(':')[1]);
  manifest['restartPort'] = port;
  const afterRestart = await retainedSnapshot();
  assert.deepEqual(
    afterRestart,
    beforeRestart,
    'source epoch or retained data changed across service restart',
  );
  assert.equal(
    await record('retained-container', 'docker', [
      ...compose,
      'ps',
      '-q',
      'source',
    ]),
    containerId,
  );
  await writeFile(
    join(artifactDir, 'restart-evidence.json'),
    JSON.stringify(
      {
        test: 'T10-retained-restart',
        beforeRestart,
        afterRestart,
        unchanged: true,
      },
      null,
      2,
    ) + '\n',
  );
  manifest['retainedRestart'] =
    'PASS: identical epoch, entities and immutable outbox; no runtime sessions';
  manifest['status'] = 'PASS';
} catch (error) {
  diagnostics.fail(error);
} finally {
  cleaningUp = true;
  if (started) {
    await diagnostics.finalize('postgres logs', () =>
      record('postgres-logs', 'docker', [
        ...compose,
        'logs',
        '--no-color',
        'source',
      ]),
    );
    await diagnostics.finalize('compose down', () =>
      record('down', 'docker', [
        ...compose,
        'down',
        '--volumes',
        '--timeout',
        '10',
      ]),
    );
    const resources: Record<string, string> = {};
    for (const kind of ['container', 'volume', 'network']) {
      await diagnostics.finalize(`remaining ${kind}`, async () => {
        resources[kind] = await record(`remaining-${kind}`, 'docker', [
          kind,
          'ls',
          ...(kind === 'container' ? ['-a'] : []),
          '-q',
          '--filter',
          `label=com.docker.compose.project=${runId}`,
        ]);
        assert.equal(resources[kind], '', `Owned ${kind} leaked`);
      });
    }
    manifest['cleanup'] = {
      status:
        Object.keys(resources).length === 3 &&
        Object.values(resources).every((value) => value === '')
          ? 'PASS'
          : 'FAIL',
      resources,
    };
  }
  await diagnostics.finalize('temporary credentials', async () => {
    if (temporaryDir) await rm(temporaryDir, { recursive: true, force: true });
  });
  for (const file of ['tests.xml', 'tests.json', 'sql-evidence.jsonl']) {
    await diagnostics.finalize(`sanitize ${file}`, async () => {
      const path = join(artifactDir, file);
      try {
        await writeFile(path, clean(await readFile(path, 'utf8')));
      } catch (error) {
        // Missing required evidence is a failure too. Never upload an unsanitized file.
        try {
          await rm(path, { force: true });
        } catch (cleanup) {
          throw new CleanupFailure(error, [cleanup]);
        }
        throw error;
      }
    });
  }
  if (interrupted) diagnostics.fail(new Error(`Interrupted by ${interrupted}`));
  if (
    !manifest['acceptance'] ||
    !manifest['retainedRestart'] ||
    !manifest['cleanup']
  )
    diagnostics.fail(
      new Error('Required acceptance, restart or cleanup evidence incomplete'),
    );
  manifest['finishedAt'] = new Date().toISOString();
  const updateStatus = () => {
    manifest['status'] = diagnostics.ok ? 'PASS' : 'FAIL';
    manifest['primaryError'] = diagnostics.primary;
    manifest['cleanupErrors'] = diagnostics.cleanup;
  };
  updateStatus();
  await diagnostics.finalize('acceptance summary', () =>
    writeFile(
      join(artifactDir, 'summary.json'),
      JSON.stringify(
        {
          format: 1,
          milestone: 'M1.1',
          runId,
          status: manifest['status'],
          head: manifest['head'],
          contentSha256: manifest['contentSha256'],
          developmental: manifest['developmental'],
          acceptance: manifest['acceptance'],
          cleanup: manifest['cleanup'],
          primaryError: diagnostics.primary,
          cleanupErrors: diagnostics.cleanup,
          evidenceLocation:
            'Local run directory or downloaded CI artifact; no public evidence URL',
        },
        null,
        2,
      ) + '\n',
    ),
  );
  updateStatus();
  await diagnostics.finalize('run manifest', () =>
    writeFile(
      join(artifactDir, 'run.json'),
      clean(JSON.stringify(manifest, null, 2)) + '\n',
    ),
  );
  await diagnostics.finalize('public evidence', async () => {
    const publicDir = join(artifactDir, 'public');
    await mkdir(publicDir, { recursive: true });
    const approved = new Set([
      'run.json',
      'summary.json',
      'tests.json',
      'tests.xml',
      'sql-evidence.jsonl',
      'restart-evidence.json',
    ]);
    for (const file of await readdir(artifactDir)) {
      if (
        approved.has(file) ||
        (/\.(stdout|stderr)\.log$/.test(file) &&
          !file.includes('worktree-diff') &&
          !file.includes('tracked-inputs'))
      ) {
        await writeFile(
          join(publicDir, file),
          clean(await readFile(join(artifactDir, file), 'utf8')),
        );
      }
    }
  });
  // A failed final write invalidates any earlier summary. Remove stale PASS evidence.
  if (!diagnostics.ok) {
    process.exitCode = 1;
    if (
      diagnostics.cleanup.some((error) =>
        ['acceptance summary', 'run manifest', 'public evidence'].includes(
          error.step,
        ),
      )
    ) {
      for (const file of ['summary.json', 'run.json', 'public']) {
        await diagnostics.finalize(`invalidate ${file}`, () =>
          rm(join(artifactDir, file), { recursive: true, force: true }),
        );
      }
    }
    console.error(
      JSON.stringify(
        { primary: diagnostics.primary, cleanup: diagnostics.cleanup },
        null,
        2,
      ),
    );
  }
  console.log(`${diagnostics.ok ? 'PASS' : 'FAIL'}: ${artifactDir}/run.json`);
}
