import { createServer } from 'node:net';
import {
  initializeCaptureFixture,
  captureSnapshot,
} from '../tests/support/capture-upgrade.ts';
import { captureCases } from './required-capture-cases.ts';
import { pipelineIdentity } from '../src/pipeline.ts';
import {
  reproductionCases,
  isolationCases,
  registrationCases,
} from './required-isolation-cases.ts';
import {
  migrateReader,
  migratePipeline,
  sourceSnapshot,
  pipelineSnapshot,
  migrateCapturePipeline,
  migrateCaptureSource,
} from './migrate-staging.ts';
import { stagingCases } from './required-staging-cases.ts';
import { Source } from '../src/source.ts';
import { randomUUID } from 'node:crypto';
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
import { requiredCases } from './required-cases.ts';
import { commandCases, commandFaultCases } from './required-command-cases.ts';
import { Diagnostics } from './finalization.ts';
import { CleanupFailure } from './support.ts';

const profile = process.argv[2] ?? 'm1';
assert.ok(
  ['m1', 'm2a', 'm2b', 'm2c', 'm2c1', 'm2c1-repro'].includes(profile),
  'Expected an explicitly supported acceptance or reproduction profile',
);
const reproduction = profile === 'm2c1-repro';
const guarded = profile === 'm2c1';
const captureProfile = profile === 'm2c' || guarded || reproduction;
const twoDatabases = profile === 'm2b' || captureProfile;
const upgrade = process.argv[3] === '--upgrade';
assert.ok(
  process.argv[3] === undefined || (twoDatabases && !reproduction && upgrade),
);
const inventory = reproduction
  ? reproductionCases
  : guarded
    ? [
        ...captureCases,
        ...isolationCases,
        ...(upgrade ? [] : registrationCases),
      ]
    : profile === 'm2c'
      ? captureCases
      : profile === 'm2b'
        ? stagingCases
        : profile === 'm2a'
          ? [...requiredCases, ...commandCases, ...commandFaultCases]
          : requiredCases;
const runId = `${profile}-${new Date().toISOString().replace(/[^0-9]/g, '')}-${randomBytes(4).toString('hex')}`;
const artifactDir = resolve(`artifacts/${profile}`, runId);
await mkdir(artifactDir, { recursive: true });
let temporaryDir: string | undefined;
const adminPassword = randomBytes(24).toString('hex');
const writerPassword = randomBytes(24).toString('hex');
const commandPassword = randomBytes(24).toString('hex');
const readerPassword = randomBytes(24).toString('hex');
const pipelinePassword = randomBytes(24).toString('hex');
const stagerPassword = randomBytes(24).toString('hex');
const capturePassword = randomBytes(24).toString('hex');
const pipelineCapturePassword = randomBytes(24).toString('hex');
let pipelineId = '';
const secrets = [
  adminPassword,
  writerPassword,
  commandPassword,
  readerPassword,
  pipelinePassword,
  stagerPassword,
  capturePassword,
  pipelineCapturePassword,
];
const env = {
  ...process.env,
  M1_PASSWORD_FILE: '',
  M2B_PASSWORD_FILE: '',
  M2C_PIPELINE_PORT: '0',
};
if (captureProfile) {
  const reservation = createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once('error', reject);
    reservation.listen(0, '127.0.0.1', resolve);
  });
  const address = reservation.address();
  assert.ok(address && typeof address !== 'string');
  env.M2C_PIPELINE_PORT = String(address.port);
  await new Promise<void>((resolve, reject) =>
    reservation.close((error) => (error ? reject(error) : resolve())),
  );
}

const compose = ['compose', '-p', runId, '-f', resolve('compose.m1.yaml')];
if (twoDatabases) compose.push('-f', resolve('compose.m2b.yaml'));
let pipelinePort = 0;
let sourceEpoch = '';
function pipelineAdmin(label: string) {
  return new pg.Client({
    host: '127.0.0.1',
    port: pipelinePort,
    database: 'pipeline_m2b',
    user: 'pipeline_admin',
    password: pipelinePassword,
    application_name: `${runId}:${label}`,
    connectionTimeoutMillis: 5000,
    query_timeout: 10000,
  });
}
const manifest: Record<string, unknown> = {
  runId,
  profile,
  migrationMode: upgrade
    ? guarded
      ? 'populated registered M2C upgrade'
      : profile === 'm2c'
        ? 'populated M2B upgrade'
        : 'populated M2A upgrade'
    : 'fresh',
  artifactDir,
  startedAt: new Date().toISOString(),
  status: 'RUNNING',
  commands: [],
  intent: reproduction
    ? 'Historical defect reproduction only; intentionally missing work is not acceptance'
    : 'Acceptance',
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
  if (twoDatabases) {
    env.M2B_PASSWORD_FILE = join(temporaryDir, 'pipeline-password');
    await writeFile(env.M2B_PASSWORD_FILE, pipelinePassword, { mode: 0o600 });
  }
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
  console.log(
    `${profile.toUpperCase()} isolated run ${runId}; evidence: ${artifactDir}`,
  );
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
  if (twoDatabases) {
    const pAddress = await record('pipeline-port', 'docker', [
      ...compose,
      'port',
      'pipeline',
      '5432',
    ]);
    assert.match(pAddress, /^127\.0\.0\.1:\d+$/);
    pipelinePort = Number(pAddress.split(':')[1]);
    manifest['pipelinePort'] = pipelinePort;
    const pContainer = await record('pipeline-container', 'docker', [
      ...compose,
      'ps',
      '-q',
      'pipeline',
    ]);
    manifest['pipelineContainer'] = pContainer;
    manifest['pipelineImage'] = await record('pipeline-image', 'docker', [
      'inspect',
      '--format',
      '{{.Image}} {{.Config.Image}}',
      pContainer,
    ]);
  }
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
    await migrateSource(
      admin,
      writerPassword,
      profile !== 'm1' ? commandPassword : undefined,
    );
    manifest['migration'] =
      profile !== 'm1'
        ? '001-source.sql and forward 002-source-commands.sql committed'
        : '001-source.sql committed';
    const initial = first(
      (
        await admin.query<Record<string, unknown>>(`SELECT source_epoch::text,
      (SELECT count(*)::text FROM source.entities) AS entities,
      (SELECT count(*)::text FROM source.outbox) AS outbox
      ${profile !== 'm1' ? ', (SELECT count(*)::text FROM source.command_receipts) AS receipts' : ''}
      FROM source.source_identity`)
      ).rows,
    );
    assert.equal(initial['entities'], '0');
    assert.equal(initial['outbox'], '0');
    if (profile !== 'm1') assert.equal(initial['receipts'], '0');
    manifest['initialState'] = initial;
    assert.equal(typeof initial['source_epoch'], 'string');
    sourceEpoch = String(initial['source_epoch']);
    await writeFile(
      join(artifactDir, 'sql-evidence.jsonl'),
      JSON.stringify({
        test: 'T10-fresh-start',
        runId,
        at: new Date().toISOString(),
        data: initial,
      }) + '\n',
    );
    if (twoDatabases) {
      if (upgrade) {
        const owner = new Source({
          host: '127.0.0.1',
          port,
          database: 'source_m1',
          user: 'source_command',
          password: commandPassword,
          application_name: `${runId}:upgrade-fixture`,
        });
        const common = { sourceEpoch, contractVersion: 1 };
        const created = await owner.command({
          ...common,
          commandId: randomUUID(),
          operation: 'create',
          entityId: null,
          payloadJson: '{"upgrade": 9007199254740993}',
        });
        for (const payloadJson of [
          '{"upgrade": 9007199254740994}',
          '{ "upgrade":9007199254740994.0 }',
        ])
          await owner.command({
            ...common,
            commandId: randomUUID(),
            operation: 'update',
            entityId: created.result.entity_id,
            payloadJson,
          });
      }
      const before = await sourceSnapshot(admin);
      await migrateReader(admin, readerPassword);
      const after = await sourceSnapshot(admin);
      assert.deepEqual(after, before);
      await writeFile(
        join(artifactDir, 'upgrade-evidence.json'),
        JSON.stringify(
          {
            mode: upgrade ? 'populated' : 'fresh',
            before,
            after,
            unchanged: true,
          },
          null,
          2,
        ) + '\n',
      );
      manifest['readerMigration'] =
        '003-source-reader.sql committed; source/receipt snapshot unchanged';
      const pAdmin = pipelineAdmin('pipeline-setup');
      await withCleanup(
        async () => {
          await pAdmin.connect();
          const settings = (
            await pAdmin.query(
              "SELECT version(),current_setting('fsync') AS fsync,current_setting('synchronous_commit') AS synchronous_commit,current_setting('full_page_writes') AS full_page_writes,current_setting('server_encoding') AS server_encoding",
            )
          ).rows as Record<string, unknown>[];
          const setting = first(settings);
          for (const key of ['fsync', 'synchronous_commit', 'full_page_writes'])
            assert.equal(setting[key], 'on');
          assert.equal(setting['server_encoding'], 'UTF8');
          manifest['pipelinePostgres'] = setting;
          await migratePipeline(pAdmin, stagerPassword, sourceEpoch);
          manifest['initialPipeline'] = await pipelineSnapshot(pAdmin);
          if (reproduction) {
            await migrateCapturePipeline(pAdmin, pipelineCapturePassword);
            await migrateCaptureSource(admin, capturePassword);
            const identity = await pipelineIdentity({
              host: '127.0.0.1',
              port: pipelinePort,
              database: 'pipeline_m2b',
              user: 'pipeline_capture',
              password: pipelineCapturePassword,
              application_name: `${runId}:identity`,
            });
            pipelineId = identity.pipelineId;
            manifest['pipelineId'] = pipelineId;
          } else if (captureProfile) {
            const result = await initializeCaptureFixture(admin, pAdmin, {
              source: {
                host: '127.0.0.1',
                port,
                database: 'source_m1',
                user: 'm1_admin',
                password: adminPassword,
                application_name: `${runId}:capture-setup`,
              },
              pipeline: {
                host: '127.0.0.1',
                port: pipelinePort,
                database: 'pipeline_m2b',
                user: 'pipeline_admin',
                password: pipelinePassword,
                application_name: `${runId}:capture-setup`,
              },
              writerPassword,
              ...(guarded
                ? {
                    isolation: upgrade
                      ? ('registered' as const)
                      : ('unregistered' as const),
                  }
                : {}),
              commandPassword,
              stagerPassword,
              capturePassword,
              pipelineCapturePassword,
              epoch: sourceEpoch,
              upgrade,
            });
            pipelineId = result.pipelineId;
            manifest['pipelineId'] = pipelineId;
            await writeFile(
              join(artifactDir, 'capture-upgrade.json'),
              JSON.stringify(result, null, 2) + '\n',
            );
          }
        },
        () => pAdmin.end(),
      );
    }
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
    M2A_COMMAND_PASSWORD: commandPassword,
    SOURCE_PROFILE: profile,
    SOURCE_EPOCH: sourceEpoch,
    SOURCE_READER_PASSWORD: readerPassword,
    PIPELINE_PORT: String(pipelinePort),
    PIPELINE_ADMIN_PASSWORD: pipelinePassword,
    PIPELINE_STAGER_PASSWORD: stagerPassword,
    SOURCE_CAPTURE_PASSWORD: capturePassword,
    PIPELINE_CAPTURE_PASSWORD: pipelineCapturePassword,
    PIPELINE_ID: pipelineId,
    M2C_PIPELINE_CONTAINER:
      typeof manifest['pipelineContainer'] === 'string'
        ? manifest['pipelineContainer']
        : '',
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
      ...new Set(inventory.map((entry) => entry.file)),
    ],
    testEnv,
    captureProfile ? 240_000 : 180_000,
  );
  if (output) console.log(output);
  manifest['acceptance'] = checkAcceptance(
    await readFile(join(artifactDir, 'tests.json'), 'utf8'),
    { code: 0, signal: null, timedOut: false, outputOverflow: false },
    inventory,
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
            "SELECT pid, application_name, state FROM pg_stat_activity WHERE usename IN ('source_writer','source_command','source_reader','source_capture')",
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
        const receipts =
          profile !== 'm1'
            ? (
                await connection.query<Record<string, unknown>>(
                  'SELECT source_epoch::text, command_id::text, contract_version, operation, target_id::text, request_payload::text, completed, result_entity_id::text, result_version::text, result_change_id::text, result_recorded_at::text, result_deleted, result_payload::text FROM source.command_receipts ORDER BY source_epoch,command_id',
                )
              ).rows
            : [];
        for (const receipt of receipts)
          assert.equal(receipt['completed'], true);
        const capture = captureProfile
          ? await captureSnapshot(connection)
          : undefined;
        return { epoch, entities, outbox, receipts, sessions, capture };
      },
      () => connection.end(),
    );
  }
  async function retainedPipeline() {
    const c = pipelineAdmin('restart-pipeline');
    return withCleanup(
      async () => {
        await c.connect();
        return pipelineSnapshot(c);
      },
      () => c.end(),
    );
  }
  const pipelineBefore = twoDatabases ? await retainedPipeline() : undefined;
  const beforeRestart = await retainedSnapshot();
  await record('restart', 'docker', [
    ...compose,
    'restart',
    '--timeout',
    '10',
    'source',
    ...(twoDatabases ? ['pipeline'] : []),
  ]);
  await record('restart-ready', 'docker', [
    ...compose,
    'up',
    '-d',
    '--wait',
    '--wait-timeout',
    '60',
    'source',
    ...(twoDatabases ? ['pipeline'] : []),
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
  let pipelineAfter;
  if (twoDatabases) {
    const address = await record('pipeline-restart-port', 'docker', [
      ...compose,
      'port',
      'pipeline',
      '5432',
    ]);
    assert.match(address, /^127\.0\.0\.1:\d+$/);
    pipelinePort = Number(address.split(':')[1]);
    pipelineAfter = await retainedPipeline();
    assert.deepEqual(
      pipelineAfter,
      pipelineBefore,
      'Pipeline data changed across retained-volume restart',
    );
    assert.equal(
      await record('retained-pipeline', 'docker', [
        ...compose,
        'ps',
        '-q',
        'pipeline',
      ]),
      manifest['pipelineContainer'],
    );
  }
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
        pipelineBefore,
        pipelineAfter,
        unchanged: true,
      },
      null,
      2,
    ) + '\n',
  );
  manifest['retainedRestart'] =
    'PASS: identical epoch, entities, immutable outbox and command receipts; no runtime sessions';
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
        ...(twoDatabases ? ['pipeline'] : []),
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
          milestone: guarded
            ? 'M2C.1'
            : profile === 'm1'
              ? 'M1.1'
              : profile.toUpperCase(),
          migrationMode: manifest['migrationMode'],
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
      'upgrade-evidence.json',
      'capture-upgrade.json',
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
