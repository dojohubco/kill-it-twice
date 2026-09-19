import { Capture } from '../src/capture.ts';
import { startRabbit } from './rabbit-service.ts';
import {
  migrateRabbit,
  prepareRabbit,
  setupTopology,
  initializeConsumer,
  migrateConsumerBatch,
  bindRabbit,
  rabbitSnapshot,
  consumerSnapshot,
} from './rabbit-setup.ts';
import { batchReproductionCases, batchCases } from './required-batch-cases.ts';
import { checkBatchEvidence } from './batch-evidence.ts';
import { rabbitCases } from './required-rabbit-cases.ts';
import { EsTransport } from '../src/es/transport.ts';
import { EsLedger } from '../src/es/ledger.ts';
import { EsAdapter } from '../src/es/adapter.ts';
import { Delivery } from '../src/es/worker.ts';
import {
  observePrerequisite,
  requirePrerequisite,
  retainPrerequisite,
} from './es-prerequisite.ts';
import {
  sanitizeStageEvidence,
  MissingEvidenceError,
} from './stage-evidence.ts';
import {
  oracleReproductionCases,
  oracleCases,
  expectationInventoryCase,
} from './required-oracle-cases.ts';
import { checkRabbitEvidence } from './rabbit-evidence.ts';
import { checkEsEvidence, checkOracleEvidence } from './es-evidence.ts';
import { startEs } from './es-service.ts';
import { migrateEs, registerEs, esSnapshot } from './es-setup.ts';
import { esCases } from './required-es-cases.ts';
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
  migrateCaptureIsolation,
  registerCapture,
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
  [
    'm1',
    'm2a',
    'm2b',
    'm2c',
    'm2c1',
    'm2c1-repro',
    'm3',
    'm3-repro',
    'm3-oracle',
    'm4',
    'm41-repro',
    'm41',
  ].includes(profile),
  'Expected an explicitly supported acceptance or reproduction profile',
);
const oracleReproduction = profile === 'm3-repro';
const oracleAcceptance = profile === 'm3-oracle';
const batchReproduction = profile === 'm41-repro';
const batchProfile = profile === 'm41';
const rabbitProfile = profile === 'm4' || batchReproduction || batchProfile;
const esProfile =
  profile === 'm3' || oracleReproduction || oracleAcceptance || rabbitProfile;
let rabbitService: Awaited<ReturnType<typeof startRabbit>> | undefined;
let rabbitTarget: Awaited<ReturnType<typeof prepareRabbit>> | undefined;
let rabbitCredentials: Awaited<ReturnType<typeof setupTopology>> | undefined;
const mqPasswords = Array.from({ length: 4 }, () =>
  randomBytes(24).toString('hex'),
);
const [
  publisherSqlPassword = '',
  receiptSqlPassword = '',
  consumerSqlPassword = '',
  consumerReaderPassword = '',
] = mqPasswords;
let esService: Awaited<ReturnType<typeof startEs>> | undefined;
let receiver: Awaited<ReturnType<typeof registerEs>> | undefined;
const esPassword = randomBytes(24).toString('hex');
const reproduction = profile === 'm2c1-repro';
const guarded = profile === 'm2c1';
const captureProfile =
  profile === 'm2c' || guarded || reproduction || esProfile;
const twoDatabases = profile === 'm2b' || captureProfile;
const upgrade = process.argv[3] === '--upgrade';
assert.ok(
  process.argv[3] === undefined || (twoDatabases && !reproduction && upgrade),
);
const inventory = batchReproduction
  ? batchReproductionCases
  : batchProfile
    ? batchCases
    : rabbitProfile
      ? rabbitCases
      : oracleAcceptance
        ? oracleCases
        : oracleReproduction
          ? oracleReproductionCases
          : esProfile
            ? [...esCases, expectationInventoryCase]
            : reproduction
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
                      ? [
                          ...requiredCases,
                          ...commandCases,
                          ...commandFaultCases,
                        ]
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
  esPassword,
  adminPassword,
  writerPassword,
  commandPassword,
  readerPassword,
  pipelinePassword,
  stagerPassword,
  capturePassword,
  pipelineCapturePassword,
  ...mqPasswords,
];
const env = {
  ...process.env,
  M1_PASSWORD_FILE: '',
  M2B_PASSWORD_FILE: '',
  M2C_PIPELINE_PORT: '0',
};
async function reservePipelinePort() {
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
    ? batchProfile
      ? 'populated M4 consumer upgrade'
      : rabbitProfile
        ? 'populated M3.1 upgrade'
        : esProfile
          ? 'populated guarded M2C.1 upgrade'
          : guarded
            ? 'populated registered M2C upgrade'
            : profile === 'm2c'
              ? 'populated M2B upgrade'
              : 'populated M2A upgrade'
    : 'fresh',
  artifactDir,
  startedAt: new Date().toISOString(),
  status: 'RUNNING',
  commands: [],
  intent: batchReproduction
    ? 'Historical byte-accounting diagnostic: expected rejection is not successful consumer processing'
    : oracleReproduction
      ? 'Historical M3 oracle counterexample only; expected assertion failure is not M3 acceptance'
      : reproduction
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
let testsAttempted = false;
let sqlEvidenceAttempted = false;
const evidenceErrors: string[] = [];
try {
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
  if (esProfile) {
    const prerequisite = await observePrerequisite(
      'standalone-m3-before-services',
    );
    manifest['prerequisite'] = prerequisite;
    await retainPrerequisite(prerequisite, artifactDir);
    requirePrerequisite(prerequisite);
  }
  await reservePipelinePort();
  temporaryDir = await mkdtemp(join(tmpdir(), `${runId}-`));
  env.M1_PASSWORD_FILE = join(temporaryDir, 'postgres-password');
  await writeFile(env.M1_PASSWORD_FILE, adminPassword, { mode: 0o600 });
  if (twoDatabases) {
    env.M2B_PASSWORD_FILE = join(temporaryDir, 'pipeline-password');
    await writeFile(env.M2B_PASSWORD_FILE, pipelinePassword, { mode: 0o600 });
  }
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
    sqlEvidenceAttempted = true;
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
      if (upgrade && !rabbitProfile) {
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
          } else if (rabbitProfile) {
            await migrateCapturePipeline(pAdmin, pipelineCapturePassword);
            await migrateCaptureSource(admin, capturePassword);
            await migrateCaptureIsolation(admin);
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
            await registerCapture(admin, pipelineId, sourceEpoch);
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
              ...(guarded || esProfile
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
  if (esProfile) {
    esService = await startEs(`${runId}-es`);
    const runningEs = esService;
    manifest['elasticsearchPrerequisite'] = runningEs.prerequisite;
    secrets.push(runningEs.config.password);
    for (const service of ['elasticsearch', 'toxiproxy']) {
      const id = (await runningEs.compose(['ps', '-q', service])).trim();
      manifest[`${service}Container`] = id;
      manifest[`${service}Image`] = await record(`${service}-image`, 'docker', [
        'inspect',
        '--format',
        '{{.Image}} {{.Config.Image}}',
        id,
      ]);
    }
    const p = pipelineAdmin('es-setup');
    await withCleanup(
      async () => {
        await p.connect();
        const sourceEvidence = async () => {
          const c = new pg.Client({
            host: '127.0.0.1',
            port,
            database: 'source_m1',
            user: 'm1_admin',
            password: adminPassword,
            application_name: `${runId}:es-upgrade-source`,
            connectionTimeoutMillis: 5000,
            query_timeout: 10000,
          });
          return withCleanup(
            async () => {
              await c.connect();
              return {
                source: await sourceSnapshot(c),
                capture: await captureSnapshot(c),
              };
            },
            () => c.end(),
          );
        };
        const sourceBeforeEs = await sourceEvidence();
        const before = await pipelineSnapshot(p);
        await migrateEs(p, esPassword);
        const sourceAfterEs = await sourceEvidence();
        assert.deepEqual(sourceAfterEs, sourceBeforeEs);
        const after = await pipelineSnapshot(p);
        for (const name of [
          'source_binding',
          'destinations',
          'events',
          'consumer_observations',
          'integrity_incidents',
        ])
          assert.deepEqual(after[name], before[name]);
        const original = (
          await p.query<{ text: string }>(
            `SELECT row_to_json(t)::text text FROM (SELECT event_id,kind,destination_id,state,created_at FROM pipeline.delivery_intents) t ORDER BY row_to_json(t)::text COLLATE "C"`,
          )
        ).rows.map((r) => r.text);
        assert.deepEqual(original, before['delivery_intents']);
        receiver = await registerEs(
          p,
          runningEs.client,
          pipelineId,
          sourceEpoch,
          runningEs.provisionRuntime,
        );
        secrets.push(receiver.password);
        await writeFile(
          join(artifactDir, 'es-upgrade.json'),
          JSON.stringify(
            {
              before,
              after,
              sourceBeforeEs,
              sourceAfterEs,
              original,
              receiver: {
                index: receiver.index,
                indexUuid: receiver.indexUuid,
                clusterUuid: receiver.clusterUuid,
                destinationId: receiver.destinationId,
              },
            },
            null,
            2,
          ) + '\n',
        );
      },
      () => p.end(),
    );
  }
  if (rabbitProfile) {
    assert.ok(esService && receiver);
    rabbitService = await startRabbit(`${runId}-rabbit`);
    secrets.push(rabbitService.password);
    manifest['rabbitmq'] = {
      version: rabbitService.info['rabbitmq_version'],
      listeners: rabbitService.info['listeners'],
      container: await rabbitService.compose(['ps', '-q', 'rabbitmq']),
      image: await rabbitService.compose(['images', '--format', 'json']),
    };
    const p = pipelineAdmin('rabbit-setup');
    await withCleanup(
      async () => {
        await p.connect();
        const pConfig = {
          host: '127.0.0.1',
          port: pipelinePort,
          database: 'pipeline_m2b',
          user: 'pipeline_admin',
          password: pipelinePassword,
          application_name: `${runId}:rabbit-upgrade`,
        };
        assert.ok(esService && receiver && rabbitService);
        if (upgrade) {
          const sourceConfig = {
            host: '127.0.0.1',
            port,
            database: 'source_m1',
            user: 'source_command',
            password: commandPassword,
            application_name: `${runId}:m4-upgrade-command`,
          };
          const source = new Source(sourceConfig);
          const journal = [];
          const create = {
            sourceEpoch,
            contractVersion: 1,
            commandId: randomUUID(),
            operation: 'create' as const,
            entityId: null,
            payloadJson:
              '{"name":"M4 upgrade","country":"GE","loyalty_points":1}',
          };
          const created = await source.command(create);
          journal.push({ command: create, reply: created });
          const update = {
            ...create,
            commandId: randomUUID(),
            operation: 'update' as const,
            entityId: created.result.entity_id,
            payloadJson:
              '{"name":"M4 upgrade changed","country":"GE","loyalty_points":2}',
          };
          journal.push({
            command: update,
            reply: await source.command(update),
          });
          await writeFile(
            join(artifactDir, 'm4-initial-journal.json'),
            JSON.stringify(journal, null, 2) + '\n',
          );
          const capture = new Capture({
            source: {
              ...sourceConfig,
              user: 'source_capture',
              password: capturePassword,
            },
            pipeline: {
              ...pConfig,
              user: 'pipeline_capture',
              password: pipelineCapturePassword,
            },
            binding: { sourceEpoch, pipelineId },
          });
          await capture.captureOnce();
          const transport = new EsTransport({
            ...esService.config,
            username: receiver.username,
            password: receiver.password,
          });
          await withCleanup(
            async () => {
              const worker = new Delivery(
                new EsLedger({
                  ...pConfig,
                  user: 'pipeline_es',
                  password: esPassword,
                }),
                new EsAdapter(transport),
              );
              await worker.once();
            },
            () => transport.close(),
          );
        }
        const sourceEvidence = async () => {
          const db = new pg.Client({
            host: '127.0.0.1',
            port,
            database: 'source_m1',
            user: 'm1_admin',
            password: adminPassword,
            connectionTimeoutMillis: 5000,
            query_timeout: 10000,
          });
          return withCleanup(
            async () => {
              await db.connect();
              return {
                ...(await sourceSnapshot(db)),
                ...(await captureSnapshot(db)),
              };
            },
            () => db.end(),
          );
        };
        const sourceBeforeRabbit = await sourceEvidence();
        const oldIntents = (
          await p.query<{ text: string }>(
            'SELECT to_jsonb(t)::text text FROM pipeline.delivery_intents t ORDER BY event_id,kind',
          )
        ).rows;
        const oldObservations = (
          await p.query<{ text: string }>(
            'SELECT to_jsonb(t)::text text FROM pipeline.consumer_observations t ORDER BY event_id',
          )
        ).rows;
        const before = {
          ...(await pipelineSnapshot(p)),
          ...(await esSnapshot(p)),
        };
        await migrateRabbit(p, publisherSqlPassword, receiptSqlPassword);
        const after = {
          ...(await pipelineSnapshot(p)),
          ...(await esSnapshot(p)),
        };
        for (const table of [
          'source_binding',
          'destinations',
          'events',
          'integrity_incidents',
          'es_target',
          'es_attempts',
          'es_dead_letters',
        ])
          assert.deepEqual(after[table], before[table]);
        const preservedIntents = (
          await p.query<{ text: string }>(
            "SELECT (to_jsonb(t)-'rabbit_attempt_id')::text text FROM pipeline.delivery_intents t ORDER BY event_id,kind",
          )
        ).rows;
        const preservedObservations = (
          await p.query<{ text: string }>(
            "SELECT (to_jsonb(t)-ARRAY['next_check_at','consumer_id','registration_id','receipt_hash','receipt_bytes','receipt_id','observed_at'])::text text FROM pipeline.consumer_observations t ORDER BY event_id",
          )
        ).rows;
        assert.deepEqual(preservedIntents, oldIntents);
        assert.deepEqual(preservedObservations, oldObservations);
        rabbitTarget = await prepareRabbit(p, pipelineId, sourceEpoch);
        await initializeConsumer(
          p,
          pConfig,
          rabbitTarget,
          consumerSqlPassword,
          consumerReaderPassword,
        );
        if (batchProfile && !upgrade) {
          const c = new pg.Client({
            ...pConfig,
            database: 'consumer_m4',
            connectionTimeoutMillis: 5000,
            query_timeout: 15000,
          });
          await withCleanup(
            async () => {
              await c.connect();
              await migrateConsumerBatch(c);
            },
            () => c.end(),
          );
        }
        rabbitCredentials = await setupTopology(
          rabbitService.api,
          rabbitTarget,
          false,
          {
            host: '127.0.0.1',
            port: rabbitService.amqpPort,
            username: 'm4_setup',
            password: rabbitService.password,
            ca: rabbitService.ca,
            vhost: rabbitTarget.vhost,
          },
        );
        secrets.push(...Object.values(rabbitCredentials));
        await bindRabbit(p, rabbitTarget);
        assert.deepEqual(await sourceEvidence(), sourceBeforeRabbit);
        await writeFile(
          join(artifactDir, 'rabbit-upgrade.json'),
          JSON.stringify(
            {
              before,
              after,
              registration: rabbitTarget,
              sourceBeforeRabbit,
              sourceAfterRabbit: await sourceEvidence(),
              oldIntents,
              preservedIntents,
              oldObservations,
              preservedObservations,
              mode: upgrade ? 'populated M3.1' : 'fresh',
            },
            null,
            2,
          ) + '\n',
        );
      },
      () => p.end(),
    );
  }
  const testEnv = {
    ...env,
    M1_RUN_ID: runId,
    M4_UPGRADE: String(upgrade),
    M41_UPGRADE: String(batchProfile && upgrade),
    PIPELINE_RABBIT_PASSWORD: publisherSqlPassword,
    PIPELINE_RECEIPTS_PASSWORD: receiptSqlPassword,
    CONSUMER_PASSWORD: consumerSqlPassword,
    CONSUMER_READER_PASSWORD: consumerReaderPassword,
    RABBIT_PORT: String(rabbitService?.amqpPort ?? 0),
    RABBIT_CA: rabbitService?.ca ?? '',
    RABBIT_API: rabbitService?.config.url ?? '',
    RABBIT_SETUP_PASSWORD: rabbitService?.password ?? '',
    RABBIT_PUBLISHER_PASSWORD: rabbitCredentials?.publisher ?? '',
    RABBIT_CONSUMER_PASSWORD: rabbitCredentials?.consumer ?? '',
    RABBIT_OBSERVER_PASSWORD: rabbitCredentials?.observer ?? '',
    RABBIT_TARGET: JSON.stringify(rabbitTarget ?? {}),
    RABBIT_PROJECT: `${runId}-rabbit`,
    PIPELINE_ES_PASSWORD: esPassword,
    ES_URL: esService?.config.node ?? '',
    ES_CA: esService?.config.ca ?? '',
    ES_SETUP_PASSWORD: esService?.config.password ?? '',
    ES_USERNAME: receiver?.username ?? '',
    ES_PASSWORD: receiver?.password ?? '',
    ES_INDEX: receiver?.index ?? '',
    ES_PROXY_URL: esService?.proxyNode ?? '',
    ES_PROXY_API: esService?.proxyApi ?? '',
    ES_PROJECT: `${runId}-es`,
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
  testsAttempted = true;
  const output = await record(
    'tests',
    process.execPath,
    [
      '--test',
      '--test-concurrency=1',
      esProfile ? '--test-timeout=240000' : '--test-timeout=60000',
      '--test-reporter=./scripts/test-reporter.ts',
      `--test-reporter-destination=${join(artifactDir, 'tests.json')}`,
      '--test-reporter=junit',
      `--test-reporter-destination=${join(artifactDir, 'tests.xml')}`,
      ...new Set(inventory.map((entry) => entry.file)),
    ],
    testEnv,
    esProfile ? 1200000 : captureProfile ? 240_000 : 180_000,
  );
  if (output) console.log(output);
  manifest['acceptance'] = checkAcceptance(
    await readFile(join(artifactDir, 'tests.json'), 'utf8'),
    { code: 0, signal: null, timedOut: false, outputOverflow: false },
    inventory,
  );
  if (batchReproduction || batchProfile)
    manifest['batchEvidence'] = await checkBatchEvidence(
      join(artifactDir, 'sql-evidence.jsonl'),
      batchReproduction,
    );
  if (profile === 'm4')
    manifest['rabbitEvidence'] = await checkRabbitEvidence(
      join(artifactDir, 'sql-evidence.jsonl'),
    );
  if (profile === 'm3')
    manifest['esEvidence'] = await checkEsEvidence(
      join(artifactDir, 'sql-evidence.jsonl'),
    );
  if (oracleAcceptance)
    manifest['oracleEvidence'] = await checkOracleEvidence(
      join(artifactDir, 'sql-evidence.jsonl'),
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
        const base = await pipelineSnapshot(c);
        return esProfile
          ? {
              ...base,
              ...(await esSnapshot(c)),
              ...(rabbitProfile ? await rabbitSnapshot(c) : {}),
            }
          : base;
      },
      () => c.end(),
    );
  }
  async function retainedConsumer() {
    const c = new pg.Client({
      host: '127.0.0.1',
      port: pipelinePort,
      database: 'consumer_m4',
      user: 'pipeline_admin',
      password: pipelinePassword,
      application_name: `${runId}:consumer-restart`,
      connectionTimeoutMillis: 5000,
      query_timeout: 10000,
    });
    return withCleanup(
      async () => {
        await c.connect();
        assert.deepEqual(
          (
            await c.query(
              "SELECT pid FROM pg_stat_activity WHERE usename IN ('pipeline_rabbit','pipeline_receipts','consumer_runtime','consumer_receipt_reader')",
            )
          ).rows,
          [],
        );
        return consumerSnapshot(c);
      },
      () => c.end(),
    );
  }
  const consumerBefore = rabbitProfile ? await retainedConsumer() : undefined;
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
  const consumerAfter = rabbitProfile ? await retainedConsumer() : undefined;
  assert.deepEqual(consumerAfter, consumerBefore);
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
        consumerBefore,
        consumerAfter,
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
  if (rabbitService) {
    const runningRabbit = rabbitService;
    await diagnostics.finalize('RabbitMQ logs', async () =>
      writeFile(
        join(artifactDir, 'rabbit.log'),
        clean(
          await runningRabbit.compose(['logs', '--tail', '150', '--no-color']),
        ),
      ),
    );
    await diagnostics.finalize('RabbitMQ cleanup', runningRabbit.cleanup);
    const resources: Record<string, string> = {};
    for (const kind of ['container', 'volume', 'network'])
      await diagnostics.finalize(`RabbitMQ remaining ${kind}`, async () => {
        resources[kind] = await record(`rabbit-remaining-${kind}`, 'docker', [
          kind,
          'ls',
          ...(kind === 'container' ? ['-a'] : []),
          '-q',
          '--filter',
          `label=com.docker.compose.project=${runId}-rabbit`,
        ]);
        assert.equal(resources[kind], '');
      });
    manifest['rabbitCleanup'] = {
      status:
        Object.keys(resources).length === 3 &&
        Object.values(resources).every((v) => v === '')
          ? 'PASS'
          : 'FAIL',
      resources,
    };
  }
  if (esService) {
    const runningEs = esService;
    await diagnostics.finalize('Elasticsearch logs', async () =>
      writeFile(
        join(artifactDir, 'es.log'),
        clean(await runningEs.compose(['logs', '--tail', '200', '--no-color'])),
      ),
    );
    await diagnostics.finalize('Elasticsearch cleanup', runningEs.cleanup);
    const esResources: Record<string, string> = {};
    for (const kind of ['container', 'volume', 'network'])
      await diagnostics.finalize(`ES remaining ${kind}`, async () => {
        const remaining = await record(`es-remaining-${kind}`, 'docker', [
          kind,
          'ls',
          ...(kind === 'container' ? ['-a'] : []),
          '-q',
          '--filter',
          `label=com.docker.compose.project=${runId}-es`,
        ]);
        assert.equal(remaining, '');
        esResources[kind] = remaining;
      });
    manifest['elasticsearchCleanup'] = {
      status: Object.keys(esResources).length === 3 ? 'PASS' : 'FAIL',
      resources: esResources,
    };
  }
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
  const evidenceStatus: Record<string, string> = {};
  for (const file of ['tests.xml', 'tests.json', 'sql-evidence.jsonl']) {
    await diagnostics.finalize(`sanitize ${file}`, async () => {
      try {
        evidenceStatus[file] = await sanitizeStageEvidence(
          join(artifactDir, file),
          file === 'sql-evidence.jsonl' ? sqlEvidenceAttempted : testsAttempted,
          clean,
        );
      } catch (error) {
        if (!(error instanceof MissingEvidenceError)) throw error;
        evidenceStatus[file] = 'MISSING REQUIRED EVIDENCE';
        evidenceErrors.push(clean(error.message));
        diagnostics.fail(error);
      }
    });
  }
  manifest['evidenceStatus'] = evidenceStatus;
  manifest['evidenceErrors'] = evidenceErrors;
  manifest['testExecution'] = {
    status: !testsAttempted
      ? 'NOT RUN'
      : manifest['acceptance']
        ? 'PASS'
        : 'FAILED OR INCOMPLETE',
    requiredCaseIds: inventory.map((c) => c.id),
  };
  if (!started)
    manifest['cleanup'] = {
      status: 'NOT NEEDED',
      reason: 'No owned service launch attempted',
      resources: {},
    };
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
          prerequisite: manifest['prerequisite'],
          testExecution: manifest['testExecution'],
          evidenceStatus: manifest['evidenceStatus'],
          evidenceErrors,
          acceptance: manifest['acceptance'],
          cleanup: manifest['cleanup'],
          elasticsearchCleanup: manifest['elasticsearchCleanup'],
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
      'es-upgrade.json',
      'es.log',
      'prerequisite.json',
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
