import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import pg from 'pg';
import { command, redact } from './support.ts';

const runId = `m1-${new Date().toISOString().replace(/[^0-9]/g, '')}-${randomBytes(4).toString('hex')}`;
const artifactDir = resolve('artifacts/m1', runId);
await mkdir(artifactDir, { recursive: true });
const temporaryDir = await mkdtemp(join(tmpdir(), `${runId}-`));
const adminPassword = randomBytes(24).toString('hex');
const writerPassword = randomBytes(24).toString('hex');
const secrets = [adminPassword, writerPassword];
const passwordFile = join(temporaryDir, 'postgres-password');
await writeFile(passwordFile, adminPassword, { mode: 0o600 });
const env = { ...process.env, M1_PASSWORD_FILE: passwordFile };
const compose = ['compose', '-p', runId, '-f', resolve('compose.m1.yaml')];
const manifest: Record<string, unknown> = { runId, artifactDir, startedAt: new Date().toISOString(), status: 'RUNNING', commands: [] };
let sequence = 0;
const clean = (text: string) => redact(text, secrets);
async function record(name: string, executable: string, args: string[], runEnv = env, timeout = 90_000) {
  const result = await command(executable, args, runEnv, timeout);
  const base = `${String(++sequence).padStart(2, '0')}-${name}`;
  await writeFile(join(artifactDir, `${base}.stdout.log`), clean(result.stdout));
  await writeFile(join(artifactDir, `${base}.stderr.log`), clean(result.stderr));
  (manifest.commands as unknown[]).push({ executable, args, code: result.code, signal: result.signal, timedOut: result.timedOut, stdout: `${base}.stdout.log`, stderr: `${base}.stderr.log` });
  if (result.code !== 0 || result.signal || result.timedOut) throw new Error(`${name} failed: ${JSON.stringify({ code: result.code, signal: result.signal, timedOut: result.timedOut })}\n${clean(result.stderr)}`);
  return result.stdout.trim();
}
let started = false;
try {
  assert.equal(process.versions.node, (await readFile('.node-version', 'utf8')).trim());
  manifest.head = await record('head', 'git', ['rev-parse', 'HEAD']);
  manifest.gitStatus = await record('git-status', 'git', ['status', '--porcelain']);
  manifest.node = process.version;
  manifest.npm = await record('npm-version', 'npm', ['--version']);
  manifest.docker = await record('docker-version', 'docker', ['version', '--format', '{{.Server.Version}}']);
  manifest.compose = await record('compose-version', 'docker', ['compose', 'version', '--short']);
  console.log(`M1 isolated run ${runId}; evidence: ${artifactDir}`);
  started = true;
  await record('up', 'docker', [...compose, 'up', '-d', '--wait', '--wait-timeout', '60'], env, 120_000);
  const containerId = await record('container', 'docker', [...compose, 'ps', '-q', 'source']);
  manifest.containerId = containerId;
  manifest.image = JSON.parse(await record('image', 'docker', ['inspect', '--format', '{{json .Image}}', containerId]));
  manifest.imageReference = await record('image-ref', 'docker', ['inspect', '--format', '{{.Config.Image}}', containerId]);
  const address = await record('port', 'docker', [...compose, 'port', 'source', '5432']);
  assert.match(address, /^127\.0\.0\.1:\d+$/);
  const port = Number(address.split(':')[1]);
  manifest.port = port;
  const admin = new pg.Client({ host: '127.0.0.1', port, database: 'source_m1', user: 'm1_admin', password: adminPassword, application_name: `${runId}:setup`, connectionTimeoutMillis: 5_000, query_timeout: 10_000 });
  await admin.connect();
  try {
    const settings = (await admin.query("SELECT version(), current_setting('server_version') AS server_version, current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes")).rows[0];
    assert.match(settings.server_version as string, /^18\.6(?:\s|$)/);
    for (const name of ['fsync', 'synchronous_commit', 'full_page_writes']) assert.equal(settings[name], 'on');
    manifest.postgres = settings;
  } finally { await admin.end(); }
  const testEnv = { ...env, M1_RUN_ID: runId, M1_ARTIFACT_DIR: artifactDir, M1_PORT: String(port), M1_ADMIN_PASSWORD: adminPassword, M1_WRITER_PASSWORD: writerPassword };
  const output = await record('tests', process.execPath, ['--test', '--test-concurrency=1', '--test-timeout=60000', '--test-reporter=spec', '--test-reporter-destination=stdout', '--test-reporter=junit', `--test-reporter-destination=${join(artifactDir, 'tests.xml')}`, 'tests/integration/*.test.ts'], testEnv, 180_000);
  console.log(output);
  manifest.status = 'PASS';
} catch (error) {
  manifest.status = 'FAIL';
  manifest.error = clean(error instanceof Error ? error.stack ?? error.message : String(error));
  console.error(manifest.error);
  process.exitCode = 1;
} finally {
  if (started) {
    try { await record('postgres-logs', 'docker', [...compose, 'logs', '--no-color', 'source']); }
    catch (error) { manifest.logError = clean(String(error)); }
    try {
      await record('down', 'docker', [...compose, 'down', '--volumes', '--timeout', '10']);
      const resources: Record<string, string> = {};
      for (const kind of ['container', 'volume', 'network']) {
        resources[kind] = await record(`remaining-${kind}`, 'docker', [kind, 'ls', '-q', '--filter', `label=com.docker.compose.project=${runId}`]);
        assert.equal(resources[kind], '', `Owned ${kind} leaked`);
      }
      manifest.cleanup = { status: 'PASS', resources };
    } catch (error) {
      manifest.cleanup = { status: 'FAIL', error: clean(String(error)) };
      manifest.status = 'FAIL';
      process.exitCode = 1;
    }
  }
  await rm(temporaryDir, { recursive: true, force: true });
  // Keep all text artifacts sanitized, including the test runner's direct XML output.
  const xml = join(artifactDir, 'tests.xml');
  await readFile(xml, 'utf8').then((text) => writeFile(xml, clean(text))).catch((error: NodeJS.ErrnoException) => { if (error.code !== 'ENOENT') throw error; });
  manifest.finishedAt = new Date().toISOString();
  await writeFile(join(artifactDir, 'run.json'), JSON.stringify(manifest, null, 2) + '\n');
  console.log(`${manifest.status}: ${artifactDir}/run.json`);
}
