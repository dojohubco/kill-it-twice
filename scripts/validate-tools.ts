import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { command } from './support.ts';
import { object } from './acceptance.ts';
import { actionlint } from './tool-pins.ts';

async function checked(executable: string, args: string[], env = process.env) {
  const started = performance.now();
  const result = await command(executable, args, env, 30_000, true);
  assert.ok(
    result.code === 0 &&
      !result.signal &&
      !result.timedOut &&
      !result.outputOverflow &&
      result.cleanupErrors.length === 0,
    `${executable} ${JSON.stringify(args)} failed: ${JSON.stringify({
      code: result.code,
      signal: result.signal,
      timedOut: result.timedOut,
      outputOverflow: result.outputOverflow,
      cleanupErrors: result.cleanupErrors,
      elapsedMs: performance.now() - started,
      timeoutMs: 30_000,
    })}\n${result.stderr}\n${result.stdout}`,
  );
  console.log(result.stdout.trim());
  return result.stdout.trim();
}
const mode = process.argv[2];
if (mode === 'compose') {
  for (const files of [
    ['compose.m1.yaml'],
    ['compose.m1.yaml', 'compose.m2b.yaml'],
    ['compose.m3.yaml'],
    ['compose.m4.yaml'],
    ['compose.m6.yaml'],
    ['compose.yaml'],
  ])
    await checked(
      'docker',
      [
        'compose',
        '-p',
        'm11-quality-validation',
        ...files.flatMap((file) => ['-f', file]),
        'config',
        '--quiet',
      ],
      {
        ...process.env,
        M1_PASSWORD_FILE: '/nonsecret-quality-fixture/postgres-password',
        M2B_PASSWORD_FILE: '/nonsecret-quality-fixture/pipeline-password',
        M2C_PIPELINE_PORT: '0',
        M3_PRIVATE_DIR: '/nonsecret-quality-fixture/es',
        M4_PRIVATE_DIR: '/nonsecret-quality-fixture/rabbit',
        CONTROL_CONFIG_FILE: '/nonsecret-quality-fixture/control.json',
      },
    );
  const runtime = object(
    JSON.parse(
      await checked('docker', [
        'compose',
        '-f',
        'compose.yaml',
        '-p',
        'quality-runtime-budget',
        'config',
        '--format',
        'json',
      ]),
    ),
  );
  const pipeline = object(object(runtime['services'])['pipeline']);
  assert.equal(
    pipeline['mem_limit'],
    '1073741824',
    'Pipeline total-memory budget changed',
  );
  assert.equal(
    pipeline['shm_size'],
    '268435456',
    'Pipeline shared-memory budget changed',
  );
  for (const file of ['infra/runtime/provision.sh', 'infra/runtime/realm.sh'])
    await checked('bash', ['-n', file]);
} else if (mode === 'workflow') {
  const binary = join(actionlint.directory, 'actionlint');
  let bytes: Buffer, verification: Record<string, unknown>;
  try {
    bytes = await readFile(binary);
    verification = object(
      JSON.parse(
        await readFile(join(actionlint.directory, 'verified.json'), 'utf8'),
      ),
    );
  } catch (error) {
    throw new Error(
      'Missing actionlint; run npm run tools:provision before make quality',
      { cause: error },
    );
  }
  assert.equal(verification['version'], actionlint.version);
  assert.equal(verification['archiveSha256'], actionlint.archiveSha256);
  assert.equal(
    createHash('sha256').update(bytes).digest('hex'),
    actionlint.binarySha256,
    'actionlint binary changed after provisioning',
  );
  assert.equal(
    (await checked(binary, ['-version'])).split('\n')[0],
    actionlint.version,
  );
  // Workflow syntax is checked here; runtime shell syntax is validated by the Compose check.
  await checked(binary, ['-color', '-shellcheck=', '-pyflakes=']);
} else {
  throw new Error('Expected compose or workflow validation');
}
