import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { command } from './support.ts';
import { object } from './acceptance.ts';
import { actionlint } from './tool-pins.ts';

async function checked(executable: string, args: string[], env = process.env) {
  const result = await command(executable, args, env, 30_000, true);
  assert.ok(
    result.code === 0 &&
      !result.signal &&
      !result.timedOut &&
      !result.outputOverflow &&
      result.cleanupErrors.length === 0,
    `${executable} failed: ${result.stderr}`,
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
      },
    );
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
  // Only workflow validation is in scope. No shell files or Python workflows exist.
  await checked(binary, ['-color', '-shellcheck=', '-pyflakes=']);
} else {
  throw new Error('Expected compose or workflow validation');
}
