import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { actionlint } from './tool-pins.ts';
import { command } from './support.ts';

assert.equal(process.platform, 'linux', 'Provisioning is pinned for Linux x64');
assert.equal(process.arch, 'x64', 'Provisioning is pinned for Linux x64');
await mkdir(actionlint.directory, { recursive: true });
const response = await fetch(actionlint.url, {
  signal: AbortSignal.timeout(30_000),
});
assert.ok(response.ok, `actionlint download HTTP ${response.status}`);
const bytes = Buffer.from(await response.arrayBuffer());
assert.equal(
  createHash('sha256').update(bytes).digest('hex'),
  actionlint.archiveSha256,
  'actionlint archive checksum',
);
const archive = join(actionlint.directory, 'release.tar.gz');
await writeFile(archive, bytes);
const extracted = await command(
  'tar',
  ['-xzf', archive, '-C', actionlint.directory, 'actionlint'],
  process.env,
  10_000,
  true,
);
assert.equal(extracted.code, 0, extracted.stderr);
const binary = join(actionlint.directory, 'actionlint');
assert.equal(
  createHash('sha256')
    .update(await readFile(binary))
    .digest('hex'),
  actionlint.binarySha256,
);
const version = await command(binary, ['-version']);
assert.equal(version.code, 0, version.stderr);
assert.equal(version.stdout.split('\n')[0], actionlint.version);
await writeFile(
  join(actionlint.directory, 'verified.json'),
  JSON.stringify({
    version: actionlint.version,
    archiveSha256: actionlint.archiveSha256,
    binarySha256: createHash('sha256')
      .update(await readFile(binary))
      .digest('hex'),
  }) + '\n',
);
console.log(
  `Provisioned actionlint ${actionlint.version}; release checksum verified`,
);
