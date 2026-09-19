// Capture the same local commands, including failures before PostgreSQL starts.
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Diagnostics } from './finalization.ts';
import { command, redact } from './support.ts';

const [executable, ...args] = process.argv.slice(2);
if (!executable) throw new Error('Expected a command');
const directory = 'artifacts/ci/public';
await mkdir(directory, { recursive: true });
const stem = `${Date.now()}-${process.pid}`;
const secrets = [
  'GITHUB_TOKEN',
  'NODE_AUTH_TOKEN',
  'ACTIONS_RUNTIME_TOKEN',
].flatMap((name) => (process.env[name] ? [process.env[name]] : []));
const clean = (text: string) => redact(text, secrets);
const diagnostics = new Diagnostics(clean);
try {
  const timeout =
    executable === 'make' && args.length === 1 && args[0] === 'verify-m3'
      ? 1800000
      : 600000;
  const result = await command(executable, args, process.env, timeout, true, {
    secrets,
  });
  if (
    result.code !== 0 ||
    result.signal ||
    result.timedOut ||
    result.outputOverflow ||
    result.cleanupErrors.length
  ) {
    diagnostics.fail(
      new Error(
        `Command failed: ${JSON.stringify({ executable, args, code: result.code, signal: result.signal, timedOut: result.timedOut, outputOverflow: result.outputOverflow, cleanupErrors: result.cleanupErrors })}`,
      ),
    );
  }
  for (const stream of ['stdout', 'stderr'] as const) {
    await diagnostics.finalize(stream, () =>
      writeFile(join(directory, `${stem}.${stream}.log`), result[stream]),
    );
  }
  await diagnostics.finalize('metadata', () =>
    writeFile(
      join(directory, `${stem}.json`),
      clean(
        JSON.stringify(
          {
            executable,
            args,
            code: result.code,
            signal: result.signal,
            timedOut: result.timedOut,
            outputOverflow: result.outputOverflow,
            cleanupErrors: result.cleanupErrors,
          },
          null,
          2,
        ),
      ) + '\n',
    ),
  );
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
} catch (error) {
  diagnostics.fail(error);
}
if (!diagnostics.ok) {
  process.exitCode = 1;
  const diagnostic = JSON.stringify(
    { primary: diagnostics.primary, cleanup: diagnostics.cleanup },
    null,
    2,
  );
  console.error(diagnostic);
  await diagnostics.finalize('failure log', () =>
    writeFile(join(directory, `${stem}.failure.log`), diagnostic),
  );
  if (diagnostics.cleanup.length)
    console.error(JSON.stringify(diagnostics.cleanup));
}
