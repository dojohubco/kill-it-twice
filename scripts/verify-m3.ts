// Fixed complete M3/M3.1 command sequence. No prerequisite bypass or host mutation.
import { fileURLToPath } from 'node:url';
import { command, errorText } from './support.ts';
import {
  observePrerequisite,
  requirePrerequisite,
  retainPrerequisite,
  type PrerequisiteObservation,
} from './es-prerequisite.ts';
interface VerificationDependencies {
  observe: () => Promise<PrerequisiteObservation>;
  record: (observation: PrerequisiteObservation) => Promise<void>;
  execute: (command: readonly string[]) => Promise<void>;
}
export async function verifyM3(
  dependencies: VerificationDependencies,
): Promise<void> {
  const observation = await dependencies.observe();
  await dependencies.record(observation);
  requirePrerequisite(observation);
  // Earlier profiles run once each through the existing bounded gate.
  for (const args of [
    ['make', 'verify-m2c1'],
    ['npm', 'run', 'test:integration:m3'],
    ['npm', 'run', 'test:integration:m3', '--', '--upgrade'],
    ['npm', 'run', 'test:integration:m31'],
  ])
    await dependencies.execute(args);
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await verifyM3({
      observe: () =>
        observePrerequisite('complete-verification-before-profiles'),
      record: async (observation) => {
        const path = await retainPrerequisite(observation);
        console.log(
          `Elasticsearch prerequisite ${observation.status}: ${path}`,
        );
      },
      execute: async (args) => {
        const [executable, ...tail] = args;
        if (!executable) throw new Error('Missing verification command');
        const result = await command(
          executable,
          tail,
          process.env,
          1200000,
          true,
        );
        await new Promise<void>((resolve, reject) =>
          process.stdout.write(result.stdout, (error) =>
            error ? reject(error) : resolve(),
          ),
        );
        await new Promise<void>((resolve, reject) =>
          process.stderr.write(result.stderr, (error) =>
            error ? reject(error) : resolve(),
          ),
        );
        if (
          result.code !== 0 ||
          result.signal ||
          result.timedOut ||
          result.outputOverflow ||
          result.cleanupErrors.length
        )
          throw new Error(
            `Verification command failed: ${JSON.stringify({ args, code: result.code, signal: result.signal, timedOut: result.timedOut, outputOverflow: result.outputOverflow, cleanupErrors: result.cleanupErrors.map(errorText) })}`,
          );
      },
    });
  } catch (error) {
    console.error(errorText(error));
    process.exitCode = 1;
  }
}
