// Missing evidence is NOT RUN only when its producer was never attempted.
import { readFile, writeFile, rm } from 'node:fs/promises';
import { withCleanup } from './support.ts';
export class MissingEvidenceError extends Error {}
export async function sanitizeStageEvidence(
  path: string,
  producerAttempted: boolean,
  sanitize: (text: string) => string,
): Promise<'AVAILABLE' | 'NOT RUN'> {
  try {
    let text: string;
    try {
      text = await readFile(path, 'utf8');
    } catch (error) {
      if (
        error instanceof Error &&
        'code' in error &&
        error.code === 'ENOENT'
      ) {
        if (producerAttempted)
          throw new MissingEvidenceError(
            `Required evidence missing after producer attempt: ${path}`,
            { cause: error },
          );
        return 'NOT RUN';
      }
      throw error;
    }
    await writeFile(path, sanitize(text));
    return 'AVAILABLE';
  } catch (error) {
    if (error instanceof MissingEvidenceError) throw error;
    // Never leave a readable unsanitized artifact for the upload stage.
    return withCleanup(
      () =>
        Promise.reject(
          error instanceof Error
            ? error
            : new Error('Evidence sanitization failed', { cause: error }),
        ),
      () => rm(path, { force: true }),
    );
  }
}
