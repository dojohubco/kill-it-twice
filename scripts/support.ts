import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

export function redact(value: string, secrets: readonly string[]): string {
  return secrets
    .filter(Boolean)
    .reduce((text, secret) => text.replaceAll(secret, '[REDACTED]'), value);
}
export function errorText(error: unknown): string {
  return error instanceof Error
    ? (error.stack ?? error.message)
    : String(error);
}
export function errorCode(error: unknown): unknown {
  return error && typeof error === 'object' && 'code' in error
    ? error.code
    : undefined;
}
export class CleanupFailure extends Error {
  readonly cleanupErrors: readonly unknown[];
  constructor(primary: unknown, cleanupErrors: readonly unknown[]) {
    super('Primary operation and cleanup both failed', { cause: primary });
    this.cleanupErrors = cleanupErrors;
  }
}
class DeadlineError extends Error {}
export async function bounded<T>(
  promise: Promise<T>,
  milliseconds: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new DeadlineError(`Deadline: ${label}`)),
          milliseconds,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
// Resource-bearing observers MUST react to signal and supply bounded cleanup.
// A race consumes late rejection, but cannot itself cancel arbitrary external work.
export async function waitFor<T>(
  observe: (signal: AbortSignal) => Promise<T>,
  accept: (value: T) => boolean,
  label: string,
  timeoutMs = 10_000,
  cleanup?: () => Promise<void>,
): Promise<T> {
  const end = performance.now() + timeoutMs;
  const abort = new AbortController();
  let last: T | undefined;
  const expired = () =>
    new Error(
      `Deadline waiting for ${label}; last observation: ${JSON.stringify(last)}`,
    );
  try {
    while (performance.now() < end) {
      try {
        last = await bounded(
          observe(abort.signal),
          Math.max(1, end - performance.now()),
          label,
        );
      } catch (error) {
        if (error instanceof DeadlineError || performance.now() >= end)
          throw expired();
        throw error;
      }
      if (performance.now() >= end) throw expired();
      if (accept(last)) return last;
      await delay(Math.min(25, Math.max(0, end - performance.now())));
    }
    throw expired();
  } catch (primary) {
    abort.abort(primary);
    if (cleanup) {
      try {
        await bounded(cleanup(), 2_000, `${label} observation disposal`);
      } catch (failure) {
        throw new CleanupFailure(primary, [failure]);
      }
    }
    throw primary;
  }
}
export interface CommandResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  outputOverflow: boolean;
  cleanupErrors: string[];
}
export function command(
  executable: string,
  args: string[],
  env: NodeJS.ProcessEnv = process.env,
  timeout = 90_000,
  ownProcessGroup = false,
  options: { maxOutputBytes?: number; secrets?: readonly string[] } = {},
): Promise<CommandResult> {
  const limit = options.maxOutputBytes ?? 1_048_576;
  if (!Number.isSafeInteger(limit) || limit <= 0)
    throw new Error('Output limit must be a positive integer');
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: ownProcessGroup,
    });
    const stdout: Buffer[] = [],
      stderr: Buffer[] = [],
      cleanupErrors: string[] = [];
    let captured = 0,
      timedOut = false,
      outputOverflow = false;
    const kill = () => {
      try {
        if (ownProcessGroup && child.pid) process.kill(-child.pid, 'SIGKILL');
        else child.kill('SIGKILL');
      } catch (error) {
        if (errorCode(error) !== 'ESRCH') {
          cleanupErrors.push(errorText(error));
          child.kill('SIGKILL');
        }
      }
    };
    const timer = setTimeout(() => {
      timedOut = true;
      kill();
    }, timeout);
    const collect = (target: Buffer[], chunk: Buffer) => {
      if (outputOverflow) return;
      const remaining = Math.max(0, limit - captured);
      target.push(chunk.subarray(0, remaining));
      captured += Math.min(remaining, chunk.length);
      if (chunk.length > remaining && !outputOverflow) {
        outputOverflow = true;
        kill();
      }
    };
    child.stdout.on('data', (chunk: Buffer) => collect(stdout, chunk));
    child.stderr.on('data', (chunk: Buffer) => collect(stderr, chunk));
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      const sanitize = (chunks: Buffer[]) => {
        let text = Buffer.concat(chunks).toString('utf8');
        // Do not expose a partial secret at an overflow/termination boundary.
        for (const secret of options.secrets ?? []) {
          for (let n = Math.min(secret.length - 1, text.length); n > 0; n--) {
            if (text.endsWith(secret.slice(0, n))) {
              text = text.slice(0, -n) + '[REDACTED-PARTIAL]';
              break;
            }
          }
        }
        return redact(text, options.secrets ?? []);
      };
      resolve({
        code: outputOverflow && code === 0 ? 1 : code,
        signal,
        stdout: sanitize(stdout),
        stderr: sanitize(stderr),
        timedOut,
        outputOverflow,
        cleanupErrors,
      });
    });
  });
}
