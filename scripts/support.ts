import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

export function redact(value: string, secrets: readonly string[]): string {
  return secrets.filter(Boolean).reduce((text, secret) => text.replaceAll(secret, '[REDACTED]'), value);
}

export async function waitFor<T>(
  observe: () => Promise<T>, accept: (value: T) => boolean,
  label: string, timeoutMs = 10_000,
): Promise<T> {
  const end = Date.now() + timeoutMs;
  let last: T | undefined;
  do {
    last = await observe();
    if (accept(last)) return last;
    await delay(Math.min(25, Math.max(0, end - Date.now())));
  } while (Date.now() < end);
  throw new Error(`Deadline waiting for ${label}; last observation: ${JSON.stringify(last)}`);
}

export interface CommandResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

export function command(
  executable: string, args: string[], env: NodeJS.ProcessEnv = process.env, timeout = 90_000,
  ownProcessGroup = false,
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { env, stdio: ['ignore', 'pipe', 'pipe'], detached: ownProcessGroup });
    let stdout = '', stderr = '', timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      // Only the new process group created by this exact spawn is eligible.
      // Killing a timed-out test runner must also reap its private writer children.
      if (ownProcessGroup && child.pid) {
        try { process.kill(-child.pid, 'SIGKILL'); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') child.kill('SIGKILL'); }
      } else child.kill('SIGKILL');
    }, timeout);
    child.stdout.setEncoding('utf8').on('data', (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => { stderr += chunk; });
    child.once('error', (error) => { clearTimeout(timer); reject(error); });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timedOut });
    });
  });
}
