import { randomUUID } from 'node:crypto';
import { createControlApi } from './app.ts';
import { loadOperationsConfig } from '../../src/operations/config.ts';
const started = performance.now(),
  requestId = randomUUID();
function log(operation: string, outcome: string, cleanupErrors = 0) {
  process.stdout.write(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level: outcome === 'ready' || outcome === 'closed' ? 'info' : 'error',
      service: 'control-api',
      operation,
      request_id: requestId,
      outcome,
      duration_ms: Math.round(performance.now() - started),
      error_class: outcome.endsWith('failure') ? outcome : null,
      cleanup_error_count: cleanupErrors,
    }) + '\n',
  );
}
let app: Awaited<ReturnType<typeof createControlApi>> | undefined;
try {
  const port = Number(process.env['CONTROL_PORT'] ?? '3000');
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new Error('Invalid control port');
  app = await createControlApi(await loadOperationsConfig());
  const host =
    process.env['CONTROL_CONTAINER'] === '1' ? '0.0.0.0' : '127.0.0.1';
  await app.listen(port, host);
  log('startup', 'ready');
  let closing = false;
  for (const signal of ['SIGINT', 'SIGTERM'])
    process.once(signal, () => {
      if (closing) return;
      closing = true;
      void app?.close().then(
        () => log('shutdown', 'closed'),
        () => {
          log('shutdown', 'cleanup_failure', 1);
          process.exitCode = 1;
        },
      );
    });
} catch {
  let cleanupErrors = 0;
  try {
    await app?.close();
  } catch {
    cleanupErrors = 1;
  }
  log('startup', 'startup_failure', cleanupErrors);
  process.exitCode = 1;
}
