import { createControlApi } from './app.ts';
import { loadOperationsConfig } from '../../src/operations/config.ts';
const app = await createControlApi(await loadOperationsConfig());
const port = Number(process.env['CONTROL_PORT'] ?? '3000');
if (!Number.isInteger(port) || port < 1 || port > 65535)
  throw new Error('Invalid control port');
const host = process.env['CONTROL_CONTAINER'] === '1' ? '0.0.0.0' : '127.0.0.1';
await app.listen(port, host);
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => {
    void app.close().catch(() => {
      process.exitCode = 1;
    });
  });
