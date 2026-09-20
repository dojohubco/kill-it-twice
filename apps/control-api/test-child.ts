// Private acceptance executable. Production main.ts has no fault environment hooks.
import { createControlApi } from './app.ts';
import { loadOperationsConfig } from '../../src/operations/config.ts';
const app = await createControlApi(await loadOperationsConfig(), {
  afterCommit: async (operation, result) => {
    if (operation !== process.env['CONTROL_TEST_BARRIER']) return;
    process.send?.({ type: 'barrier', operation, pid: process.pid, result });
    await new Promise<void>((resolve) =>
      process.once('message', () => resolve()),
    );
  },
});
await app.listen(0, '127.0.0.1');
process.send?.({ type: 'ready', pid: process.pid, url: await app.getUrl() });
process.once('SIGTERM', () => {
  void app.close().then(
    () => process.exit(0),
    () => process.exit(1),
  );
});
