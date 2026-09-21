import { setTimeout as delay } from 'node:timers/promises';
import { ConsumerDatabase } from '../src/rabbitmq/consumer-db.ts';
import { ReceiptObserver, receiptPollDelay } from '../src/rabbitmq/receipts.ts';
import { writeCaptureReport } from '../src/internal/capture-report.ts';
import { sqlConfig, cliMode, stopSignal } from './rabbit-cli-config.ts';
const mode = cliMode(),
  db = new ConsumerDatabase(sqlConfig('consumer_receipt_reader'));
try {
  if (mode === 'status')
    await writeCaptureReport(process.stdout, await db.status());
  else {
    const observer = new ReceiptObserver(sqlConfig('pipeline_receipts'), db),
      signal = stopSignal();
    do {
      const result = await observer.once();
      await writeCaptureReport(process.stdout, result);
      if (mode !== 'follow') break;
      try {
        await delay(receiptPollDelay(result), undefined, { signal });
      } catch (error) {
        if (!signal.aborted) throw error;
      }
    } while (!signal.aborted);
  }
} catch {
  console.error(
    'Consumer receipt observation failed; absent/unvalidated evidence remains pending',
  );
  process.exitCode = 1;
}
