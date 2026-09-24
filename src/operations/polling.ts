import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { bigint, integer, record } from './validation.ts';
export type PollingRole = 'capture' | 'backfill';
export interface PollingObservation {
  revision: string;
  delay_ms: number;
}
/** Root-runtime capability only; a failed read never invents defaults. */
export function runtimePolling(
  connection: ConnectionConfig,
  role: PollingRole,
  report: (observation: PollingObservation) => void | Promise<void>,
): () => Promise<number> {
  let cached: PollingObservation | undefined;
  let readAt = -Infinity;
  return async () => {
    if (cached && performance.now() - readAt < 5000) return cached.delay_ms;
    const owner = new TransactionOwner(connection, (client, operation) => ({
      read: () =>
        operation(async () => {
          await client.query('SET TRANSACTION READ ONLY');
          await client.query('SET LOCAL statement_timeout=2500');
          const result = await client.query<{ value: unknown }>(
            'SELECT pipeline.read_polling() value',
          );
          const row = record(result.rows[0]?.value);
          return {
            revision: bigint(row['revision']),
            delay_ms: integer(
              row[role === 'capture' ? 'capture_poll_ms' : 'backfill_idle_ms'],
              50,
              30000,
            ),
          };
        }),
    }));
    const observed = await owner.transaction((w) => w.read());
    if (observed.revision !== cached?.revision) await report(observed);
    cached = observed;
    readAt = performance.now();
    return observed.delay_ms;
  };
}
