import { withOperationalCleanup } from './cleanup.ts';
import pg from 'pg';
import type { ConnectionConfig } from '../internal/transaction.ts';
import { ControlError } from './validation.ts';
// Session lock serializes one configured proxy's HTTP operations. No transaction
// remains open across the remote call; durable pending intent survives process death.
export async function networkLock<T>(
  config: ConnectionConfig,
  sink: string,
  work: () => Promise<T>,
): Promise<T> {
  const client = new pg.Client({
    ...config,
    connectionTimeoutMillis: 3000,
    query_timeout: 3000,
    statement_timeout: 2500,
  });
  client.on('error', () => {});
  return withOperationalCleanup(
    async () => {
      await client.connect();
      const r = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock(hashtextextended($1,919)) acquired',
        [sink],
      );
      if (r.rows[0]?.acquired !== true) throw new ControlError(409, 'conflict');
      return await work();
    },
    () => client.end(),
  );
}
