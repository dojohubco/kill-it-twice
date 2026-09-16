import { waitFor } from '../../scripts/support.ts';
import { Source } from '../../src/source.ts';
import { appendFileSync } from 'node:fs';
import { join } from 'node:path';
import pg from 'pg';

export function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}; run npm run test:integration:m1`);
  return value;
}

export async function connect(role: 'admin' | 'writer', label: string): Promise<pg.Client> {
  const client = new pg.Client({
    host: '127.0.0.1', port: Number(required('M1_PORT')), database: 'source_m1',
    user: role === 'admin' ? 'm1_admin' : 'source_writer',
    password: required(role === 'admin' ? 'M1_ADMIN_PASSWORD' : 'M1_WRITER_PASSWORD'),
    application_name: `${required('M1_RUN_ID')}:${label}`,
    connectionTimeoutMillis: 5_000, query_timeout: 15_000,
    statement_timeout: 12_000, idle_in_transaction_session_timeout: 30_000,
  });
  await client.connect();
  return client;
}

export function evidence(test: string, data: unknown): void {
  appendFileSync(join(required('M1_ARTIFACT_DIR'), 'sql-evidence.jsonl'), JSON.stringify({ test, runId: required('M1_RUN_ID'), at: new Date().toISOString(), data }) + '\n');
}

export function sourceOwner(label: string): Source {
  return new Source({ host: '127.0.0.1', port: Number(required('M1_PORT')), database: 'source_m1', user: 'source_writer', password: required('M1_WRITER_PASSWORD'), application_name: `${required('M1_RUN_ID')}:${label}` });
}

// Every resource-bearing polling observation owns disposal of its connection on failure.
export function databaseWaitFor<T>(client: pg.Client, observe: () => Promise<T>, accept: (value: T) => boolean, label: string, timeout = 10_000): Promise<T> {
  let disposal: Promise<void> | undefined;
  let configured = false;
  return waitFor(async (signal) => {
    const close = () => { disposal ??= client.end(); void disposal.catch(() => undefined); };
    signal.addEventListener('abort', close, { once: true });
    try {
      // Socket disposal alone need not immediately interrupt server-side pg_sleep.
      // Bound each server observation as well; never queue work after abort.
      if (!configured) { await client.query('SET statement_timeout = 500'); configured = true; }
      signal.throwIfAborted();
      return await observe();
    }
    finally { signal.removeEventListener('abort', close); }
  }, accept, label, timeout, () => disposal ?? client.end());
}
