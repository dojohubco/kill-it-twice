import assert from 'node:assert/strict';
import { readFile, writeFile, rename, chown, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { record } from '../../src/operations/validation.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import { withCleanup } from '../support.ts';
export async function password(role: string): Promise<string> {
  assert.match(role, /^(source|pipeline|consumer|es|rabbit|operator)_[a-z_]+$/);
  const value = (await readFile(`/private/${role}`, 'utf8')).trim();
  assert.match(value, /^[a-f0-9]{48}$/);
  return value;
}
export async function readObject(
  path: string,
): Promise<Record<string, unknown>> {
  const text = await readFile(path, 'utf8');
  assert.ok(
    Buffer.byteLength(text) <= 262144,
    'Private configuration is bounded',
  );
  const value: unknown = JSON.parse(text);
  return record(value);
}
export async function atomic(
  path: string,
  value: unknown,
  owner = 1000,
): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.${randomUUID()}.tmp`;
  await writeFile(temporary, JSON.stringify(value, null, 2) + '\n', {
    mode: 0o600,
    flag: 'wx',
  });
  if (process.getuid?.() === 0) await chown(temporary, owner, 0);
  await rename(temporary, path);
}
export function sql(role: string, secret: string): ConnectionConfig {
  assert.match(role, /^(source|pipeline|consumer)_[a-z_]+$/);
  return {
    host: role.startsWith('source') ? 'source' : 'pipeline',
    port: 5432,
    database: role.startsWith('source')
      ? 'source_m1'
      : role.startsWith('consumer')
        ? 'consumer_m4'
        : 'pipeline_m2b',
    user: role,
    password: secret,
    application_name: `runtime:${role}`,
  };
}
export async function adminConfig(
  store: 'source' | 'pipeline' | 'consumer',
): Promise<ConnectionConfig> {
  const role = store === 'source' ? 'source_admin' : 'pipeline_admin';
  return {
    ...sql(role, await password(role)),
    database: store === 'consumer' ? 'consumer_m4' : sql(role, '').database,
  };
}
export async function database<T>(
  config: ConnectionConfig,
  work: (client: pg.Client) => Promise<T>,
): Promise<T> {
  const client = new pg.Client({
    ...config,
    connectionTimeoutMillis: 5000,
    statement_timeout: 30000,
    query_timeout: 35000,
  });
  return withCleanup(
    async () => {
      await client.connect();
      return work(client);
    },
    () => client.end(),
  );
}
export function safeFailure(error: unknown): Record<string, unknown> {
  const code =
    error && typeof error === 'object' && 'code' in error ? error.code : null;
  return {
    type: 'runtime_failure',
    error_class: error instanceof Error ? error.name : 'unknown',
    code: typeof code === 'string' && /^[A-Z0-9_]+$/.test(code) ? code : null,
  };
}
export async function existing(
  path: string,
): Promise<Record<string, unknown> | null> {
  try {
    return await readObject(path);
  } catch (error) {
    if (
      error &&
      typeof error === 'object' &&
      'code' in error &&
      error.code === 'ENOENT'
    )
      return null;
    throw error;
  }
}
