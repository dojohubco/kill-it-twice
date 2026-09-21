import assert from 'node:assert/strict';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import { readObject } from './private.ts';
export async function environment(expected?: string): Promise<string> {
  const config = await readObject('/config/runtime.json');
  assert.equal(typeof config['role'], 'string');
  const role = String(config['role']);
  assert.ok(
    [
      'capture',
      'backfill',
      'elasticsearch',
      'publisher',
      'consumer',
      'observer',
      'seed',
      'writer',
    ].includes(role),
  );
  if (expected)
    assert.equal(role, expected, 'Wrong role-specific configuration');
  const env = config['env'];
  assert.ok(env && typeof env === 'object' && !Array.isArray(env));
  for (const [name, value] of Object.entries(env)) {
    assert.match(
      name,
      /^(SOURCE|PIPELINE|CONSUMER|RABBIT|ES|BOOTSTRAP)_[A-Z_]+$/,
    );
    assert.equal(typeof value, 'string');
    assert.ok(String(value).length <= 4096);
    process.env[name] = String(value);
  }
  return role;
}
export function required(key: string): string {
  const v = process.env[key];
  if (!v) throw new Error(`Missing ${key}`);
  return v;
}
export function connection(role: string): ConnectionConfig {
  const prefix = role.toUpperCase();
  const port = Number(required(`${prefix}_PORT`));
  assert.ok(Number.isInteger(port) && port > 0 && port <= 65535);
  return {
    host: required(`${prefix}_HOST`),
    port,
    user: role,
    password: required(`${prefix}_PASSWORD`),
    database: role.startsWith('source')
      ? 'source_m1'
      : role.startsWith('consumer')
        ? 'consumer_m4'
        : 'pipeline_m2b',
    application_name: `runtime:${role}`,
  };
}
