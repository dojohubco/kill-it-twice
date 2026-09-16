import assert from 'node:assert/strict';
export function first<T>(rows: readonly T[]): T {
  const row = rows[0];
  assert.ok(row !== undefined, 'Expected a database row');
  return row;
}
