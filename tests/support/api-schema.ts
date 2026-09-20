import assert from 'node:assert/strict';
import { record } from '../../src/operations/validation.ts';
// Validate actual HTTP samples against the response schemas generated from controllers.
// Only the JSON Schema keywords used by our response contract are needed here.
function validateSample(
  value: unknown,
  schema: Record<string, unknown>,
  path = '$',
): void {
  if (schema['nullable'] === true && value === null) return;
  if (Array.isArray(schema['enum']))
    assert.ok(schema['enum'].includes(value), path);
  const kind = schema['type'];
  if (kind === 'object') {
    const r = record(value);
    const properties =
      schema['properties'] === undefined ? {} : record(schema['properties']);
    if (Array.isArray(schema['required']))
      for (const key of schema['required']) {
        assert.equal(typeof key, 'string');
        assert.ok(
          Object.hasOwn(r, String(key)),
          `${path}.${String(key)} required`,
        );
      }
    for (const [key, child] of Object.entries(properties))
      if (Object.hasOwn(r, key))
        validateSample(r[key], record(child), `${path}.${key}`);
  } else if (kind === 'array') {
    assert.ok(Array.isArray(value), path);
    if (typeof schema['maxItems'] === 'number')
      assert.ok(value.length <= schema['maxItems'], path);
    for (const child of value)
      validateSample(child, record(schema['items']), path + '[]');
  } else if (kind === 'string') {
    assert.equal(typeof value, 'string', path);
    const text = String(value);
    if (typeof schema['pattern'] === 'string')
      assert.match(text, new RegExp(schema['pattern']), path);
    if (typeof schema['maxLength'] === 'number')
      assert.ok(text.length <= schema['maxLength'], path);
    if (schema['format'] === 'uuid')
      assert.match(
        text,
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
        path,
      );
    if (schema['format'] === 'date-time')
      assert.ok(Number.isFinite(Date.parse(text)), path);
  } else if (kind === 'boolean') assert.equal(typeof value, 'boolean', path);
  else if (kind === 'number') assert.equal(typeof value, 'number', path);
}
export function validateResponse(
  document: Record<string, unknown>,
  path: string,
  method: string,
  status: number,
  value: unknown,
) {
  if (path === '/metrics' || path === '/api/v1/openapi.json') return;
  const pathname = path.split('?')[0] ?? '';
  const routes = record(document['paths']);
  const route = Object.keys(routes).find((p) =>
    new RegExp('^' + p.replaceAll(/\{[^}]+\}/g, '[^/]+') + '$').test(pathname),
  );
  if (!route) return; // Unknown routes are intentionally tested for 404.
  const op = record(routes[route])[method.toLowerCase()];
  if (!op) return;
  const response = record(record(op)['responses'])[String(status)];
  assert.ok(response, `Undocumented response ${method} ${route} ${status}`);
  const content = record(response)['content'];
  if (!content) return;
  validateSample(
    value,
    record(record(record(content)['application/json'])['schema']),
  );
}
