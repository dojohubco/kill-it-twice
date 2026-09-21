import assert from 'node:assert/strict';
import { Source } from '../../src/source.ts';
import { shape, string } from '../../src/operations/validation.ts';
import { uuid } from '../../src/envelope.ts';
import { environment, required, connection } from './environment.ts';
import { safeFailure } from './private.ts';
try {
  await environment('writer');
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of process.stdin as AsyncIterable<Buffer>) {
    assert.ok(Buffer.isBuffer(chunk));
    size += chunk.length;
    assert.ok(size <= 65536, 'Source command input exceeds 64 KiB');
    chunks.push(chunk);
  }
  const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  const r = shape(value, [
    'command_id',
    'operation',
    'entity_id',
    'payload_json',
  ]);
  const operation = string(r['operation'], 16);
  assert.ok(
    operation === 'create' ||
      operation === 'update' ||
      operation === 'delete' ||
      operation === 'restore',
  );
  assert.ok(r['entity_id'] === null || typeof r['entity_id'] === 'string');
  assert.ok(
    r['payload_json'] === null || typeof r['payload_json'] === 'string',
  );
  const result = await new Source(connection('source_command')).command({
    sourceEpoch: required('SOURCE_EPOCH'),
    commandId: uuid(r['command_id']),
    contractVersion: 1,
    operation,
    entityId: r['entity_id'],
    payloadJson: r['payload_json'],
  });
  console.log(JSON.stringify(result));
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
