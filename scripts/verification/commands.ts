// Bounded verification workload: each line is an ordinary source command, never direct DML.
import assert from 'node:assert/strict';
import { Source } from '../../src/source.ts';
import { environment, connection, required } from '../runtime/environment.ts';
import { uuid } from '../../src/envelope.ts';
import { record } from '../../src/operations/validation.ts';
import { safeFailure } from '../runtime/private.ts';
try {
  await environment('writer');
  const source = new Source(connection('source_command'));
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of process.stdin as AsyncIterable<Buffer>) {
    bytes += chunk.length;
    assert.ok(bytes <= 4 * 1024 * 1024, 'Bounded command input');
    chunks.push(chunk);
  }
  const input = Buffer.concat(chunks).toString('utf8').trimEnd().split('\n');
  let count = 0;
  for (const line of input) {
    assert.ok(
      ++count <= 1000 &&
        bytes <= 4 * 1024 * 1024 &&
        Buffer.byteLength(line) <= 65536,
      'Bounded workload',
    );
    const r = record(JSON.parse(line) as unknown);
    assert.deepEqual(Object.keys(r).sort(), [
      'command_id',
      'entity_id',
      'operation',
      'payload_json',
    ]);
    const operation = r['operation'];
    assert.ok(
      operation === 'create' ||
        operation === 'update' ||
        operation === 'delete' ||
        operation === 'restore',
    );
    const entity = r['entity_id'],
      payload = r['payload_json'];
    assert.ok(entity === null || typeof entity === 'string');
    assert.ok(payload === null || typeof payload === 'string');
    const result = await source.command({
      sourceEpoch: required('SOURCE_EPOCH'),
      commandId: uuid(r['command_id']),
      contractVersion: 1,
      operation,
      entityId: entity,
      payloadJson: payload,
    });
    await new Promise<void>((resolve, reject) =>
      process.stdout.write(
        JSON.stringify({ command_id: r['command_id'], ...result }) + '\n',
        (e) => (e ? reject(e) : resolve()),
      ),
    );
  }
} catch (e) {
  console.error(JSON.stringify(safeFailure(e)));
  process.exitCode = 1;
}
