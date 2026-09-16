// Historical reproductions, separately invoked. A passing reproduction confirms a bug.
import assert from 'node:assert/strict';
import test from 'node:test';
import { createEntity, transaction, SourceTransactionError } from './legacy-source.ts';
import { connect, evidence } from '../support/db.ts';

test('F01 baseline returns success for PostgreSQL ROLLBACK completion', async () => {
  const writer = await connect('writer', 'F01');
  const observer = await connect('admin', 'F01-observer');
  const tags: string[] = [];
  // Instrument the public query result, without pg internal state.
  const query = writer.query.bind(writer);
  writer.query = async (...args: Parameters<typeof query>) => {
    const result = await query(...args);
    if (!Array.isArray(result)) tags.push(result.command);
    return result;
  };
  let id = '';
  let sqlState: unknown;
  try {
    const value = await transaction(writer, async (c) => {
      id = (await createEntity(c, '{"reproduction":"F01"}')).entity_id;
      try { await c.query('SELECT 1/0'); }
      catch (error) { assert.ok(error instanceof Error && 'code' in error); sqlState = error.code; }
      return 'incorrect-success';
    });
    const persisted = (await observer.query('SELECT (SELECT count(*)::text FROM source.entities WHERE entity_id=$1) AS entities, (SELECT count(*)::text FROM source.outbox WHERE entity_id=$1) AS outbox', [id])).rows;
    assert.equal(value, 'incorrect-success');
    assert.equal(tags.at(-1), 'ROLLBACK');
    assert.deepEqual(persisted, [{ entities: '0', outbox: '0' }]);
    evidence('F01-reproduced', { value, tags, sqlState, id, persisted });
  } finally { await writer.end(); await observer.end(); }
});

test('F02 baseline nested and concurrent calls prematurely commit outer work', async () => {
  for (const mode of ['nested', 'concurrent']) {
    const writer = await connect('writer', `F02-${mode}`);
    const observer = await connect('admin', `F02-${mode}-observer`);
    let id = '';
    let release: () => void = () => undefined;
    const released = new Promise<void>((resolve) => { release = resolve; });
    let entered: () => void = () => undefined;
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    try {
      const outer = transaction(writer, async (c) => {
        id = (await createEntity(c, JSON.stringify({ reproduction: 'F02', mode }))).entity_id;
        entered();
        if (mode === 'nested') await transaction(c, async () => 'inner success');
        else await released;
        throw new Error('outer failure after inner success');
      }).catch((error: unknown) => error);
      await ready;
      if (mode === 'concurrent') { await transaction(writer, async () => 'overlap success'); release(); }
      const error = await outer;
      assert.ok(error instanceof SourceTransactionError);
      assert.equal(error.outcome, 'rolled_back');
      const persisted = (await observer.query('SELECT (SELECT count(*)::text FROM source.entities WHERE entity_id=$1) AS entities, (SELECT count(*)::text FROM source.outbox WHERE entity_id=$1) AS outbox', [id])).rows;
      assert.deepEqual(persisted, [{ entities: '1', outbox: '1' }]);
      evidence('F02-reproduced', { mode, claimedOutcome: error.outcome, id, persisted });
    } finally { release(); await writer.end(); await observer.end(); }
  }
});
