import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import pg from 'pg';
import {
  SourceOwnershipError,
  SourceTransactionError,
} from '../../src/source.ts';
import type { SourceWork } from '../../src/source.ts';
import { connect, evidence, sourceOwner } from '../support/db.ts';

async function persisted(client: pg.Client, ids: string[]) {
  return {
    entities: (
      await client.query<{
        entity_id: string;
        entity_version: string;
        payload: string;
      }>(
        'SELECT entity_id::text, entity_version::text, payload::text FROM source.entities WHERE entity_id=ANY($1::bigint[]) ORDER BY entity_id',
        [ids],
      )
    ).rows,
    outbox: (
      await client.query<{
        entity_id: string;
        entity_version: string;
        payload: string;
      }>(
        'SELECT entity_id::text, entity_version::text, payload::text FROM source.outbox WHERE entity_id=ANY($1::bigint[]) ORDER BY entity_id,entity_version',
        [ids],
      )
    ).rows,
  };
}

void test('M11-F01 caught SQL failure cannot turn rollback into caller success', async () => {
  const owner = sourceOwner('F01-fixed');
  const observer = await connect('admin', 'F01-observer');
  let id = '';
  try {
    await assert.rejects(
      owner.transaction(async (tx) => {
        id = (await tx.create('{"test":"F01"}')).entity_id;
        await assert.rejects(tx.create('[]'), { code: '23514' });
        return 'must not succeed';
      }),
      (error: unknown) => {
        assert.ok(error instanceof SourceTransactionError);
        assert.equal(error.outcome, 'rolled_back');
        assert.equal(error.sqlState, '23514');
        assert.equal(error.completionTag, 'ROLLBACK');
        assert.deepEqual(error.cleanupErrors, []);
        evidence('M11-F01-error', {
          outcome: error.outcome,
          sqlState: error.sqlState,
          tag: error.completionTag,
        });
        return true;
      },
    );
    const state = await persisted(observer, [id]);
    assert.deepEqual(state, { entities: [], outbox: [] });
    evidence('M11-F01', { id, state });
  } finally {
    await observer.end();
  }
});

void test('M11-F01-TAG actual COMMIT ROLLBACK tag is rejected by the owner', async () => {
  // Explicit receiver is restored with Reflect.apply in this private instrumentation.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const original = pg.Client.prototype.query;
  let tag: string | undefined;
  const query = mock.method(
    pg.Client.prototype,
    'query',
    async function (this: pg.Client, sql: string, values?: unknown[]) {
      // Test-only public-method instrumentation, real server SQL and real completion tag.
      const invoke = (text: string, params?: unknown[]) =>
        Reflect.apply(original, this, [text, params]) as Promise<
          pg.QueryResult<Record<string, unknown>>
        >;
      if (sql === 'COMMIT') await invoke('SELECT 1/0').catch(() => undefined);
      const result = await invoke(sql, values);
      if (sql === 'COMMIT') tag = result.command;
      return result;
    },
  );
  let id = '';
  try {
    await assert.rejects(
      sourceOwner('F01-tag').transaction(async (tx) => {
        id = (await tx.create('{}')).entity_id;
      }),
      (error: unknown) =>
        error instanceof SourceTransactionError &&
        error.outcome === 'rolled_back' &&
        error.completionTag === 'ROLLBACK',
    );
  } finally {
    query.mock.restore();
  }
  const observer = await connect('admin', 'F01-tag-observer');
  try {
    assert.equal(tag, 'ROLLBACK');
    const state = await persisted(observer, [id]);
    assert.deepEqual(state, { entities: [], outbox: [] });
    evidence('M11-F01-TAG', { tag, state });
  } finally {
    await observer.end();
  }
});

void test('M11-F02 nested and concurrent owners reject before SQL and outer rollback remains true', async () => {
  const owner = sourceOwner('F02-fixed');
  const observer = await connect('admin', 'F02-observer');
  const ids: string[] = [];
  const connectProbe = mock.method(pg.Client.prototype, 'connect');
  const queryProbe = mock.method(pg.Client.prototype, 'query');
  const guardEvidence: unknown[] = [];
  let invoked = 0;
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  try {
    const outer = owner.transaction(async (tx) => {
      ids.push((await tx.create('{}')).entity_id);
      const beforeNested = [
        connectProbe.mock.callCount(),
        queryProbe.mock.callCount(),
      ];
      await assert.rejects(
        owner.transaction(() => {
          invoked++;
          return Promise.resolve();
        }),
        SourceOwnershipError,
      );
      assert.deepEqual(
        [connectProbe.mock.callCount(), queryProbe.mock.callCount()],
        beforeNested,
      );
      guardEvidence.push({
        attempt: 'nested',
        before: beforeNested,
        after: [connectProbe.mock.callCount(), queryProbe.mock.callCount()],
      });
      entered.resolve();
      await release.promise;
      throw new Error('outer failure');
    });
    const rejected = assert.rejects(
      outer,
      (error: unknown) =>
        error instanceof SourceTransactionError &&
        error.outcome === 'rolled_back',
    );
    await entered.promise;
    const beforeConcurrent = [
      connectProbe.mock.callCount(),
      queryProbe.mock.callCount(),
    ];
    await assert.rejects(
      owner.transaction(() => {
        invoked++;
        return Promise.resolve();
      }),
      SourceOwnershipError,
    );
    assert.deepEqual(
      [connectProbe.mock.callCount(), queryProbe.mock.callCount()],
      beforeConcurrent,
    );
    guardEvidence.push({
      attempt: 'concurrent',
      before: beforeConcurrent,
      after: [connectProbe.mock.callCount(), queryProbe.mock.callCount()],
    });
    release.resolve();
    await rejected;
    assert.equal(invoked, 0);
    const state = await persisted(observer, ids);
    assert.deepEqual(state, { entities: [], outbox: [] });
    evidence('M11-F02', { invoked, ids, state, guardEvidence });
  } finally {
    release.resolve();
    connectProbe.mock.restore();
    queryProbe.mock.restore();
    await observer.end();
  }
});

void test('M11-LIFETIME success, rollback, expired capability and sequential owner reuse', async () => {
  const owner = sourceOwner('lifetime');
  const observer = await connect('admin', 'lifetime-observer');
  let expired: SourceWork | undefined;
  try {
    const first = await owner.transaction(async (tx) => {
      expired = tx;
      return tx.create('{"step":1}');
    });
    assert.ok(expired);
    await assert.rejects(expired.create('{}'), SourceOwnershipError);
    const before = await persisted(observer, [first.entity_id]);
    await assert.rejects(
      owner.transaction(async (tx) => {
        await tx.mutate(first.entity_id, 'update', '{"step":2}');
        throw new Error('rollback');
      }),
      (error: unknown) =>
        error instanceof SourceTransactionError &&
        error.outcome === 'rolled_back',
    );
    assert.deepEqual(await persisted(observer, [first.entity_id]), before);
    const next = await owner.transaction((tx) =>
      tx.mutate(first.entity_id, 'update', '{"step":3}'),
    );
    assert.equal(next.entity_version, '2');
    const state = await persisted(observer, [first.entity_id]);
    assert.deepEqual(
      state.outbox.map((r) => r.payload),
      ['{"step": 1}', '{"step": 3}'],
    );
    evidence('M11-LIFETIME', state);
  } finally {
    await observer.end();
  }
});

void test(
  'M11-DEADLOCK real 40P01 victim loses both mutations and survivor retains correct revisions',
  { timeout: 25_000 },
  async () => {
    const seed = sourceOwner('deadlock-seed');
    const observer = await connect('admin', 'deadlock-observer');
    const ids = await seed.transaction(async (tx) => [
      (await tx.create('{"initial":true}')).entity_id,
      (await tx.create('{"initial":true}')).entity_id,
    ]);
    const ready = [
      Promise.withResolvers<void>(),
      Promise.withResolvers<void>(),
    ];
    const sessions: unknown[] = [];
    try {
      const results = await Promise.all(
        [0, 1].map(async (index) => {
          const first = ids[index],
            second = ids[1 - index],
            mine = ready[index],
            other = ready[1 - index];
          assert.ok(first && second && mine && other);
          try {
            await sourceOwner(`deadlock-${index}`).transaction(async (tx) => {
              await tx.mutate(
                first,
                'update',
                JSON.stringify({ writer: index }),
              );
              sessions.push((await tx.inspect(first)).session);
              mine.resolve();
              await other.promise;
              await tx.mutate(
                second,
                'update',
                JSON.stringify({ writer: index }),
              );
            });
            return { index, status: 'committed' as const };
          } catch (error) {
            mine.resolve();
            assert.ok(error instanceof SourceTransactionError);
            assert.equal(error.sqlState, '40P01');
            assert.equal(error.outcome, 'rolled_back');
            return {
              index,
              status: 'rolled_back' as const,
              sqlState: error.sqlState,
              completionTag: error.completionTag,
            };
          }
        }),
      );
      const survivors = results.filter((r) => r.status === 'committed');
      assert.equal(survivors.length, 1);
      const survivor = survivors[0];
      assert.ok(survivor);
      assert.equal(results.filter((r) => r.status === 'rolled_back').length, 1);
      const state = await persisted(observer, ids);
      assert.equal(state.entities.length, 2);
      assert.equal(state.outbox.length, 4);
      for (const row of state.entities) {
        assert.equal(row.entity_version, '2');
        assert.equal(row.payload, `{"writer": ${survivor.index}}`);
      }
      for (const id of ids)
        assert.deepEqual(
          state.outbox
            .filter((r) => r.entity_id === id)
            .map((r) => [r.entity_version, r.payload]),
          [
            ['1', '{"initial": true}'],
            ['2', `{"writer": ${survivor.index}}`],
          ],
        );
      evidence('M11-DEADLOCK', { sessions, results, state });
    } finally {
      for (const barrier of ready) barrier.resolve();
      await observer.end();
    }
  },
);
