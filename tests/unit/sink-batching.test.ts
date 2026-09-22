// Coordination/unit tests only; real SQL and receiver faults are checked separately.
import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import {
  orderedSinkBatch,
  sinkChunks,
  sinkPollDelay,
} from '../../src/internal/sink-batch.ts';
import {
  EsLedger,
  type EsClaim,
  type EsSettlement,
  type Target,
} from '../../src/es/ledger.ts';
import {
  RabbitLedger,
  type RabbitClaim,
  type RabbitSettlement,
  type RabbitTarget,
} from '../../src/rabbitmq/ledger.ts';
import {
  TransactionOwner,
  type ConnectionConfig,
} from '../../src/internal/transaction.ts';
const config: ConnectionConfig = {
  host: 'not-contacted',
  port: 5432,
  database: 'fixture',
  user: 'fixture',
  password: 'not-a-secret',
  application_name: 'unit',
};
const epoch = '11111111-1111-4111-8111-111111111111';
const id = (n: number) => `${epoch}:${n}:1`;
const target: Target = {
  id: epoch,
  generation: '1',
  pipelineId: epoch,
  epoch,
  index: 'fixture',
  indexUuid: 'fixture',
  clusterUuid: 'fixture',
  configuration: {},
  mode: 'ready',
  failures: '0',
};
const rabbitTarget: RabbitTarget = {
  id: epoch,
  generation: '1',
  mode: 'ready',
  registrationId: epoch,
  consumerId: epoch,
  pipelineId: epoch,
  epoch,
  vhost: 'fixture',
  queue: 'fixture',
  exchange: 'fixture',
  routingKey: 'fixture',
};
function claim(n: number): EsClaim {
  return {
    eventId: id(n),
    generation: '9007199254740993',
    attemptId: epoch,
    bytes: '100',
    probeGeneration: '0',
    backendPid: '1',
    transactionId: '1',
    claimedAt: '2026-09-22T00:00:00Z',
  };
}
void test('sink groups are bounded, unique and lock in deterministic identity order', () => {
  assert.deepEqual(
    orderedSinkBatch([id(3), id(1), id(2)], (x) => x),
    [id(1), id(2), id(3)],
  );
  assert.deepEqual(sinkChunks([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  for (const n of [0, 33, 1.5, NaN, Infinity])
    assert.throws(() => sinkChunks([1], n));
  assert.throws(() => orderedSinkBatch([], (x) => String(x)));
  assert.throws(() => orderedSinkBatch([id(1), id(1)], (x) => x), /Duplicate/);
  assert.throws(() =>
    orderedSinkBatch(
      Array.from({ length: 33 }, (_, n) => id(n)),
      (x) => x,
    ),
  );
  assert.equal(sinkPollDelay(0, 32, 1000), 1000);
  assert.equal(sinkPollDelay(5, 1, 1000), 1000);
  assert.equal(sinkPollDelay(5, 32, 1000), 10);
});
void test('ES local groups validate every result and preserve per-item stale outcomes', async (t) => {
  t.after(() => mock.restoreAll());
  let transactions = 0;
  const calls: string[] = [];
  let invalid = false;
  type Work = {
    settle: (
      target: Target,
      c: EsClaim,
      ...args: unknown[]
    ) => Promise<string | undefined>;
    renew: (target: Target, c: EsClaim, ...args: unknown[]) => Promise<boolean>;
  };
  mock.method(
    TransactionOwner.prototype,
    'transaction',
    async (fn: (work: Work) => Promise<unknown>) => {
      transactions++;
      return fn({
        settle: (_t, c) => {
          calls.push(c.eventId);
          return Promise.resolve(
            invalid ? undefined : c.eventId === id(2) ? 'stale' : 'settled',
          );
        },
        renew: (_t, c) => Promise.resolve(c.eventId !== id(2)),
      });
    },
  );
  const ledger = new EsLedger(config);
  const items: EsSettlement[] = [3, 2, 1].map((n) => ({
    claim: claim(n),
    outcome: 'applied',
    remote: '1',
    witness: id(n),
    context: 'verified unit result',
    delay: 1,
  }));
  assert.deepEqual(await ledger.settleMany(target, epoch, items), [
    { eventId: id(1), status: 'settled' },
    { eventId: id(2), status: 'stale' },
    { eventId: id(3), status: 'settled' },
  ]);
  assert.equal(transactions, 1);
  assert.deepEqual(calls, [id(1), id(2), id(3)]);
  assert.equal(items[0]?.claim.generation, '9007199254740993');
  assert.deepEqual(
    await ledger.renewMany(
      target,
      items.map((x) => x.claim),
      epoch,
      30000,
    ),
    [
      { eventId: id(1), renewed: true },
      { eventId: id(2), renewed: false },
      { eventId: id(3), renewed: true },
    ],
  );
  invalid = true;
  await assert.rejects(
    ledger.settleMany(target, epoch, items),
    /Invalid ES settlement/,
  );
  const before = transactions;
  await assert.rejects(ledger.settleMany(target, epoch, []));
  assert.equal(transactions, before);
});
void test('broker local groups use one transaction and do not treat stale as new confirmation', async (t) => {
  t.after(() => mock.restoreAll());
  let transactions = 0;
  type Work = {
    settle: (
      target: RabbitTarget,
      c: RabbitClaim,
      ...args: unknown[]
    ) => Promise<string>;
    renew: (
      target: RabbitTarget,
      c: RabbitClaim,
      ...args: unknown[]
    ) => Promise<boolean>;
  };
  mock.method(
    TransactionOwner.prototype,
    'transaction',
    async (fn: (work: Work) => Promise<unknown>) => {
      transactions++;
      return fn({
        settle: (_t, c) =>
          Promise.resolve(c.eventId === id(2) ? 'stale' : 'settled'),
        renew: () => Promise.resolve(true),
      });
    },
  );
  const ledger = new RabbitLedger(config);
  const items: RabbitSettlement[] = [3, 2, 1].map((n) => ({
    claim: claim(n),
    outcome: 'confirmed',
    channel: epoch,
    context: 'actual protocol evidence in integration',
    delay: 1,
  }));
  const result = await ledger.settleMany(rabbitTarget, epoch, items);
  assert.equal(transactions, 1);
  assert.deepEqual(
    result.map((x) => x.status),
    ['settled', 'stale', 'settled'],
  );
  assert.deepEqual(
    result.map((x) => x.eventId),
    [id(1), id(2), id(3)],
  );
  assert.equal(
    (
      await ledger.renewMany(
        rabbitTarget,
        items.map((x) => x.claim),
        epoch,
        30000,
      )
    ).length,
    3,
  );
});
void test('projection cache preserves identities and refuses over-limit transfers before another read', async (t) => {
  t.after(() => mock.restoreAll());
  let count = 0;
  let wrong = false;
  let oversized = false;
  type Work = { read: (id: string) => Promise<Record<string, unknown>[]> };
  mock.method(
    TransactionOwner.prototype,
    'transaction',
    async (fn: (work: Work) => Promise<unknown>) =>
      fn({
        read: (key) => {
          count++;
          return Promise.resolve([
            {
              event_id: wrong ? id(99) : key,
              document_id: key.slice(0, -2),
              version: '9007199254740993',
              projection: oversized ? null : '{}',
              bytes: oversized ? '4194305' : '2',
            },
          ]);
        },
      }),
  );
  const ledger = new EsLedger(config);
  const values = await ledger.readBatch([id(2), id(1)]);
  assert.deepEqual(
    values.map((v) => v.eventId),
    [id(1), id(2)],
  );
  assert.equal(values[0]?.version, '9007199254740993');
  const previous = count;
  await assert.rejects(
    ledger.readBatch(Array.from({ length: 17 }, (_, n) => id(n))),
  );
  assert.equal(count, previous);
  wrong = true;
  await assert.rejects(ledger.readBatch([id(1)]), /Wrong bounded projection/);
  wrong = false;
  oversized = true;
  count = 0;
  await assert.rejects(ledger.readBatch([id(1), id(2)]), /cache exceeds/);
  assert.equal(count, 1);
});
