// Pure coordination tests; real database/broker invariants remain integration requirements.
import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import {
  ReceiptObserver,
  receiptPollDelay,
} from '../../src/rabbitmq/receipts.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { RabbitLedger } from '../../src/rabbitmq/ledger.ts';
import {
  TransactionOwner,
  type ConnectionConfig,
} from '../../src/internal/transaction.ts';
import { canonicalEvent } from '../../src/envelope.ts';
const epoch = '11111111-1111-4111-8111-111111111111';
const instance = '22222222-2222-4222-8222-222222222222';
const consumerId = '33333333-3333-4333-8333-333333333333';
const registration = '44444444-4444-4444-8444-444444444444';
const config: ConnectionConfig = {
  host: 'not-contacted',
  port: 5432,
  database: 'not-contacted',
  user: 'fixture',
  password: 'not-a-secret',
  application_name: 'unit',
};
function event(id: string) {
  return canonicalEvent({
    schema_version: 1,
    source_epoch: epoch,
    entity_id: id,
    entity_version: '1',
    event_id: `${epoch}:${id}:1`,
    source_change_id: null,
    source_recorded_at: '2026-09-21T00:00:00.000000Z',
    kind: 'baseline',
    is_deleted: false,
    payload_encoding: 'pg18-jsonb-text/v1',
    payload_json: '{"exact": 9007199254740993}',
  });
}
void test('receipt polling yields on useful progress and backs off when none is committed', () => {
  assert.equal(receiptPollDelay({ observed: [] }), 1000);
  assert.equal(
    receiptPollDelay({ observed: [{ status: 'already_observed' }] }),
    1000,
  );
  assert.equal(receiptPollDelay({ observed: [{ status: 'observed' }] }), 10);
  assert.equal(
    receiptPollDelay({
      observed: [{ status: 'already_observed' }, { status: 'observed' }],
    }),
    10,
  );
});
void test('bounded receipt batch validates before writes and uses one sorted local transaction', async (t) => {
  const events = [event('2'), event('1')];
  const db = new ConsumerDatabase(config);
  t.after(() => mock.restoreAll());
  mock.method(RabbitLedger.prototype, 'target', () =>
    Promise.resolve({
      consumerId,
      pipelineId: instance,
      epoch,
      registrationId: registration,
    }),
  );
  mock.method(db, 'identity', () =>
    Promise.resolve({
      consumerId,
      pipelineId: instance,
      epoch,
      registrationId: registration,
      codec: 'pg18-jsonb-text/v1',
    }),
  );
  let corrupt = false,
    transactions = 0;
  const writes: string[] = [];
  mock.method(db, 'receipts', () =>
    Promise.resolve(
      events.map((e, i) => ({
        event_id: e.body.event_id,
        state: 'processed',
        receipt_id: e.body.event_id,
        body: e.bodyBytes,
        hash: corrupt && i === 1 ? '0'.repeat(64) : e.contentSha256,
      })),
    ),
  );
  type Work = {
    due: () => Promise<Record<string, unknown>[]>;
    observe: (...args: unknown[]) => Promise<string>;
  };
  mock.method(
    TransactionOwner.prototype,
    'transaction',
    async (fn: (work: Work) => Promise<unknown>) => {
      transactions++;
      return fn({
        due: () =>
          Promise.resolve(
            events.map((e) => ({ body: e.bodyBytes, hash: e.contentSha256 })),
          ),
        observe: (id: unknown) => {
          assert.equal(typeof id, 'string');
          writes.push(String(id));
          return Promise.resolve('observed');
        },
      });
    },
  );
  const observer = new ReceiptObserver(config, db);
  const result = await observer.once();
  assert.equal(
    transactions,
    2,
    'one bounded selection, one local observation COMMIT',
  );
  assert.deepEqual(writes, events.map((e) => e.body.event_id).sort());
  assert.equal(result.observed.length, 2);
  corrupt = true;
  transactions = 0;
  writes.length = 0;
  await assert.rejects(observer.once(), /content mismatch/);
  assert.equal(
    transactions,
    1,
    'all returned receipts must validate before observation transaction',
  );
  assert.deepEqual(writes, []);
});
