// Pure scheduling regression; mock results do not represent receiver execution.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Delivery } from '../../src/es/worker.ts';
import { EsLedger, type EsClaim, type Target } from '../../src/es/ledger.ts';
import { EsAdapter, type ItemOutcome } from '../../src/es/adapter.ts';
import { EsTransport } from '../../src/es/transport.ts';
import type { Projection } from '../../src/es/projection.ts';
import { orderedSinkBatch } from '../../src/internal/sink-batch.ts';
const epoch = '11111111-1111-4111-8111-111111111111';
void test('a claim lost during bulk flush never produces an empty prefetch or false cooldown', async (t) => {
  const transport = new EsTransport({
    node: 'https://127.0.0.1:9',
    username: 'fixture',
    password: 'not-secret',
    ca: '',
  });
  t.after(() => transport.close());
  const ledger = new EsLedger({
    host: 'not-contacted',
    port: 5432,
    database: 'fixture',
    user: 'fixture',
    password: 'not-secret',
    application_name: 'unit',
  });
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
  const adapter = new EsAdapter(transport);
  const json = JSON.stringify({ padding: 'x'.repeat(261680) });
  const bytes = String(Buffer.byteLength(json));
  const claims: EsClaim[] = Array.from({ length: 17 }, (_, i) => ({
    eventId: `${epoch}:${i + 1}:1`,
    generation: '1',
    attemptId: epoch,
    bytes,
    probeGeneration: '0',
    backendPid: '1',
    transactionId: '1',
    claimedAt: '2026-09-22T00:00:00Z',
  }));
  const lost = claims[16];
  assert.ok(lost);
  let flushing = false;
  let release: () => void = () => {
    throw new Error('No pending flush');
  };
  const renewed = new Promise<void>((resolve) => {
    release = resolve;
  });
  const reads: number[] = [];
  const admission: string[] = [];
  t.mock.method(ledger, 'target', () => Promise.resolve(target));
  t.mock.method(ledger, 'claim', () => Promise.resolve(claims));
  t.mock.method(ledger, 'readBatch', (ids: string[]) => {
    reads.push(ids.length);
    return Promise.resolve(
      orderedSinkBatch(ids, (x) => x, 16).map((eventId) => ({
        eventId,
        documentId: eventId.slice(0, -2),
        version: '1',
        json,
        bytes,
      })),
    );
  });
  t.mock.method(ledger, 'renewMany', (_t: Target, group: EsClaim[]) => {
    assert.ok(flushing);
    release();
    return Promise.resolve(
      group.map((c) => ({
        eventId: c.eventId,
        renewed: c.eventId !== lost.eventId,
      })),
    );
  });
  t.mock.method(
    ledger,
    'settleMany',
    (_t: Target, _owner: string, items: { claim: EsClaim }[]) =>
      Promise.resolve(
        items.map((x) => ({ eventId: x.claim.eventId, status: 'settled' })),
      ),
  );
  t.mock.method(ledger, 'admission', (...args: unknown[]) => {
    admission.push(String(args[4]));
    return Promise.resolve(true);
  });
  t.mock.method(ledger, 'status', () => Promise.resolve({}));
  t.mock.method(adapter, 'validate', () => Promise.resolve());
  t.mock.method(
    adapter,
    'bulk',
    async (_t: Target, projections: Projection[]): Promise<ItemOutcome[]> => {
      assert.equal(projections.length, 16);
      flushing = true;
      await renewed;
      return projections.map((projection) => ({
        projection,
        outcome: 'applied',
        remote: '1',
        witness: projection.eventId,
        context: 'unit outcome',
      }));
    },
  );
  t.mock.method(adapter, 'resolve', (_t: Target, item: ItemOutcome) =>
    Promise.resolve(item),
  );
  const result = await new Delivery(ledger, adapter, {
    count: 17,
    databaseBatchSize: 32,
    leaseMs: 600,
    renewalMs: 50,
  }).once();
  assert.equal(result.outcomes.length, 16);
  assert.deepEqual(
    reads,
    [16],
    'Lost next claim must not become a spurious invalid read request',
  );
  assert.deepEqual(admission, ['healthy']);
});
