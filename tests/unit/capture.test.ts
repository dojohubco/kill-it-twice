import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import { randomUUID } from 'node:crypto';
import {
  Capture,
  CaptureFailure,
  partitionClaims,
  retryDelay,
  type CaptureConfig,
} from '../../src/capture.ts';
import { SourceCapture, type Claim } from '../../src/source-capture.ts';
import { Pipeline } from '../../src/pipeline.ts';
import { SourceReader } from '../../src/source-reader.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { canonicalEvent } from '../../src/envelope.ts';
const epoch = '00000000-0000-4000-8000-000000000001';
const owner = '00000000-0000-4000-8000-000000000002';
const claim: Claim = {
  entityId: '9007199254740993',
  version: '1',
  generation: '9223372036854775807',
  ownerId: owner,
  leaseUntil: '2026-09-17T00:00:30.000000Z',
  observedAt: '2026-09-17T00:00:00.000000Z',
  transferBytes: '1024',
};
void test('capture partitions medium metadata before transfer and isolates oversized records', () => {
  const claims = [
    { ...claim, transferBytes: '65537' },
    ...Array.from({ length: 6 }, (_, i) => ({
      ...claim,
      entityId: String(i + 1),
      transferBytes: '48000',
    })),
  ];
  const result = partitionClaims(claims);
  assert.equal(result.blocked.length, 1);
  assert.deepEqual(
    result.batches.map((b) => b.length),
    [5, 1],
  );
  assert.deepEqual(result.batches.flat(), claims.slice(1));
  assert.throws(() => partitionClaims([{ ...claim, transferBytes: '0' }]));
});
void test('capture retry delay uses exact generations with positive bounded jitter', () => {
  for (const [generation, min, max] of [
    ['1', 1000, 2000],
    ['2', 2000, 4000],
    ['9223372036854775807', 15000, 30000],
  ] as const) {
    for (let i = 0; i < 20; i++) {
      const delay = retryDelay(generation);
      assert.ok(Number.isInteger(delay) && delay > min && delay <= max);
    }
  }
  assert.throws(() => retryDelay('0'));
});
for (const fault of [
  'committed cleanup failure',
  'unknown commit',
  'missing result identity',
  'missing claimed revision',
] as const)
  void test(`capture refuses acknowledgement after ${fault}`, async () => {
    const config = {
      host: 'unused.invalid',
      port: 1,
      database: 'unit',
      user: 'unit',
      password: 'unit',
      application_name: 'unit',
    };
    const cfg: CaptureConfig = {
      source: config,
      pipeline: config,
      binding: { pipelineId: randomUUID(), sourceEpoch: epoch },
    };
    const event = canonicalEvent({
      schema_version: 1,
      source_epoch: epoch,
      entity_id: claim.entityId,
      entity_version: '1',
      event_id: `${epoch}:${claim.entityId}:1`,
      source_change_id: randomUUID(),
      source_recorded_at: claim.observedAt,
      kind: 'mutation',
      is_deleted: false,
      payload_encoding: 'pg18-jsonb-text/v1',
      payload_json: '{"n": 9007199254740993}',
    });
    const mocks = [
      mock.method(SourceCapture.prototype, 'summary', () =>
        Promise.resolve({
          observed_at: claim.observedAt,
          pending_due: '1',
          pending_delayed: '0',
          leased_current: '0',
          leased_expired: '0',
          blocked: '0',
          acknowledged: '0',
          missing: '0',
        }),
      ),
      mock.method(SourceCapture.prototype, 'claim', () =>
        Promise.resolve([claim]),
      ),
      mock.method(SourceCapture.prototype, 'defer', () =>
        Promise.resolve({
          status: 'defer',
          observed_at: claim.observedAt,
          lease_until: null,
        }),
      ),
      mock.method(SourceReader.prototype, 'outbox', () =>
        Promise.resolve(
          fault === 'missing claimed revision'
            ? { events: [], notVisible: [event.body.event_id] }
            : { events: [event], notVisible: [] },
        ),
      ),
      mock.method(Pipeline.prototype, 'stage', () => {
        if (fault === 'missing result identity') return Promise.resolve([]);
        return Promise.reject(
          new TransactionError(
            new Error('controlled client failure'),
            fault === 'unknown commit' ? 'unknown' : 'committed',
            fault === 'unknown commit' ? 'commit' : 'close',
          ),
        );
      }),
    ];
    const ack = mock.method(SourceCapture.prototype, 'acknowledge', () =>
      Promise.reject(new Error('ACK must not execute')),
    );
    try {
      await assert.rejects(
        new Capture(cfg, {}, owner).captureOnce(),
        CaptureFailure,
      );
      assert.equal(ack.mock.callCount(), 0);
    } finally {
      for (const mocked of mocks) mocked.mock.restore();
      ack.mock.restore();
    }
  });
