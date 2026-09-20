import test from 'node:test';
import assert from 'node:assert/strict';
import {
  id,
  bigint,
  eventId,
  page,
  cursor,
  encodeCursor,
  shape,
  ControlError,
} from '../../src/operations/validation.ts';
import { metricDefinitions, metrics } from '../../src/operations/metrics.ts';
import type { Snapshot } from '../../src/operations/service.ts';
await test('operational input validators reject unsafe conversions and malformed cursor scope', () => {
  const epoch = '12345678-1234-4234-8234-123456789abc';
  assert.equal(id(epoch), epoch);
  assert.equal(bigint('9223372036854775807'), '9223372036854775807');
  assert.equal(eventId(`${epoch}:1:2`), `${epoch}:1:2`);
  for (const v of [
    '0',
    '-1',
    '01',
    '1e3',
    '9223372036854775808',
    1,
    null,
    {},
    '1'.repeat(10000),
  ])
    assert.throws(() => bigint(v), ControlError);
  for (const q of [
    { limit: '101' },
    { limit: '1e2' },
    { limit: ['1'] },
    { limit: '0' },
    { extra: 'x' },
  ])
    assert.throws(() => page(q), ControlError);
  assert.deepEqual(page({ limit: '100' }), { limit: 100, cursor: undefined });
  const encoded = encodeCursor('failures', { after: 'es-active:a' });
  assert.equal(cursor(encoded, 'failures')?.['after'], 'es-active:a');
  assert.throws(() => cursor(encoded, 'entities'), ControlError);
  assert.throws(
    () => shape({ fixture: 'fixture-01', sql: 'SELECT' }, ['fixture']),
    ControlError,
  );
});
await test('operational metrics omit unavailable facts and use immutable observation timestamps', () => {
  const s: Snapshot = {
    observed_at: '2026-09-20T00:00:10Z',
    health: 'unavailable',
    backfill: null,
    dependencies: {
      source: {
        observed_at: null,
        freshness: 'unavailable',
        health: 'unavailable',
        data: null,
      },
    },
  };
  const missing = metrics(s);
  assert.match(missing, /pipeline_source_reachable 0/);
  assert.doesNotMatch(missing, /^pipeline_source_pending /m);
  for (const [name, type, help] of metricDefinitions) {
    assert.ok(missing.includes(`# HELP ${name} ${help}\n`));
    assert.ok(missing.includes(`# TYPE ${name} ${type}\n`));
  }
  s.dependencies['source'] = {
    observed_at: '2026-09-20T00:00:00Z',
    freshness: 'fresh',
    health: 'healthy',
    data: {
      pending: '0',
      oldest_pending_at: null,
      counts: {
        pending_delayed: '0',
        leased_current: '0',
        leased_expired: '0',
        blocked: '0',
        missing: '0',
      },
    },
  };
  const fresh = metrics(s);
  assert.match(fresh, /pipeline_source_pending 0/);
  assert.doesNotMatch(
    fresh,
    /^pipeline_source_oldest_pending_timestamp_seconds /m,
  );
  assert.ok(
    fresh.includes(
      `pipeline_source_last_observation_timestamp_seconds ${Date.parse('2026-09-20T00:00:00Z') / 1000}\n`,
    ),
  );
  s.observed_at = '2026-09-20T00:00:20Z';
  assert.equal(metrics(s), fresh);
});

await test('operational resource cleanup retains both primary and secondary evidence', async () => {
  const { withOperationalCleanup, OperationalCleanupError } =
    await import('../../src/operations/cleanup.ts');
  const primary = new Error('private primary'),
    secondary = new Error('private cleanup');
  await assert.rejects(
    withOperationalCleanup(
      () => Promise.reject(primary),
      () => Promise.reject(secondary),
    ),
    (e: unknown) =>
      e instanceof OperationalCleanupError &&
      e.primary === primary &&
      e.cleanup[0] === secondary,
  );
  await assert.rejects(
    withOperationalCleanup(
      () => Promise.reject(primary),
      () => Promise.resolve(),
    ),
    (e) => e === primary,
  );
  assert.equal(
    await withOperationalCleanup(
      () => Promise.resolve(42),
      () => Promise.resolve(),
    ),
    42,
  );
});
