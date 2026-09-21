import { OperationsService } from '../../src/operations/service.ts';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { command, withCleanup } from '../../scripts/support.ts';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import {
  RecoveryService,
  type RecoveryRequest,
} from '../../src/operations/recovery.ts';
import { OperationalMonitor } from '../../src/operations/monitor.ts';
import type { OperationsConfig } from '../../src/operations/config.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import { record, string } from '../../src/operations/validation.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { exactJson } from '../../src/es/projection.ts';
import { create, esConfig, worker } from './es.ts';
import { drain as captureDrain } from './capture.ts';
import { required, evidence } from './db.ts';
import { retained, objects } from './operations.ts';
import { recoveryFaults } from './recovery-faults.ts';

interface Context {
  db: { s: pg.Client; p: pg.Client; c: pg.Client };
  cfg: OperationsConfig;
  deliver: () => Promise<void>;
}
export function recoveryCaseDefinitions(context: () => Context) {
  const cases: { id: string; run: () => Promise<void> }[] = [];
  const bads: Awaited<ReturnType<typeof create>>[] = [];
  let batch: RecoveryRequest;
  let original: unknown;
  let failedSnapshot: unknown;
  async function failure(event: string) {
    const row = (
      await context().db.p.query<{
        attempt: string;
        destination: string;
        generation: string;
      }>(
        "SELECT d.attempt_id::text attempt,d.destination_id::text destination,t.generation::text generation FROM pipeline.delivery_intents d JOIN pipeline.es_target t USING(destination_id) WHERE d.event_id=$1 AND d.kind='elasticsearch' AND d.state='dead_letter'",
        [event],
      )
    ).rows[0];
    assert.ok(row);
    return row;
  }
  async function requestFor(
    event: string,
    reason = 'explicit recovery check',
  ): Promise<RecoveryRequest> {
    const f = await failure(event);
    return {
      request_id: randomUUID(),
      actor: 'acceptance-operator',
      reason,
      destination_id: f.destination,
      generation: f.generation,
      selection: [{ event_id: event, attempt_id: f.attempt }],
    };
  }
  const svc = () => new RecoveryService(context().cfg);
  const state = (value: unknown) => record(value)['state'];
  const sqlState = (wanted: string) => (e: unknown) =>
    e instanceof TransactionError && e.sqlState === wanted;
  async function nonEs() {
    const { db } = context();
    return {
      source: await retained(db.s, [
        'source.entities',
        'source.outbox',
        'source.capture_work',
        'source.command_receipts',
      ]),
      consumer: await retained(db.c, [
        'consumer.processed_events',
        'consumer.mutation_effects',
        'consumer.entity_totals',
      ]),
      events: await retained(db.p, ['pipeline.events']),
      rabbit: (
        await db.p.query<{ value: string }>(
          "SELECT row_to_json(d)::text value FROM pipeline.delivery_intents d WHERE kind='rabbitmq' ORDER BY event_id",
        )
      ).rows,
      observations: await retained(db.p, ['pipeline.consumer_observations']),
    };
  }
  cases.push({
    id: 'RC01',
    run: async () => {
      const { db, deliver } = context();
      for (let i = 0; i < 3; i++)
        bads.push(
          await create(
            JSON.stringify({
              name: `recovery fixture ${i}`,
              loyalty_points: 'not-a-number',
            }),
          ),
        );
      await deliver();
      const first = bads[0],
        second = bads[1],
        third = bads[2];
      assert.ok(first && second && third);
      const r1 = await requestFor(first.eventId),
        r2 = await requestFor(second.eventId);
      batch = { ...r1, selection: [...r2.selection, ...r1.selection] };
      const before = await nonEs();
      const count = async () =>
        (
          await db.p.query<{ n: string }>(
            'SELECT count(*)::text n FROM pipeline.recovery_requests',
          )
        ).rows[0]?.n;
      const oldCount = await count();
      await assert.rejects(
        svc().replay({
          ...batch,
          request_id: randomUUID(),
          selection: [
            ...r1.selection,
            {
              event_id: `${required('SOURCE_EPOCH')}:9223372036854775807:1`,
              attempt_id: randomUUID(),
            },
          ],
        }),
        sqlState('P9001'),
      );
      assert.equal(await count(), oldCount);
      original = await retained(db.p, ['pipeline.es_dead_letters']);
      const admitted = await svc().replay(batch);
      assert.equal(state(admitted), 'pending');
      assert.equal(objects(admitted['items']).length, 2);
      const duplicate = await svc().replay({
        ...batch,
        selection: [...batch.selection].reverse(),
      });
      assert.equal(duplicate['replayed'], true);
      assert.equal(duplicate['created_at'], admitted['created_at']);
      await assert.rejects(
        svc().replay({ ...batch, reason: 'different intent' }),
        sqlState('P9001'),
      );
      await assert.rejects(
        svc().replay({ ...batch, request_id: randomUUID() }),
        sqlState('P9001'),
      );
      const competing = await requestFor(third.eventId);
      const races = await Promise.allSettled([
        svc().replay(competing),
        svc().replay({ ...competing, request_id: randomUUID() }),
      ]);
      assert.equal(races.filter((r) => r.status === 'fulfilled').length, 1);
      assert.equal(races.filter((r) => r.status === 'rejected').length, 1);
      assert.deepEqual(await nonEs(), before);
      evidence('RC01', {
        batch: admitted,
        duplicate,
        races: races.map((r) => r.status),
        scope:
          'actual concurrent operator sessions; exactly one admitted current failure',
      });
    },
  });
  cases.push({
    id: 'RC02',
    run: async () => {
      const { db, deliver } = context();
      const before = await nonEs();
      await deliver();
      const status = await svc().status(batch.request_id);
      assert.equal(status['state'], 'complete_with_errors');
      for (const item of objects(status['items'])) {
        assert.equal(item['state'], 'failed');
        assert.equal(item['outcome'], 'mapping');
        assert.equal(item['attempts'], '1');
        assert.notEqual(item['latest_attempt'], item['expected_attempt']);
      }
      const old = record(original)['pipeline.es_dead_letters'];
      const actual = record(await retained(db.p, ['pipeline.es_dead_letters']))[
        'pipeline.es_dead_letters'
      ];
      for (const value of objects(old))
        assert.ok(objects(actual).some((v) => v['value'] === value['value']));
      assert.deepEqual(await nonEs(), before);
      const duplicate = await svc().replay(batch);
      assert.equal(duplicate['replayed'], true);
      assert.equal(duplicate['state'], 'complete_with_errors');
      failedSnapshot = status;
      evidence('RC02', {
        status,
        retainedOriginalFailures: objects(old).length,
        currentFailures: objects(actual).length,
      });
    },
  });
  cases.push({
    id: 'RC03',
    run: async () => {
      const { db, deliver, cfg } = context();
      const first = bads[0],
        second = bads[1],
        third = bads[2];
      assert.ok(first && second && third);
      await create(
        '{"name":"corrected first","loyalty_points":73}',
        first.id,
        'update',
      );
      const corrected = await create(
        '{"name":"corrected second","loyalty_points":74}',
        second.id,
        'update',
      );
      await deliver();
      const history = await retained(db.p, ['pipeline.es_dead_letters']);
      const external = await nonEs();
      const result = await svc().supersede(await requestFor(first.eventId));
      assert.equal(result['state'], 'passed');
      assert.equal(
        objects(result['items'])[0]?.['witness_event_id'],
        `${first.documentId}:2`,
      );
      assert.equal(
        (await svc().status(batch.request_id))['state'],
        record(failedSnapshot)['state'],
      );
      const admin = new EsTransport(esConfig(true));
      try {
        const doc = record(
          await admin.request(
            'GET',
            `/${required('ES_INDEX')}/_doc/${corrected.documentId}`,
          ),
        );
        const body = record(doc['_source']);
        await admin.request(
          'PUT',
          `/${required('ES_INDEX')}/_doc/${corrected.documentId}?version=2&version_type=external_gte`,
          exactJson({
            ...body,
            search_fields: {
              ...record(body['search_fields']),
              name: 'corrupted copied hash',
            },
          }),
        );
        const corrupt = await svc().supersede(await requestFor(second.eventId));
        assert.equal(corrupt['state'], 'failed');
        assert.equal(
          objects(corrupt['items'])[0]?.['error_class'],
          'integrity',
        );
        await admin.request(
          'PUT',
          `/${required('ES_INDEX')}/_doc/${corrected.documentId}?version=2&version_type=external_gte`,
          exactJson(body),
        );
        const network = new OperationsService(cfg);
        const unavailable = await withCleanup(
          async () => {
            await network.network(randomUUID(), randomUUID(), 'elasticsearch', {
              state: 'disconnected',
            });
            const remoteStillPresent = record(
              await admin.request(
                'GET',
                `/${required('ES_INDEX')}/_doc/${corrected.documentId}`,
              ),
            );
            assert.equal(
              record(remoteStillPresent['_source'])['content_sha256'],
              body['content_sha256'],
            );
            const failed = await svc().supersede(
              await requestFor(second.eventId),
            );
            assert.equal(failed['state'], 'failed');
            assert.equal(
              objects(failed['items'])[0]?.['error_class'],
              'transient',
            );
            return failed;
          },
          async () => {
            await network.network(randomUUID(), randomUUID(), 'elasticsearch', {
              state: 'connected',
            });
          },
        );
        const repaired = await svc().supersede(
          await requestFor(second.eventId),
        );
        assert.equal(repaired['state'], 'passed');
        const absent = await svc().supersede(await requestFor(third.eventId));
        assert.equal(absent['state'], 'failed');
        const badCredentials = new RecoveryService({
          ...cfg,
          es: { ...cfg.es, password: 'deliberately-wrong-fixture-password' },
        });
        const unauthorized = await badCredentials.supersede(
          await requestFor(third.eventId),
        );
        assert.equal(unauthorized['state'], 'failed');
        assert.equal(
          objects(unauthorized['items'])[0]?.['error_class'],
          'auth',
        );
        evidence('RC03', {
          result,
          corrupt,
          repaired,
          absent,
          unauthorized,
          unavailable,
          negativeControl:
            'privileged exact copied-hash projection corruption, restored on owned index',
        });
      } finally {
        await admin.close();
      }
      assert.deepEqual(
        await retained(db.p, ['pipeline.es_dead_letters']),
        history,
      );
      assert.deepEqual(await nonEs(), external);
    },
  });
  cases.push({
    id: 'RC04',
    run: async () => {
      const { db, deliver, cfg } = context();
      const fixture = await create(
        '{"name":"target probe","loyalty_points":10}',
      );
      await captureDrain();
      const bad = new EsTransport({
        ...esConfig(),
        password: 'invalid-probe-password',
      });
      try {
        await assert.rejects(
          worker(bad).once(),
          /Receiver credential rejected/,
        );
      } finally {
        await bad.close();
      }
      const t = (
        await db.p.query<{
          id: string;
          generation: string;
          mode: string;
          uuid: string;
        }>(
          'SELECT destination_id::text id,generation::text,mode,index_uuid uuid FROM pipeline.es_target',
        )
      ).rows[0];
      assert.ok(t);
      assert.equal(t.mode, 'blocked');
      const input = {
        request_id: randomUUID(),
        actor: 'target-operator',
        reason: 'verify retained same receiver',
        destination_id: t.id,
        generation: t.generation,
        selection: [],
      };
      const badService = new RecoveryService({
        ...cfg,
        es: { ...cfg.es, password: 'invalid-probe-password' },
      });
      const denied = await badService.verifyTarget(input);
      assert.equal(denied['state'], 'failed');
      assert.equal(
        (
          await db.p.query<{ mode: string }>(
            'SELECT mode FROM pipeline.es_target',
          )
        ).rows[0]?.mode,
        'blocked',
      );
      const passed = await svc().verifyTarget({
        ...input,
        request_id: randomUUID(),
      });
      assert.equal(passed['state'], 'passed');
      const replay = await svc().verifyTarget({
        ...input,
        request_id: string(passed['request_id']),
      });
      assert.equal(replay['replayed'], true);
      await deliver();
      assert.equal(
        (
          await db.p.query<{ state: string }>(
            "SELECT state FROM pipeline.delivery_intents WHERE event_id=$1 AND kind='elasticsearch'",
            [fixture.eventId],
          )
        ).rows[0]?.state,
        'satisfied',
      );
      assert.equal(
        (
          await db.p.query<{ uuid: string }>(
            'SELECT index_uuid uuid FROM pipeline.es_target',
          )
        ).rows[0]?.uuid,
        t.uuid,
      );
      evidence('RC04', { denied, passed, originalIndexUuid: t.uuid });
    },
  });
  cases.push({
    id: 'RC06',
    run: async () => {
      await recoveryFaults(context());
    },
  });
  cases.push({
    id: 'RC07',
    run: async () => {
      const { cfg, db } = context();
      const keys: string[] = [];
      let next: unknown = undefined;
      do {
        const page = await svc().list('operations', {
          limit: 2,
          ...(next === undefined ? {} : { cursor: next }),
        });
        assert.ok(Buffer.byteLength(JSON.stringify(page)) <= 262144);
        keys.push(...objects(page['items']).map((r) => string(r['key'])));
        next = page['next_cursor'] ?? undefined;
      } while (next !== undefined);
      assert.equal(new Set(keys).size, keys.length);
      const expected = (
        await db.p.query<{ key: string }>(
          "SELECT 'operation:'||request_id key FROM pipeline.recovery_requests ORDER BY request_id",
        )
      ).rows.map((r) => r.key);
      assert.deepEqual(keys.sort(), expected.sort());
      const b = bads[0];
      assert.ok(b);
      const attempts = await svc().list('attempts', {
        event_id: b.eventId,
        limit: 100,
      });
      assert.ok(objects(attempts['items']).length >= 3);
      const role = new pg.Client(cfg.pipeline);
      await role.connect();
      try {
        for (const sql of [
          "UPDATE pipeline.recovery_requests SET reason='forged'",
          'DELETE FROM pipeline.recovery_items',
          'DELETE FROM pipeline.replay_attempt_links',
          "UPDATE pipeline.es_target SET mode='ready'",
          'TRUNCATE pipeline.recovery_checks',
        ])
          await assert.rejects(
            role.query(sql),
            (e: unknown) => e instanceof pg.DatabaseError && e.code === '42501',
          );
      } finally {
        await role.end();
      }
      await assert.rejects(
        svc().list('attempts', { event_id: b.eventId, limit: 101 }),
      );
      const dir = await mkdtemp(join(tmpdir(), 'kit-operator-cli-'));
      const cli = await withCleanup(
        async () => {
          const file = join(dir, 'config.json');
          await writeFile(file, JSON.stringify(cfg), { mode: 0o600 });
          const reply = await command(
            process.execPath,
            ['scripts/operations.ts', 'operation', batch.request_id],
            { ...process.env, CONTROL_CONFIG_FILE: file },
            20000,
            true,
            {
              maxOutputBytes: 262144,
              secrets: [
                cfg.token,
                cfg.source.password,
                cfg.pipeline.password,
                cfg.consumer.password,
                cfg.es.password,
              ],
            },
          );
          assert.equal(reply.code, 0);
          assert.equal(reply.stderr, '');
          assert.equal(reply.timedOut, false);
          assert.equal(reply.outputOverflow, false);
          const parsed: unknown = JSON.parse(reply.stdout);
          assert.equal(record(parsed)['request_id'], batch.request_id);
          for (const secret of [
            cfg.token,
            cfg.source.password,
            cfg.pipeline.password,
            cfg.consumer.password,
            cfg.es.password,
          ])
            assert.ok(!reply.stdout.includes(secret));
          const invalid = await command(
            process.execPath,
            ['scripts/operations.ts', 'unsupported-action'],
            { ...process.env, CONTROL_CONFIG_FILE: file },
            20000,
            true,
            { maxOutputBytes: 262144 },
          );
          assert.equal(invalid.code, 1);
          assert.equal(invalid.stdout, '');
          assert.match(invalid.stderr, /invalid_request/);
          return {
            request: record(parsed),
            exit: reply.code,
            invalidExit: invalid.code,
          };
        },
        () => rm(dir, { recursive: true, force: true }),
      );
      evidence('RC07', { keys, attempts, deniedOperations: 5, cli });
    },
  });
  cases.push({
    id: 'RC08',
    run: async () => {
      const { cfg, db, deliver } = context();
      let clock = 1000;
      const monitor = new OperationalMonitor(cfg, () => clock);
      const first = await monitor.snapshot();
      assert.equal(
        record(first.throughput['mutation_effects'])['state'],
        'warming',
      );
      clock += 1000;
      const second = await monitor.snapshot();
      assert.equal(
        record(second.throughput['mutation_effects'])['per_second'],
        '0.000000',
      );
      await create('{"name":"sample delta","loyalty_points":11}');
      await deliver();
      clock += 1000;
      const changed = await monitor.snapshot();
      assert.equal(
        record(changed.throughput['mutation_effects'])['per_second'],
        '1.000000',
      );
      const expected = (
        await db.c.query<{ n: string }>(
          'SELECT count(*)::text n FROM consumer.mutation_effects',
        )
      ).rows[0]?.n;
      assert.equal(
        record(record(changed.dependencies['consumer'])['data'])['effects'],
        expected,
      );
      const control = new pg.Client({
        ...cfg.source,
        user: 'm1_admin',
        password: required('M1_ADMIN_PASSWORD'),
        database: 'postgres',
      });
      await control.connect();
      try {
        await control.query('ALTER DATABASE source_m1 ALLOW_CONNECTIONS false');
        clock += 1000;
        const unavailable = await monitor.snapshot();
        const source = record(unavailable.dependencies['source']);
        assert.equal(source['data'], null);
        assert.equal(source['freshness'], 'unavailable');
        assert.equal(record(source['last_known'])['freshness'], 'stale');
        assert.equal(
          record(unavailable.dependencies['consumer'])['freshness'],
          'fresh',
        );
        evidence('RC08', {
          first,
          second,
          changed,
          unavailable,
          scope:
            'fresh current evidence stays separate from timestamped last-known values',
        });
      } finally {
        try {
          await control.query(
            'ALTER DATABASE source_m1 ALLOW_CONNECTIONS true',
          );
        } finally {
          await control.end();
        }
      }
      clock += 1000;
      assert.equal(
        record((await monitor.snapshot()).dependencies['source'])['freshness'],
        'fresh',
      );
    },
  });
  return cases;
}
