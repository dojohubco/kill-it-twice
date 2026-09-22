// Real receiver outcomes and injected local SQL failures on an isolated capacity fixture.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { Client } from 'pg';
import { EsLedger, type EsSettlement } from '../../src/es/ledger.ts';
import { EsTransport } from '../../src/es/transport.ts';
import { EsAdapter } from '../../src/es/adapter.ts';
import {
  RabbitLedger,
  type RabbitSettlement,
} from '../../src/rabbitmq/ledger.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { BrokerMetadata } from '../../src/rabbitmq/metadata.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import {
  database,
  adminConfig,
  sql,
  password,
  readObject,
  safeFailure,
} from '../runtime/private.ts';
import { withCleanup } from '../support.ts';
async function state(c: Client, kind: string, ids: string[]) {
  return (
    await c.query<{ value: string }>(
      'SELECT row_to_json(d)::text value FROM pipeline.delivery_intents d WHERE d.kind=$1 AND event_id=ANY($2) ORDER BY event_id COLLATE "C"',
      [kind, ids],
    )
  ).rows;
}
async function expectRollback(
  c: Client,
  kind: string,
  ids: string[],
  operation: () => Promise<unknown>,
) {
  const before = await state(c, kind, ids);
  const reject = [...ids].sort()[1];
  assert.ok(reject && /^[a-f0-9-]{36}:[1-9][0-9]*:[1-9][0-9]*$/.test(reject));
  await c.query(
    `CREATE FUNCTION runtime_setup.reject_sink_fixture() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN IF NEW.kind='${kind}' AND NEW.event_id='${reject}' AND NEW.state='satisfied' THEN RAISE EXCEPTION 'Controlled second settlement fault' USING ERRCODE='P9812'; END IF; RETURN NEW; END $$; CREATE TRIGGER capacity_sink_fault BEFORE UPDATE ON pipeline.delivery_intents FOR EACH ROW EXECUTE FUNCTION runtime_setup.reject_sink_fixture();`,
  );
  await withCleanup(
    async () => {
      await assert.rejects(
        operation(),
        (error: unknown) =>
          error instanceof TransactionError &&
          error.sqlState === 'P9812' &&
          error.outcome === 'rolled_back',
      );
    },
    async () => {
      await c.query(
        'DROP TRIGGER capacity_sink_fault ON pipeline.delivery_intents; DROP FUNCTION runtime_setup.reject_sink_fixture()',
      );
    },
  );
  assert.deepEqual(
    await state(c, kind, ids),
    before,
    'No first-item settlement may commit on the second-item SQL error',
  );
  return {
    kind,
    rejected_event: reject,
    sqlstate: 'P9812',
    unchanged_deliveries: before.length,
  };
}
try {
  const ca = await readFile('/private/server.crt', 'utf8');
  const es = new EsLedger(sql('pipeline_es', await password('pipeline_es')));
  const transport = new EsTransport({
    node: 'https://toxiproxy:8666',
    username: 'kit_runtime',
    password: await password('es_runtime'),
    ca,
  });
  const esProof = await withCleanup(
    async () => {
      const adapter = new EsAdapter(transport),
        target = await es.target(),
        owner = randomUUID();
      const claims = await es.claim(target, owner, 32, 30000);
      assert.equal(claims.length, 32);
      const projections = [
        ...(await es.readBatch(claims.slice(0, 16).map((c) => c.eventId))),
        ...(await es.readBatch(claims.slice(16).map((c) => c.eventId))),
      ];
      await adapter.validate(target);
      const raw = await adapter.bulk(target, projections, randomUUID());
      assert.equal(raw.length, 32);
      const items: EsSettlement[] = [];
      for (const item of raw) {
        const resolved = await adapter.resolve(target, item, es);
        assert.equal(resolved.outcome, 'applied');
        const claim = claims.find(
          (c) => c.eventId === resolved.projection.eventId,
        );
        assert.ok(claim);
        items.push({
          claim,
          outcome: resolved.outcome,
          remote: resolved.remote,
          witness: resolved.witness,
          context: 'Actual receiver application; local batch regression',
          delay: 1,
        });
      }
      await adapter.validate(target);
      return database(await adminConfig('pipeline'), async (c) => {
        const rollback = await expectRollback(
          c,
          'elasticsearch',
          claims.map((x) => x.eventId),
          () => es.settleMany(target, owner, items),
        );
        const first = items[0];
        assert.ok(first);
        const stale = {
          ...first,
          claim: {
            ...first.claim,
            generation: (BigInt(first.claim.generation) + 1n).toString(),
          },
        };
        const renew = await es.renewMany(
          target,
          [stale.claim, ...items.slice(1).map((x) => x.claim)],
          owner,
          30000,
        );
        assert.equal(renew.filter((x) => x.renewed).length, 31);
        const result = await es.settleMany(target, owner, [
          stale,
          ...items.slice(1),
        ]);
        assert.equal(result.filter((x) => x.status === 'settled').length, 31);
        assert.equal(
          result.find((x) => x.eventId === first.claim.eventId)?.status,
          'stale',
        );
        const txs = (
          await c.query<{ tx: string }>(
            "SELECT DISTINCT xmin::text tx FROM pipeline.delivery_intents WHERE kind='elasticsearch' AND state='satisfied' AND event_id=ANY($1)",
            [claims.map((x) => x.eventId)],
          )
        ).rows;
        assert.equal(txs.length, 1);
        assert.equal(
          (await es.settleMany(target, owner, [first]))[0]?.status,
          'settled',
        );
        const before = await state(
          c,
          'elasticsearch',
          claims.map((x) => x.eventId),
        );
        assert.ok(
          (await es.settleMany(target, owner, items)).every(
            (x) => x.status === 'stale',
          ),
        );
        assert.deepEqual(
          await state(
            c,
            'elasticsearch',
            claims.map((x) => x.eventId),
          ),
          before,
        );
        return {
          rollback,
          actual_applied: 32,
          stale_neighbor: result,
          shared_transaction: txs,
          terminal_replay_unchanged: true,
        };
      });
    },
    () => transport.close(),
  );
  const rabbit = new RabbitLedger(
    sql('pipeline_rabbit', await password('pipeline_rabbit')),
  );
  const target = await rabbit.target(),
    owner = randomUUID(),
    credentials = await readObject('/private/rabbit-credentials.json');
  assert.equal(typeof credentials['publisher'], 'string');
  assert.equal(typeof credentials['observer'], 'string');
  const publisher = new Publisher(
    {
      host: 'toxiproxy',
      port: 8667,
      username: `publisher-${target.registrationId}`,
      password: String(credentials['publisher']),
      ca,
      vhost: target.vhost,
    },
    new BrokerMetadata({
      url: 'https://rabbitmq:15671',
      username: `observer-${target.registrationId}`,
      password: String(credentials['observer']),
      ca,
    }),
  );
  const claims = await rabbit.claim(target, owner, 32, 30000);
  assert.equal(claims.length, 32);
  const wires = await rabbit.read(claims.map((c) => c.eventId));
  const actual = await publisher.publish(
    target,
    claims.map((c) => {
      const value = wires.find((w) => w.eventId === c.eventId);
      assert.ok(value);
      return { eventId: c.eventId, attemptId: c.attemptId, wire: value.wire };
    }),
  );
  assert.equal(actual.length, 32);
  assert.ok(actual.every((x) => x.outcome === 'confirmed' && !x.returned));
  const items: RabbitSettlement[] = claims.map((claim) => {
    const r = actual.find((v) => v.attemptId === claim.attemptId);
    assert.ok(r);
    return {
      claim,
      outcome: r.outcome,
      channel: r.channelId,
      context: r.context,
      delay: 1,
    };
  });
  const rabbitProof = await database(
    await adminConfig('pipeline'),
    async (c) => {
      const rollback = await expectRollback(
        c,
        'rabbitmq',
        claims.map((x) => x.eventId),
        () => rabbit.settleMany(target, owner, items),
      );
      const first = items[0];
      assert.ok(first);
      const stale = {
        ...first,
        claim: {
          ...first.claim,
          generation: (BigInt(first.claim.generation) + 1n).toString(),
        },
      };
      const renewal = await rabbit.renewMany(
        target,
        [stale.claim, ...items.slice(1).map((x) => x.claim)],
        owner,
        30000,
      );
      assert.equal(renewal.filter((x) => x.renewed).length, 31);
      const result = await rabbit.settleMany(target, owner, [
        stale,
        ...items.slice(1),
      ]);
      assert.equal(result.filter((x) => x.status === 'settled').length, 31);
      const txs = (
        await c.query<{ tx: string }>(
          "SELECT DISTINCT xmin::text tx FROM pipeline.delivery_intents WHERE kind='rabbitmq' AND state='satisfied' AND event_id=ANY($1)",
          [claims.map((x) => x.eventId)],
        )
      ).rows;
      assert.equal(txs.length, 1);
      assert.equal(
        (await rabbit.settleMany(target, owner, [first]))[0]?.status,
        'settled',
      );
      const before = await state(
        c,
        'rabbitmq',
        claims.map((x) => x.eventId),
      );
      assert.ok(
        (await rabbit.settleMany(target, owner, items)).every(
          (x) => x.status === 'stale',
        ),
      );
      assert.deepEqual(
        await state(
          c,
          'rabbitmq',
          claims.map((x) => x.eventId),
        ),
        before,
      );
      return {
        rollback,
        actual_confirmed: 32,
        stale_neighbor: result,
        shared_transaction: txs,
        terminal_replay_unchanged: true,
      };
    },
  );
  console.log(
    JSON.stringify({
      status: 'PASS',
      scope:
        'Actual remote outcomes with local group rollback and stale-neighbor fencing',
      elasticsearch: esProof,
      rabbitmq: rabbitProof,
    }),
  );
} catch (error) {
  console.error(JSON.stringify(safeFailure(error)));
  process.exitCode = 1;
}
