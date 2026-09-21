// Real SQL fault in an isolated fixture; never selected by ordinary runtime workers.
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { ReceiptObserver } from '../../src/rabbitmq/receipts.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { TransactionError } from '../../src/internal/transaction.ts';
import {
  database,
  adminConfig,
  sql,
  password,
  safeFailure,
} from '../runtime/private.ts';
import { withCleanup } from '../support.ts';
try {
  const observer = new ReceiptObserver(
    sql('pipeline_receipts', await password('pipeline_receipts')),
    new ConsumerDatabase(
      sql('consumer_receipt_reader', await password('consumer_receipt_reader')),
    ),
    32,
  );
  const proof = await database(await adminConfig('pipeline'), async (c) => {
    const candidates = (
      await c.query<{ event_id: string }>(
        "SELECT event_id FROM pipeline.consumer_observations WHERE state='pending' AND next_check_at<=clock_timestamp() ORDER BY next_check_at,event_id LIMIT 32",
      )
    ).rows.map((r) => r.event_id);
    assert.equal(candidates.length, 32);
    const sorted = [...candidates].sort();
    const reject = sorted[1];
    assert.ok(reject && /^[a-f0-9-]{36}:[1-9][0-9]*:[1-9][0-9]*$/.test(reject));
    const before = (
      await c.query<{ event_id: string; state: string }>(
        'SELECT event_id,state,observed_at FROM pipeline.consumer_observations WHERE event_id=ANY($1) ORDER BY event_id',
        [candidates],
      )
    ).rows;
    await c.query(
      `CREATE FUNCTION runtime_setup.reject_receipt_fixture() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN IF NEW.event_id='${reject}' AND NEW.state='processed' THEN RAISE EXCEPTION 'Controlled second observation fault' USING ERRCODE='P9811'; END IF; RETURN NEW; END $$; CREATE TRIGGER capacity_fault BEFORE UPDATE ON pipeline.consumer_observations FOR EACH ROW EXECUTE FUNCTION runtime_setup.reject_receipt_fixture();`,
    );
    await withCleanup(
      async () => {
        await assert.rejects(
          observer.once(),
          (error: unknown) =>
            error instanceof TransactionError &&
            error.sqlState === 'P9811' &&
            error.outcome === 'rolled_back',
        );
      },
      async () => {
        await c.query(
          'DROP TRIGGER capacity_fault ON pipeline.consumer_observations; DROP FUNCTION runtime_setup.reject_receipt_fixture()',
        );
      },
    );
    const after = (
      await c.query<{ event_id: string; state: string }>(
        'SELECT event_id,state,observed_at FROM pipeline.consumer_observations WHERE event_id=ANY($1) ORDER BY event_id',
        [candidates],
      )
    ).rows;
    assert.deepEqual(
      after,
      before,
      'No prefix observation may commit on batch failure',
    );
    await delay(1100);
    const result = await observer.once();
    assert.equal(result.observed.length, 32);
    const rows = (
      await c.query<{ event_id: string; state: string; tx: string }>(
        'SELECT event_id,state,xmin::text tx FROM pipeline.consumer_observations WHERE event_id=ANY($1) ORDER BY event_id',
        [result.observed.map((r) => r.eventId)],
      )
    ).rows;
    assert.equal(rows.length, 32);
    assert.ok(rows.every((r) => r.state === 'processed'));
    assert.equal(new Set(rows.map((r) => r.tx)).size, 1);
    return {
      rejected_sqlstate: 'P9811',
      rolled_back_prefix: true,
      first_candidate: candidates[0],
      rejected_event: reject,
      processed: rows,
      observed: result.observed,
    };
  });
  console.log(
    JSON.stringify({
      status: 'PASS',
      scope: 'Actual receipt batch rollback/COMMIT evidence',
      proof,
    }),
  );
} catch (e) {
  console.error(JSON.stringify(safeFailure(e)));
  process.exitCode = 1;
}
