import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { ConsumerDatabase } from './consumer-db.ts';
import { RabbitLedger } from './ledger.ts';
import { field, record } from './metadata.ts';
import { validateEvent } from '../envelope.ts';
import { digest, decodeWire } from './protocol.ts';
export class ReceiptObserver {
  readonly #pipeline: ConnectionConfig;
  readonly #consumer: ConsumerDatabase;
  readonly #batchSize: number;
  constructor(
    pipeline: ConnectionConfig,
    consumer: ConsumerDatabase,
    batchSize = 8,
  ) {
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 32)
      throw new Error('Invalid bounded receipt batch size');
    this.#batchSize = batchSize;
    this.#pipeline = { ...pipeline };
    this.#consumer = consumer;
  }
  #owner() {
    return new TransactionOwner(this.#pipeline, (client, op) => ({
      due: () =>
        op(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM pipeline.receipt_due($1,1000)',
                [this.#batchSize],
              )
            ).rows,
        ),
      observe: (
        id: string,
        consumer: string,
        registration: string,
        epoch: string,
        pipeline: string,
        state: string,
        bytes: Buffer,
        hash: string,
        receipt: string,
      ) =>
        op(
          async () =>
            (
              await client.query<{ status: string }>(
                'SELECT pipeline.observe_receipt($1,$2,$3,$4,$5,$6,$7,$8,$9) status',
                [
                  id,
                  consumer,
                  registration,
                  epoch,
                  pipeline,
                  state,
                  bytes,
                  hash,
                  receipt,
                ],
              )
            ).rows[0]?.status,
        ),
    }));
  }
  async once() {
    const t = await new RabbitLedger(this.#pipeline).target();
    const identity = await this.#consumer.identity();
    if (
      identity.consumerId !== t.consumerId ||
      identity.pipelineId !== t.pipelineId ||
      identity.epoch !== t.epoch ||
      identity.registrationId !== t.registrationId ||
      identity.codec !== 'pg18-jsonb-text/v1'
    )
      throw new Error('Consumer receipt identity mismatch');
    const due = await this.#owner().transaction((tx) => tx.due());
    if (due.length > this.#batchSize)
      throw new Error('Oversized expected receipt set');
    const events = due.map((r) => {
      if (!Buffer.isBuffer(r['body']))
        throw new Error('Invalid expected receipt');
      return validateEvent(r['body'], field(r, 'hash'));
    });
    if (!events.length) return { requested: [], observed: [] };
    const receipts = await this.#consumer.receipts(
      events.map((e) => e.body.event_id),
      events.map((e) => digest(e.wireBytes)),
    );
    const observed: { eventId: string; state: string; status: string }[] = [];
    const verified: {
      id: string;
      state: string;
      bytes: Buffer;
      hash: string;
      receipt: string;
    }[] = [];
    const seen = new Set<string>();
    for (const r of receipts) {
      const id = field(r, 'event_id');
      const e = events.find((e) => e.body.event_id === id);
      if (!e || seen.has(id))
        throw new Error('Unexpected or duplicate consumer receipt');
      seen.add(id);
      const state = field(r, 'state');
      if (state === 'processed') {
        if (
          !Buffer.isBuffer(r['body']) ||
          !e.bodyBytes.equals(r['body']) ||
          r['hash'] !== e.contentSha256
        )
          throw new Error('Consumer processing receipt content mismatch');
      } else if (state === 'quarantined') {
        if (
          !Buffer.isBuffer(r['raw']) ||
          !Buffer.isBuffer(r['metadata']) ||
          !e.wireBytes.equals(r['raw'])
        )
          throw new Error('Quarantine receipt does not match expected event');
        const actual = decodeWire(r['raw']);
        if (actual.contentSha256 !== e.contentSha256)
          throw new Error('Quarantine canonical mismatch');
        const parsed: unknown = JSON.parse(r['metadata'].toString('utf8'));
        const m = record(parsed),
          h = record(m['headers']);
        if (
          m['messageId'] !== id ||
          m['contentType'] !== 'application/json' ||
          m['contentEncoding'] !== 'utf-8' ||
          m['type'] !== 'revision-v1' ||
          m['deliveryMode'] !== 2 ||
          h['pipeline_id'] !== t.pipelineId ||
          h['registration_id'] !== t.registrationId
        )
          throw new Error('Forged claimed ID is not expected-event quarantine');
      } else throw new Error('Unknown consumer receipt disposition');
      verified.push({
        id,
        state,
        bytes: e.bodyBytes,
        hash: e.contentSha256,
        receipt: field(r, 'receipt_id'),
      });
    }
    // Validate the entire bounded remote response before one local transaction.
    // Sorted locks avoid inversions between two observers selecting the same identities.
    verified.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    if (verified.length)
      await this.#owner().transaction(async (tx) => {
        for (const item of verified) {
          const status = await tx.observe(
            item.id,
            identity.consumerId,
            identity.registrationId,
            identity.epoch,
            identity.pipelineId,
            item.state,
            item.bytes,
            item.hash,
            item.receipt,
          );
          if (status !== 'observed' && status !== 'already_observed')
            throw new Error('Missing committed consumer observation');
          observed.push({ eventId: item.id, state: item.state, status });
        }
      });
    return { requested: events.map((e) => e.body.event_id), observed };
  }
}

// Progress drains bounded work with a yield; missing/duplicate-only results retain cooldown.
export function receiptPollDelay(value: {
  observed: readonly { status: string }[];
  requested?: readonly string[];
}): number {
  if (value.observed.some((r) => r.status === 'observed')) return 10;
  return value.requested?.length ? 50 : 1000;
}
