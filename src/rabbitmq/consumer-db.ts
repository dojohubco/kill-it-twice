import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import type { CanonicalEvent } from '../envelope.ts';
import { field, type Topology } from './metadata.ts';
export interface ConsumerIdentity {
  consumerId: string;
  epoch: string;
  pipelineId: string;
  registrationId: string;
  codec: string;
}
export class ConsumerDatabase {
  readonly #config: ConnectionConfig;
  constructor(config: ConnectionConfig) {
    this.#config = { ...config };
  }
  #owner() {
    return new TransactionOwner(this.#config, (client, op) => ({
      identity: () =>
        op(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT consumer_id::text,source_epoch::text,pipeline_id::text,registration_id::text,payload_encoding FROM consumer.read_identity()',
              )
            ).rows,
        ),
      process: (t: Topology, items: readonly CanonicalEvent[]) =>
        op(
          async () =>
            (
              await client.query<{ event_id: string; status: string }>(
                'SELECT * FROM consumer.process_batch($1,$2,$3,$4,$5::jsonb)',
                [
                  t.consumerId,
                  t.epoch,
                  t.pipelineId,
                  t.registrationId,
                  JSON.stringify(
                    items.map((e) => ({
                      body: e.bodyBytes.toString('hex'),
                      hash: e.contentSha256,
                    })),
                  ),
                ],
              )
            ).rows,
        ),
      quarantine: (
        raw: Buffer,
        metadata: Buffer,
        claimed: string | null,
        reason: string,
        context: string,
      ) =>
        op(
          async () =>
            (
              await client.query<{ id: string }>(
                'SELECT consumer.retain_quarantine($1,$2,$3,$4,$5)::text id',
                [raw, metadata, claimed, reason, context],
              )
            ).rows[0]?.id,
        ),
      receipts: (ids: string[], hashes: string[]) =>
        op(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM consumer.receipts($1,$2)',
                [ids, hashes],
              )
            ).rows,
        ),
      status: () =>
        op(
          async () =>
            (
              await client.query<{ value: unknown }>(
                'SELECT consumer.status() value',
              )
            ).rows[0]?.value,
        ),
    }));
  }
  async identity(): Promise<ConsumerIdentity> {
    const rows = await this.#owner().transaction((tx) => tx.identity());
    const r = rows[0];
    if (rows.length !== 1 || !r) throw new Error('Missing consumer identity');
    return {
      consumerId: field(r, 'consumer_id'),
      epoch: field(r, 'source_epoch'),
      pipelineId: field(r, 'pipeline_id'),
      registrationId: field(r, 'registration_id'),
      codec: field(r, 'payload_encoding'),
    };
  }
  transaction<T>(
    work: (tx: {
      process: (
        target: Topology,
        items: readonly CanonicalEvent[],
      ) => Promise<{ event_id: string; status: string }[]>;
    }) => Promise<T>,
  ): Promise<T> {
    return this.#owner().transaction(work);
  }
  process(t: Topology, items: readonly CanonicalEvent[]) {
    return this.transaction((tx) => tx.process(t, items));
  }
  async quarantine(
    raw: Buffer,
    metadata: Buffer,
    claimed: string | null,
    reason: string,
    context: string,
  ) {
    const id = await this.#owner().transaction((tx) =>
      tx.quarantine(raw, metadata, claimed, reason, context),
    );
    if (!id) throw new Error('Missing committed quarantine identity');
    return id;
  }
  receipts(ids: string[], hashes: string[]) {
    return this.#owner().transaction((tx) => tx.receipts(ids, hashes));
  }
  status() {
    return this.#owner().transaction((tx) => tx.status());
  }
}
