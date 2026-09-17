import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { object } from './transport.ts';
import type { Projection } from './projection.ts';
function text(r: Record<string, unknown>, k: string): string {
  const v = r[k];
  if (typeof v !== 'string') throw new Error(`Invalid ledger field ${k}`);
  return v;
}
export interface Target {
  id: string;
  generation: string;
  pipelineId: string;
  epoch: string;
  index: string;
  indexUuid: string;
  clusterUuid: string;
  configuration: unknown;
  mode: string;
  failures: string;
}
export interface EsClaim {
  eventId: string;
  generation: string;
  attemptId: string;
  bytes: string;
  probeGeneration: string;
  backendPid: string;
  transactionId: string;
  claimedAt: string;
}
export type Outcome =
  | 'applied'
  | 'already_applied'
  | 'superseded'
  | 'mapping'
  | 'oversized'
  | 'transient'
  | 'auth'
  | 'configuration'
  | 'integrity';
export class EsLedger {
  readonly #config: ConnectionConfig;
  constructor(config: ConnectionConfig) {
    this.#config = { ...config };
  }
  #owner() {
    return new TransactionOwner(this.#config, (client, operation) => ({
      identity: () =>
        operation(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT destination_id::text,generation::text,pipeline_id::text,source_epoch::text,index_name,index_uuid,cluster_uuid,configuration,mode,failures::text FROM pipeline.es_identity()',
              )
            ).rows,
        ),
      claim: (target: Target, worker: string, count: number, lease: number) =>
        operation(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM pipeline.es_claim($1,$2,$3,$4,$5)',
                [target.id, target.generation, worker, count, lease],
              )
            ).rows,
        ),
      read: (id: string) =>
        operation(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM pipeline.es_read($1)',
                [id],
              )
            ).rows,
        ),
      renew: (t: Target, c: EsClaim, w: string, lease: number) =>
        operation(
          async () =>
            (
              await client.query<{ value: boolean }>(
                'SELECT pipeline.es_renew($1,$2,$3,$4,$5,$6) AS value',
                [t.id, t.generation, c.eventId, w, c.generation, lease],
              )
            ).rows[0]?.value === true,
        ),
      settle: (
        t: Target,
        c: EsClaim,
        w: string,
        o: Outcome,
        remote: string | null,
        witness: string | null,
        context: string,
        delay: number,
      ) =>
        operation(
          async () =>
            (
              await client.query<{ value: string }>(
                'SELECT pipeline.es_settle($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) AS value',
                [
                  t.id,
                  t.generation,
                  c.eventId,
                  w,
                  c.generation,
                  o,
                  remote,
                  witness,
                  context,
                  delay,
                ],
              )
            ).rows[0]?.value,
        ),
      admission: (
        t: Target,
        w: string,
        probe: string,
        anchor: EsClaim,
        o: 'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity',
        reason: string,
        delay: number,
      ) =>
        operation(
          async () =>
            (
              await client.query<{ value: boolean }>(
                'SELECT pipeline.es_admission($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) AS value',
                [
                  t.id,
                  t.generation,
                  w,
                  probe,
                  anchor.eventId,
                  anchor.generation,
                  anchor.attemptId,
                  o,
                  reason,
                  delay,
                ],
              )
            ).rows[0]?.value === true,
        ),
      status: (n: number) =>
        operation(
          async () =>
            (
              await client.query<{ value: unknown }>(
                'SELECT pipeline.es_status($1) AS value',
                [n],
              )
            ).rows[0]?.value,
        ),
    }));
  }
  async target(): Promise<Target> {
    const rows = await this.#owner().transaction((w) => w.identity());
    if (rows.length !== 1) throw new Error('ES receiver registration missing');
    const r = object(rows[0]);
    return {
      id: text(r, 'destination_id'),
      generation: text(r, 'generation'),
      pipelineId: text(r, 'pipeline_id'),
      epoch: text(r, 'source_epoch'),
      index: text(r, 'index_name'),
      indexUuid: text(r, 'index_uuid'),
      clusterUuid: text(r, 'cluster_uuid'),
      configuration: r['configuration'],
      mode: text(r, 'mode'),
      failures: text(r, 'failures'),
    };
  }
  async claim(
    t: Target,
    worker: string,
    count: number,
    lease: number,
  ): Promise<EsClaim[]> {
    return (
      await this.#owner().transaction((w) => w.claim(t, worker, count, lease))
    ).map((r) => ({
      eventId: text(r, 'event_id'),
      generation: text(r, 'generation'),
      attemptId: text(r, 'attempt_id'),
      bytes: text(r, 'projection_bytes'),
      probeGeneration: text(r, 'probe_generation'),
      backendPid: text(r, 'backend_pid'),
      transactionId: text(r, 'transaction_id'),
      claimedAt: text(r, 'claimed_at'),
    }));
  }
  async read(id: string): Promise<Projection> {
    const rows = await this.#owner().transaction((w) => w.read(id));
    if (rows.length !== 1) throw new Error('Missing ledger witness');
    const r = object(rows[0]);
    return {
      eventId: text(r, 'event_id'),
      documentId: text(r, 'document_id'),
      version: text(r, 'version'),
      json: r['projection'] === null ? null : text(r, 'projection'),
      bytes: text(r, 'bytes'),
    };
  }
  renew(t: Target, c: EsClaim, w: string, lease: number) {
    return this.#owner().transaction((tx) => tx.renew(t, c, w, lease));
  }
  settle(
    t: Target,
    c: EsClaim,
    w: string,
    o: Outcome,
    remote: string | null,
    witness: string | null,
    context: string,
    delay: number,
  ) {
    return this.#owner().transaction((tx) =>
      tx.settle(t, c, w, o, remote, witness, context, delay),
    );
  }
  admission(
    t: Target,
    w: string,
    probe: string,
    anchor: EsClaim,
    o: 'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity',
    reason: string,
    delay: number,
  ) {
    return this.#owner().transaction((tx) =>
      tx.admission(t, w, probe, anchor, o, reason, delay),
    );
  }
  status(n = 20) {
    return this.#owner().transaction((tx) => tx.status(n));
  }
}
