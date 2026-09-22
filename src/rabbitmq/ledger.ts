import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { field, type Topology } from './metadata.ts';
import { orderedSinkBatch } from '../internal/sink-batch.ts';
export interface RabbitTarget extends Topology {
  id: string;
  generation: string;
  mode: string;
}
export interface RabbitClaim {
  eventId: string;
  generation: string;
  attemptId: string;
  bytes: string;
  probeGeneration: string;
  backendPid: string;
  transactionId: string;
  claimedAt: string;
}
export type RabbitOutcome =
  'confirmed' | 'transient' | 'auth' | 'configuration' | 'integrity';
export interface WireRecord {
  eventId: string;
  wire: Buffer;
  bytes: string;
}
export interface RabbitSettlement {
  claim: RabbitClaim;
  outcome: RabbitOutcome;
  channel: string | null;
  context: string;
  delay: number;
}
export class RabbitLedger {
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
                'SELECT destination_id::text,generation::text,registration_id::text,consumer_id::text,pipeline_id::text,source_epoch::text,vhost,exchange,queue,routing_key,mode FROM pipeline.rabbit_identity()',
              )
            ).rows,
        ),
      claim: (t: RabbitTarget, w: string, n: number, lease: number) =>
        op(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM pipeline.rabbit_claim($1,$2,$3,$4,$5)',
                [t.id, t.generation, w, n, lease],
              )
            ).rows,
        ),
      read: (ids: string[]) =>
        op(
          async () =>
            (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM pipeline.rabbit_read($1)',
                [ids],
              )
            ).rows,
        ),
      renew: (t: RabbitTarget, c: RabbitClaim, w: string, lease: number) =>
        op(
          async () =>
            (
              await client.query<{ value: boolean }>(
                'SELECT pipeline.rabbit_renew($1,$2,$3,$4,$5,$6) value',
                [t.id, t.generation, c.eventId, w, c.generation, lease],
              )
            ).rows[0]?.value === true,
        ),
      settle: (
        t: RabbitTarget,
        c: RabbitClaim,
        w: string,
        o: RabbitOutcome,
        channel: string | null,
        context: string,
        delay: number,
      ) =>
        op(
          async () =>
            (
              await client.query<{ value: string }>(
                'SELECT pipeline.rabbit_settle($1,$2,$3,$4,$5,$6,$7,$8,$9) value',
                [
                  t.id,
                  t.generation,
                  c.eventId,
                  w,
                  c.generation,
                  o,
                  channel,
                  context,
                  delay,
                ],
              )
            ).rows[0]?.value,
        ),
      admission: (
        t: RabbitTarget,
        c: RabbitClaim,
        w: string,
        o: 'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity',
        reason: string,
        delay: number,
      ) =>
        op(
          async () =>
            (
              await client.query<{ value: boolean }>(
                'SELECT pipeline.rabbit_admission($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) value',
                [
                  t.id,
                  t.generation,
                  w,
                  c.probeGeneration,
                  c.eventId,
                  c.generation,
                  c.attemptId,
                  o,
                  reason,
                  delay,
                ],
              )
            ).rows[0]?.value === true,
        ),
      status: (n: number) =>
        op(
          async () =>
            (
              await client.query<{ value: unknown }>(
                'SELECT pipeline.rabbit_status($1) value',
                [n],
              )
            ).rows[0]?.value,
        ),
    }));
  }
  async target(): Promise<RabbitTarget> {
    const rows = await this.#owner().transaction((w) => w.identity());
    const r = rows[0];
    if (rows.length !== 1 || !r) throw new Error('Missing broker registration');
    return {
      id: field(r, 'destination_id'),
      generation: field(r, 'generation'),
      registrationId: field(r, 'registration_id'),
      consumerId: field(r, 'consumer_id'),
      pipelineId: field(r, 'pipeline_id'),
      epoch: field(r, 'source_epoch'),
      vhost: field(r, 'vhost'),
      exchange: field(r, 'exchange'),
      queue: field(r, 'queue'),
      routingKey: field(r, 'routing_key'),
      mode: field(r, 'mode'),
    };
  }
  async claim(
    t: RabbitTarget,
    w: string,
    n: number,
    lease: number,
  ): Promise<RabbitClaim[]> {
    return (
      await this.#owner().transaction((tx) => tx.claim(t, w, n, lease))
    ).map((r) => ({
      eventId: field(r, 'event_id'),
      generation: field(r, 'generation'),
      attemptId: field(r, 'attempt_id'),
      bytes: field(r, 'wire_bytes'),
      probeGeneration: field(r, 'probe_generation'),
      backendPid: field(r, 'backend_pid'),
      transactionId: field(r, 'transaction_id'),
      claimedAt: field(r, 'claimed_at'),
    }));
  }
  async read(ids: string[]): Promise<WireRecord[]> {
    const rows = await this.#owner().transaction((tx) => tx.read(ids));
    return rows.map((r) => {
      const wire = r['wire'];
      if (!Buffer.isBuffer(wire))
        throw new Error('Missing or oversized broker wire record');
      return { eventId: field(r, 'event_id'), wire, bytes: field(r, 'bytes') };
    });
  }
  renew(t: RabbitTarget, c: RabbitClaim, w: string, lease: number) {
    return this.#owner().transaction((tx) => tx.renew(t, c, w, lease));
  }
  settle(
    t: RabbitTarget,
    c: RabbitClaim,
    w: string,
    o: RabbitOutcome,
    channel: string | null,
    context: string,
    delay: number,
  ) {
    return this.#owner().transaction((tx) =>
      tx.settle(t, c, w, o, channel, context, delay),
    );
  }
  admission(
    t: RabbitTarget,
    c: RabbitClaim,
    w: string,
    o: 'healthy' | 'transient' | 'auth' | 'configuration' | 'integrity',
    reason: string,
    delay: number,
  ) {
    return this.#owner().transaction((tx) =>
      tx.admission(t, c, w, o, reason, delay),
    );
  }
  async renewMany(
    t: RabbitTarget,
    claims: readonly RabbitClaim[],
    owner: string,
    lease: number,
  ) {
    const group = orderedSinkBatch(
      claims.map((c) => ({ ...c })),
      (c) => c.eventId,
    );
    return this.#owner().transaction(async (tx) => {
      const result: { eventId: string; renewed: boolean }[] = [];
      for (const c of group)
        result.push({
          eventId: c.eventId,
          renewed: await tx.renew(t, c, owner, lease),
        });
      return result;
    });
  }
  async settleMany(
    t: RabbitTarget,
    owner: string,
    items: readonly RabbitSettlement[],
  ) {
    const group = orderedSinkBatch(
      items.map((r) => ({ ...r, claim: { ...r.claim } })),
      (r) => r.claim.eventId,
    );
    return this.#owner().transaction(async (tx) => {
      const result: { eventId: string; status: string }[] = [];
      for (const r of group) {
        const status = await tx.settle(
          t,
          r.claim,
          owner,
          r.outcome,
          r.channel,
          r.context,
          r.delay,
        );
        if (status !== 'settled' && status !== 'stale')
          throw new Error('Invalid publisher settlement result');
        result.push({ eventId: r.claim.eventId, status });
      }
      return result;
    });
  }
  status(n = 20) {
    return this.#owner().transaction((tx) => tx.status(n));
  }
}
