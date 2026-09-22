import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { validateEvent, uuid } from '../envelope.ts';
import { limits } from '../limits.ts';
import { backfillPageRecords } from './bounds.ts';
import {
  object,
  claim,
  status,
  type Claim,
  type Page,
  type Binding,
} from './types.ts';
export interface PageRequest {
  claim: Claim;
  batchId: string;
  page: Page;
}
interface Work {
  start(
    run: string,
    binding: Binding,
    ranges: number,
    observation: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
  claim(run: string, worker: string, lease: number): Promise<Claim | null>;
  page(request: PageRequest): Promise<Record<string, unknown>>;
  change(
    c: Claim,
    action: 'renew' | 'defer' | 'block',
    duration: number,
    reason?: string,
  ): Promise<boolean>;
  attach(run: string, fence: Record<string, unknown>): Promise<void>;
  advance(run: string): Promise<Record<string, unknown>>;
  advanceIfReady(run: string): Promise<void>;
  pause(run: string, paused: boolean): Promise<void>;
  status(run: string): Promise<ReturnType<typeof status>>;
  poll(run: string): Promise<ReturnType<typeof status>>;
}
export class BackfillLedger {
  readonly #owner: TransactionOwner<Work>;
  constructor(config: ConnectionConfig, records: number = limits.records) {
    records = backfillPageRecords(records);
    this.#owner = new TransactionOwner(config, (client, operation) => {
      let used = false;
      const query = (sql: string, args: unknown[]) =>
        operation(async () => {
          if (used) throw new Error('One backfill operation per transaction');
          used = true;
          const r = await client.query<{ value: unknown }>(sql, args);
          if (r.rows.length !== 1) throw new Error('Missing backfill result');
          return r.rows[0]?.value;
        });
      return Object.freeze({
        start: async (
          run: string,
          b: Binding,
          ranges: number,
          o: Record<string, unknown>,
        ) =>
          object(
            await query(
              'SELECT pipeline.start_backfill($1,$2,$3,$4,$5) value',
              [
                uuid(run),
                uuid(b.sourceEpoch),
                uuid(b.pipelineId),
                ranges,
                JSON.stringify(o),
              ],
            ),
          ),
        claim: async (run: string, worker: string, lease: number) =>
          claim(
            await query('SELECT pipeline.backfill_claim($1,$2,$3) value', [
              uuid(run),
              uuid(worker),
              lease,
            ]),
          ),
        page: async (request: PageRequest) => {
          const { claim: c, page: p } = request;
          if (
            p.blocked ||
            p.events.length > records ||
            p.events.length !== p.keys.length
          )
            throw new Error('Invalid admitted backfill page');
          let bytes = 0;
          const inputs = p.events.map((e, i) => {
            const event = validateEvent(e.bodyBytes, e.contentSha256);
            bytes += event.wireBytes.length;
            if (event.wireBytes.length > limits.recordBytes)
              throw new Error('Record exceeds backfill bound');
            return {
              key: p.keys[i],
              body: event.bodyBytes.toString('hex'),
              hash: event.contentSha256,
            };
          });
          if (bytes > limits.batchBytes)
            throw new Error('Page exceeds backfill byte bound');
          return object(
            await query(
              'SELECT pipeline.backfill_page($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) value',
              [
                c.runId,
                c.range,
                c.owner,
                c.generation,
                uuid(request.batchId),
                c.checkpoint,
                p.next,
                p.eof,
                JSON.stringify(inputs),
                JSON.stringify(p.observation),
              ],
            ),
          );
        },
        change: async (
          c: Claim,
          action: 'renew' | 'defer' | 'block',
          duration: number,
          reason?: string,
        ) => {
          const v = await query(
            'SELECT pipeline.backfill_change($1,$2,$3,$4,$5,$6,$7) value',
            [
              c.runId,
              c.range,
              c.owner,
              c.generation,
              action,
              duration,
              reason ?? null,
            ],
          );
          if (typeof v !== 'boolean')
            throw new Error('Invalid range ownership result');
          return v;
        },
        attach: async (run: string, f: Record<string, unknown>) => {
          await query('SELECT pipeline.attach_backfill_fence($1,$2) value', [
            uuid(run),
            JSON.stringify(f),
          ]);
        },
        advance: async (run: string) =>
          object(
            await query('SELECT pipeline.backfill_advance($1) value', [
              uuid(run),
            ]),
          ),
        advanceIfReady: async (run: string) => {
          await query('SELECT pipeline.backfill_advance_if_ready($1) value', [
            uuid(run),
          ]);
        },
        pause: async (run: string, paused: boolean) => {
          await query('SELECT pipeline.backfill_pause($1,$2) value', [
            uuid(run),
            paused,
          ]);
        },
        poll: async (run: string) =>
          status(
            await query('SELECT pipeline.backfill_poll($1) value', [uuid(run)]),
          ),
        status: async (run: string) =>
          status(
            await query('SELECT pipeline.backfill_status($1) value', [
              uuid(run),
            ]),
          ),
      });
    });
  }
  transaction<T>(fn: (work: Work) => Promise<T>) {
    return this.#owner.transaction(fn);
  }
  start(run: string, b: Binding, ranges: number, o: Record<string, unknown>) {
    return this.transaction((tx) => tx.start(run, b, ranges, o));
  }
  claim(run: string, worker: string, lease: number) {
    return this.transaction((tx) => tx.claim(run, worker, lease));
  }
  page(request: PageRequest) {
    return this.transaction((tx) => tx.page(request));
  }
  change(
    c: Claim,
    action: 'renew' | 'defer' | 'block',
    duration: number,
    reason?: string,
  ) {
    return this.transaction((tx) => tx.change(c, action, duration, reason));
  }
  attach(run: string, f: Record<string, unknown>) {
    return this.transaction((tx) => tx.attach(run, f));
  }
  advance(run: string) {
    return this.transaction((tx) => tx.advance(run));
  }
  advanceIfReady(run: string) {
    return this.transaction((tx) => tx.advanceIfReady(run));
  }
  pause(run: string, paused: boolean) {
    return this.transaction((tx) => tx.pause(run, paused));
  }
  poll(run: string) {
    return this.transaction((tx) => tx.poll(run));
  }
  status(run: string) {
    return this.transaction((tx) => tx.status(run));
  }
}
