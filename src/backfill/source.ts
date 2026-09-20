import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { canonicalEvent, codec, uuid } from '../envelope.ts';
import { limits } from '../limits.ts';
import { object, text, key, flag, type Binding, type Page } from './types.ts';
interface Work {
  identity(): Promise<Record<string, unknown>>;
  fence(run: string): Promise<Record<string, unknown>>;
  page(after: string, end: string, run?: string): Promise<Page>;
}
export class BackfillSource {
  readonly #owner: TransactionOwner<Work>;
  constructor(config: ConnectionConfig, binding: Binding) {
    uuid(binding.sourceEpoch);
    uuid(binding.pipelineId);
    this.#owner = new TransactionOwner(config, (client, operation) => {
      let used = false;
      const query = async (sql: string, args: unknown[]) =>
        operation(async () => {
          if (used)
            throw new Error(
              'One bounded source backfill operation per transaction',
            );
          used = true;
          const result = await client.query<{ value: unknown }>(sql, args);
          if (result.rows.length !== 1)
            throw new Error('Missing source backfill result');
          return object(result.rows[0]?.value);
        });
      return Object.freeze({
        identity: () =>
          query('SELECT source.backfill_identity($1,$2) value', [
            binding.sourceEpoch,
            binding.pipelineId,
          ]),
        fence: (run: string) =>
          query('SELECT source.seal_backfill_fence($1,$2,$3) value', [
            uuid(run),
            binding.sourceEpoch,
            binding.pipelineId,
          ]),
        page: async (after: string, end: string, run?: string) => {
          const r = await query(
            'SELECT source.backfill_page($1,$2,$3,$4,$5,$6) value',
            [
              binding.sourceEpoch,
              binding.pipelineId,
              key(after),
              key(end),
              limits.records,
              run === undefined ? null : uuid(run),
            ],
          );
          const raw = r['items'];
          if (!Array.isArray(raw) || raw.length > limits.records)
            throw new Error('Invalid source page count');
          const keys: string[] = [];
          let previous = BigInt(after),
            bytes = 0;
          const events = raw.map((value: unknown) => {
            const row = object(value),
              position = key(row['key']);
            if (BigInt(position) <= previous || BigInt(position) > BigInt(end))
              throw new Error('Unordered source page');
            previous = BigInt(position);
            keys.push(position);
            const event = canonicalEvent({
              schema_version: 1,
              source_epoch: row['source_epoch'],
              entity_id: row['entity_id'],
              entity_version: row['entity_version'],
              event_id: `${text(row['source_epoch'])}:${text(row['entity_id'])}:${text(row['entity_version'])}`,
              source_change_id: row['change_id'],
              source_recorded_at: row['recorded_at'],
              kind: row['change_id'] === null ? 'baseline' : 'mutation',
              is_deleted: row['is_deleted'],
              payload_encoding: codec,
              payload_json: row['payload_json'],
            });
            if (
              event.body.source_epoch !== binding.sourceEpoch ||
              event.wireBytes.length > limits.recordBytes
            )
              throw new Error('Source page binding or record bound');
            bytes += event.wireBytes.length;
            return event;
          });
          if (bytes > limits.batchBytes)
            throw new Error('Source page transfer bound');
          const next = key(r['next_key']);
          if (BigInt(next) !== previous)
            throw new Error('Source page cursor differs from prefix');
          let blocked: Page['blocked'] = null;
          if (r['blocked'] !== null) {
            const b = object(r['blocked']),
              reason = b['reason'];
            if (reason !== 'oversized_record' && reason !== 'missing_revision')
              throw new Error('Invalid source block');
            blocked = { key: key(b['key']), bytes: key(b['bytes']), reason };
          }
          const eof = flag(r['eof']);
          if (blocked && (events.length || eof))
            throw new Error('Invalid blocked page');
          return {
            events,
            keys,
            next,
            eof,
            blocked,
            observation: {
              observed_at: text(r['observed_at']),
              snapshot: text(r['snapshot']),
            },
          };
        },
      });
    });
  }
  transaction<T>(fn: (tx: Work) => Promise<T>) {
    return this.#owner.transaction(fn);
  }
  identity() {
    return this.transaction((tx) => tx.identity());
  }
  fence(run: string) {
    return this.transaction((tx) => tx.fence(run));
  }
  page(after: string, end: string, run?: string) {
    return this.transaction((tx) => tx.page(after, end, run));
  }
}
