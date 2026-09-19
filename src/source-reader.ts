import { TransactionOwner } from './internal/transaction.ts';
import {
  canonicalEvent,
  codec,
  uuid,
  type CanonicalEvent,
} from './envelope.ts';
import { positiveBigint, type SourceConfig } from './source.ts';
import { limits, countBound, LimitError } from './limits.ts';
export interface RevisionKey {
  entityId: string;
  version: string;
}
export interface Selection {
  events: CanonicalEvent[];
  notVisible: string[];
}
interface ReadWork {
  read(
    keys: readonly RevisionKey[],
    mode: 'current' | 'outbox' | 'baseline',
  ): Promise<Selection>;
}
export class SourceReader {
  #owner: TransactionOwner<ReadWork>;
  constructor(config: SourceConfig, expectedEpoch: string) {
    uuid(expectedEpoch);
    this.#owner = new TransactionOwner(config, (client, operation) => ({
      read: (keys, mode) =>
        operation(async () => {
          countBound(keys.length);
          const current = mode === 'current';
          const table = current
            ? 'entities'
            : mode === 'baseline'
              ? 'baseline_revisions'
              : 'outbox';
          const ids = keys.map((key) => positiveBigint(key.entityId));
          const versions = keys.map((key) => positiveBigint(key.version));
          const settings = (
            await client.query<Record<string, unknown>>(
              `SELECT source_epoch::text, current_setting('server_encoding') AS server_encoding, current_setting('client_encoding') AS client_encoding, current_setting('server_version_num') AS version FROM source.source_identity WHERE singleton`,
            )
          ).rows[0];
          if (
            !settings ||
            settings['source_epoch'] !== expectedEpoch ||
            settings['server_encoding'] !== 'UTF8' ||
            settings['client_encoding'] !== 'UTF8' ||
            typeof settings['version'] !== 'string' ||
            !/^18\d{4}$/.test(settings['version'])
          )
            throw new Error('Source epoch or pg18 UTF-8 codec mismatch');
          // One statement sees one coherent source revision, including its payload.
          // Payload is measured inside PostgreSQL and never transferred on overflow.
          const rows = (
            await client.query<Record<string, unknown>>(
              `WITH selected AS MATERIALIZED (
        SELECT DISTINCT e.entity_id,e.entity_version,e.source_epoch,e.change_id,e.recorded_at,e.is_deleted,e.payload::text AS exported
        FROM source.${table} e JOIN unnest($1::bigint[],$2::bigint[]) k(id,version)
        ON e.entity_id=k.id ${current ? '' : 'AND e.entity_version=k.version'} WHERE e.source_epoch=$3
      ), measured AS MATERIALIZED (
        SELECT *, coalesce(octet_length(to_json(exported)::text),4)+1024 AS transfer_bytes FROM selected
      ), bounded AS (
        SELECT *, bool_and(transfer_bytes <= $4) OVER () AND sum(transfer_bytes) OVER () <= $5 AS fits FROM measured
      ) SELECT entity_id::text,entity_version::text,source_epoch::text,change_id::text,
        to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS recorded_at,is_deleted,
        transfer_bytes,fits,CASE WHEN fits THEN exported ELSE NULL END AS payload_json
        FROM bounded ORDER BY entity_id,entity_version`,
              [
                ids,
                versions,
                expectedEpoch,
                limits.recordBytes,
                limits.batchBytes,
              ],
            )
          ).rows;
          for (const row of rows)
            if (row['fits'] !== true)
              throw new LimitError(
                `Source export exceeds record/transfer bound; retained revision ${String(row['entity_id'])}:${String(row['entity_version'])}, estimated bytes ${String(row['transfer_bytes'])}`,
              );
          const events = rows.map((row) =>
            canonicalEvent({
              schema_version: 1,
              source_epoch: row['source_epoch'],
              entity_id: row['entity_id'],
              entity_version: row['entity_version'],
              event_id: `${String(row['source_epoch'])}:${String(row['entity_id'])}:${String(row['entity_version'])}`,
              source_change_id: row['change_id'],
              source_recorded_at: row['recorded_at'],
              kind: row['change_id'] === null ? 'baseline' : 'mutation',
              is_deleted: row['is_deleted'],
              payload_encoding: codec,
              payload_json: row['payload_json'],
            }),
          );
          const present = new Set(
            events.map((event) =>
              current ? event.body.entity_id : event.body.event_id,
            ),
          );
          return {
            events,
            notVisible: keys
              .filter(
                (key) =>
                  !present.has(
                    current
                      ? key.entityId
                      : `${expectedEpoch}:${key.entityId}:${key.version}`,
                  ),
              )
              .map((key) => `${expectedEpoch}:${key.entityId}:${key.version}`),
          };
        }),
    }));
  }
  outbox(keys: readonly RevisionKey[]): Promise<Selection> {
    countBound(keys.length);
    return this.#owner.transaction((tx) => tx.read(keys, 'outbox'));
  }
  baseline(keys: readonly RevisionKey[]): Promise<Selection> {
    countBound(keys.length);
    return this.#owner.transaction((tx) => tx.read(keys, 'baseline'));
  }
  current(entityId: string): Promise<Selection> {
    return this.#owner.transaction((tx) =>
      tx.read([{ entityId, version: '1' }], 'current'),
    );
  }
}
