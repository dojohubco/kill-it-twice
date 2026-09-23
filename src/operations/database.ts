import {
  TransactionOwner,
  type ConnectionConfig,
} from '../internal/transaction.ts';
import { queries } from './queries.ts';
import { record } from './validation.ts';
export type Store = keyof typeof queries;
export class OperationalDatabase {
  readonly configs: Record<Store, ConnectionConfig>;
  constructor(configs: Record<Store, ConnectionConfig>) {
    this.configs = configs;
  }
  async read<S extends Store>(
    store: S,
    name: keyof (typeof queries)[S],
    args: unknown[] = [],
  ): Promise<Record<string, unknown>[]> {
    const sql = queries[store][name];
    if (typeof sql !== 'string') throw new Error('Unknown operational read');
    const owner = new TransactionOwner(
      this.configs[store],
      (client, operation) => ({
        read: () =>
          operation(async () => {
            await client.query('SET TRANSACTION READ ONLY');
            await client.query('SET LOCAL statement_timeout=2500');
            let selected: string = sql;
            if (store === 'pipeline' && name === 'backfill') {
              const capability = await client.query<{ present: boolean }>(
                "SELECT to_regprocedure('pipeline.backfill_observation(uuid)') IS NOT NULL AS present",
              );
              if (capability.rows[0]?.present === true)
                selected = 'SELECT pipeline.backfill_observation($1) value';
            }
            const r = await client.query<{ value: unknown }>(selected, args);
            return r.rows.map((r) => record(r.value));
          }),
      }),
    );
    return owner.transaction((w) => w.read());
  }
  async #write(store: Store, sql: string, args: unknown[]) {
    const owner = new TransactionOwner(
      this.configs[store],
      (client, operation) => ({
        write: () =>
          operation(async () => {
            const r = await client.query<{ value: unknown }>(sql, args);
            return record(r.rows[0]?.value);
          }),
      }),
    );
    return owner.transaction((w) => w.write());
  }
  replay(
    key: string,
    correlation: string,
    event: string,
    destination: string,
    generation: string,
    attempt: string,
    reason: string,
  ) {
    return this.#write(
      'pipeline',
      'SELECT pipeline.request_es_replay($1,$2,$3,$4,$5,$6,$7) value',
      [key, correlation, event, destination, generation, attempt, reason],
    );
  }
  backfill(
    key: string,
    correlation: string,
    operation: string,
    run: string,
    epoch: string,
    instance: string,
    ranges: number,
    observation: unknown,
  ) {
    return this.#write(
      'pipeline',
      'SELECT pipeline.operator_backfill($1,$2,$3,$4,$5,$6,$7,$8) value',
      [
        key,
        correlation,
        operation,
        run,
        epoch,
        instance,
        ranges,
        JSON.stringify(observation),
      ],
    );
  }
  source(
    epoch: string,
    key: string,
    correlation: string,
    fixture: string,
    operation: string,
    value: number,
  ) {
    return this.#write(
      'source',
      'SELECT source.operator_change($1,$2,$3,$4,$5,$6) value',
      [epoch, key, correlation, fixture, operation, value],
    );
  }
  network(
    key: string,
    correlation: string,
    sink: string,
    desired: string,
    finish: boolean,
  ) {
    return this.#write(
      'pipeline',
      'SELECT pipeline.network_request($1,$2,$3,$4,$5) value',
      [key, correlation, sink, desired, finish],
    );
  }
}
