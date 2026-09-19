import {
  TransactionOwner,
  type ConnectionConfig,
} from './internal/transaction.ts';
import { codec, uuid } from './envelope.ts';
import { positiveBigint } from './source.ts';
import { pipelineIdentity } from './pipeline.ts';
export interface BootstrapRecipe {
  epoch: string;
  key: string;
  version: number;
  seed: string;
  count: string;
  chunkSize: number;
}
export interface BootstrapStatus {
  source_epoch: string;
  phase: 'unselected' | 'bootstrapping' | 'sealed' | 'active';
  bootstrap_key: string | null;
  requested_count: string | null;
  completed_count: string;
  chunk_size: number | null;
}
export interface SeedMember {
  ordinal: string;
  entity_id: string;
  recorded_at: string;
  replayed: boolean;
}
interface Work {
  status(epoch: string): Promise<BootstrapStatus>;
  begin(recipe: BootstrapRecipe): Promise<BootstrapStatus>;
  chunk(epoch: string, key: string, first: string): Promise<SeedMember[]>;
  seal(epoch: string, key: string): Promise<BootstrapStatus>;
  activate(
    epoch: string,
    key: string,
    pipeline: string,
  ): Promise<BootstrapStatus>;
}
function status(value: unknown): BootstrapStatus {
  if (!value || typeof value !== 'object')
    throw new TypeError('Missing bootstrap status');
  const r = value as Record<string, unknown>;
  const phase = r['phase'];
  if (
    phase !== 'unselected' &&
    phase !== 'bootstrapping' &&
    phase !== 'sealed' &&
    phase !== 'active'
  )
    throw new TypeError('Invalid bootstrap phase');
  const completed = r['completed_count'];
  if (typeof completed !== 'string' || !/^(0|[1-9][0-9]*)$/.test(completed))
    throw new TypeError('Invalid seed progress');
  const bound = r['chunk_size'];
  if (!(
    bound === null ||
    (typeof bound === 'number' &&
      Number.isInteger(bound) &&
      bound >= 1 &&
      bound <= 128)
  ))
    throw new TypeError('Invalid chunk bound');
  return {
    source_epoch: uuid(r['source_epoch']),
    phase,
    bootstrap_key:
      r['bootstrap_key'] === null ? null : uuid(r['bootstrap_key']),
    requested_count:
      r['requested_count'] === null
        ? null
        : positiveBigint(r['requested_count']),
    completed_count: completed,
    chunk_size: bound,
  };
}
export class Bootstrap {
  readonly #owner: TransactionOwner<Work>;
  constructor(config: ConnectionConfig) {
    this.#owner = new TransactionOwner(config, (client, operation) => {
      let used = false;
      const once = <T>(fn: () => Promise<T>): Promise<T> =>
        operation(async () => {
          if (used)
            throw new Error('One bounded bootstrap operation per transaction');
          used = true;
          return fn();
        });
      const result = async (sql: string, args: unknown[]) => {
        const rows = (await client.query<{ value: unknown }>(sql, args)).rows;
        if (rows.length !== 1) throw new Error('Missing bootstrap result');
        return status(rows[0]?.value);
      };
      return Object.freeze({
        status: (epoch: string) =>
          once(() =>
            result('SELECT source.bootstrap_status($1) value', [uuid(epoch)]),
          ),
        begin: (r: BootstrapRecipe) =>
          once(() =>
            result('SELECT source.begin_bootstrap($1,$2,$3,$4,$5,$6) value', [
              uuid(r.epoch),
              uuid(r.key),
              r.version,
              r.seed,
              positiveBigint(r.count),
              r.chunkSize,
            ]),
          ),
        chunk: (epoch: string, key: string, first: string) =>
          once(async () => {
            const rows = (
              await client.query<Record<string, unknown>>(
                'SELECT * FROM source.seed_chunk($1,$2,$3)',
                [uuid(epoch), uuid(key), positiveBigint(first)],
              )
            ).rows;
            if (rows.length < 1 || rows.length > 128)
              throw new Error('Invalid chunk result bound');
            return rows.map((r) => {
              if (
                typeof r['recorded_at'] !== 'string' ||
                typeof r['replayed'] !== 'boolean'
              )
                throw new TypeError('Invalid seed mapping');
              return {
                ordinal: positiveBigint(r['ordinal']),
                entity_id: positiveBigint(r['entity_id']),
                recorded_at: r['recorded_at'],
                replayed: r['replayed'],
              };
            });
          }),
        seal: (epoch: string, key: string) =>
          once(() =>
            result('SELECT source.seal_bootstrap($1,$2) value', [
              uuid(epoch),
              uuid(key),
            ]),
          ),
        activate: (epoch: string, key: string, pipeline: string) =>
          once(() =>
            result('SELECT source.activate_bootstrap($1,$2,$3,$4) value', [
              uuid(epoch),
              uuid(key),
              uuid(pipeline),
              codec,
            ]),
          ),
      });
    });
  }
  transaction<T>(fn: (work: Work) => Promise<T>): Promise<T> {
    return this.#owner.transaction(fn);
  }
  status(epoch: string) {
    return this.transaction((tx) => tx.status(epoch));
  }
  begin(recipe: BootstrapRecipe) {
    return this.transaction((tx) => tx.begin(recipe));
  }
  chunk(epoch: string, key: string, first: string) {
    return this.transaction((tx) => tx.chunk(epoch, key, first));
  }
  seal(epoch: string, key: string) {
    return this.transaction((tx) => tx.seal(epoch, key));
  }
  // Identity validation is outside the source transaction. SQL checks the same registered values locally.
  async activate(
    epoch: string,
    key: string,
    expectedPipeline: string,
    pipelineConfig: ConnectionConfig,
  ) {
    const identity = await pipelineIdentity(pipelineConfig);
    if (
      identity.pipelineId !== uuid(expectedPipeline) ||
      identity.sourceEpoch !== uuid(epoch) ||
      identity.codec !== codec
    )
      throw new Error('Activation pipeline identity mismatch');
    return this.transaction((tx) => tx.activate(epoch, key, expectedPipeline));
  }
}
