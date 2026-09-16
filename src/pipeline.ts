import { validateEvent, uuid, codec, type CanonicalEvent } from './envelope.ts';
import { byteBound, countBound } from './limits.ts';
import {
  TransactionOwner,
  TransactionError,
  OwnershipError,
  type ConnectionConfig,
} from './internal/transaction.ts';
import type { SourceReader, RevisionKey } from './source-reader.ts';
export interface EventInput {
  bodyBytes: Uint8Array;
  contentSha256: string;
}
export interface StageResult {
  eventId: string;
  status: 'inserted' | 'already_staged';
}
export interface PipelineWork {
  stage(inputs: readonly EventInput[]): Promise<StageResult[]>;
}
class Conflict extends Error {
  readonly eventId: string;
  readonly contentSha256: string;
  readonly code: 'P3001' | 'P3002';
  constructor(event: CanonicalEvent, code: 'P3001' | 'P3002', cause?: unknown) {
    super(`Pipeline integrity failure ${code}: ${event.body.event_id}`, {
      cause,
    });
    this.eventId = event.body.event_id;
    this.contentSha256 = event.contentSha256;
    this.code = code;
  }
}
export class IntegrityError extends Error {
  readonly primary: unknown;
  readonly diagnosticFailure: unknown;
  readonly code: string;
  readonly postgresError: unknown;
  constructor(
    primary: unknown,
    conflict: Conflict,
    diagnosticFailure: unknown,
  ) {
    super(conflict.message, { cause: primary });
    this.primary = primary;
    this.diagnosticFailure = diagnosticFailure;
    this.code = conflict.code;
    this.postgresError = conflict.cause;
  }
}
function prepare(inputs: readonly EventInput[]): CanonicalEvent[] {
  countBound(inputs.length);
  // Validate and copy all content before any database effects, including within-batch conflicts.
  const events = inputs.map((input) =>
    validateEvent(input.bodyBytes, input.contentSha256),
  );
  byteBound(events.map((event) => event.wireBytes.length));
  const unique = new Map<string, CanonicalEvent>();
  for (const event of events) {
    const prior = unique.get(event.body.event_id);
    if (
      prior &&
      (!prior.bodyBytes.equals(event.bodyBytes) ||
        prior.contentSha256 !== event.contentSha256)
    )
      throw new Conflict(event, 'P3001');
    unique.set(event.body.event_id, event);
  }
  return [...unique.values()].sort((a, b) =>
    a.body.event_id < b.body.event_id
      ? -1
      : a.body.event_id > b.body.event_id
        ? 1
        : 0,
  );
}
export class Pipeline {
  #owner: TransactionOwner<PipelineWork>;
  #integrityCauses = new WeakMap<Error, Conflict>();
  #diagnostic: TransactionOwner<{ record(conflict: Conflict): Promise<void> }>;
  constructor(config: ConnectionConfig, expectedPipelineId?: string) {
    if (expectedPipelineId !== undefined) uuid(expectedPipelineId);
    this.#owner = new TransactionOwner(config, (client, operation) => {
      let used = false;
      return Object.freeze({
        stage: (inputs: readonly EventInput[]) =>
          operation(async () => {
            if (used)
              throw new OwnershipError(
                'One bounded stage batch per pipeline transaction',
              );
            used = true;
            const events = prepare(inputs);
            const results: StageResult[] = [];
            for (const event of events) {
              try {
                const row = (
                  await client.query<{ status: string }>(
                    expectedPipelineId === undefined
                      ? 'SELECT pipeline.stage_event($1,$2) AS status'
                      : 'SELECT pipeline.stage_bound_event($1,$2,$3) AS status',
                    expectedPipelineId === undefined
                      ? [event.bodyBytes, event.contentSha256]
                      : [
                          expectedPipelineId,
                          event.bodyBytes,
                          event.contentSha256,
                        ],
                  )
                ).rows[0];
                if (
                  !row ||
                  (row.status !== 'inserted' && row.status !== 'already_staged')
                )
                  throw new Error('Invalid stage result');
                results.push({
                  eventId: event.body.event_id,
                  status: row.status,
                });
              } catch (error) {
                if (
                  error instanceof Error &&
                  'code' in error &&
                  (error.code === 'P3001' || error.code === 'P3002')
                )
                  this.#integrityCauses.set(
                    error,
                    new Conflict(event, error.code, error),
                  );
                throw error;
              }
            }
            return results;
          }),
      });
    });
    this.#diagnostic = new TransactionOwner(
      { ...config, application_name: `${config.application_name}:incident` },
      (client, operation) => ({
        record: (conflict) =>
          operation(async () => {
            await client.query('SELECT pipeline.record_incident($1,$2,$3)', [
              conflict.eventId,
              conflict.code,
              conflict.contentSha256,
            ]);
          }),
      }),
    );
  }
  stage(inputs: readonly EventInput[]): Promise<StageResult[]> {
    return this.transaction((tx) => tx.stage(inputs));
  }
  async transaction<T>(work: (tx: PipelineWork) => Promise<T>): Promise<T> {
    try {
      return await this.#owner.transaction(work);
    } catch (primary) {
      const cause =
        primary instanceof TransactionError ? primary.cause : primary;
      const conflict =
        cause instanceof Conflict
          ? cause
          : cause instanceof Error
            ? this.#integrityCauses.get(cause)
            : undefined;
      if (!conflict) throw primary;
      let diagnosticFailure: unknown;
      try {
        await this.#diagnostic.transaction((tx) => tx.record(conflict));
      } catch (error) {
        diagnosticFailure = error;
      }
      throw new IntegrityError(primary, conflict, diagnosticFailure);
    }
  }
}
export async function stageSelected(
  reader: SourceReader,
  pipeline: Pipeline,
  keys: readonly RevisionKey[],
) {
  const selected = await reader.outbox(keys);
  return {
    notVisible: selected.notVisible,
    results: selected.events.length
      ? await pipeline.stage(selected.events)
      : [],
  };
}

// Read-only identity is informational; stage_bound_event rechecks inside every staging transaction.
export async function pipelineIdentity(config: ConnectionConfig) {
  const owner = new TransactionOwner(config, (client, operation) => ({
    identity: () =>
      operation(async () => {
        const rows = (
          await client.query<Record<string, unknown>>(
            'SELECT * FROM pipeline.capture_identity()',
          )
        ).rows;
        const row = rows[0];
        if (rows.length !== 1 || !row || row['payload_encoding'] !== codec)
          throw new Error('Invalid pipeline identity');
        return {
          pipelineId: uuid(row['pipeline_id']),
          sourceEpoch: uuid(row['source_epoch']),
          codec,
        };
      }),
  }));
  return owner.transaction((tx) => tx.identity());
}
