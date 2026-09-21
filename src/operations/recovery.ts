import { randomUUID } from 'node:crypto';
import { TransactionOwner } from '../internal/transaction.ts';
import { EsAdapter, classify } from '../es/adapter.ts';
import { EsLedger } from '../es/ledger.ts';
import { EsTransport } from '../es/transport.ts';
import type { OperationsConfig } from './config.ts';
import { withOperationalCleanup, OperationalCleanupError } from './cleanup.ts';
import {
  ControlError,
  record,
  shape,
  string,
  id,
  bigint,
  integer,
  eventId,
  cursor,
  encodeCursor,
} from './validation.ts';

export interface RecoveryRequest {
  request_id: string;
  actor: string;
  reason: string;
  destination_id: string;
  generation: string;
  selection: { event_id: string; attempt_id: string }[];
}
export function recoveryInput(
  value: unknown,
  kind: 'replay' | 'supersession' | 'verify_target',
): RecoveryRequest {
  const r = shape(value, [
    'request_id',
    'actor',
    'reason',
    'destination_id',
    'generation',
    'selection',
  ]);
  if (!Array.isArray(r['selection']))
    throw new ControlError(400, 'invalid_request');
  const selected: unknown[] = r['selection'];
  if (
    (kind === 'replay' && (selected.length < 1 || selected.length > 50)) ||
    (kind === 'supersession' && selected.length !== 1) ||
    (kind === 'verify_target' && selected.length !== 0)
  )
    throw new ControlError(400, 'invalid_request');
  const selection = selected
    .map((v) => {
      const s = shape(v, ['event_id', 'attempt_id']);
      return {
        event_id: eventId(s['event_id']),
        attempt_id: id(s['attempt_id']),
      };
    })
    .sort((a, b) =>
      a.event_id < b.event_id ? -1 : a.event_id > b.event_id ? 1 : 0,
    );
  if (new Set(selection.map((v) => v.event_id)).size !== selection.length)
    throw new ControlError(400, 'invalid_request');
  const actor = string(r['actor'], 64),
    reason = string(r['reason'], 512);
  if (
    Buffer.byteLength(actor) > 64 ||
    Buffer.byteLength(reason) > 512 ||
    Buffer.byteLength(JSON.stringify(selection)) > 16384
  )
    throw new ControlError(400, 'invalid_request');
  return {
    request_id: id(r['request_id']),
    actor,
    reason,
    destination_id: id(r['destination_id']),
    generation: bigint(r['generation']),
    selection,
  };
}

/** Restricted domain boundary shared by a private CLI, not a new HTTP surface. */
export class RecoveryService {
  readonly #config: OperationsConfig;
  constructor(config: OperationsConfig) {
    this.#config = config;
  }
  async #query(
    sql: string,
    args: unknown[],
    readOnly = false,
  ): Promise<Record<string, unknown>> {
    const owner = new TransactionOwner(
      this.#config.pipeline,
      (client, operation) => ({
        execute: () =>
          operation(async () => {
            if (readOnly) await client.query('SET TRANSACTION READ ONLY');
            const result = await client.query<{ value: unknown }>(sql, args);
            const value = record(result.rows[0]?.value);
            if (Buffer.byteLength(JSON.stringify(value)) > 262144)
              throw new ControlError(422, 'response_bound');
            return value;
          }),
      }),
    );
    return owner.transaction((w) => w.execute());
  }
  async replay(value: unknown, correlation = randomUUID()) {
    const r = recoveryInput(value, 'replay');
    return this.#query(
      'SELECT pipeline.request_batch_replay($1,$2,$3,$4,$5,$6,$7,$8) value',
      [
        r.request_id,
        id(correlation),
        this.#config.pipelineId,
        r.destination_id,
        r.generation,
        r.actor,
        r.reason,
        JSON.stringify(r.selection),
      ],
    );
  }
  status(key: string) {
    return this.#query(
      'SELECT pipeline.recovery_status($1) value',
      [id(key)],
      true,
    );
  }
  overview() {
    return this.#query('SELECT pipeline.recovery_overview() value', [], true);
  }
  run(run: string) {
    return this.#query(
      'SELECT pipeline.recovery_run($1) value',
      [id(run)],
      true,
    );
  }
  async list(
    category: string,
    input: unknown = {},
  ): Promise<Record<string, unknown>> {
    if (!['operations', 'runs', 'failures', 'attempts'].includes(category))
      throw new ControlError(400, 'invalid_request');
    const q = shape(input, ['limit', 'cursor', 'event_id']);
    const n = q['limit'] === undefined ? 50 : integer(q['limit'], 1, 100);
    const event = category === 'attempts' ? eventId(q['event_id']) : null;
    if (category !== 'attempts' && q['event_id'] !== undefined)
      throw new ControlError(400, 'invalid_request');
    const c = cursor(q['cursor'], 'recovery-list');
    if (c) {
      shape(c, ['scope', 'v', 'category', 'event_id', 'after']);
      if (c['category'] !== category || c['event_id'] !== event)
        throw new ControlError(400, 'invalid_request');
    }
    const result = await this.#query(
      'SELECT pipeline.recovery_list($1,$2,$3,$4) value',
      [category, c ? string(c['after'], 256) : '', n, event],
      true,
    );
    const next = result['next_key'];
    return {
      ...result,
      next_cursor:
        next === null
          ? null
          : encodeCursor('recovery-list', {
              category,
              event_id: event,
              after: string(next, 256),
            }),
    };
  }
  supersede(value: unknown, correlation = randomUUID()) {
    return this.#verify('supersession', value, correlation);
  }
  verifyTarget(value: unknown, correlation = randomUUID()) {
    return this.#verify('verify_target', value, correlation);
  }
  async #verify(
    kind: 'supersession' | 'verify_target',
    value: unknown,
    correlation: string,
  ) {
    const r = recoveryInput(value, kind),
      incarnation = randomUUID();
    const admitted = await this.#query(
      'SELECT pipeline.begin_recovery_check($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) value',
      [
        r.request_id,
        id(correlation),
        kind,
        this.#config.pipelineId,
        r.destination_id,
        r.generation,
        r.actor,
        r.reason,
        JSON.stringify(r.selection),
        incarnation,
        30000,
      ],
    );
    if (admitted['state'] !== 'leased') return admitted;
    const items = admitted['items'];
    if (!Array.isArray(items)) throw new ControlError(422, 'integrity_block');
    const claim = record(items[0]),
      generation = bigint(claim['generation']);
    if (claim['owner_id'] !== incarnation)
      throw new ControlError(409, 'conflict');
    let remote: string | null = null,
      witness: string | null = null,
      failure: string | null = null;
    let remoteError: unknown;
    let cleanupFailures = 0;
    // No pipeline transaction is open while validating the registered receiver.
    // A remote validation failure is audited; a later SQL COMMIT ambiguity is NOT relabeled a remote failure.
    try {
      const transport = new EsTransport(this.#config.es);
      await withOperationalCleanup(
        async () => {
          const ledger = new EsLedger(this.#config.pipeline),
            target = await ledger.target();
          if (
            target.id !== r.destination_id ||
            target.generation !== r.generation ||
            target.pipelineId !== this.#config.pipelineId ||
            target.epoch !== this.#config.sourceEpoch
          )
            throw new ControlError(422, 'integrity_block');
          const adapter = new EsAdapter(transport);
          await adapter.validate(target);
          if (kind === 'supersession') {
            const member = r.selection[0];
            if (!member) throw new ControlError(422, 'integrity_block');
            const projection = await ledger.read(member.event_id);
            const result = await adapter.resolve(
              target,
              {
                projection,
                outcome: 'already_applied',
                remote: null,
                witness: null,
                context:
                  'Explicit operator verification, not an observed bulk response',
              },
              ledger,
            );
            if (result.outcome !== 'superseded') failure = 'not_superseded';
            else {
              remote = result.remote;
              witness = result.witness;
            }
          }
          await adapter.validate(target);
        },
        () => transport.close(),
      );
    } catch (error) {
      remoteError = error;
      const primary =
        error instanceof OperationalCleanupError ? error.primary : error;
      cleanupFailures =
        error instanceof OperationalCleanupError ? error.cleanup.length : 0;
      failure =
        primary instanceof ControlError
          ? 'integrity'
          : classify(primary).classification;
    }
    try {
      return await this.#query(
        'SELECT pipeline.finish_recovery_check($1,$2,$3,$4,$5,$6,$7,$8) value',
        [
          r.request_id,
          incarnation,
          generation,
          failure === null,
          remote,
          witness,
          failure,
          cleanupFailures,
        ],
      );
    } catch (recordingError) {
      if (remoteError !== undefined)
        throw new OperationalCleanupError(remoteError, [recordingError]);
      throw recordingError;
    }
  }
}
