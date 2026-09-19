// Verifier-owned workload declarations, never inferred from pipeline outcomes.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import type pg from 'pg';
import { object, version } from '../../src/es/transport.ts';
import { evidence } from './db.ts';
import type { create } from './es.ts';

export interface ExpectedRejection {
  readonly eventId: string;
  readonly commandId: string;
  readonly payloadText: string;
  readonly payloadSha256: string;
  readonly errorType: 'document_parsing_exception';
  readonly field: 'search_fields.loyalty_points';
  readonly rejectedToken: '"not-a-number"';
  readonly fixture: string;
}
export async function declareMappingRejection(
  source: pg.Client,
  pipeline: pg.Client,
  command: Awaited<ReturnType<typeof create>>,
  fixture: string,
): Promise<ExpectedRejection> {
  const rows = (
    await source.query<{ payload: string; token: string }>(
      `SELECT o.payload::text payload,(o.payload->'loyalty_points')::text token
     FROM source.outbox o JOIN source.command_receipts r
       ON r.source_epoch=o.source_epoch AND r.result_entity_id=o.entity_id
       AND r.result_version=o.entity_version AND r.result_change_id=o.change_id
     WHERE r.command_id=$1 AND o.source_epoch=$2 AND o.entity_id=$3 AND o.entity_version=$4`,
      [
        command.command.commandId,
        command.command.sourceEpoch,
        command.id,
        command.reply.result.entity_version,
      ],
    )
  ).rows;
  assert.equal(rows.length, 1);
  const row = rows[0];
  assert.ok(row);
  assert.equal(
    row.token,
    '"not-a-number"',
    'This declaration supports only the explicit bad-string fixture, not a second mapper',
  );
  assert.equal(
    (
      await pipeline.query(
        'SELECT 1 FROM pipeline.es_attempts WHERE event_id=$1',
        [command.eventId],
      )
    ).rows.length,
    0,
    'Declare expectations before any delivery attempt',
  );
  const expected: ExpectedRejection = Object.freeze({
    eventId: command.eventId,
    commandId: command.command.commandId,
    payloadText: row.payload,
    payloadSha256: createHash('sha256').update(row.payload).digest('hex'),
    errorType: 'document_parsing_exception',
    field: 'search_fields.loyalty_points',
    rejectedToken: '"not-a-number"',
    fixture,
  });
  evidence('expected-rejection-declared-before-delivery', expected);
  return expected;
}
export function checkExpectedOutcome(
  eventId: string,
  state: string,
  expected: ExpectedRejection | undefined,
  diagnostic: Record<string, unknown> | undefined,
): void {
  if (!expected) {
    assert.equal(
      state,
      'satisfied',
      `Undeclared rejection or unresolved admissible event ${eventId}`,
    );
    assert.equal(diagnostic, undefined, `Unexpected dead letter ${eventId}`);
    return;
  }
  assert.equal(expected.eventId, eventId);
  assert.equal(
    state,
    'dead_letter',
    `Planned rejection is not actual failure evidence: ${eventId}`,
  );
  assert.ok(diagnostic, `Missing actual receiver diagnostic for ${eventId}`);
  assert.equal(diagnostic['event_id'], eventId);
  assert.equal(diagnostic['error_class'], 'mapping');
  assert.equal(diagnostic['attempt_event'], eventId);
  assert.equal(diagnostic['attempt_outcome'], 'mapping');
  assert.equal(diagnostic['attempt_id'], diagnostic['terminal_attempt']);
  assert.equal(diagnostic['destination_id'], diagnostic['attempt_destination']);
  assert.equal(diagnostic['context'], diagnostic['attempt_context']);
  assert.ok(diagnostic['finished_at']);
  const context = diagnostic['context'];
  assert.ok(typeof context === 'string');
  assert.match(context, /^request=[a-f0-9-]{36}; /);
  assert.ok(context.includes(expected.errorType));
  assert.ok(context.includes(expected.field));
  assert.ok(context.includes('not-a-number'));
}
export interface ExpectedReceiver {
  eventId: string;
  documentId: string;
  version: string;
  source: Record<string, unknown>;
}
export function checkReceiver(
  documentId: string,
  expected: ExpectedReceiver | undefined,
  actual: Record<string, unknown>,
): void {
  assert.equal(actual['_id'], documentId);
  if (!expected) {
    assert.equal(actual['found'], false, `Expected absence for ${documentId}`);
    assert.equal(actual['_source'], undefined);
    return;
  }
  assert.equal(
    actual['found'],
    true,
    `Missing admissible receiver revision ${expected.eventId}`,
  );
  assert.equal(version(actual['_version']), expected.version);
  assert.deepEqual(
    object(actual['_source']),
    expected.source,
    `Receiver content mismatch for ${expected.eventId}`,
  );
}
export function checkSearchIdentities(
  expected: readonly string[],
  hits: readonly unknown[],
): void {
  assert.deepEqual(
    hits.map((h) => object(h)['_id']).sort(),
    [...expected].sort(),
    'Receiver search identities differ from independently admissible history',
  );
}
