import { createHash } from 'node:crypto';
import type { Message, Options } from 'amqplib';
import { canonicalEvent, uuid, type CanonicalEvent } from '../envelope.ts';
import { record, type Topology } from './metadata.ts';
export const transportCap = 131072;
export class PoisonMessage extends Error {
  readonly classification: 'validation' | 'conflicting_content';
  constructor(
    message: string,
    classification: 'validation' | 'conflicting_content' = 'validation',
  ) {
    super(message);
    this.classification = classification;
  }
}
export function decodeWire(bytes: Buffer): CanonicalEvent {
  if (bytes.length > transportCap)
    throw new Error('Transport cap exceeded; no ACK is permitted');
  try {
    const raw: unknown = JSON.parse(
      new TextDecoder('utf-8', { fatal: true }).decode(bytes),
    );
    const r = record(raw);
    if (Object.keys(r).sort().join(',') !== 'body,content_sha256')
      throw new Error('Unknown wire fields');
    const e = canonicalEvent(r['body']);
    if (e.contentSha256 !== r['content_sha256'] || !e.wireBytes.equals(bytes))
      throw new Error('Noncanonical bytes/hash');
    return e;
  } catch {
    throw new PoisonMessage('Invalid canonical wire envelope');
  }
}
export function properties(
  t: Topology,
  eventId: string,
  attempt: string,
): Options.Publish {
  uuid(attempt);
  return {
    mandatory: true,
    persistent: true,
    messageId: eventId,
    correlationId: attempt,
    contentType: 'application/json',
    contentEncoding: 'utf-8',
    type: 'revision-v1',
    headers: { pipeline_id: t.pipelineId, registration_id: t.registrationId },
  };
}
// Stable bounded metadata excludes correlation/channel/redelivery, which change on retry.
export function retainedMetadata(message: Message): Buffer {
  const raw: unknown = message.properties;
  const p = record(raw);
  const headers: unknown = p['headers'];
  const values = {
    messageId: p['messageId'] ?? null,
    contentType: p['contentType'] ?? null,
    contentEncoding: p['contentEncoding'] ?? null,
    type: p['type'] ?? null,
    deliveryMode: p['deliveryMode'] ?? null,
    headers:
      headers && typeof headers === 'object' && !Array.isArray(headers)
        ? {
            pipeline_id: record(headers)['pipeline_id'] ?? null,
            registration_id: record(headers)['registration_id'] ?? null,
          }
        : (headers ?? null),
  };
  const text = JSON.stringify(values);
  if (Buffer.byteLength(text) > 4096)
    throw new Error('AMQP metadata exceeds supported retention bound; no ACK');
  return Buffer.from(text);
}
export function validateMessage(t: Topology, message: Message): CanonicalEvent {
  const e = decodeWire(message.content);
  try {
    const meta: unknown = JSON.parse(
      retainedMetadata(message).toString('utf8'),
    );
    const p = record(meta),
      h = record(p['headers']);
    if (
      p['messageId'] !== e.body.event_id ||
      p['contentType'] !== 'application/json' ||
      p['contentEncoding'] !== 'utf-8' ||
      p['type'] !== 'revision-v1' ||
      p['deliveryMode'] !== 2 ||
      h['pipeline_id'] !== t.pipelineId ||
      h['registration_id'] !== t.registrationId ||
      e.body.source_epoch !== t.epoch
    )
      throw new Error('Message identity/protocol mismatch');
    return e;
  } catch {
    throw new PoisonMessage('Message identity/protocol mismatch');
  }
}
export const digest = (bytes: Buffer) =>
  createHash('sha256').update(bytes).digest('hex');
