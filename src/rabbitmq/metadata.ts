import { request } from 'node:https';
export type FailureKind = 'transient' | 'auth' | 'configuration' | 'integrity';
export class BrokerFailure extends Error {
  readonly classification: FailureKind;
  constructor(classification: FailureKind, message: string) {
    super(message);
    this.classification = classification;
  }
}
export interface MetadataConnection {
  url: string;
  username: string;
  password: string;
  ca: string;
}
// Bounded control-plane requests. No AMQP or administrator credentials in errors.
export class BrokerMetadata {
  readonly #config: MetadataConnection;
  constructor(config: MetadataConnection) {
    this.#config = { ...config };
  }
  request(method: string, path: string, value?: unknown): Promise<unknown> {
    const data = value === undefined ? undefined : JSON.stringify(value);
    if (data && Buffer.byteLength(data) > 65536)
      throw new Error('Metadata request exceeds 64 KiB');
    return new Promise((resolve, reject) => {
      const url = new URL(path, this.#config.url);
      if (url.protocol !== 'https:')
        throw new Error('Broker metadata requires TLS');
      const req = request(
        url,
        {
          method,
          ca: this.#config.ca,
          rejectUnauthorized: true,
          auth: `${this.#config.username}:${this.#config.password}`,
          agent: false,
          signal: AbortSignal.timeout(5000),
          headers: data
            ? {
                'content-type': 'application/json',
                'content-length': Buffer.byteLength(data),
              }
            : {},
        },
        (res) => {
          const parts: Buffer[] = [];
          let size = 0;
          res.on('data', (part: Buffer) => {
            size += part.length;
            if (size > 65536) {
              res.destroy(
                new BrokerFailure(
                  'integrity',
                  'Broker metadata exceeds 64 KiB',
                ),
              );
              return;
            }
            parts.push(part);
          });
          res.on('error', reject);
          res.on('end', () => {
            const status = res.statusCode ?? 0;
            if (status < 200 || status >= 300) {
              reject(
                new BrokerFailure(
                  status === 401 || status === 403
                    ? 'auth'
                    : status === 404
                      ? 'configuration'
                      : 'transient',
                  `Broker metadata HTTP ${status}`,
                ),
              );
              return;
            }
            try {
              const text = Buffer.concat(parts).toString('utf8');
              const parsed: unknown = text ? JSON.parse(text) : null;
              resolve(parsed);
            } catch {
              reject(
                new BrokerFailure('integrity', 'Malformed broker metadata'),
              );
            }
          });
        },
      );
      req.on('error', () =>
        reject(
          new BrokerFailure('transient', 'Broker metadata request unavailable'),
        ),
      );
      req.end(data);
    });
  }
}
export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new BrokerFailure('integrity', 'Expected broker object');
  return value as Record<string, unknown>;
}
export function field(value: Record<string, unknown>, key: string): string {
  const x = value[key];
  if (typeof x !== 'string')
    throw new BrokerFailure('integrity', `Invalid broker field ${key}`);
  return x;
}
export const queueArguments = {
  'x-queue-type': 'quorum',
  'x-delivery-limit': -1,
  'x-overflow': 'reject-publish',
  'x-max-length-bytes': 16777216,
};
export interface Topology {
  registrationId: string;
  pipelineId: string;
  epoch: string;
  consumerId: string;
  vhost: string;
  exchange: string;
  queue: string;
  routingKey: string;
}
export async function validateTopology(
  api: BrokerMetadata,
  t: Topology,
): Promise<void> {
  const v = encodeURIComponent(t.vhost),
    e = encodeURIComponent(t.exchange),
    q = encodeURIComponent(t.queue);
  const exchange = record(await api.request('GET', `/api/exchanges/${v}/${e}`));
  const queue = record(await api.request('GET', `/api/queues/${v}/${q}`));
  const bindings = await api.request('GET', `/api/bindings/${v}/e/${e}/q/${q}`);
  const args = record(queue['arguments']);
  if (
    exchange['type'] !== 'direct' ||
    exchange['durable'] !== true ||
    exchange['auto_delete'] !== false ||
    exchange['internal'] !== false ||
    Object.keys(record(exchange['arguments'])).length ||
    queue['type'] !== 'quorum' ||
    queue['durable'] !== true ||
    queue['exclusive'] !== false ||
    queue['auto_delete'] !== false ||
    Object.keys(args).length !== Object.keys(queueArguments).length ||
    Object.entries(queueArguments).some(([k, val]) => args[k] !== val) ||
    (queue['policy'] !== undefined &&
      queue['policy'] !== null &&
      queue['policy'] !== '') ||
    (queue['operator_policy'] !== undefined &&
      queue['operator_policy'] !== null &&
      queue['operator_policy'] !== '') ||
    Object.keys(record(queue['effective_policy_definition'] ?? {})).length !==
      0 ||
    !Array.isArray(bindings) ||
    bindings.length !== 1
  )
    throw new BrokerFailure(
      'configuration',
      'Registered broker topology differs',
    );
  const b = record(bindings[0]);
  if (
    b['source'] !== t.exchange ||
    b['destination'] !== t.queue ||
    b['destination_type'] !== 'queue' ||
    b['routing_key'] !== t.routingKey ||
    Object.keys(record(b['arguments'])).length
  )
    throw new BrokerFailure('configuration', 'Registered routing differs');
}
