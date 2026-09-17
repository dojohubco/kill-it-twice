import { Client, Serializer } from '@elastic/elasticsearch';
import { parse, isLosslessNumber } from 'lossless-json';

class ExactSerializer extends Serializer {
  override deserialize<T = unknown>(json: string): T {
    // The official hook is generic. All calls below request unknown and narrow it;
    // this cast implements that signature, never promises a generated ES type.
    const value: unknown = parse(json);
    return value as T;
  }
}
export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Expected receiver object');
  return value as Record<string, unknown>;
}
export function exactInteger(value: unknown): string {
  if (!isLosslessNumber(value) || !/^(0|[1-9][0-9]*)$/.test(value.value))
    throw new Error('Expected exact receiver integer token');
  return value.value;
}
export function version(value: unknown): string {
  const text = exactInteger(value);
  if (BigInt(text) < 1n || BigInt(text) > 9223372036854775807n)
    throw new Error('Receiver version outside signed BIGINT');
  return text;
}
export type EsConnection = {
  node: string;
  username: string;
  password: string;
  ca: string;
};
export class EsTransport {
  readonly #client: Client;
  constructor(config: EsConnection) {
    const url = new URL(config.node);
    if (url.protocol !== 'https:' || url.username || url.password)
      throw new Error('ES requires HTTPS with separate credentials');
    this.#client = new Client({
      node: config.node,
      auth: { username: config.username, password: config.password },
      tls: { ca: config.ca, rejectUnauthorized: true },
      Serializer: ExactSerializer,
      maxRetries: 0,
      sniffOnStart: false,
      sniffOnConnectionFault: false,
      sniffInterval: false,
      requestTimeout: 10_000,
      maxResponseSize: 4 * 1024 * 1024,
      maxCompressedResponseSize: 4 * 1024 * 1024,
    });
  }
  async request(
    method: 'GET' | 'POST' | 'PUT' | 'DELETE',
    path: string,
    body?: string,
    correlation?: string,
  ): Promise<unknown> {
    if (body !== undefined && Buffer.byteLength(body) > 4 * 1024 * 1024)
      throw new Error('ES request exceeds 4 MiB');
    return this.#client.transport.request<unknown>(
      {
        method,
        path,
        ...(body === undefined ? {} : { body }),
      },
      {
        headers: {
          'content-type': path.endsWith('_bulk')
            ? 'application/x-ndjson'
            : 'application/json',
        },
        signal: AbortSignal.timeout(10_000),
        ...(correlation ? { opaqueId: correlation } : {}),
      },
    );
  }
  async close() {
    await this.#client.close();
  }
}
