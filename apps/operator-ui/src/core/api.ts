import { Injectable, signal } from '@angular/core';
import { parse } from 'lossless-json';
import { record, value, type Data } from './value';
export class ApiFailure extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    readonly requestId: string | null,
  ) {
    super(code);
  }
  get ambiguous(): boolean {
    return this.status === 0 || this.status >= 500;
  }
  get instruction(): string {
    switch (this.code) {
      case 'operator_required':
        return 'Operator access is required. Check the token and retry.';
      case 'conflict':
        return 'The record or request changed. Refresh its latest state before starting a new action.';
      case 'not_found':
        return 'This item was not found. Return to the list and refresh.';
      case 'invalid_request':
        return 'Check the requested values and try again.';
      case 'integrity_block':
        return 'This operation is blocked by an integrity or configuration check. Inspect its current state.';
      case 'response_too_large':
        return 'The response exceeded the interface limit. Use a smaller page or inspect the CLI.';
      default:
        return 'The service is unavailable or the request did not finish. Check the connection and refresh.';
    }
  }
}
@Injectable({ providedIn: 'root' })
export class Api {
  readonly operator = signal(false);
  #token = '';
  unlock(token: string): void {
    if (token.length < 32 || token.length > 1024 || /[\r\n]/.test(token))
      throw new Error(
        'Use the run-local token from the private control configuration.',
      );
    this.#token = token;
    this.operator.set(true);
  }
  lock(): void {
    this.#token = '';
    this.operator.set(false);
  }
  async read(path: string, signal?: AbortSignal): Promise<Data> {
    return (await this.request(path, 'GET', undefined, undefined, signal)).data;
  }
  async mutate(
    path: string,
    method: 'POST' | 'PUT',
    body: Data,
    key: string,
  ): Promise<{ data: Data; requestId: string | null; outcome: string }> {
    if (!this.operator()) throw new ApiFailure(401, 'operator_required', null);
    return this.request(path, method, body, key);
  }
  private async request(
    path: string,
    method: string,
    body?: Data,
    key?: string,
    signal?: AbortSignal,
  ): Promise<{ data: Data; requestId: string | null; outcome: string }> {
    if (!path.startsWith('/api/v1/') || path.includes('://'))
      throw new Error('Only the local operational API is allowed.');
    const headers: Record<string, string> = { Accept: 'application/json' };
    if (key) {
      headers['Authorization'] = `Bearer ${this.#token}`;
      headers['Idempotency-Key'] = key;
      headers['Content-Type'] = 'application/json';
    }
    const deadline = AbortSignal.timeout(12000),
      combined = signal ? AbortSignal.any([deadline, signal]) : deadline;
    try {
      const response = await fetch(path, {
        method,
        headers,
        cache: 'no-store',
        credentials: 'omit',
        redirect: 'error',
        signal: combined,
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      let bytes = 0;
      const chunks: Uint8Array[] = [];
      const reader = response.body?.getReader();
      if (!reader)
        throw new ApiFailure(
          503,
          'unavailable',
          response.headers.get('x-request-id'),
        );
      try {
        for (;;) {
          const part = await reader.read();
          if (part.done) break;
          bytes += part.value.byteLength;
          if (bytes > 4 * 1024 * 1024) {
            await reader.cancel();
            throw new ApiFailure(
              413,
              'response_too_large',
              response.headers.get('x-request-id'),
            );
          }
          chunks.push(part.value);
        }
      } finally {
        reader.releaseLock();
      }
      const all = new Uint8Array(bytes);
      let at = 0;
      for (const chunk of chunks) {
        all.set(chunk, at);
        at += chunk.byteLength;
      }
      const envelope = record(
          parse(new TextDecoder('utf-8', { fatal: true }).decode(all)),
        ),
        requestId =
          value(
            envelope['request_id'],
            response.headers.get('x-request-id') ?? '',
          ) || null;
      if (!response.ok)
        throw new ApiFailure(
          response.status,
          value(record(envelope['error'])['code'], 'unavailable'),
          requestId,
        );
      const raw = envelope['data'];
      if (raw === null || typeof raw !== 'object' || Array.isArray(raw))
        throw new ApiFailure(502, 'invalid_response', requestId);
      const data = record(raw);
      const route = path.split('?')[0];
      if (
        method === 'GET' &&
        (route === '/api/v1/entities' || route === '/api/v1/failures')
      ) {
        if (
          !Array.isArray(data['items']) ||
          !(
            data['next_cursor'] === null ||
            typeof data['next_cursor'] === 'string'
          )
        )
          throw new ApiFailure(502, 'invalid_response', requestId);
        if (
          data['items'].some(
            (item: unknown) =>
              item === null || typeof item !== 'object' || Array.isArray(item),
          )
        )
          throw new ApiFailure(502, 'invalid_response', requestId);
      }
      return {
        data,
        requestId,
        outcome: value(envelope['outcome'], 'observed'),
      };
    } catch (e) {
      if (e instanceof ApiFailure) throw e;
      throw new ApiFailure(0, 'unavailable', null);
    }
  }
}
