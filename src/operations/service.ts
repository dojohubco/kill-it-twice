import { withOperationalCleanup } from './cleanup.ts';
import { networkLock } from './network-lock.ts';
import { BrokerMetadata } from '../rabbitmq/metadata.ts';
import { errors } from '@elastic/elasticsearch';
import { EsTransport, object, version } from '../es/transport.ts';
import { EsAdapter } from '../es/adapter.ts';
import { EsLedger } from '../es/ledger.ts';
import { BackfillSource } from '../backfill/source.ts';
import { OperationalDatabase } from './database.ts';
import type { OperationsConfig } from './config.ts';
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
  page,
} from './validation.ts';
export interface Observation {
  observed_at: string | null;
  freshness: 'fresh' | 'stale' | 'unavailable';
  health: 'healthy' | 'degraded' | 'unavailable';
  data: Record<string, unknown> | null;
}
export interface Snapshot {
  observed_at: string;
  health: string;
  dependencies: Record<string, Observation>;
  backfill: Record<string, unknown> | null;
}
function one(rows: Record<string, unknown>[]) {
  if (!rows[0]) throw new ControlError(404, 'not_found');
  return rows[0];
}
function rows(v: unknown): Record<string, unknown>[] {
  if (!Array.isArray(v)) throw new Error('Invalid observation');
  return v.map((x: unknown) => record(x));
}
export class OperationsService {
  readonly db: OperationalDatabase;
  readonly #config: OperationsConfig;
  constructor(config: OperationsConfig) {
    this.#config = config;
    this.db = new OperationalDatabase({
      source: config.source,
      pipeline: config.pipeline,
      consumer: config.consumer,
    });
  }
  async #observe(
    fn: () => Promise<Record<string, unknown>>,
  ): Promise<Observation> {
    try {
      const data = await fn();
      const at =
        typeof data['observed_at'] === 'string'
          ? data['observed_at']
          : new Date().toISOString();
      const fresh =
        Number.isFinite(Date.parse(at)) &&
        Math.abs(Date.now() - Date.parse(at)) <= 30000;
      return {
        observed_at: at,
        freshness: fresh ? 'fresh' : 'stale',
        health: fresh ? 'healthy' : 'degraded',
        data,
      };
    } catch {
      return {
        observed_at: null,
        freshness: 'unavailable',
        health: 'unavailable',
        data: null,
      };
    }
  }
  async #es<T>(fn: (transport: EsTransport) => Promise<T>): Promise<T> {
    const transport = new EsTransport(this.#config.es);
    return withOperationalCleanup(
      () => fn(transport),
      () => transport.close(),
    );
  }
  async #target(transport: EsTransport) {
    const target = await new EsLedger(this.#config.pipeline).target();
    await new EsAdapter(transport).validate(target);
    return target;
  }
  async #proxy(sink: 'elasticsearch' | 'rabbitmq', enabled?: boolean) {
    const path = `${this.#config.proxyApi}/proxies/${this.#config.proxies[sink]}`;
    const response = await fetch(path, {
      method: enabled === undefined ? 'GET' : 'POST',
      signal: AbortSignal.timeout(3000),
      ...(enabled === undefined
        ? {}
        : {
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ enabled }),
          }),
    });
    if (!response.ok) throw new ControlError(503, 'unavailable');
    const chunks: Uint8Array[] = [];
    let size = 0;
    if (!response.body) throw new ControlError(503, 'unavailable');
    const reader = response.body.getReader();
    try {
      for (;;) {
        const r = await reader.read();
        if (r.done) break;
        size += r.value.length;
        if (size > 8192) {
          await reader.cancel();
          throw new ControlError(503, 'unavailable');
        }
        chunks.push(r.value);
      }
    } finally {
      reader.releaseLock();
    }
    const text = Buffer.concat(chunks).toString('utf8');
    const value: unknown = JSON.parse(text),
      r = record(value);
    if (
      r['name'] !== this.#config.proxies[sink] ||
      typeof r['enabled'] !== 'boolean'
    )
      throw new ControlError(422, 'integrity_block');
    return {
      state: r['enabled'] ? 'connected' : 'disconnected',
      observed_at: new Date().toISOString(),
    };
  }
  async status(): Promise<Snapshot> {
    const [source, pipeline, consumer, elasticsearch, rabbitmq] =
      await Promise.all([
        this.#observe(async () => {
          const r = one(
            await this.db.read('source', 'snapshot', [
              this.#config.pipelineId,
              this.#config.sourceEpoch,
            ]),
          );
          const c = record(r['counts']);
          const pending = [
            'pending_due',
            'pending_delayed',
            'leased_current',
            'leased_expired',
            'blocked',
            'missing',
          ].reduce((n, k) => n + BigInt(string(c[k])), 0n);
          return {
            ...r,
            pending: pending.toString(),
            oldest_pending_age_seconds:
              r['oldest_pending_at'] === null
                ? null
                : Math.max(
                    0,
                    (Date.parse(string(r['observed_at'])) -
                      Date.parse(string(r['oldest_pending_at']))) /
                      1000,
                  ),
          };
        }),
        this.#observe(async () =>
          one(await this.db.read('pipeline', 'snapshot')),
        ),
        this.#observe(async () =>
          one(await this.db.read('consumer', 'snapshot')),
        ),
        this.#observe(() =>
          this.#es(async (t) => {
            const target = await this.#target(t);
            return {
              observed_at: new Date().toISOString(),
              destination_id: target.id,
              generation: target.generation,
              mode: target.mode,
            };
          }),
        ),
        this.#observe(async () => {
          const proxy = await this.#proxy('rabbitmq');
          await new BrokerMetadata(this.#config.rabbit).request(
            'GET',
            '/api/whoami',
          );
          return proxy;
        }),
      ]);
    let backfill: Record<string, unknown> | null = null;
    if (pipeline.data?.['backfill_id']) {
      try {
        backfill = await this.backfill(string(pipeline.data['backfill_id']));
      } catch {
        pipeline.health = 'unavailable';
        pipeline.freshness = 'unavailable';
      }
    }
    if (source.data) {
      const c = record(source.data['counts']);
      if (c['blocked'] !== '0' || c['missing'] !== '0')
        source.health = 'degraded';
    }
    if (consumer.data?.['quarantine'] !== '0' && consumer.data)
      consumer.health = 'degraded';
    if (rabbitmq.data?.['state'] === 'disconnected')
      rabbitmq.health = 'unavailable';
    if (pipeline.data) {
      for (const [sink, observation] of [
        ['es', elasticsearch],
        ['rabbit', rabbitmq],
      ] as const) {
        const target = pipeline.data[sink];
        if (!target) observation.health = 'degraded';
        else if (
          record(target)['mode'] !== 'ready' &&
          observation.health === 'healthy'
        )
          observation.health = 'degraded';
      }
      if (
        rows(pipeline.data['deliveries']).some(
          (r) => r['state'] === 'dead_letter' && r['count'] !== '0',
        ) ||
        backfill?.['blocked'] === true
      )
        pipeline.health = 'degraded';
    }
    const dependencies = {
      source,
      pipeline,
      consumer,
      elasticsearch,
      rabbitmq,
    };
    const health = Object.values(dependencies).some(
      (d) => d.health === 'unavailable',
    )
      ? 'unavailable'
      : Object.values(dependencies).some((d) => d.health === 'degraded')
        ? 'degraded'
        : 'healthy';
    const dimensions = Object.fromEntries(
      Object.entries(dependencies).map(([component, o]) => [
        component,
        {
          ...o,
          dependency_health:
            o.freshness === 'unavailable' ||
            (component === 'rabbitmq' && o.data?.['state'] === 'disconnected')
              ? 'unavailable'
              : 'healthy',
          data_health:
            o.data === null
              ? 'unknown'
              : o.health === 'degraded'
                ? 'degraded'
                : 'healthy',
        },
      ]),
    );
    return {
      observed_at: new Date().toISOString(),
      health,
      dependencies: dimensions,
      backfill,
    };
  }
  async backfill(run: string) {
    const value = one(await this.db.read('pipeline', 'backfill', [id(run)]));
    const sealed = value['sealed_at'] !== null;
    const counts = record(value['counts']);
    return {
      ...value,
      required_sealed: sealed,
      counts: { ...counts, required: sealed ? counts['required'] : null },
      denominator: sealed ? counts['required'] : null,
    };
  }
  async start(key: string, correlation: string, body: unknown) {
    const r = shape(body, ['run_id', 'ranges']),
      run = id(r['run_id']),
      ranges = integer(r['ranges'], 1, 16);
    if (key !== run) throw new ControlError(400, 'invalid_request');
    const existing = await this.db.read('pipeline', 'receipt', [key]);
    if (existing[0])
      return this.db.backfill(
        key,
        correlation,
        'backfill_start',
        run,
        this.#config.sourceEpoch,
        this.#config.pipelineId,
        ranges,
        null,
      );
    const observation = await new BackfillSource(this.#config.source, {
      sourceEpoch: this.#config.sourceEpoch,
      pipelineId: this.#config.pipelineId,
    }).identity();
    return this.db.backfill(
      key,
      correlation,
      'backfill_start',
      run,
      this.#config.sourceEpoch,
      this.#config.pipelineId,
      ranges,
      observation,
    );
  }
  pause(
    key: string,
    correlation: string,
    run: string,
    paused: boolean,
    body: unknown,
  ) {
    shape(body, []);
    return this.db.backfill(
      key,
      correlation,
      paused ? 'backfill_pause' : 'backfill_resume',
      id(run),
      this.#config.sourceEpoch,
      this.#config.pipelineId,
      0,
      null,
    );
  }
  async entities(query: unknown) {
    const q = shape(query, ['limit', 'cursor', 'q', 'include_deleted']),
      p = page(
        Object.fromEntries(
          Object.entries(q).filter(([k]) => ['limit', 'cursor'].includes(k)),
        ),
      );
    const text = q['q'] === undefined ? '' : string(q['q']);
    if (
      q['include_deleted'] !== undefined &&
      q['include_deleted'] !== 'true' &&
      q['include_deleted'] !== 'false'
    )
      throw new ControlError(400, 'invalid_request');
    const include = q['include_deleted'] === 'true',
      c = cursor(p.cursor, 'entities');
    let after: string[] | undefined;
    if (c) {
      shape(c, ['scope', 'v', 'epoch', 'entity', 'q', 'include_deleted']);
      if (c['q'] !== text || c['include_deleted'] !== include)
        throw new ControlError(400, 'invalid_request');
      after = [id(c['epoch']), bigint(c['entity'])];
    }
    return this.#es(async (t) => {
      const target = await this.#target(t);
      const response = object(
        await t.request(
          'POST',
          `/${target.index}/_search`,
          JSON.stringify({
            size: p.limit + 1,
            version: true,
            track_total_hits: false,
            _source: { excludes: ['canonical_body_json'] },
            sort: [{ source_epoch: 'asc' }, { entity_id: 'asc' }],
            ...(after ? { search_after: after } : {}),
            query: {
              bool: {
                filter: include ? [] : [{ term: { is_deleted: false } }],
                must: text ? [{ match: { 'search_fields.name': text } }] : [],
              },
            },
          }),
        ),
      );
      const hits = rows(object(response['hits'])['hits']);
      const items: Record<string, unknown>[] = hits
        .slice(0, p.limit)
        .map((h) => ({
          ...record(h['_source']),
          receiver_version: version(h['_version']),
        }));
      const last = items.at(-1);
      return {
        observed_at: new Date().toISOString(),
        freshness: 'near_realtime',
        items,
        next_cursor:
          hits.length > p.limit && last
            ? encodeCursor('entities', {
                epoch: last['source_epoch'],
                entity: last['entity_id'],
                q: text,
                include_deleted: include,
              })
            : null,
      };
    });
  }
  async entity(epoch: string, entity: string) {
    id(epoch);
    bigint(entity);
    return this.#es(async (t) => {
      const target = await this.#target(t);
      let remote: Record<string, unknown>;
      try {
        remote = object(
          await t.request('GET', `/${target.index}/_doc/${epoch}:${entity}`),
        );
      } catch (e) {
        if (e instanceof errors.ResponseError && e.statusCode === 404)
          throw new ControlError(404, 'not_found');
        throw e;
      }
      const doc = record(remote['_source']);
      const { canonical_body_json: _body, ...projection } = doc;
      void _body;
      const [source, pipeline] = await Promise.all([
        this.db.read('source', 'entity', [epoch, entity]),
        this.db.read('pipeline', 'entity', [epoch, entity]),
      ]);
      return {
        observed_at: new Date().toISOString(),
        freshness: 'realtime',
        projection,
        receiver_version: version(remote['_version']),
        source: source[0] ?? null,
        pipeline,
        convergence:
          source[0]?.['entity_version'] === projection['entity_version']
            ? 'current'
            : 'degraded',
      };
    });
  }
  async event(value: string) {
    const event = eventId(value),
      parts = event.split(':');
    const result = one(await this.db.read('pipeline', 'event', [event]));
    const source = await this.db.read('source', 'event', parts);
    return {
      ...result,
      source_capture: source[0] ?? null,
      observed_at: new Date().toISOString(),
    };
  }
  async failures(query: unknown) {
    const p = page(query),
      c = cursor(p.cursor, 'failures');
    if (c) shape(c, ['scope', 'v', 'after']);
    const after = c ? string(c['after'], 256) : '';
    if (after && !/^[a-z0-9:-]+$/.test(after))
      throw new ControlError(400, 'invalid_request');
    const results = await Promise.all([
      this.db.read('source', 'failures', [after, p.limit + 1]),
      this.db.read('pipeline', 'failures', [after, p.limit + 1]),
      this.db.read('consumer', 'failures', [after, p.limit + 1]),
    ]);
    const ordered = results
        .flat()
        .sort((a, b) => (String(a['key']) < String(b['key']) ? -1 : 1)),
      items = ordered.slice(0, p.limit),
      last = items.at(-1);
    return {
      observed_at: new Date().toISOString(),
      items,
      next_cursor:
        ordered.length > p.limit && last
          ? encodeCursor('failures', { after: last['key'] })
          : null,
    };
  }
  replay(key: string, correlation: string, event: string, body: unknown) {
    const r = shape(body, [
      'attempt_id',
      'destination_id',
      'generation',
      'reason',
    ]);
    return this.db.replay(
      key,
      correlation,
      eventId(event),
      id(r['destination_id']),
      bigint(r['generation']),
      id(r['attempt_id']),
      string(r['reason']),
    );
  }
  source(key: string, correlation: string, body: unknown, corrupt = false) {
    const r = shape(
        body,
        corrupt ? ['fixture'] : ['fixture', 'operation', 'value'],
      ),
      fixture = string(r['fixture']);
    if (!/^fixture-(0[1-9]|1[0-6])$/.test(fixture))
      throw new ControlError(400, 'invalid_request');
    const operation = corrupt ? 'corrupt' : string(r['operation']);
    if (
      !['create', 'update', 'delete', 'restore', 'corrupt'].includes(
        operation,
      ) ||
      (!corrupt && operation === 'corrupt')
    )
      throw new ControlError(400, 'invalid_request');
    return this.db.source(
      this.#config.sourceEpoch,
      key,
      correlation,
      fixture,
      operation,
      corrupt ? 0 : integer(r['value'], 0, 1000000),
    );
  }
  async network(
    key: string,
    correlation: string,
    sink: 'elasticsearch' | 'rabbitmq',
    body: unknown,
  ) {
    const r = shape(body, ['state']),
      state = string(r['state']);
    if (!['connected', 'disconnected'].includes(state))
      throw new ControlError(400, 'invalid_request');
    return networkLock(this.#config.pipeline, sink, async () => {
      const admitted = await this.db.network(
        key,
        correlation,
        sink,
        state,
        false,
      );
      if (admitted['completed'] === true) return admitted;
      await this.#proxy(sink, state === 'connected');
      const actual = await this.#proxy(sink);
      if (actual.state !== state) throw new ControlError(503, 'unavailable');
      await this.db.network(key, correlation, sink, state, true);
      return { ...admitted, completed: true };
    });
  }

  async simulations() {
    const [elasticsearch, rabbitmq, fixtures] = await Promise.all([
      this.#proxy('elasticsearch'),
      this.#proxy('rabbitmq'),
      this.db.read('source', 'fixtures'),
    ]);
    return {
      observed_at: new Date().toISOString(),
      generator: 'explicit_commands_only',
      elasticsearch,
      rabbitmq,
      fixtures,
    };
  }
  async config() {
    const snapshot = one(await this.db.read('pipeline', 'snapshot'));
    return {
      source_epoch: this.#config.sourceEpoch,
      pipeline_id: this.#config.pipelineId,
      limits: {
        page: 100,
        attempt_history: 8,
        body_bytes: 8192,
        ranges: 16,
        fixtures: 16,
        freshness_seconds: 30,
      },
      receivers: {
        elasticsearch: snapshot['es'],
        rabbitmq: snapshot['rabbit'],
      },
      immutable: ['source_epoch', 'pipeline_id', 'receivers'],
      restart_required: ['connections', 'operator_token', 'proxy_routes'],
      quarantine_replay: false,
    };
  }
}
