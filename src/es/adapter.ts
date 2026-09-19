import { errors } from '@elastic/elasticsearch';
import { EsTransport, object, exactInteger, version } from './transport.ts';
import {
  exactJson,
  sameProjection,
  bulkLine,
  type Projection,
} from './projection.ts';
import {
  EsLedger,
  MissingLedgerWitness,
  type Target,
  type Outcome,
} from './ledger.ts';
export class EsFailure extends Error {
  readonly classification: 'transient' | 'auth' | 'configuration' | 'integrity';
  constructor(
    classification: EsFailure['classification'],
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.classification = classification;
  }
}
export function classify(error: unknown): EsFailure {
  if (error instanceof EsFailure) return error;
  if (error instanceof errors.ResponseError) {
    if (error.statusCode === 401 || error.statusCode === 403)
      return new EsFailure('auth', 'Receiver credential rejected');
    if (error.statusCode === 404)
      return new EsFailure('configuration', 'Registered receiver missing');
  }
  return new EsFailure(
    'transient',
    'Receiver request incomplete or unavailable',
  );
}
export interface ItemOutcome {
  projection: Projection;
  outcome: Outcome;
  remote: string | null;
  witness: string | null;
  context: string;
}
export class EsAdapter {
  readonly #transport: EsTransport;
  constructor(transport: EsTransport) {
    this.#transport = transport;
  }
  async validate(t: Target) {
    const info = object(await this.#transport.request('GET', '/'));
    const index = object(
      object(await this.#transport.request('GET', `/${t.index}`))[t.index],
    );
    const settings = object(object(index['settings'])['index']);
    if (
      info['cluster_uuid'] !== t.clusterUuid ||
      settings['uuid'] !== t.indexUuid
    )
      throw new EsFailure('configuration', 'Receiver UUID mismatch');
    const selected = {
      settings: {
        number_of_shards: settings['number_of_shards'],
        number_of_replicas: settings['number_of_replicas'],
        translog: settings['translog'],
        mapping: settings['mapping'],
      },
      mappings: index['mappings'],
    };
    if (exactJson(selected) !== exactJson(t.configuration))
      throw new EsFailure(
        'configuration',
        'Receiver projection configuration mismatch',
      );
  }
  async bulk(
    t: Target,
    projections: readonly Projection[],
    requestId: string,
  ): Promise<ItemOutcome[]> {
    if (projections.length < 1 || projections.length > 500)
      throw new EsFailure('integrity', 'Invalid bulk count');
    const body = projections.map(bulkLine).join('');
    if (Buffer.byteLength(body) > 4 * 1024 * 1024)
      throw new EsFailure('integrity', 'Bulk request byte bound');
    const reply = object(
      await this.#transport.request(
        'POST',
        `/${t.index}/_bulk`,
        body,
        requestId,
      ),
    );
    return validateBulkResponse(reply, projections, t.index);
  }
  async resolve(
    t: Target,
    r: ItemOutcome,
    ledger: EsLedger,
  ): Promise<ItemOutcome> {
    if (r.outcome !== 'already_applied') return r;
    let remote: Record<string, unknown>;
    try {
      remote = object(
        await this.#transport.request(
          'GET',
          `/${t.index}/_doc/${encodeURIComponent(r.projection.documentId)}`,
        ),
      );
    } catch (error) {
      if (error instanceof errors.ResponseError && error.statusCode === 404)
        throw new EsFailure('integrity', 'Missing conflict witness', {
          cause: error,
        });
      throw error;
    }
    if (
      remote['found'] !== true ||
      remote['_id'] !== r.projection.documentId ||
      remote['_index'] !== t.index
    )
      throw new EsFailure('integrity', 'Missing conflict witness');
    let rv: string;
    try {
      rv = version(remote['_version']);
    } catch (cause) {
      throw new EsFailure('integrity', 'Malformed remote version witness', {
        cause,
      });
    }
    if (BigInt(rv) < BigInt(r.projection.version))
      throw new EsFailure('integrity', 'Lower remote conflict witness');
    const witness = `${r.projection.documentId}:${rv}`;
    let expected: Projection;
    try {
      expected = await ledger.read(witness);
    } catch (error) {
      if (error instanceof MissingLedgerWitness)
        throw new EsFailure('integrity', 'Unknown higher ledger witness', {
          cause: error,
        });
      throw error;
    }
    if (
      expected.json === null ||
      !sameProjection(remote['_source'], expected.json)
    )
      throw new EsFailure(
        'integrity',
        'Remote projection differs from immutable ledger',
      );
    return {
      ...r,
      outcome: rv === r.projection.version ? 'already_applied' : 'superseded',
      remote: rv,
      witness,
      context: 'Realtime GET verified exact projection and ledger witness',
    };
  }
}

export function validateBulkResponse(
  value: unknown,
  projections: readonly Projection[],
  index: string,
): ItemOutcome[] {
  try {
    const reply = object(value);
    const items = reply['items'];
    if (
      typeof reply['errors'] !== 'boolean' ||
      !Array.isArray(items) ||
      items.length !== projections.length
    )
      throw new EsFailure('integrity', 'Invalid bulk correspondence');
    const results: ItemOutcome[] = [];
    for (let i = 0; i < items.length; i++) {
      const wrapper = object(items[i]);
      const item = object(wrapper['index']);
      const p = projections[i];
      if (
        !p ||
        Object.keys(wrapper).length !== 1 ||
        item['_id'] !== p.documentId ||
        item['_index'] !== index
      )
        throw new EsFailure('integrity', 'Bulk item identity mismatch');
      const status = exactInteger(item['status']);
      if (status === '200' || status === '201') {
        if (
          version(item['_version']) !== p.version ||
          item['error'] !== undefined ||
          !['created', 'updated'].includes(String(item['result']))
        )
          throw new EsFailure('integrity', 'Invalid successful item');
        results.push({
          projection: p,
          outcome: 'applied',
          remote: p.version,
          witness: p.eventId,
          context: `HTTP ${status}`,
        });
      } else {
        const error = object(item['error']);
        const type = error['type'];
        const context = Buffer.from(exactJson(error))
          .subarray(0, 1800)
          .toString('utf8');
        if (status === '409' && type === 'version_conflict_engine_exception')
          results.push({
            projection: p,
            outcome: 'already_applied',
            remote: null,
            witness: null,
            context: 'Requires realtime conflict evidence',
          });
        else if (
          status === '400' &&
          [
            'document_parsing_exception',
            'mapper_parsing_exception',
            'strict_dynamic_mapping_exception',
          ].includes(String(type))
        )
          results.push({
            projection: p,
            outcome: 'mapping',
            remote: null,
            witness: null,
            context,
          });
        else if (
          status === '429' ||
          status === '503' ||
          status === '502' ||
          status === '500' ||
          status === '504'
        )
          results.push({
            projection: p,
            outcome: 'transient',
            remote: null,
            witness: null,
            context: `HTTP ${status}`,
          });
        else
          throw new EsFailure(
            status === '401' || status === '403' ? 'auth' : 'integrity',
            `Unexpected bulk item HTTP ${status}`,
          );
      }
    }
    if (reply['errors'] !== results.some((r) => r.outcome !== 'applied'))
      throw new EsFailure('integrity', 'Contradictory bulk errors flag');
    return results;
  } catch (error) {
    if (error instanceof EsFailure) throw error;
    throw new EsFailure('integrity', 'Malformed bulk response', {
      cause: error,
    });
  }
}
