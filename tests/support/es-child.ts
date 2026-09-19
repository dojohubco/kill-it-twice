// Private process instrumentation; subclass wrappers call the real production operations.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { Delivery } from '../../src/es/worker.ts';
import { EsLedger, type Target, type EsClaim } from '../../src/es/ledger.ts';
import { EsAdapter } from '../../src/es/adapter.ts';
import {
  EsTransport,
  object,
  type EsConnection,
} from '../../src/es/transport.ts';
import type { ConnectionConfig } from '../../src/internal/transaction.ts';
import type { Projection } from '../../src/es/projection.ts';
import { deadline } from './fault-protocol.ts';
const stop = new AbortController();
let intentional = false;
let proxyApi = '';

let selected = '';
let reached = false;
let claims: EsClaim[] = [];
process.once('disconnect', () => {
  if (!intentional) stop.abort(new Error('Unexpected parent loss'));
});
for (const sig of ['SIGTERM', 'SIGINT']) process.once(sig, () => stop.abort());
async function barrier(boundary: string) {
  if (reached || selected !== boundary) return;
  reached = true;
  const release = deadline(
    once(process, 'message', { signal: stop.signal }),
    'ES private release',
    20000,
  );
  void release.catch(() => undefined);
  if (!process.send) throw new Error('Missing parent channel');
  await new Promise<void>((resolve, reject) =>
    process.send?.(
      { type: 'es-barrier', boundary, pid: process.pid, claims },
      (e) => (e ? reject(e) : resolve()),
    ),
  );
  assert.deepEqual((await release)[0], { type: 'release', boundary });
}
class FaultLedger extends EsLedger {
  override async claim(...args: Parameters<EsLedger['claim']>) {
    claims = await super.claim(...args);
    await barrier('es.after_claim_commit.before_request');
    return claims;
  }
}
class FaultAdapter extends EsAdapter {
  override async bulk(
    t: Target,
    p: readonly Projection[],
    correlation: string,
  ) {
    if (selected === 'es.inflight') {
      const response = await fetch(`${proxyApi}/proxies/es/toxics`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          name: 'downstream',
          type: 'latency',
          stream: 'downstream',
          toxicity: 1,
          attributes: { latency: 6000, jitter: 0 },
        }),
        signal: AbortSignal.timeout(5000),
      });
      assert.equal(response.status, 200);
      const pending = super.bulk(t, p, correlation);
      void pending.catch(() => undefined);
      await barrier('es.inflight');
      return pending;
    }
    const result = await super.bulk(t, p, correlation);
    await barrier('es.after_remote_apply.before_local_commit');
    return result;
  }
}
function connection(value: unknown): ConnectionConfig {
  const r = object(value);
  for (const k of ['host', 'database', 'user', 'password', 'application_name'])
    assert.equal(typeof r[k], 'string');
  assert.equal(typeof r['port'], 'number');
  return {
    host: String(r['host']),
    database: String(r['database']),
    user: String(r['user']),
    password: String(r['password']),
    application_name: String(r['application_name']),
    port: Number(r['port']),
  };
}
function receiver(value: unknown): EsConnection {
  const r = object(value);
  for (const k of ['node', 'username', 'password', 'ca'])
    assert.equal(typeof r[k], 'string');
  return {
    node: String(r['node']),
    username: String(r['username']),
    password: String(r['password']),
    ca: String(r['ca']),
  };
}
let transport: EsTransport | undefined;
try {
  const received: unknown[] = await deadline(
    once(process, 'message', { signal: stop.signal }),
    'ES child configuration',
  );
  const msg = object(received[0]);
  assert.equal(typeof msg['boundary'], 'string');
  selected = String(msg['boundary']);
  if (selected === 'es.inflight') {
    assert.equal(typeof msg['proxyApi'], 'string');
    proxyApi = String(msg['proxyApi']);
  }
  transport = new EsTransport(receiver(msg['receiver']));
  const lease = msg['leaseMs'];
  assert.equal(typeof lease, 'number');
  assert.ok(typeof lease === 'number');
  const delivery = new Delivery(
    new FaultLedger(connection(msg['ledger'])),
    new FaultAdapter(transport),
    { leaseMs: lease, renewalMs: Math.max(50, Math.floor(lease / 5)) },
  );
  const result = await delivery.once();
  await barrier('es.after_local_commit.before_success');
  stop.signal.throwIfAborted();
  await new Promise<void>((resolve, reject) =>
    process.stdout.write(
      JSON.stringify({ type: 'es-success', ...result }) + '\n',
      (e) => (e ? reject(e) : resolve()),
    ),
  );
} catch (error) {
  console.error(error instanceof Error ? error.message : 'ES child failed');
  process.exitCode = 1;
} finally {
  await transport?.close();
  intentional = true;
  if (process.connected) process.disconnect();
}
