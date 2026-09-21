// Private integrated verifier entry point. Never selected by the normal Compose worker.
import assert from 'node:assert/strict';
import { readFile, writeFile, rename, appendFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { mock } from 'node:test';
import { stringify } from 'lossless-json';
import type { ConsumeMessage } from 'amqplib';
import { BackfillLedger } from '../../src/backfill/ledger.ts';
import { ConsumerDatabase } from '../../src/rabbitmq/consumer-db.ts';
import { Publisher } from '../../src/rabbitmq/publisher.ts';
import { AmqpSession } from '../../src/rabbitmq/session.ts';
import { EsTransport } from '../../src/es/transport.ts';
const directory = '/verification';
const role = process.argv[2];
assert.ok(
  role && ['backfill', 'consumer', 'publisher', 'es-worker'].includes(role),
);
const started = performance.now();
const rawControl: unknown = JSON.parse(
  await readFile(`${directory}/${role}-control.json`, 'utf8'),
);
assert.ok(rawControl && typeof rawControl === 'object');
const control = rawControl as Record<string, unknown>;
let records = 0;
async function trace(type: string, data: unknown) {
  if (control['record'] !== true) return;
  assert.ok(++records <= 20000, 'Bounded verifier trace');
  const line = stringify({
    type,
    role,
    pid: process.pid,
    elapsed_ms: performance.now() - started,
    data,
  });
  assert.ok(line && Buffer.byteLength(line) <= 4 * 1024 * 1024);
  await appendFile(`${directory}/${role}-trace.jsonl`, line + '\n');
}
let paused = false;
async function boundary(name: string, data: unknown, eligible = true) {
  if (paused || !eligible) return;
  const config = control;
  if (config['boundary'] !== name) return;
  const token = config['token'];
  assert.ok(typeof token === 'string' && /^[a-f0-9-]{36}$/.test(token));
  paused = true;
  const evidence = { role, boundary: name, token, pid: process.pid, data };
  const file = `${directory}/${token}.reached.json`;
  await writeFile(file + '.tmp', stringify(evidence) + '\n');
  await rename(file + '.tmp', file);
  const deadline = performance.now() + 24000;
  while (performance.now() < deadline) {
    const released: string = await readFile(
      `${directory}/${token}.release`,
      'utf8',
    ).catch((e: unknown) => {
      if (e && typeof e === 'object' && 'code' in e && e.code === 'ENOENT')
        return '';
      throw e;
    });
    if (released === token) return;
    await delay(25);
  }
  throw new Error('Private verification barrier deadline');
}
if (role === 'backfill') {
  mock.method(
    BackfillLedger.prototype,
    'page',
    async function (
      this: BackfillLedger,
      request: Parameters<BackfillLedger['page']>[0],
    ) {
      return this.transaction(async (tx) => {
        const result = await tx.page(request);
        await boundary(
          'backfill.before_page_commit',
          { request, result },
          request.claim.range > 0 && BigInt(request.claim.checkpoint) >= 32n,
        );
        return result;
      });
    },
  );
}
if (role === 'consumer') {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- private wrapper retains the original receiver
  const processBatch = ConsumerDatabase.prototype.process;
  mock.method(
    ConsumerDatabase.prototype,
    'process',
    async function (
      this: ConsumerDatabase,
      ...args: Parameters<ConsumerDatabase['process']>
    ) {
      const result = await Reflect.apply(processBatch, this, args);
      await trace('consumer-commit', {
        events: args[1].map((e) => e.body.event_id),
        result,
      });
      await boundary('consumer.after_commit.before_ack', {
        events: args[1].map((e) => e.body.event_id),
        result,
      });
      return result;
    },
  );
  // eslint-disable-next-line @typescript-eslint/unbound-method -- forwarded public channel call, no protocol result is substituted
  const originalChannel = AmqpSession.prototype.channel;
  mock.method(
    AmqpSession.prototype,
    'channel',
    async function (this: AmqpSession, confirm: false, highWaterMark?: number) {
      const channel = await Reflect.apply(originalChannel, this, [
        confirm,
        highWaterMark,
      ]);
      channel.on('delivery', (message: ConsumeMessage) => {
        const messageId: unknown = message.properties.messageId;
        assert.equal(typeof messageId, 'string');
        void trace('consumer-delivery', {
          channel_id: this.id,
          redelivered: message.fields.redelivered,
          message_id: messageId,
          wire_sha256: createHash('sha256')
            .update(message.content)
            .digest('hex'),
        }).catch(() => {
          process.exitCode = 1;
        });
      });
      return channel;
    },
  );
}
if (role === 'publisher') {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- private observation after actual broker confirmation
  const publish = Publisher.prototype.publish;
  mock.method(
    Publisher.prototype,
    'publish',
    async function (
      this: Publisher,
      ...args: Parameters<Publisher['publish']>
    ) {
      const result = await Reflect.apply(publish, this, args);
      await trace('publisher-result', result);
      await boundary(
        'publisher.after_confirm.before_local_commit',
        result,
        result.some((r) => r.outcome === 'confirmed'),
      );
      return result;
    },
  );
}
if (role === 'es-worker') {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- record the real response after the normal transport returns
  const request = EsTransport.prototype.request;
  mock.method(
    EsTransport.prototype,
    'request',
    async function (
      this: EsTransport,
      ...args: Parameters<EsTransport['request']>
    ) {
      const response = await Reflect.apply(request, this, args);
      if (args[0] === 'POST' && args[1].endsWith('/_bulk')) {
        assert.equal(typeof args[2], 'string');
        const body = String(args[2]);
        await trace('bulk-response', {
          request_sha256: createHash('sha256').update(body).digest('hex'),
          operations: body.trimEnd().split('\n').length / 2,
          response,
        });
      }
      return response;
    },
  );
}
// The ordinary runtime still supplies only this worker's restricted role configuration.
process.argv = [process.execPath, 'verification-worker'];
await import('../runtime/worker.ts');
