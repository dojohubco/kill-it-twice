import { setTimeout as delay } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import type { Writable } from 'node:stream';
import { loadOperationsConfig } from '../src/operations/config.ts';
import { RecoveryService } from '../src/operations/recovery.ts';
import { OperationalMonitor } from '../src/operations/monitor.ts';
import { OperationsService } from '../src/operations/service.ts';
import {
  ControlError,
  shape,
  record,
  id,
  integer,
} from '../src/operations/validation.ts';
import { TransactionError } from '../src/internal/transaction.ts';

async function input(): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  const timer = setTimeout(
    () => process.stdin.destroy(new Error('Input deadline')),
    10000,
  );
  try {
    for await (const raw of process.stdin) {
      const chunk = Buffer.isBuffer(raw) ? raw : Buffer.from(String(raw));
      size += chunk.length;
      if (size > 16384) throw new ControlError(400, 'input_bound');
      chunks.push(chunk);
    }
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return value;
  } finally {
    clearTimeout(timer);
  }
}
async function output(stream: Writable, value: unknown) {
  const bytes = Buffer.from(JSON.stringify(value) + '\n');
  if (bytes.length > 262144) throw new ControlError(422, 'output_bound');
  await new Promise<void>((resolve, reject) => {
    let finished = false;
    const end = (error?: Error | null) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      if (error) {
        stream.once('close', () => stream.off('error', onError));
        stream.destroy();
        reject(error);
      } else {
        stream.off('error', onError);
        resolve();
      }
    };
    const onError = (error: Error) => end(error);
    const timer = setTimeout(() => end(new Error('Output deadline')), 5000);
    stream.on('error', onError);
    stream.write(bytes, end);
  });
}
async function main() {
  const [mode, arg, ...extra] = process.argv.slice(2);
  if (extra.length) throw new ControlError(400, 'invalid_request');
  const config = await loadOperationsConfig(),
    recovery = new RecoveryService(config);
  if (mode === 'snapshot' || mode === 'follow') {
    if (arg !== undefined) throw new ControlError(400, 'invalid_request');
    const monitor = new OperationalMonitor(config);
    let stopped = false;
    const stop = () => {
      stopped = true;
    };
    process.once('SIGTERM', stop);
    process.once('SIGINT', stop);
    try {
      do {
        await output(process.stdout, await monitor.snapshot());
        if (mode !== 'follow' || stopped) break;
        await delay(1000);
      } while (!stopped);
    } finally {
      process.off('SIGTERM', stop);
      process.off('SIGINT', stop);
    }
    return;
  }
  let result: unknown;
  if (mode === 'operation') result = await recovery.status(id(arg));
  else if (mode === 'run') result = await recovery.run(id(arg));
  else if (mode === 'list') {
    if (!arg) throw new ControlError(400, 'invalid_request');
    result = await recovery.list(arg, await input());
  } else if (
    mode === 'replay' ||
    mode === 'supersede' ||
    mode === 'verify-target'
  ) {
    if (arg !== undefined) throw new ControlError(400, 'invalid_request');
    const request = await input();
    result =
      mode === 'replay'
        ? await recovery.replay(request)
        : mode === 'supersede'
          ? await recovery.supersede(request)
          : await recovery.verifyTarget(request);
    if (
      ['failed', 'stale', 'complete_with_errors'].includes(
        String(record(result)['state']),
      )
    )
      process.exitCode = 1;
  } else if (mode === 'pause' || mode === 'resume') {
    const request = record(await input());
    if (Object.keys(request).some((k) => k !== 'request_id'))
      throw new ControlError(400, 'invalid_request');
    result = await new OperationsService(config).pause(
      id(request['request_id']),
      randomUUID(),
      id(arg),
      mode === 'pause',
      {},
    );
  } else if (mode === 'failures') {
    const request = shape(await input(), ['limit', 'cursor']);
    const count =
      request['limit'] === undefined ? 50 : integer(request['limit'], 1, 100);
    result = await new OperationsService(config).failures({
      limit: String(count),
      ...(request['cursor'] === undefined ? {} : { cursor: request['cursor'] }),
    });
  } else throw new ControlError(400, 'invalid_request');
  await output(process.stdout, result);
}
try {
  await main();
} catch (error) {
  process.exitCode = 1;
  await output(process.stderr, {
    error: {
      code:
        error instanceof ControlError
          ? error.code
          : error instanceof TransactionError
            ? 'transaction_failure'
            : 'operation_unavailable',
      ...(error instanceof TransactionError
        ? { outcome: error.outcome, sqlstate: error.sqlState ?? null }
        : {}),
    },
  }).catch(() => undefined);
}
