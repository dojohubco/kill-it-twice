// Assignment controls against this verifier's real retained installation; no HTTP fixtures.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright-core';
import { uiBrowserExecutable } from '../tests/support/ui-browser.ts';
import { withCleanup } from './support.ts';
const [url, output, project] = process.argv.slice(2);
assert.ok(url && /^http:\/\/127\.0\.0\.1:[0-9]+$/.test(url));
assert.ok(
  output && project && /^kit-verify-[0-9]{14}-[a-f0-9]{8}$/.test(project),
);
const execute = promisify(execFile);
const compose = ['compose', '-f', resolve('compose.yaml'), '-p', project];
function object(value: unknown): Record<string, unknown> {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}
function rows(value: unknown): Record<string, unknown>[] {
  assert.ok(Array.isArray(value));
  return (value as unknown[]).map(object);
}
async function inspect(key: string) {
  const result = await execute(
    'docker',
    [
      ...compose,
      'run',
      '--rm',
      '--no-deps',
      '-T',
      'inspect',
      'node',
      'scripts/verification/inspect.ts',
      'event',
      key,
    ],
    { timeout: 30000, maxBuffer: 1048576 },
  );
  return object(JSON.parse(result.stdout) as unknown);
}
const secret = (
  await execute(
    'docker',
    [
      ...compose,
      'run',
      '--rm',
      '--no-deps',
      '-T',
      'inspect',
      'node',
      'scripts/runtime/inspect.ts',
      'token',
    ],
    { timeout: 30000, maxBuffer: 65536 },
  )
).stdout.trim();
assert.match(secret, /^[a-f0-9]{48}$/);
const browser = await chromium.launch({
  executablePath: await uiBrowserExecutable(),
  headless: true,
  chromiumSandbox: true,
});
await withCleanup(
  async () => {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1000 },
    });
    // Observe a bounded clone of the real fetch stream. Chrome may discard the
    // DevTools response body even while the application successfully consumes it.
    // No request is repeated, intercepted, fulfilled or replaced.
    await page.addInitScript(() => {
      const captures = new Map<
        string,
        { status: number; body: string | null; error: string | null }
      >();
      Object.defineProperty(window, '__kitControlResponses', {
        value: captures,
      });
      const nativeFetch = window.fetch.bind(window);
      window.fetch = async (input, init) => {
        const response = await nativeFetch(input, init);
        const request = input instanceof Request ? input : null;
        const method = init?.method ?? request?.method ?? 'GET';
        const target = new URL(
          input instanceof Request ? input.url : input,
          location.href,
        );
        const key = new Headers(init?.headers ?? request?.headers).get(
          'idempotency-key',
        );
        if (
          method !== 'GET' &&
          target.origin === location.origin &&
          target.pathname.startsWith('/api/v1/') &&
          key
        ) {
          const copy = response.clone();
          void (async () => {
            try {
              const reader = copy.body?.getReader();
              if (!reader)
                throw new Error('Actual control response has no body');
              const chunks: Uint8Array[] = [];
              let bytes = 0;
              try {
                for (;;) {
                  const part = await reader.read();
                  if (part.done) break;
                  bytes += part.value.byteLength;
                  if (bytes > 4 * 1024 * 1024) {
                    await reader.cancel();
                    throw new Error('Control response exceeds the UI byte cap');
                  }
                  chunks.push(part.value);
                }
              } finally {
                reader.releaseLock();
              }
              const body = new Uint8Array(bytes);
              let offset = 0;
              for (const chunk of chunks) {
                body.set(chunk, offset);
                offset += chunk.byteLength;
              }
              captures.set(key, {
                status: response.status,
                body: new TextDecoder('utf-8', { fatal: true }).decode(body),
                error: null,
              });
            } catch (error) {
              captures.set(key, {
                status: response.status,
                body: null,
                error: String(error),
              });
            }
          })();
        }
        return response;
      };
    });
    const errors: string[] = [];
    const writes: { path: string; key: string | undefined }[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    page.on('request', (request) => {
      const path = new URL(request.url()).pathname;
      if (path.startsWith('/api/v1/') && request.method() !== 'GET')
        writes.push({ path, key: request.headers()['idempotency-key'] });
    });
    async function read(path: string) {
      const response = await page.request.get(url + path, { timeout: 10000 });
      assert.equal(response.status(), 200);
      return object(object((await response.json()) as unknown)['data']);
    }
    async function until<T>(
      action: () => Promise<T>,
      predicate: (value: T) => boolean,
    ): Promise<T> {
      const deadline = performance.now() + 90000;
      while (performance.now() < deadline) {
        const value = await action();
        if (predicate(value)) return value;
        await delay(1000);
      }
      throw new Error('Real operator outcome deadline');
    }
    async function confirm(title: string, label: string) {
      const dialog = page.getByRole('dialog', { name: title, exact: true });
      const key = await dialog.locator('.request-identity code').innerText();
      assert.match(key, /^[a-f0-9-]{36}$/);
      const response = page.waitForResponse(
        (r) =>
          new URL(r.url()).pathname.startsWith('/api/v1/') &&
          r.request().method() !== 'GET' &&
          r.request().headers()['idempotency-key'] === key,
      );
      const [received] = await Promise.all([
        response,
        dialog.getByRole('button', { name: label, exact: true }).click(),
      ]);
      assert.equal(received.status(), 202);
      const captureHandle = await page.waitForFunction(
        (requestKey) =>
          (
            window as typeof window & {
              __kitControlResponses: Map<string, unknown>;
            }
          ).__kitControlResponses.get(requestKey),
        key,
        { timeout: 12000 },
      );
      const captured = object(await captureHandle.jsonValue());
      await captureHandle.dispose();
      assert.equal(captured['error'], null);
      assert.equal(captured['status'], 202);
      const body = captured['body'];
      assert.equal(typeof body, 'string');
      assert.ok(typeof body === 'string');
      const envelope = object(JSON.parse(body) as unknown);
      assert.equal(
        envelope['request_id'],
        await received.headerValue('x-request-id'),
      );
      const data = object(envelope['data']);
      await dialog.getByText('Request accepted', { exact: true }).waitFor();
      assert.match(await dialog.innerText(), /not end-to-end delivery/);
      await dialog.getByRole('button', { name: 'Done', exact: true }).click();
      await dialog.waitFor({ state: 'hidden' });
      return data;
    }
    await page.goto(url + '/configuration');
    await page
      .getByRole('heading', { name: 'Configuration', exact: true })
      .waitFor();
    await page
      .getByText('Identity is not an editable preference.', { exact: true })
      .waitFor();
    assert.match(
      await page.locator('main').innerText(),
      /configuration and restart/,
    );
    await page
      .getByRole('button', { name: 'Pause refresh', exact: true })
      .click();
    await page
      .getByRole('button', { name: 'Resume refresh', exact: true })
      .waitFor();
    assert.equal(Number(writes.length), 0);
    await page
      .getByRole('button', { name: 'Connect operator', exact: true })
      .click();
    await page.getByLabel('Operator token', { exact: true }).fill(secret);
    await page.getByRole('button', { name: 'Use token', exact: true }).click();
    await page
      .getByRole('button', { name: 'Disconnect operator', exact: true })
      .waitFor();
    await page.getByRole('link', { name: 'Backfill', exact: true }).click();
    await page
      .getByRole('button', { name: 'Start backfill', exact: true })
      .click();
    await confirm('Start a backfill run?', 'Start backfill');
    await page
      .getByRole('button', { name: 'Show latest run', exact: true })
      .click();
    await page
      .getByRole('button', { name: 'Request pause', exact: true })
      .click();
    await confirm('Pause new page admission?', 'Request pause');
    await page
      .getByRole('button', { name: 'Show latest run', exact: true })
      .click();
    await page
      .getByRole('button', { name: 'Resume backfill', exact: true })
      .click();
    await confirm('Resume this backfill?', 'Resume backfill');
    const run = object((await read('/api/v1/status'))['backfill']);
    assert.equal(run['desired_paused'], false);
    assert.equal(run['completed_at'], null); // The scanner is intentionally stopped in this control fixture.
    await page.getByRole('link', { name: 'Simulations', exact: true }).click();
    const connection = page.locator('article.connection-control').filter({
      has: page.getByRole('heading', { name: 'Elasticsearch', exact: true }),
    });
    await connection
      .getByRole('button', { name: 'Disconnect', exact: true })
      .click();
    await confirm('Disconnect Elasticsearch route?', 'Disconnect route');
    assert.equal(
      object((await read('/api/v1/simulations'))['elasticsearch'])['state'],
      'disconnected',
    );
    await connection
      .getByRole('button', { name: 'Reconnect', exact: true })
      .click();
    await confirm('Reconnect Elasticsearch route?', 'Reconnect route');
    assert.equal(
      object((await read('/api/v1/simulations'))['elasticsearch'])['state'],
      'connected',
    );
    await page
      .getByRole('button', { name: 'Review source command', exact: true })
      .click();
    await confirm('Create fixture-01?', 'Create fixture');
    const fixture = rows((await read('/api/v1/simulations'))['fixtures']).find(
      (f) => f['name'] === 'fixture-01',
    );
    assert.ok(fixture && typeof fixture['entity_id'] === 'string');
    assert.equal(fixture['entity_version'], '1');
    await page
      .getByRole('button', { name: 'Create corrupt revision…', exact: true })
      .click();
    await confirm(
      'Insert invalid mapped data in fixture-01?',
      'Create corrupt revision',
    );
    const failures = await until(
      () => read('/api/v1/failures?limit=25'),
      (value) =>
        rows(value['items']).some((x) => x['type'] === 'elasticsearch_active'),
    );
    const failure = rows(failures['items']).find(
      (x) => x['type'] === 'elasticsearch_active',
    );
    assert.ok(failure && typeof failure['event_id'] === 'string');
    const key = failure['event_id'];
    const before = await until(
      () => inspect(key),
      (value) => rows(object(value['consumer'])['effects']).length === 1,
    );
    await page.getByRole('link', { name: 'Failures', exact: true }).click();
    await page
      .getByRole('button', {
        name: 'Inspect failure ' + String(failure['key']),
        exact: true,
      })
      .click();
    await page
      .getByLabel('Reason for replay', { exact: true })
      .fill(
        'Acceptance: preserve original invalid revision and observe another diagnosed attempt',
      );
    await page
      .getByRole('button', { name: 'Review replay', exact: true })
      .click();
    await confirm('Replay this Elasticsearch failure?', 'Schedule replay');
    await until(
      () => read('/api/v1/failures?limit=25'),
      (value) =>
        rows(value['items']).some(
          (x) =>
            x['type'] === 'elasticsearch_active' &&
            x['event_id'] === key &&
            x['attempt_id'] !== failure['attempt_id'],
        ),
    );
    const after = await inspect(key);
    assert.deepEqual(
      object(before['pipeline'])['event'],
      object(after['pipeline'])['event'],
    );
    assert.deepEqual(before['consumer'], after['consumer']);
    assert.deepEqual(
      rows(object(before['pipeline'])['deliveries']).filter(
        (d) => d['kind'] === 'rabbitmq',
      ),
      rows(object(after['pipeline'])['deliveries']).filter(
        (d) => d['kind'] === 'rabbitmq',
      ),
    );
    await page
      .getByRole('button', { name: 'Refresh failures', exact: true })
      .click();
    await page.screenshot({
      path: resolve(output, 'operator-replay.png'),
      fullPage: true,
    });
    await page.getByRole('link', { name: 'Simulations', exact: true }).click();
    await page.getByLabel('Operation', { exact: true }).selectOption('update');
    await page
      .getByRole('button', { name: 'Review source command', exact: true })
      .click();
    await confirm('Update fixture-01?', 'Update fixture');
    assert.equal(
      rows((await read('/api/v1/simulations'))['fixtures']).find(
        (f) => f['name'] === 'fixture-01',
      )?.['entity_version'],
      '3',
    );
    assert.ok(
      writes.length === 9 &&
        writes.every((x) => x.key && /^[a-f0-9-]{36}$/.test(x.key)),
    );
    assert.deepEqual(errors, []);
    await page
      .getByRole('button', { name: 'Disconnect operator', exact: true })
      .click();
    console.log(
      JSON.stringify({
        status: 'PASS',
        scope: 'Real controls on separate 257-baseline retained fixture',
        browser: browser.version(),
        interceptedResponses: 0,
        controlResponseCapture: 'bounded clone of actual browser fetch stream',
        writes,
        pageErrors: errors,
        replay: {
          event: key,
          originalAttempt: failure['attempt_id'],
          canonicalBytesRetained: true,
          consumerEffectRetained: true,
          brokerOutcomeRetained: true,
        },
        run: { run_id: run['run_id'], completed_at: run['completed_at'] },
        screenshot: 'operator-replay.png',
      }),
    );
  },
  () => browser.close(),
);
