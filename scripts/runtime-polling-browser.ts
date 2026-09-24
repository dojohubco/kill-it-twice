// Real UI and HTTP settings protocol; no response substitution.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { chromium } from 'playwright-core';
import { AxeBuilder } from '@axe-core/playwright';
import { uiBrowserExecutable } from '../tests/support/ui-browser.ts';
import { withCleanup } from './support.ts';
const [url, output, project] = process.argv.slice(2);
assert.ok(url && /^http:\/\/127\.0\.0\.1:[0-9]+$/.test(url));
assert.ok(
  output && project && /^kit-verify-[0-9]{14}-[a-f0-9]{8}$/.test(project),
);
const execute = promisify(execFile);
const secret = (
  await execute(
    'docker',
    [
      'compose',
      '-f',
      resolve('compose.yaml'),
      '-p',
      project,
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
function record(v: unknown): Record<string, unknown> {
  assert.ok(v && typeof v === 'object' && !Array.isArray(v));
  return v as Record<string, unknown>;
}
await withCleanup(
  async () => {
    const context = await browser.newContext({
      viewport: { width: 1280, height: 1000 },
    });
    const page = await context.newPage();
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    const writes: { key: string; body: Record<string, unknown> }[] = [];
    page.on('request', (r) => {
      if (
        r.method() === 'PUT' &&
        new URL(r.url()).pathname === '/api/v1/config/polling'
      ) {
        writes.push({
          key: r.headers()['idempotency-key'] ?? '',
          body: record(JSON.parse(r.postData() ?? 'null') as unknown),
        });
      }
    });
    async function read() {
      const response = await page.request.get(url + '/api/v1/config/polling', {
        timeout: 10000,
      });
      assert.equal(response.status(), 200);
      return record(record((await response.json()) as unknown)['data']);
    }
    async function put(
      body: Record<string, unknown>,
      status: number,
      key: string = randomUUID(),
      authorized = true,
    ) {
      const response = await page.request.put(url + '/api/v1/config/polling', {
        timeout: 10000,
        data: body,
        headers: {
          'Idempotency-Key': key,
          ...(authorized ? { Authorization: 'Bearer ' + secret } : {}),
        },
      });
      assert.equal(response.status(), status);
      const envelope = record((await response.json()) as unknown);
      return status < 300 ? record(envelope['data']) : envelope;
    }
    const initial = await read();
    assert.equal(initial['revision'], '1');
    const input = {
      expected_revision: '1',
      capture_poll_ms: 250,
      backfill_idle_ms: 750,
    };
    await put(input, 401, randomUUID(), false);
    for (const body of [
      { ...input, capture_poll_ms: 49 },
      { ...input, backfill_idle_ms: 30001 },
      { ...input, capture_poll_ms: 50.5 },
      { ...input, capture_poll_ms: '1000' },
      { ...input, extra: true },
      { capture_poll_ms: 1000, backfill_idle_ms: 1000 },
    ])
      await put(body, 400);
    assert.equal((await read())['revision'], '1');
    await page.goto(url + '/configuration');
    await page
      .getByLabel('Incremental capture interval (ms)', { exact: true })
      .waitFor();
    assert.equal(
      await page
        .getByRole('button', { name: 'Review interval changes', exact: true })
        .isDisabled(),
      true,
    );
    await page
      .getByRole('button', { name: 'Connect operator', exact: true })
      .click();
    await page.getByLabel('Operator token', { exact: true }).fill(secret);
    await page.getByRole('button', { name: 'Use token', exact: true }).click();
    await page
      .getByRole('button', { name: 'Disconnect operator', exact: true })
      .waitFor();
    const capture = page.getByLabel('Incremental capture interval (ms)', {
      exact: true,
    });
    const backfill = page.getByLabel('Idle backfill interval (ms)', {
      exact: true,
    });
    await capture.fill('49');
    await page
      .getByRole('button', { name: 'Review interval changes', exact: true })
      .click();
    assert.equal(
      await capture.evaluate(
        (el) => (el as HTMLInputElement).validity.rangeUnderflow,
      ),
      true,
    );
    assert.equal(
      await page
        .getByRole('dialog', { name: 'Save polling intervals?' })
        .count(),
      0,
    );
    await capture.fill('250');
    await backfill.fill('750');
    page.once('dialog', (confirmation) => {
      assert.match(confirmation.message(), /unsaved polling changes/);
      void confirmation.dismiss();
    });
    await page.getByRole('link', { name: 'Overview', exact: true }).click();
    assert.equal(new URL(page.url()).pathname, '/configuration');
    assert.equal(await capture.inputValue(), '250');
    await page
      .getByRole('button', { name: 'Refresh configuration', exact: true })
      .click();
    assert.equal(await capture.inputValue(), '250');
    await page
      .getByRole('button', { name: 'Review interval changes', exact: true })
      .click();
    let dialog = page.getByRole('dialog', { name: 'Save polling intervals?' });
    await dialog.waitFor();
    assert.match(await dialog.innerText(), /250 ms/);
    await page.keyboard.press('Escape');
    await dialog.waitFor({ state: 'hidden' });
    assert.equal(writes.length, 0);
    await page
      .getByRole('button', { name: 'Review interval changes', exact: true })
      .focus();
    await page.keyboard.press('Enter');
    dialog = page.getByRole('dialog', { name: 'Save polling intervals?' });
    await dialog
      .getByRole('button', { name: 'Save intervals', exact: true })
      .click();
    await dialog.getByText('Request accepted', { exact: true }).waitFor();
    await dialog.getByRole('button', { name: 'Done', exact: true }).click();
    await page
      .getByTestId('polling-revision')
      .filter({ hasText: '2' })
      .waitFor();
    assert.equal(writes.length, 1);
    const request = writes[0];
    assert.ok(request);
    assert.equal((await read())['capture_poll_ms'], 250);
    assert.equal((await put(request.body, 200, request.key))['replayed'], true);
    await put({ ...request.body, capture_poll_ms: 251 }, 409, request.key);
    await put(input, 409);
    const competing = await Promise.all([
      page.request.put(url + '/api/v1/config/polling', {
        data: {
          expected_revision: '2',
          capture_poll_ms: 300,
          backfill_idle_ms: 800,
        },
        headers: {
          Authorization: 'Bearer ' + secret,
          'Idempotency-Key': randomUUID(),
        },
        timeout: 10000,
      }),
      page.request.put(url + '/api/v1/config/polling', {
        data: {
          expected_revision: '2',
          capture_poll_ms: 350,
          backfill_idle_ms: 850,
        },
        headers: {
          Authorization: 'Bearer ' + secret,
          'Idempotency-Key': randomUUID(),
        },
        timeout: 10000,
      }),
    ]);
    assert.deepEqual(competing.map((r) => r.status()).sort(), [202, 409]);
    const concurrent = await read();
    assert.equal(concurrent['revision'], '3');
    await put(
      { expected_revision: '3', capture_poll_ms: 400, backfill_idle_ms: 900 },
      202,
    );
    assert.equal((await put(request.body, 200, request.key))['revision'], '2');
    assert.equal((await read())['revision'], '4');
    await page.reload();
    await page
      .getByTestId('polling-revision')
      .filter({ hasText: '4' })
      .waitFor();
    assert.equal(await capture.inputValue(), '400');
    assert.equal(await backfill.inputValue(), '900');
    const audits = [];
    for (const width of [1280, 320]) {
      await page.setViewportSize({ width, height: 1000 });
      await page.emulateMedia({ reducedMotion: 'reduce' });
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      const audit = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();
      assert.deepEqual(
        audit.violations.map((v) => v.id),
        [],
      );
      audits.push({ width, violations: 0 });
      await page.screenshot({
        path: resolve(output, `polling-${width}.png`),
        fullPage: true,
      });
    }
    await page.setViewportSize({ width: 1280, height: 1000 });
    await page.evaluate(() => {
      const elements = Array.from(
        document.querySelectorAll<HTMLElement>('body,body *'),
      ).map((el) => ({ el, size: parseFloat(getComputedStyle(el).fontSize) }));
      for (const { el, size } of elements) el.style.fontSize = size * 2 + 'px';
    });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      true,
    );
    await page.screenshot({
      path: resolve(output, 'polling-text-200.png'),
      fullPage: true,
    });
    assert.deepEqual(errors, []);
    console.log(
      JSON.stringify({
        status: 'PASS',
        scope: 'Real settings browser/API, no intercepted responses',
        saved: await read(),
        checks: [
          'operator authorization',
          'HTTP input bounds',
          'native numeric validation',
          'draft survives observation refresh',
          'confirmation cancel without mutation',
          'keyboard confirmation',
          'same-key original receipt',
          'changed-key-input conflict',
          'stale revision conflict',
          'concurrent edit has exactly one winner',
          'old retry does not overwrite newer settings',
          'browser reload persistence',
          '320px and 200-percent text reflow',
        ],
        audits,
        pageErrors: errors,
        interceptedResponses: 0,
      }),
    );
  },
  () => browser.close(),
);
