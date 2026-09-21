import { uiBrowserExecutable } from '../support/ui-browser.ts';
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
import { chromium, type Browser, type Page } from 'playwright-core';
import { AxeBuilder } from '@axe-core/playwright';
import {
  routeFixtures,
  entityId,
  eventId,
  token,
  type UiFixtureState,
} from './fixtures.ts';
const output = resolve(
  process.env['UI_EVIDENCE_DIR'] ?? 'artifacts/ui/browser',
);
assert.ok(
  output.startsWith(resolve('artifacts/ui') + '/'),
  'Browser artifacts must remain inside the owned UI directory.',
);
const assets = resolve('artifacts/operator-ui-build/browser');
let server: Server;
let browser: Browser;
let origin: string;
const results: Record<string, unknown>[] = [];
before(async () => {
  await mkdir(output, { recursive: true });
  server = createServer((req, res) => {
    void (async () => {
      const path = new URL(req.url ?? '/', 'http://127.0.0.1').pathname;
      if (path.startsWith('/api/')) {
        res.writeHead(503, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: { code: 'unavailable' } }));
        return;
      }
      const file = resolve(
        assets,
        '.' + (extname(path) ? path : '/index.html'),
      );
      if (!file.startsWith(assets + '/')) {
        res.writeHead(403);
        res.end();
        return;
      }
      try {
        const bytes = await readFile(file);
        res.setHeader(
          'content-type',
          extname(file) === '.js'
            ? 'text/javascript'
            : extname(file) === '.css'
              ? 'text/css'
              : 'text/html',
        );
        res.end(bytes);
      } catch {
        res.writeHead(404);
        res.end();
      }
    })().catch(() => {
      res.writeHead(500);
      res.end();
    });
  });
  await new Promise<void>((done, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', done);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  origin = `http://127.0.0.1:${address.port}`;
  browser = await chromium.launch({
    executablePath: await uiBrowserExecutable(),
    headless: true,
    chromiumSandbox: true,
  });
});
after(async () => {
  await writeFile(
    resolve(output, 'evidence.json'),
    JSON.stringify(
      {
        scope:
          'Real browser with isolated API response fixtures; not end-to-end backend acceptance',
        results,
      },
      null,
      2,
    ),
  );
  await browser?.close();
  if (server)
    await new Promise<void>((done, reject) =>
      server.close((e) => (e ? reject(e) : done())),
    );
});
async function pageCase(
  id: string,
  work: (page: Page, state: UiFixtureState) => Promise<void>,
): Promise<void> {
  const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      colorScheme: 'light',
    }),
    page = await context.newPage(),
    errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const state = await routeFixtures(page);
  try {
    await work(page, state);
    assert.deepEqual(errors, []);
    results.push({ id, status: 'PASS', pageErrors: errors });
  } catch (e) {
    await page.screenshot({
      path: resolve(output, id + '-failure.png'),
      fullPage: true,
    });
    results.push({
      id,
      status: 'FAIL',
      pageErrors: errors,
      error: e instanceof Error ? e.message : 'Unknown error',
    });
    throw e;
  } finally {
    await context.close();
  }
}
async function open(page: Page, path = '/overview') {
  await page.goto(origin + path);
  await page.locator('main h1').waitFor();
}
async function unlock(page: Page) {
  await page
    .getByRole('button', { name: 'Connect operator', exact: true })
    .click();
  await page.getByLabel('Operator token', { exact: true }).fill(token);
  await page.getByRole('button', { name: 'Use token', exact: true }).click();
  await page
    .getByRole('button', { name: 'Disconnect operator', exact: true })
    .waitFor();
}
void test(
  'U01 unavailable and empty states never invent data',
  { timeout: 30000 },
  async () =>
    pageCase('U01', async (page, state) => {
      state.unavailable = true;
      await open(page);
      await page
        .getByText('Connect the operational API', { exact: true })
        .waitFor();
      assert.equal(
        await page.locator('.metric-number').first().textContent(),
        '—',
      );
      state.unavailable = false;
      state.empty = true;
      await page.getByRole('link', { name: 'Records', exact: true }).click();
      await page.getByText('No matching records', { exact: true }).waitFor();
      assert.equal(await page.locator('tbody tr').count(), 0);
    }),
);
void test(
  'U02 observations preserve stale distinction and rate warm-up',
  { timeout: 30000 },
  async () =>
    pageCase('U02', async (page, state) => {
      await open(page);
      await page.getByText('144', { exact: true }).first().waitFor();
      assert.match(
        await page.locator('.metric-rate').first().innerText(),
        /Warming/,
      );
      state.unavailable = true;
      await page
        .getByRole('button', { name: 'Refresh status', exact: true })
        .click();
      await page
        .getByText('Live updates are unavailable', { exact: true })
        .waitFor();
      assert.equal(
        await page.locator('.metric-number').first().textContent(),
        '—',
      );
      state.unavailable = false;
      await page
        .getByRole('button', { name: 'Refresh status', exact: true })
        .click();
      await page.getByText('Healthy', { exact: true }).first().waitFor();
      state.configUnavailable = true;
      await page
        .getByRole('link', { name: 'Configuration', exact: true })
        .click();
      await page
        .getByRole('button', { name: 'Refresh configuration', exact: true })
        .click();
      await page
        .getByText(
          'Configuration is last-known, not a fresh receiver observation.',
          { exact: true },
        )
        .waitFor();
    }),
);
void test(
  'U03 bounded record navigation retains exact identifiers',
  { timeout: 30000 },
  async () =>
    pageCase('U03', async (page) => {
      await open(page, '/records');
      await page.getByText(entityId, { exact: true }).waitFor();
      await page.getByRole('button', { name: 'Next', exact: true }).click();
      await page.getByText('9007199254740994', { exact: true }).waitFor();
      await page.getByRole('button', { name: 'Previous', exact: true }).click();
      await page.getByText(entityId, { exact: true }).waitFor();
      await page
        .getByRole('button', { name: 'Inspect record ' + entityId })
        .click();
      await page
        .getByRole('heading', { name: 'Kartli Trading', exact: true })
        .waitFor();
      assert.match(
        await page
          .locator('#record-detail-title')
          .locator('..')
          .locator('..')
          .locator('..')
          .innerText(),
        /9 \/ 8/,
      );
      assert.equal(
        await page.getByText('9007199254740992', { exact: true }).count(),
        0,
      );
    }),
);
void test(
  'U04 mutation confirmation retries the same unknown command',
  { timeout: 30000 },
  async () =>
    pageCase('U04', async (page, state) => {
      await open(page, '/backfill');
      assert.equal(state.requests.filter((r) => r.method !== 'GET').length, 0);
      await unlock(page);
      await page
        .getByRole('button', { name: 'Start backfill', exact: true })
        .click();
      const dialog = page.getByRole('dialog', {
        name: 'Start a backfill run?',
      });
      await dialog.waitFor();
      state.ambiguousOnce = true;
      await dialog
        .getByRole('button', { name: 'Start backfill', exact: true })
        .click();
      await dialog
        .getByText('Outcome not confirmed', { exact: true })
        .waitFor();
      await dialog
        .getByRole('button', { name: 'Retry same request', exact: true })
        .click();
      await dialog.getByText('Request accepted', { exact: true }).waitFor();
      const writes = state.requests.filter((r) => r.method === 'POST');
      assert.equal(writes.length, 2);
      assert.ok(writes[0]?.key);
      assert.equal(writes[0]?.key, writes[1]?.key);
      assert.equal(writes[0]?.body, writes[1]?.body);
      await dialog.getByRole('button', { name: 'Done', exact: true }).click();
      await dialog.waitFor({ state: 'hidden' });
      await page.getByRole('dialog').waitFor({ state: 'hidden' });
      assert.equal(await page.getByRole('dialog').count(), 0);
    }),
);
void test(
  'U05 backfill distinguishes unknown denominator from completion',
  { timeout: 30000 },
  async () =>
    pageCase('U05', async (page) => {
      await open(page, '/backfill');
      await page.getByText('Not sealed', { exact: true }).waitFor();
      assert.equal(
        await page
          .getByRole('button', { name: 'Request pause', exact: true })
          .isDisabled(),
        true,
      );
      await unlock(page);
      await page
        .getByRole('button', { name: 'Request pause', exact: true })
        .click();
      const dialog = page.getByRole('dialog', {
        name: 'Pause new page admission?',
      });
      await dialog
        .getByRole('button', { name: 'Request pause', exact: true })
        .click();
      await dialog.getByText('Request accepted', { exact: true }).waitFor();
      assert.match(await dialog.innerText(), /not end-to-end delivery/);
      await dialog.getByRole('button', { name: 'Done' }).click();
      assert.equal(
        await page.getByText('Scanning', { exact: true }).count(),
        1,
      );
    }),
);
void test(
  'U06 quarantine cannot invoke the Elasticsearch replay control',
  { timeout: 30000 },
  async () =>
    pageCase('U06', async (page) => {
      await open(page, '/failures');
      await page
        .getByRole('button', { name: 'Inspect failure quarantine:test-only' })
        .click();
      await page.getByText(/Replay is not available for this record/).waitFor();
      assert.equal(
        await page.getByRole('button', { name: 'Review replay' }).count(),
        0,
      );
      await page
        .getByRole('button', { name: 'Inspect failure es-active:' + eventId })
        .click();
      await page
        .getByLabel('Reason for replay')
        .fill('Reviewed the fixture mapping');
      assert.equal(
        await page.getByRole('button', { name: 'Review replay' }).isDisabled(),
        true,
      );
      await unlock(page);
      await page.getByRole('button', { name: 'Review replay' }).click();
      await page
        .getByRole('dialog', { name: 'Replay this Elasticsearch failure?' })
        .waitFor();
      await page.keyboard.press('Escape');
      await page.getByRole('dialog').waitFor({ state: 'hidden' });
      assert.equal(await page.getByRole('dialog').count(), 0);
    }),
);
void test(
  'U07 simulations require confirmation and never write on entry',
  { timeout: 30000 },
  async () =>
    pageCase('U07', async (page, state) => {
      await open(page, '/simulations');
      await page.getByText(entityId, { exact: true }).waitFor();
      assert.equal(state.requests.filter((r) => r.method !== 'GET').length, 0);
      await unlock(page);
      await page
        .getByRole('button', { name: 'Disconnect', exact: true })
        .first()
        .click();
      await page
        .getByRole('dialog', { name: 'Disconnect Elasticsearch route?' })
        .waitFor();
      await page.keyboard.press('Escape');
      assert.equal(state.requests.filter((r) => r.method !== 'GET').length, 0);
      await page
        .getByRole('button', { name: 'Create corrupt revision…', exact: true })
        .click();
      const dialog = page.getByRole('dialog', {
        name: 'Insert invalid mapped data in fixture-01?',
      });
      await dialog
        .getByRole('button', { name: 'Create corrupt revision', exact: true })
        .click();
      await dialog.getByText('Request accepted', { exact: true }).waitFor();
      const writes = state.requests.filter((r) => r.method !== 'GET');
      assert.equal(writes.length, 1);
      assert.equal(writes[0]?.path, '/api/v1/simulations/corrupt-record');
      assert.equal(writes[0]?.body, '{"fixture":"fixture-01"}');
    }),
);
void test(
  'U08 rendered pages have named controls and measured accessible contrast',
  { timeout: 90000 },
  async () =>
    pageCase('U08', async (page) => {
      const audits: unknown[] = [];
      for (const path of [
        'overview',
        'backfill',
        'records',
        'failures',
        'simulations',
        'configuration',
      ]) {
        await open(page, '/' + path);
        await page
          .getByRole('button', { name: 'Connect operator', exact: true })
          .waitFor();
        await page.locator('body').click({ position: { x: 10, y: 10 } });
        const audit = await new AxeBuilder({ page })
          .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
          .analyze();
        audits.push({ path, violations: audit.violations });
        assert.deepEqual(
          audit.violations.map((v) => ({
            id: v.id,
            nodes: v.nodes.map((n) => n.target),
          })),
          [],
          path,
        );
        await page.screenshot({
          path: resolve(output, path + '-desktop.png'),
          fullPage: true,
        });
      }
      await writeFile(
        resolve(output, 'accessibility.json'),
        JSON.stringify(audits, null, 2),
      );
    }),
);
void test(
  'U09 narrow navigation dialog keyboard and reduced motion remain usable',
  { timeout: 45000 },
  async () =>
    pageCase('U09', async (page) => {
      await page.setViewportSize({ width: 320, height: 760 });
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await open(page);
      await page
        .getByRole('button', { name: 'Toggle workspace navigation' })
        .click();
      await page.getByRole('link', { name: 'Records', exact: true }).click();
      await page.getByText(entityId, { exact: true }).waitFor();
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      await page.screenshot({
        path: resolve(output, 'records-320.png'),
        fullPage: true,
      });
      await page
        .getByRole('button', { name: 'Connect operator', exact: true })
        .click();
      const dialog = page.getByRole('dialog', {
        name: 'Connect operator access',
      });
      await dialog.waitFor();
      for (let i = 0; i < 12; i++) {
        await page.keyboard.press('Tab');
        assert.equal(
          await page.evaluate(() =>
            document
              .querySelector('dialog[open]')
              ?.contains(document.activeElement),
          ),
          true,
        );
      }
      await page.keyboard.press('Escape');
      await page.getByRole('dialog').waitFor({ state: 'hidden' });
      assert.equal(await page.getByRole('dialog').count(), 0);
      assert.equal(
        await page
          .getByRole('button', { name: 'Connect operator', exact: true })
          .evaluate((el) => el === document.activeElement),
        true,
      );
      await page.emulateMedia({ forcedColors: 'active' });
      await page.screenshot({
        path: resolve(output, 'records-forced-colors.png'),
        fullPage: true,
      });
      await page.emulateMedia({ forcedColors: 'none' });
      await page.setViewportSize({ width: 720, height: 500 });
      await open(page, '/overview');
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      const transitions = await page
        .locator('.button')
        .evaluateAll((nodes) =>
          nodes.map((n) => getComputedStyle(n).transitionDuration),
        );
      assert.ok(transitions.every((v) => v === '0s'));
      await page.setViewportSize({ width: 1280, height: 1000 });
      await page.reload();
      await page.locator('main h1').waitFor();
      await page.evaluate(() => {
        const sizes = Array.from(
          document.querySelectorAll<HTMLElement>('body, body *'),
        ).map((el) => ({
          el,
          size: parseFloat(getComputedStyle(el).fontSize),
        }));
        for (const { el, size } of sizes) el.style.fontSize = size * 2 + 'px';
      });
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      await page.screenshot({
        path: resolve(output, 'overview-text-200.png'),
        fullPage: true,
      });
    }),
);
void test(
  'U10 access is session-memory only and UI does not ship response fixtures',
  { timeout: 30000 },
  async () =>
    pageCase('U10', async (page, state) => {
      await open(page);
      await unlock(page);
      const storage = await page.evaluate(() => ({
        local: Object.keys(localStorage),
        session: Object.keys(sessionStorage),
      }));
      assert.deepEqual(storage, { local: [], session: [] });
      await page.reload();
      await page
        .getByRole('button', { name: 'Connect operator', exact: true })
        .waitFor();
      assert.equal(state.requests.filter((r) => r.method !== 'GET').length, 0);
      const resources = await page.evaluate(() =>
        performance.getEntriesByType('resource').map((r) => r.name),
      );
      assert.ok(
        resources.filter((r) => new URL(r).pathname.endsWith('.js')).length > 0,
      );
      for (const url of resources.filter((r) =>
        new URL(r).pathname.endsWith('.js'),
      )) {
        const response = await page.request.get(url);
        assert.equal(
          (await response.text()).includes('ui-test-only-operator-token'),
          false,
        );
      }
      await writeFile(
        resolve(output, 'rendered-system.json'),
        JSON.stringify(
          await page.evaluate(() => ({
            framework: document
              .querySelector('[ng-version]')
              ?.getAttribute('ng-version'),
            viewport: { width: innerWidth, height: innerHeight },
            bodyFont: getComputedStyle(document.body).fontFamily,
            fontSize: getComputedStyle(document.body).fontSize,
            focusControls: document.querySelectorAll('button,a,input,select')
              .length,
            runningAnimations: document.getAnimations().length,
          })),
          null,
          2,
        ),
      );
    }),
);
