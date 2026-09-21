import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import { chromium } from 'playwright-core';
import { uiBrowserExecutable } from '../tests/support/ui-browser.ts';
import { withCleanup } from './support.ts';
const [url, output, expected] = process.argv.slice(2);
assert.ok(url && /^http:\/\/127\.0\.0\.1:[0-9]+$/.test(url));
assert.ok(output && expected && /^[1-9][0-9]*$/.test(expected));
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
    const errors: string[] = [],
      paths = new Set<string>();
    let mutations = 0;
    page.on('pageerror', (error) => {
      if (errors.length < 20) errors.push(error.message);
    });
    page.on('console', (message) => {
      if (
        message.type() === 'error' &&
        /Content Security Policy|inline event handler/i.test(message.text())
      )
        errors.push(message.text());
    });
    page.on('request', (request) => {
      const target = new URL(request.url());
      if (target.pathname.startsWith('/api/v1/')) {
        paths.add(target.pathname);
        if (request.method() !== 'GET') mutations++;
      }
    });
    await page.goto(url + '/overview');
    await page
      .getByRole('heading', { name: 'Overview', exact: true })
      .waitFor();
    await page.getByText(expected, { exact: true }).first().waitFor();
    assert.equal(new URL(page.url()).pathname, '/overview');
    assert.ok((await page.title()).length > 0);
    const styles = await page
      .locator('link[rel=stylesheet]')
      .evaluateAll((links) =>
        links.map((node) => {
          const link = node as HTMLLinkElement;
          return {
            loaded: !!link.sheet,
            media: link.media,
            handler: link.getAttribute('onload'),
          };
        }),
      );
    assert.ok(
      styles.length > 0 &&
        styles.every(
          (s) => s.loaded && s.media !== 'print' && s.handler === null,
        ),
      'Production stylesheet must apply without blocked inline script',
    );
    await page.screenshot({
      path: resolve(output, 'runtime-overview.png'),
      fullPage: true,
    });
    await page.getByRole('link', { name: 'Records', exact: true }).click();
    await page.getByRole('heading', { name: 'Records', exact: true }).waitFor();
    const inspect = page
      .getByRole('button', { name: /^Inspect record [0-9]+$/ })
      .first();
    await inspect.waitFor();
    const label = await inspect.getAttribute('aria-label');
    await inspect.click();
    await page.locator('#record-detail-title').waitFor();
    await page.screenshot({
      path: resolve(output, 'runtime-record.png'),
      fullPage: true,
    });
    await page.keyboard.press('Escape');
    await page.locator('#record-detail-title').waitFor({ state: 'hidden' });
    await page.setViewportSize({ width: 390, height: 844 });
    assert.ok(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    );
    await page.screenshot({
      path: resolve(output, 'runtime-mobile.png'),
      fullPage: true,
    });
    assert.deepEqual(errors, []);
    assert.equal(mutations, 0);
    assert.ok(paths.has('/api/v1/status') && paths.has('/api/v1/entities'));
    console.log(
      JSON.stringify({
        status: 'PASS',
        flow: 'retained Compose gateway -> Overview -> Records -> exact details',
        browser: browser.version(),
        browserPath: 'project Playwright; Browser plugin not available',
        interceptedResponses: 0,
        appliedStylesheets: styles,
        expectedStaged: expected,
        record: label,
        requests: [...paths],
        mutations,
        pageErrors: errors,
        screenshots: [
          'runtime-overview.png',
          'runtime-record.png',
          'runtime-mobile.png',
        ],
      }),
    );
  },
  () => browser.close(),
);
