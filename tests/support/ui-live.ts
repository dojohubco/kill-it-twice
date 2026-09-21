import { uiBrowserExecutable } from './ui-browser.ts';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve, extname, join } from 'node:path';
import { chromium } from 'playwright-core';
import { parse } from 'lossless-json';
import { record, string } from '../../src/operations/validation.ts';
import { withCleanup } from '../../scripts/support.ts';
import { required } from './db.ts';

/** Actual rendered Angular reads the actual isolated operational API; no HTTP fixtures. */
export async function inspectLiveInterface(apiUrl: string) {
  const target = new URL(apiUrl);
  assert.equal(target.hostname, '127.0.0.1');
  assert.equal(target.protocol, 'http:');
  const assets = resolve('artifacts/operator-ui-build/browser');
  await readFile(join(assets, 'index.html'));
  const paths: string[] = [];
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? '/', 'http://127.0.0.1');
      if (request.method !== 'GET') {
        response.writeHead(405);
        response.end();
        return;
      }
      if (url.pathname.startsWith('/api/v1/')) {
        paths.push(url.pathname);
        const remote = await fetch(new URL(url.pathname + url.search, target), {
          signal: AbortSignal.timeout(12000),
          redirect: 'error',
        });
        const reader = remote.body?.getReader();
        assert.ok(reader);
        const chunks: Uint8Array[] = [];
        let size = 0;
        try {
          for (;;) {
            const part = await reader.read();
            if (part.done) break;
            size += part.value.byteLength;
            if (size > 4 * 1024 * 1024) {
              await reader.cancel();
              throw new Error('Bounded live UI response exceeded');
            }
            chunks.push(part.value);
          }
        } finally {
          reader.releaseLock();
        }
        const bytes = Buffer.concat(chunks);
        response.writeHead(remote.status, {
          'content-type': 'application/json',
        });
        response.end(bytes);
        return;
      }
      const path = resolve(
        assets,
        '.' + (extname(url.pathname) ? url.pathname : '/index.html'),
      );
      assert.ok(path.startsWith(assets + '/'));
      const bytes = await readFile(path);
      response.writeHead(200, {
        'content-type':
          extname(path) === '.js'
            ? 'text/javascript'
            : extname(path) === '.css'
              ? 'text/css'
              : 'text/html',
      });
      response.end(bytes);
    })().catch(() => {
      if (!response.headersSent) response.writeHead(503);
      response.end();
    });
  });
  await new Promise<void>((done, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', done);
  });
  return withCleanup(
    async () => {
      const address = server.address();
      assert.ok(address && typeof address !== 'string');
      const browser = await chromium.launch({
        executablePath: await uiBrowserExecutable(),
        headless: true,
        chromiumSandbox: true,
      });
      return withCleanup(
        async () => {
          const context = await browser.newContext({
            viewport: { width: 1440, height: 1000 },
          });
          const page = await context.newPage();
          const errors: string[] = [];
          page.on('pageerror', (error) => errors.push(error.message));
          await page.goto(`http://127.0.0.1:${address.port}/overview`);
          await page
            .getByRole('heading', { name: 'Overview', exact: true })
            .waitFor();
          await page.waitForFunction(
            () =>
              document.querySelector('.metric-number')?.textContent?.trim() !==
              '—',
          );
          const response = await fetch(new URL('/api/v1/status', target), {
            signal: AbortSignal.timeout(12000),
          });
          assert.equal(response.status, 200);
          const actual = record(record(parse(await response.text()))['data']);
          const staged = string(
            record(record(record(actual['dependencies'])['pipeline'])['data'])[
              'staged'
            ],
          );
          assert.equal(
            (
              await page.locator('.metric-number').first().textContent()
            )?.trim(),
            BigInt(staged).toLocaleString('en-US'),
          );
          assert.equal(actual['consumer_identity_validation'], 'matched');
          assert.equal(
            await page
              .getByRole('button', { name: 'Connect operator', exact: true })
              .count(),
            1,
          );
          const screenshot = join(
            required('M1_ARTIFACT_DIR'),
            'ui-live-overview.png',
          );
          await page.screenshot({ path: screenshot, fullPage: true });
          await page
            .getByRole('link', { name: 'Records', exact: true })
            .click();
          await page
            .getByRole('heading', { name: 'Records', exact: true })
            .waitFor();
          await page.waitForFunction(
            () => document.querySelectorAll('tbody tr').length > 0,
          );
          const first = page.locator('tbody tr').first();
          const id = (await first.locator('td').first().textContent())?.trim();
          assert.ok(
            id && /^[1-9][0-9]*$/.test(id),
            'Expected a record-table entity ID after navigation; observed ' +
              String(id),
          );
          await page
            .getByRole('button', { name: `Inspect record ${id}`, exact: true })
            .click();
          await page.locator('#record-detail-title').waitFor();
          assert.deepEqual(errors, []);
          const result = {
            scope:
              'Real Angular browser -> local read-only transport -> actual isolated Nest/API/PostgreSQL/Elasticsearch observations; no intercepted data fixtures',
            browser: browser.version(),
            staged,
            inspected_entity_id: id,
            api_reads: paths,
            screenshot,
            mutations: 0,
            page_errors: errors,
          };
          await writeFile(
            join(required('M1_ARTIFACT_DIR'), 'ui-live.json'),
            JSON.stringify(result, null, 2) + '\n',
          );
          return result;
        },
        () => browser.close(),
      );
    },
    async () => {
      server.closeAllConnections();
      await new Promise<void>((done, reject) =>
        server.close((e) => (e ? reject(e) : done())),
      );
    },
  );
}
