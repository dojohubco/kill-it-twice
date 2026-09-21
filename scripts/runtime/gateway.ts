import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { extname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
const assets = resolve('artifacts/operator-ui-build/browser');
const mime: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
};
function failure(res: ServerResponse, status: number, code: string) {
  if (res.destroyed || res.headersSent) {
    res.destroy();
    return;
  }
  res.writeHead(status, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
  });
  res.end(
    JSON.stringify({
      request_id: randomUUID(),
      error: { code, message: code.replaceAll('_', ' ') },
    }),
  );
}
async function bounded(
  request: IncomingMessage,
  maximum: number,
): Promise<Buffer> {
  const parts: Buffer[] = [];
  let bytes = 0;
  for await (const part of request as AsyncIterable<Buffer>) {
    bytes += part.length;
    if (bytes > maximum) throw new Error('request_bound');
    parts.push(part);
  }
  return Buffer.concat(parts);
}
async function proxy(req: IncomingMessage, res: ServerResponse, url: URL) {
  const method = req.method ?? 'GET';
  if (!['GET', 'POST', 'PUT'].includes(method)) {
    failure(res, 405, 'method_not_allowed');
    return;
  }
  const controller = new AbortController();
  res.once('close', () => {
    if (!res.writableEnded) controller.abort();
  });
  const headers: Record<string, string> = {
    accept: 'application/json',
    'content-type': 'application/json',
  };
  for (const key of ['authorization', 'idempotency-key', 'x-request-id']) {
    const v = req.headers[key];
    if (typeof v === 'string') headers[key] = v;
  }
  let body: Buffer;
  try {
    body = await bounded(req, 8192);
  } catch {
    failure(res, 413, 'request_too_large');
    return;
  }
  try {
    const response = await fetch(
      `http://control-api:3000${url.pathname}${url.search}`,
      {
        method,
        headers,
        redirect: 'error',
        signal: AbortSignal.any([
          AbortSignal.timeout(15000),
          controller.signal,
        ]),
        ...(method === 'GET' ? {} : { body: new Uint8Array(body) }),
      },
    );
    const reader = response.body?.getReader();
    if (!reader) {
      failure(res, 502, 'unavailable');
      return;
    }
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    try {
      for (;;) {
        const item = await reader.read();
        if (item.done) break;
        bytes += item.value.byteLength;
        if (bytes > 4 * 1024 * 1024) {
          await reader.cancel();
          throw new Error('response_bound');
        }
        chunks.push(item.value);
      }
    } finally {
      reader.releaseLock();
    }
    res.writeHead(response.status, {
      'content-type':
        response.headers.get('content-type') ?? 'application/json',
      'cache-control': 'no-store',
      'x-request-id': response.headers.get('x-request-id') ?? randomUUID(),
    });
    res.end(Buffer.concat(chunks));
  } catch {
    failure(res, 503, 'unavailable');
  }
}
async function serve(req: IncomingMessage, res: ServerResponse) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader(
    'Content-Security-Policy',
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
  );
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname.startsWith('/api/v1/') || url.pathname === '/metrics') {
    await proxy(req, res, url);
    return;
  }
  if (url.pathname === '/_health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"gateway":"ready"}');
    return;
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    failure(res, 405, 'method_not_allowed');
    return;
  }
  const pathname = decodeURIComponent(url.pathname);
  const routes = [
    '/',
    '/overview',
    '/backfill',
    '/records',
    '/failures',
    '/simulations',
    '/configuration',
  ];
  const file = resolve(
    assets,
    '.' + (routes.includes(pathname) ? '/index.html' : pathname),
  );
  if (
    !file.startsWith(assets + '/') ||
    pathname.split('/').some((p) => p.startsWith('.'))
  ) {
    failure(res, 404, 'not_found');
    return;
  }
  const info = await stat(file).catch(() => null);
  if (!info?.isFile() || info.size > 4 * 1024 * 1024 || !mime[extname(file)]) {
    failure(res, 404, 'not_found');
    return;
  }
  res.writeHead(200, {
    'content-type': mime[extname(file)] ?? 'application/octet-stream',
    'cache-control': file.endsWith('.html')
      ? 'no-store'
      : 'public, max-age=86400',
  });
  res.end(req.method === 'HEAD' ? undefined : await readFile(file));
}
const server = createServer(
  { requestTimeout: 20000, headersTimeout: 10000, maxHeaderSize: 8192 },
  (req, res) => {
    void serve(req, res).catch(() => failure(res, 400, 'invalid_request'));
  },
);
server.maxConnections = 128;
server.listen(4200, '0.0.0.0', () =>
  console.log('{"type":"gateway_ready","port":4200}'),
);
for (const signal of ['SIGTERM', 'SIGINT'])
  process.once(signal, () => {
    server.close();
    server.closeIdleConnections();
    const deadline = setTimeout(() => server.closeAllConnections(), 16000);
    deadline.unref();
  });
