import type { Request as ExpressRequest } from 'express';
import { OperationalCleanupError } from '../../src/operations/cleanup.ts';
import { EsFailure } from '../../src/es/adapter.ts';
import { errors as esErrors } from '@elastic/elasticsearch';
import { BrokerFailure } from '../../src/rabbitmq/metadata.ts';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import {
  Catch,
  type ArgumentsHost,
  type ExceptionFilter,
  HttpException,
} from '@nestjs/common';
import { TransactionError } from '../../src/internal/transaction.ts';
import { ControlError, id } from '../../src/operations/validation.ts';
export interface Request {
  headers: ExpressRequest['headers'];
  method: string;
  requestId: string;
  operation?: string;
  outcome?: string;
  errorClass?: string;
  cleanupErrors?: number;
  route?: { path?: string };
}
export interface Response {
  statusCode: number;
  setHeader(key: string, value: string): void;
  status(code: number): Response;
  json(value: unknown): void;
  send(value: string): void;
  on(event: string, fn: () => void): void;
}
export function operator(request: Request, token: string): string {
  const supplied = request.headers['authorization'];
  const expected = Buffer.from(`Bearer ${token}`),
    actual = Buffer.from(typeof supplied === 'string' ? supplied : '');
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected))
    throw new ControlError(401, 'operator_required');
  return id(request.headers['idempotency-key']);
}
export function beginRequest(
  req: Request,
  res: Response,
  next: (error?: unknown) => void,
) {
  const start = performance.now();
  req.requestId = randomUUID();
  res.on('finish', () => {
    const line = {
      timestamp: new Date().toISOString(),
      level: res.statusCode >= 500 ? 'error' : 'info',
      service: 'control-api',
      operation: req.operation ?? req.route?.path ?? 'unmatched',
      request_id: req.requestId,
      outcome: req.outcome ?? (res.statusCode < 400 ? 'observed' : 'rejected'),
      duration_ms: Math.round(performance.now() - start),
      error_class: req.errorClass ?? null,
      cleanup_error_count: req.cleanupErrors ?? 0,
    };
    process.stdout.write(JSON.stringify(line) + '\n');
  });
  try {
    if (req.headers['x-request-id'] !== undefined)
      req.requestId = id(req.headers['x-request-id']);
    res.setHeader('X-Request-ID', req.requestId);
    next();
  } catch (e) {
    next(e);
  }
}
@Catch()
export class Errors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost) {
    const http = host.switchToHttp(),
      req = http.getRequest<Request>(),
      res = http.getResponse<Response>();
    let status = 500,
      code = 'internal_error';
    if (error instanceof OperationalCleanupError) {
      req.cleanupErrors = error.cleanup.length;
      error = error.primary ?? new ControlError(503, 'unavailable');
    }
    if (
      error instanceof Error &&
      'type' in error &&
      error.type === 'entity.too.large'
    ) {
      status = 413;
      code = 'request_too_large';
    } else if (
      error instanceof Error &&
      'type' in error &&
      error.type === 'entity.parse.failed'
    ) {
      status = 400;
      code = 'invalid_request';
    } else if (error instanceof ControlError) {
      status = error.status;
      code = error.code;
    } else if (error instanceof TransactionError) {
      req.cleanupErrors = (req.cleanupErrors ?? 0) + error.cleanupErrors.length;
      if (['P9001', 'P2001', 'P8002', '23505'].includes(error.sqlState ?? '')) {
        status = 409;
        code = 'conflict';
      } else if (['P9004', 'P0002'].includes(error.sqlState ?? '')) {
        status = 404;
        code = 'not_found';
      } else if (
        error.sqlState?.startsWith('P') ||
        error.sqlState === '42501'
      ) {
        status = 422;
        code = 'integrity_block';
      } else {
        status = 503;
        code = 'unavailable';
      }
    } else if (error instanceof HttpException) {
      status = error.getStatus();
      code =
        status === 404
          ? 'not_found'
          : status === 413
            ? 'request_too_large'
            : 'invalid_request';
    } else if (error instanceof EsFailure || error instanceof BrokerFailure) {
      status = error.classification === 'transient' ? 503 : 422;
      code = status === 503 ? 'unavailable' : 'integrity_block';
    } else if (
      error instanceof esErrors.ConnectionError ||
      error instanceof esErrors.TimeoutError ||
      error instanceof esErrors.ResponseError ||
      error instanceof TypeError
    ) {
      status = 503;
      code = 'unavailable';
    }
    req.errorClass = code;
    req.outcome = code;
    res.setHeader('X-Request-ID', req.requestId);
    res.status(status).json({
      request_id: req.requestId,
      error: { code, message: code.replaceAll('_', ' ') },
    });
  }
}
