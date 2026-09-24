import { OperationalMonitor } from '../../src/operations/monitor.ts';
import { responseSchema, mutationSchema, errorSchema } from './schemas.ts';
import {
  Body,
  Controller,
  Get,
  Post,
  Put,
  Param,
  Query,
  Req,
  Res,
  Inject,
} from '@nestjs/common';
import {
  ApiBody,
  ApiHeader,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiSecurity,
} from '@nestjs/swagger';
import { OperationsService } from '../../src/operations/service.ts';
import { metrics } from '../../src/operations/metrics.ts';
import { operator, type Request, type Response } from './http.ts';
export const SETTINGS = 'control-settings';
export interface Settings {
  token: string;
  afterCommit?: (
    operation: string,
    result: Record<string, unknown>,
  ) => Promise<void>;
}
const uuidSchema = { type: 'string', format: 'uuid' } as const;
const decimal = { type: 'string', pattern: '^[1-9][0-9]{0,18}$' } as const;
const jsonResponse = {
  type: 'object',
  required: ['request_id'],
  properties: { request_id: uuidSchema },
};
function read(summary: string) {
  return ApiOperation({ summary });
}
@ApiResponse({
  status: 400,
  schema: errorSchema,
  description: 'Invalid bounded input',
})
@ApiResponse({
  status: 404,
  schema: errorSchema,
  description: 'Missing identity',
})
@ApiResponse({
  status: 422,
  schema: errorSchema,
  description: 'Integrity or configuration block',
})
@ApiResponse({
  status: 503,
  schema: errorSchema,
  description: 'Dependency currently unavailable',
})
@ApiResponse({
  status: 200,
  description: 'Fresh observed state or original idempotent result',
  schema: jsonResponse,
})
@Controller('api/v1')
export class ControlController {
  constructor(
    @Inject(OperationsService) private readonly service: OperationsService,
    @Inject(OperationalMonitor) private readonly monitor: OperationalMonitor,
    @Inject(SETTINGS) private readonly settings: Settings,
  ) {}
  private async result(
    req: Request,
    operation: string,
    work: Promise<unknown>,
  ) {
    req.operation = operation;
    return { request_id: req.requestId, data: await work };
  }
  private async mutate(
    req: Request,
    res: Response,
    operation: string,
    work: (key: string) => Promise<Record<string, unknown>>,
  ) {
    req.operation = operation;
    const key = operator(req, this.settings.token),
      result = await work(key);
    await this.settings.afterCommit?.(operation, result);
    const replayed = result['replayed'] === true;
    req.outcome = replayed ? 'already_applied' : 'scheduled';
    res.status(replayed ? 200 : 202);
    return { request_id: req.requestId, outcome: req.outcome, data: result };
  }
  @Get('status')
  @ApiResponse({ status: 200, schema: responseSchema('status') })
  @read('Fresh operational snapshot')
  status(@Req() req: Request) {
    return this.result(req, 'status', this.monitor.snapshot());
  }
  @Get('backfills/:runId')
  @ApiResponse({ status: 200, schema: responseSchema('backfill_status') })
  @ApiParam({ name: 'runId', schema: uuidSchema })
  backfill(@Param('runId') run: string, @Req() req: Request) {
    return this.result(req, 'backfill_status', this.service.backfill(run));
  }
  @Post('backfills')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Durably scheduled',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Identity conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['run_id', 'ranges'],
      properties: {
        run_id: uuidSchema,
        ranges: { type: 'integer', minimum: 1, maximum: 16 },
      },
    },
  })
  start(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'backfill_start', (key) =>
      this.service.start(key, req.requestId, body),
    );
  }
  @Post('backfills/:runId/pause')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Durably paused',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Identity conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiParam({ name: 'runId', schema: uuidSchema })
  @ApiBody({ schema: { type: 'object', additionalProperties: false } })
  pause(
    @Param('runId') run: string,
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'backfill_pause', (key) =>
      this.service.pause(key, req.requestId, run, true, body),
    );
  }
  @Post('backfills/:runId/resume')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Durably resumed',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Identity conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiParam({ name: 'runId', schema: uuidSchema })
  @ApiBody({ schema: { type: 'object', additionalProperties: false } })
  resume(
    @Param('runId') run: string,
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'backfill_resume', (key) =>
      this.service.pause(key, req.requestId, run, false, body),
    );
  }
  @Get('entities')
  @ApiResponse({ status: 200, schema: responseSchema('entity_search') })
  @ApiQuery({
    name: 'limit',
    required: false,
    type: Number,
    minimum: 1,
    maximum: 100,
  })
  @ApiQuery({ name: 'cursor', required: false, type: String, maxLength: 2048 })
  @ApiQuery({ name: 'q', required: false, type: String, maxLength: 128 })
  @ApiQuery({
    name: 'include_deleted',
    required: false,
    enum: ['true', 'false'],
  })
  entities(@Query() query: unknown, @Req() req: Request) {
    return this.result(req, 'entity_search', this.service.entities(query));
  }
  @Get('entities/:sourceEpoch/:entityId')
  @ApiResponse({ status: 200, schema: responseSchema('entity_detail') })
  @ApiParam({ name: 'sourceEpoch', schema: uuidSchema })
  @ApiParam({ name: 'entityId', schema: decimal })
  entity(
    @Param('sourceEpoch') epoch: string,
    @Param('entityId') entity: string,
    @Req() req: Request,
  ) {
    return this.result(
      req,
      'entity_detail',
      this.service.entity(epoch, entity),
    );
  }
  @Get('events/:eventId')
  @ApiResponse({ status: 200, schema: responseSchema('event_detail') })
  @ApiParam({ name: 'eventId', schema: { type: 'string', maxLength: 80 } })
  event(@Param('eventId') event: string, @Req() req: Request) {
    return this.result(req, 'event_detail', this.service.event(event));
  }
  @Get('failures')
  @ApiResponse({ status: 200, schema: responseSchema('failures') })
  @ApiQuery({
    name: 'limit',
    required: false,
    type: Number,
    minimum: 1,
    maximum: 100,
  })
  @ApiQuery({ name: 'cursor', required: false, type: String, maxLength: 2048 })
  failures(@Query() query: unknown, @Req() req: Request) {
    return this.result(req, 'failures', this.service.failures(query));
  }
  @Get('config/polling')
  @read('Current polling intervals and recent committed changes')
  polling(@Req() req: Request) {
    return this.result(req, 'polling_config', this.service.polling());
  }
  @Put('config/polling')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Settings durably stored; workers observe between iterations',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Stale revision or conflicting request key',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['expected_revision', 'capture_poll_ms', 'backfill_idle_ms'],
      properties: {
        expected_revision: decimal,
        capture_poll_ms: { type: 'integer', minimum: 50, maximum: 30000 },
        backfill_idle_ms: { type: 'integer', minimum: 50, maximum: 30000 },
      },
    },
  })
  setPolling(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'polling_update', (key) =>
      this.service.setPolling(key, req.requestId, body),
    );
  }
  @Get('config')
  @ApiResponse({ status: 200, schema: responseSchema('config') })
  config(@Req() req: Request) {
    return this.result(req, 'config', this.service.config());
  }
  @Post('failures/elasticsearch/:eventId/replay')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiParam({ name: 'eventId', schema: { type: 'string', maxLength: 80 } })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'ES replay durably scheduled',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Stale attempt or key conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['attempt_id', 'destination_id', 'generation', 'reason'],
      properties: {
        attempt_id: uuidSchema,
        destination_id: uuidSchema,
        generation: decimal,
        reason: { type: 'string', minLength: 1, maxLength: 128 },
      },
    },
  })
  replay(
    @Param('eventId') event: string,
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'es_replay', (key) =>
      this.service.replay(key, req.requestId, event, body),
    );
  }
  @Post('simulations/source-change')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Source command committed',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Fixture or key conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['fixture', 'operation', 'value'],
      properties: {
        fixture: { type: 'string', pattern: '^fixture-(0[1-9]|1[0-6])$' },
        operation: {
          type: 'string',
          enum: ['create', 'update', 'delete', 'restore'],
        },
        value: { type: 'integer', minimum: 0, maximum: 1000000 },
      },
    },
  })
  source(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'source_change', (key) =>
      this.service.source(key, req.requestId, body),
    );
  }
  @Post('simulations/corrupt-record')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    description: 'Source-valid corrupt mapped field committed',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Fixture or key conflict',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['fixture'],
      properties: {
        fixture: { type: 'string', pattern: '^fixture-(0[1-9]|1[0-6])$' },
      },
    },
  })
  corrupt(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'corrupt_record', (key) =>
      this.service.source(key, req.requestId, body, true),
    );
  }
  @Put('simulations/network/elasticsearch')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Configured proxy state observed',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Pending or conflicting request',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['state'],
      properties: {
        state: { type: 'string', enum: ['connected', 'disconnected'] },
      },
    },
  })
  esNetwork(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'network_elasticsearch', (key) =>
      this.service.network(key, req.requestId, 'elasticsearch', body),
    );
  }
  @Put('simulations/network/rabbitmq')
  @ApiSecurity('operator')
  @ApiHeader({ name: 'Idempotency-Key', required: true, schema: uuidSchema })
  @ApiResponse({
    status: 202,
    schema: mutationSchema,
    description: 'Configured proxy state observed',
  })
  @ApiResponse({ status: 200, schema: mutationSchema })
  @ApiResponse({
    status: 409,
    schema: errorSchema,
    description: 'Pending or conflicting request',
  })
  @ApiResponse({
    status: 401,
    schema: errorSchema,
    description: 'Operator token required',
  })
  @ApiBody({
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['state'],
      properties: {
        state: { type: 'string', enum: ['connected', 'disconnected'] },
      },
    },
  })
  rabbitNetwork(
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.mutate(req, res, 'network_rabbitmq', (key) =>
      this.service.network(key, req.requestId, 'rabbitmq', body),
    );
  }
  @Get('simulations')
  @ApiResponse({ status: 200, schema: responseSchema('simulations') })
  simulations(@Req() req: Request) {
    return this.result(req, 'simulations', this.service.simulations());
  }
}
@Controller()
export class MetricsController {
  constructor(
    @Inject(OperationsService) private readonly service: OperationsService,
  ) {}
  @Get('metrics')
  @ApiResponse({ status: 200, description: 'Prometheus text 0.0.4' })
  async metrics(@Req() req: Request, @Res() res: Response) {
    req.operation = 'metrics';
    res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    res.send(metrics(await this.service.status()));
  }
}
