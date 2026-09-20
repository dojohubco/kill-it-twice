import 'reflect-metadata';
import { exactJsonReplacer } from '../../src/operations/serialization.ts';
import { Module, Controller, Get, Inject, Req } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ExpressAdapter,
  type NestExpressApplication,
} from '@nestjs/platform-express';
import {
  DocumentBuilder,
  SwaggerModule,
  type OpenAPIObject,
} from '@nestjs/swagger';
import { OperationsService } from '../../src/operations/service.ts';
import type { OperationsConfig } from '../../src/operations/config.ts';
import {
  ControlController,
  MetricsController,
  SETTINGS,
  type Settings,
} from './controller.ts';
import { Errors, beginRequest, type Request } from './http.ts';
const SCHEMA = 'schema';
@Controller('api/v1')
class SchemaController {
  constructor(
    @Inject(SCHEMA) private readonly value: { document: OpenAPIObject | null },
  ) {}
  @Get('openapi.json') schema(@Req() req: Request) {
    req.operation = 'openapi';
    return this.value.document;
  }
}
@Module({})
class ControlModule {}
export async function createControlApi(
  config: OperationsConfig,
  settings: Partial<Settings> = {},
) {
  const schema: { document: OpenAPIObject | null } = { document: null };
  const app = await NestFactory.create<NestExpressApplication>(
    {
      module: ControlModule,
      controllers: [ControlController, MetricsController, SchemaController],
      providers: [
        { provide: OperationsService, useValue: new OperationsService(config) },
        { provide: SETTINGS, useValue: { token: config.token, ...settings } },
        { provide: SCHEMA, useValue: schema },
      ],
    },
    new ExpressAdapter(),
    { logger: false, bodyParser: false },
  );
  app.set('json replacer', exactJsonReplacer);
  app.use(beginRequest);
  app.useBodyParser('json', { limit: 8192, strict: true });
  app.useGlobalFilters(new Errors());
  schema.document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle('Kill It Twice operational control')
      .setVersion('1.0.0')
      .addSecurity('operator', { type: 'http', scheme: 'bearer' })
      .build(),
  );
  await app.init();
  return app;
}
