// Explicit bounded selection only. No watermark, poller, retry, source acknowledgement or sink call.
import { SourceReader } from '../src/source-reader.ts';
import { Pipeline, stageSelected } from '../src/pipeline.ts';
function required(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}
function config(prefix: string, database: string, user: string) {
  return {
    host: required(`${prefix}_HOST`),
    port: Number(required(`${prefix}_PORT`)),
    database,
    user,
    password: required(`${prefix}_PASSWORD`),
    application_name: 'm2b-explicit-stage',
  };
}
const keys = process.argv.slice(2).map((arg) => {
  const parts = arg.split(':');
  if (parts.length !== 2 || !parts[0] || !parts[1])
    throw new Error('Expected entity-id:version selections');
  return { entityId: parts[0], version: parts[1] };
});
const reader = new SourceReader(
  config('SOURCE_READER', 'source_m1', 'source_reader'),
  required('SOURCE_EPOCH'),
);
const pipeline = new Pipeline(
  config('PIPELINE_STAGER', 'pipeline_m2b', 'pipeline_stager'),
);
console.log(JSON.stringify(await stageSelected(reader, pipeline, keys)));
