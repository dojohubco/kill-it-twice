import { relative } from 'node:path';
import type { TestEvent } from 'node:test/reporters';
import type { CaseResult } from './acceptance.ts';

export default async function* report(events: AsyncIterable<TestEvent>): AsyncGenerator<string> {
  const results: CaseResult[] = [];
  let summary: unknown;
  for await (const event of events) {
    if (event.type === 'test:pass' || event.type === 'test:fail') {
      results.push({ name: event.data.name, file: event.data.file ? relative(process.cwd(), event.data.file) : '', status: event.type === 'test:pass' ? 'pass' : 'fail', skip: event.data.skip !== undefined && event.data.skip !== false, todo: event.data.todo !== undefined && event.data.todo !== false, nesting: event.data.nesting });
    }
    if (event.type === 'test:summary' && event.data.file === undefined) summary = event.data;
  }
  yield JSON.stringify({ format: 1, results, summary }) + '\n';
}
