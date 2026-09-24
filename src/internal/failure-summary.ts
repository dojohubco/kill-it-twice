import pg from 'pg';
import { TransactionError } from './transaction.ts';

// Diagnostics only: never emit messages, SQL, parameters, payloads or credentials.
// Traverse actual causes and cleanup failures with a fixed total/depth bound.
export function failureSummary(error: unknown) {
  const seen = new Set<unknown>();
  const nodes: Record<string, unknown>[] = [];
  const visit = (value: unknown, relation: string, depth: number) => {
    if (nodes.length >= 12 || depth > 4 || seen.has(value)) return;
    seen.add(value);
    const node: Record<string, unknown> = { relation };
    nodes.push(node);
    if (value instanceof TransactionError) {
      Object.assign(node, {
        type: 'transaction',
        phase: value.phase,
        outcome: value.outcome,
        sqlState: value.sqlState,
        completionTag: value.completionTag,
      });
      visit(value.cause, 'cause', depth + 1);
      for (const cleanup of value.cleanupErrors)
        visit(cleanup, 'cleanup', depth + 1);
    } else if (value instanceof AggregateError) {
      node['type'] = 'aggregate';
      for (const member of value.errors as unknown[])
        visit(member, 'member', depth + 1);
    } else if (value instanceof pg.DatabaseError) {
      node['type'] = 'database';
      if (value.code && /^[A-Z0-9]{5}$/.test(value.code))
        node['sqlState'] = value.code;
    } else {
      node['type'] = value instanceof Error ? 'error' : 'unknown';
      if (value instanceof Error && value.cause !== undefined)
        visit(value.cause, 'cause', depth + 1);
    }
    if (value instanceof Error) {
      // Keep only repository code locations from frames, never the first message line.
      node['locations'] = (value.stack?.split('\n').slice(1) ?? [])
        .flatMap(
          (line) =>
            line.match(/(?:src|scripts)\/[a-zA-Z0-9_./-]+\.ts:\d+:\d+/g) ?? [],
        )
        .slice(0, 6);
    }
  };
  visit(error, 'primary', 0);
  return nodes;
}
