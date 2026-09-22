import { limits } from '../limits.ts';

/** Explicit record tuning; canonical batch/record byte limits are unchanged. */
export function backfillPageRecords(value: unknown = limits.records): number {
  if (
    typeof value !== 'number' ||
    !Number.isInteger(value) ||
    value < 1 ||
    value > 64
  )
    throw new Error('Backfill page record bound must be an integer in 1..64');
  return value;
}

export function runtimePageRecords(value: string | undefined): number {
  if (value === undefined) return limits.records;
  if (!/^[1-9][0-9]?$/.test(value))
    throw new Error('Invalid backfill page record setting');
  return backfillPageRecords(Number(value));
}
