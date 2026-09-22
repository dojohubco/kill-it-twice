// Bounded local transaction groups; not a receiver-delivery guarantee.
export function orderedSinkBatch<T>(
  items: readonly T[],
  identity: (item: T) => string,
  maximum = 32,
): T[] {
  if (
    !Number.isInteger(maximum) ||
    maximum < 1 ||
    maximum > 32 ||
    items.length < 1 ||
    items.length > maximum
  )
    throw new RangeError('Invalid bounded sink database batch');
  const ids = items.map(identity);
  if (
    ids.some((id) => typeof id !== 'string' || !id.length || id.length > 100) ||
    new Set(ids).size !== ids.length
  )
    throw new TypeError('Duplicate or invalid sink batch identity');
  return [...items].sort((a, b) =>
    identity(a) < identity(b) ? -1 : identity(a) > identity(b) ? 1 : 0,
  );
}
export function sinkChunks<T>(items: readonly T[], size: number): T[][] {
  if (!Number.isInteger(size) || size < 1 || size > 32)
    throw new RangeError('Invalid sink chunk size');
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size)
    chunks.push(items.slice(i, i + size));
  return chunks;
}
export function sinkPollDelay(
  claimed: number,
  batching: number,
  idle: number,
): number {
  return claimed > 0 && batching > 1 ? 10 : idle;
}
