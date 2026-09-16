// Conservative starter configuration; no throughput or capacity claim.
export const limits = Object.freeze({
  records: 16,
  recordBytes: 64 * 1024,
  batchBytes: 256 * 1024,
});
export class LimitError extends Error {}
export function countBound(count: number): void {
  if (count < 1 || count > limits.records)
    throw new LimitError(`Selection count must be 1..${limits.records}`);
}
export function byteBound(sizes: readonly number[]): void {
  if (sizes.some((size) => size > limits.recordBytes))
    throw new LimitError(
      `Record exceeds ${limits.recordBytes} serialized bytes`,
    );
  if (sizes.reduce((sum, size) => sum + size, 0) > limits.batchBytes)
    throw new LimitError(`Batch exceeds ${limits.batchBytes} serialized bytes`);
}
