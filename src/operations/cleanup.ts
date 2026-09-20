export class OperationalCleanupError extends Error {
  readonly primary: unknown;
  readonly cleanup: readonly unknown[];
  constructor(primary: unknown, cleanup: readonly unknown[]) {
    super('Operational cleanup failed');
    this.primary = primary;
    this.cleanup = cleanup;
  }
}
export async function withOperationalCleanup<T>(
  work: () => Promise<T>,
  cleanup: () => Promise<void>,
): Promise<T> {
  let failure: { error: unknown } | undefined;
  let result: T | undefined;
  try {
    result = await work();
  } catch (error) {
    failure = { error };
  }
  try {
    await cleanup();
  } catch (error) {
    throw new OperationalCleanupError(failure?.error, [error]);
  }
  if (failure) throw failure.error;
  return result as T;
}
