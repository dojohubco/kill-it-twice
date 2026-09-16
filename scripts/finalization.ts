import { errorText } from './support.ts';

// Retain the first failure and attempt every independent finalizer.
export class Diagnostics {
  primary: string | undefined;
  cleanup: { step: string; error: string }[] = [];
  #sanitize: (text: string) => string;
  constructor(sanitize: (text: string) => string) {
    this.#sanitize = sanitize;
  }
  fail(error: unknown): void {
    this.primary ??= this.#sanitize(errorText(error));
  }
  async finalize(step: string, action: () => Promise<unknown>): Promise<void> {
    try {
      await action();
    } catch (error) {
      this.cleanup.push({ step, error: this.#sanitize(errorText(error)) });
    }
  }
  get ok(): boolean {
    return this.primary === undefined && this.cleanup.length === 0;
  }
}
