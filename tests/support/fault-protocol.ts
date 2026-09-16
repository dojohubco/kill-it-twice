import type { SourceRow } from '../../src/source.ts';

export type BarrierName = 'source.after_mutation.before_commit' | 'source.after_commit.before_caller_success';
export interface StartWriter {
  type: 'start';
  runId: string;
  barrier: BarrierName;
  payloadJson: string;
  port: number;
  password: string;
  applicationName: string;
}
export interface BarrierTelemetry {
  type: 'barrier';
  name: BarrierName;
  runId: string;
  writerPid: number;
  backendPid: number;
  transactionId: string;
  applicationName: string;
  sessionUser: string;
  effectiveUser: string;
  entity: SourceRow;
  outbox: Record<string, unknown>[];
}

export async function deadline<T>(promise: Promise<T>, label: string, milliseconds = 15_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Private fault harness deadline: ${label}`)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}
