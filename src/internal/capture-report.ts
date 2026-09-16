import type { Writable } from 'node:stream';
// Capture replies contain bounded metadata, never payloads. Admit one write at a time.
// On a blocked/broken output, dispose that stream and fail; do not grow a hidden log queue.
export async function writeCaptureReport(
  stream: Writable,
  value: unknown,
  timeoutMs = 5000,
): Promise<void> {
  const line = Buffer.from(JSON.stringify(value) + '\n');
  if (line.length > 65536) throw new Error('Capture report exceeds 64 KiB');
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) {
        // Writable can call its write callback before emitting the same error.
        // Keep the listener until destruction closes the stream, handling late notification.
        stream.once('close', () => stream.off('error', onError));
        stream.destroy();
        reject(error);
      } else {
        stream.off('error', onError);
        resolve();
      }
    };
    const onError = (error: Error) => finish(error);
    stream.on('error', onError);
    const timer = setTimeout(
      () => finish(new Error('Capture report output deadline exceeded')),
      timeoutMs,
    );
    try {
      stream.write(line, finish);
    } catch (error) {
      finish(
        error instanceof Error
          ? error
          : new Error('Capture report output failed', { cause: error }),
      );
    }
  });
}
