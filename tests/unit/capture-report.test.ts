import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { test } from 'node:test';
import { writeCaptureReport } from '../../src/internal/capture-report.ts';
import { waitFor } from '../../scripts/support.ts';
void test('capture progress output waits for the real write callback', async () => {
  let complete: (() => void) | undefined,
    bytes = '';
  const output = new Writable({
    write(chunk: Buffer, _encoding, callback) {
      bytes += chunk.toString('utf8');
      complete = callback;
    },
  });
  let finished = false;
  const pending = writeCaptureReport(output, { state: 'pending' }).then(() => {
    finished = true;
  });
  assert.equal(finished, false);
  assert.equal(bytes, '{"state":"pending"}\n');
  assert.ok(complete);
  complete();
  await pending;
  assert.equal(finished, true);
  output.destroy();
});
void test('capture progress disposes never-draining output at its deadline', async () => {
  const output = new Writable({
    write() {
      /* Deliberately stalled real Writable callback, no database fixture. */
    },
  });
  let closed = false;
  output.once('close', () => {
    closed = true;
  });
  await assert.rejects(
    writeCaptureReport(output, { state: 'pending' }, 15),
    /output deadline/,
  );
  await waitFor(() => closed, Boolean, 'owned report stream closed', 1000);
  assert.equal(output.destroyed, true);
});
void test('capture progress preserves write failure and handles its later error event', async () => {
  const expected = new Error('controlled output failure');
  const output = new Writable({
    write(_chunk, _encoding, callback) {
      callback(expected);
    },
  });
  let closed = false;
  output.once('close', () => {
    closed = true;
  });
  await assert.rejects(
    writeCaptureReport(output, { state: 'pending' }),
    (error) => error === expected,
  );
  await waitFor(() => closed, Boolean, 'failed report stream closed', 1000);
  assert.equal(output.listenerCount('error'), 0);
});
void test('capture progress rejects over-limit output before writing', async () => {
  let wrote = false;
  const output = new Writable({
    write(_chunk, _encoding, callback) {
      wrote = true;
      callback();
    },
  });
  await assert.rejects(
    writeCaptureReport(output, { oversized: 'x'.repeat(65536) }),
    /64 KiB/,
  );
  assert.equal(wrote, false);
  output.destroy();
});
