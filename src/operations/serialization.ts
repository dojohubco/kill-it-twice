import { isLosslessNumber } from 'lossless-json';
// Node 24 implements rawJSON; TypeScript 5.9 does not yet declare this API.
// Keep exact ES numeric tokens as JSON numbers, never expose parser wrapper objects.
const exactJSON = JSON as typeof JSON & { rawJSON(text: string): unknown };
export function exactJsonReplacer(_key: string, value: unknown): unknown {
  return isLosslessNumber(value) ? exactJSON.rawJSON(value.value) : value;
}
