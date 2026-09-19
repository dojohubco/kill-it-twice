// Only the complete, explicitly named gates receive the longer bounded deadline.
export function ciTimeout(executable: string, args: readonly string[]): number {
  if (executable !== 'make' || args.length !== 1) return 600000;
  if (args[0] === 'verify-m5a') return 3600000;
  return ['verify-m3', 'verify-m4', 'verify-m4-1'].includes(args[0] ?? '')
    ? 3000000
    : 600000;
}
