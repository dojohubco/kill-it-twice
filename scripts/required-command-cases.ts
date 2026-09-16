// Independent M2A inventory, recorded before implementation/execution.
export const commandCases = [
  ['C01', 'C01 command lifecycle retains exact immutable results'],
  ['C02', 'C02 normalized sequential replay executes once'],
  ['C03', 'C03 conflicting requests and wrong epoch cannot mutate'],
  ['C04', 'C04 historical replay precedes lifecycle evaluation'],
  ['C05', 'C05 no-op commands retain receipts without revisions'],
  ['C06', 'C06 concurrent duplicates wait and replay the committed winner'],
  ['C07', 'C07 concurrent mismatch rejects after the winner commits'],
  ['C08', 'C08 waiting duplicate executes after winner rollback'],
  ['C11', 'C11 SQL failure atomically removes the command reservation'],
  ['C12', 'C12 command privileges and receipt completion are enforced'],
].map(([id, name]) => {
  if (!id || !name) throw new Error('Invalid command case inventory');
  return { id, name, file: 'tests/commands/commands.test.ts' };
});
export const commandFaultCases = [
  ['C09', 'C09 real pre-COMMIT command SIGKILL permits same-key retry'],
  ['C10', 'C10 real post-COMMIT command SIGKILL recovers original result'],
  ['C09-HEALTHY', 'C09 healthy pre-COMMIT command release succeeds once'],
  ['C10-HEALTHY', 'C10 healthy post-COMMIT command release succeeds once'],
].map(([id, name]) => {
  if (!id || !name) throw new Error('Invalid command fault inventory');
  return { id, name, file: 'tests/commands/command-death.test.ts' };
});
