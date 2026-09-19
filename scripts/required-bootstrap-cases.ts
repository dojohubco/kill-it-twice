// Independent M5A inventory committed with the design, before implementation/execution.
const cases = [
  [
    'BS01',
    'fresh multi-chunk baselines have exact recipe and no mutation effects',
  ],
  ['BS02', 'manifest and chunk replay serialize without duplicate identities'],
  ['BS03', 'seed chunk pre-COMMIT SIGKILL leaves no partial progress'],
  ['BS03H', 'healthy seed chunk pre-COMMIT release'],
  ['BS04', 'seed chunk post-COMMIT SIGKILL recovers original mapping'],
  ['BS04H', 'healthy seed chunk post-COMMIT release'],
  ['BS05', 'sealing and activation serialize closed ordinary writers'],
  ['BS06', 'activation pre-COMMIT SIGKILL retains closed source'],
  ['BS06H', 'healthy activation pre-COMMIT release'],
  ['BS06A', 'activation post-COMMIT SIGKILL retains active binding'],
  ['BS06AH', 'healthy activation post-COMMIT release'],
  ['BS07', 'baseline no-op command retains exact historical result'],
  ['BS08', 'baseline lifecycle advances only through captured mutations'],
  ['BS09', 'bounded baseline and current readers preserve revision kind'],
  ['BS10', 'selected genuine baselines reach both sinks without effects'],
  ['BS11', 'higher mutation and tombstone before baseline never regress'],
  [
    'BS12',
    'bootstrap privileges and conditional history integrity fail closed',
  ],
  ['BS14', 'retained restart reconciliation detects baseline corruption'],
];
export const bootstrapCases = cases.map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid bootstrap inventory');
  return {
    id,
    name: `${id} ${label}`,
    file: 'tests/bootstrap/bootstrap.test.ts',
  };
});
export const bootstrapUpgradeCases = [
  {
    id: 'BS13',
    name: 'BS13 populated M4.1 upgrade preserves active history',
    file: 'tests/bootstrap/upgrade.test.ts',
  },
];
