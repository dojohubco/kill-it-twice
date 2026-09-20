// BF01-BF17 and healthy controls were committed before production implementation.
// BF01E splits BF01's empty/legacy requirement into a separate executable profile;
// that split and its assertions were recorded before the profile's first execution.
const cases = [
  ['BF01', 'run identity and exact fixed range coverage'],
  ['BF02', 'page staging membership and checkpoint roll back atomically'],
  ['BF03', 'real page pre-COMMIT SIGKILL retains durable cursor'],
  ['BF03H', 'healthy page pre-COMMIT release'],
  ['BF04', 'real page post-COMMIT SIGKILL recovers immutable batch'],
  ['BF04H', 'healthy page post-COMMIT release'],
  [
    'BF05',
    'independent scanners and stale generations cannot overwrite progress',
  ],
  ['BF06', 'current-state scan races preserve baseline and mutation content'],
  ['BF07', 'late source allocation behind cursor is captured automatically'],
  ['BF08', 'finished scan remains draining until independent receivers settle'],
  ['BF09', 'durable pause and resume preserve in-flight page progress'],
  [
    'BF10',
    'fence snapshot retains exact acknowledged and unacknowledged mutations',
  ],
  ['BF11', 'real fence pre-COMMIT SIGKILL leaves no partial cut'],
  ['BF11H', 'healthy fence pre-COMMIT release'],
  ['BF11A', 'real fence post-COMMIT SIGKILL reuses committed cut'],
  ['BF11AH', 'healthy fence post-COMMIT release'],
  ['BF12', 'real import COMMIT crash resumes exact fence membership'],
  ['BF12H', 'healthy fence import release'],
  [
    'BF13',
    'terminal errors and missing evidence cannot become successful completion',
  ],
  ['BF14', 'post-fence activity leaves the finite required set unchanged'],
  ['BF15', 'independent baseline mutation fence and receiver reconciliation'],
  ['BF17', 'bounded prefix pages and oversized blockage preserve coverage'],
];
export const backfillCases = cases.map(([id, label]) => {
  if (!id || !label) throw new Error('Invalid backfill inventory');
  return {
    id,
    name: `${id} ${label}`,
    file: 'tests/backfill/backfill.test.ts',
  };
});
export const backfillUpgradeCases = [
  {
    id: 'BF16',
    name: 'BF16 populated M5A upgrade permissions and retained restart',
    file: 'tests/backfill/upgrade.test.ts',
  },
];
export const backfillEmptyCases = [
  {
    id: 'BF01E',
    name: 'BF01E empty and populated legacy-active epochs retain exact run identity',
    file: 'tests/backfill/empty.test.ts',
  },
];
