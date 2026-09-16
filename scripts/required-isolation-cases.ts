// Historical counterexample only. Never part of corrected acceptance.
export const reproductionCases = [
  {
    id: 'B01',
    name: 'B01 old transaction snapshot commits retained outbox without capture work before correction',
    file: 'tests/capture/isolation-reproduction.test.ts',
  },
] as const;

// These execute only on an unregistered installation; registered upgrades have no fake reset.
export const registrationCases = [
  {
    id: 'R01',
    name: 'R01 old repeatable-read snapshot is rejected before invisible binding lookup',
    file: 'tests/capture/isolation-registration.test.ts',
  },
  {
    id: 'R02',
    name: 'R02 long-lived read-committed writer enqueues and captures after registration',
    file: 'tests/capture/isolation-registration.test.ts',
  },
  {
    id: 'R03',
    name: 'R03 both real registration lock controls retain exact work and acknowledgements',
    file: 'tests/capture/isolation-registration.test.ts',
  },
  {
    id: 'R05',
    name: 'R05 unsupported pre-registration mutations reject while read-committed work is retained',
    file: 'tests/capture/isolation-registration.test.ts',
  },
] as const;
export const isolationCases = [
  {
    id: 'R04',
    name: 'R04 legacy mutation isolation matrix rejects real state changes and permits no-ops',
    file: 'tests/capture/isolation.test.ts',
  },
  {
    id: 'R06',
    name: 'R06 command receipts and owned capture transitions retain their guarantees',
    file: 'tests/capture/isolation.test.ts',
  },
  {
    id: 'R07',
    name: 'R07 forward guard migration preserves all registered or unregistered data and identity',
    file: 'tests/capture/isolation.test.ts',
  },
  {
    id: 'R08',
    name: 'R08 runtime roles cannot replace or bypass the common enqueue guard',
    file: 'tests/capture/isolation.test.ts',
  },
] as const;
