// Independent required inventory. Never derived from discovered test results.
export const stagingCases = [
  {
    id: 'S01-GOLDEN',
    name: 'S01-GOLDEN exact flat-envelope RFC8785 vectors',
    file: 'tests/unit/envelope.test.ts',
  },
  {
    id: 'S01-REJECT',
    name: 'S01-REJECT malformed envelope bytes, identities, timestamps and Unicode',
    file: 'tests/unit/envelope.test.ts',
  },
  {
    id: 'S02',
    name: 'S02 real precision and coherent current/outbox parity',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S03',
    name: 'S03 historical outbox revisions stage independently out of order',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S04',
    name: 'S04 initial staging commits one complete immutable obligation set',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S05',
    name: 'S05 batch repeated and concurrent duplicates preserve original evidence',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S06',
    name: 'S06 conflicts roll back batches and never repair corrupt evidence',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S07',
    name: 'S07 SQL constraints and actual restricted credentials enforce the boundary',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S11',
    name: 'S11 selected uncommitted revisions remain invisible until source commit',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S12',
    name: 'S12 bounded transfers and several explicit pages retain all source records',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S13',
    name: 'S13 destinations remain unbound and every obligation remains pending',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S-UPGRADE',
    name: 'S-UPGRADE forward reader migration preserves the selected installation fixture',
    file: 'tests/staging/staging.test.ts',
  },
  {
    id: 'S08',
    name: 'S08 real pre-COMMIT pipeline SIGKILL rolls back every obligation',
    file: 'tests/staging/staging-death.test.ts',
  },
  {
    id: 'S09',
    name: 'S09 real post-COMMIT pipeline SIGKILL recovers from durable evidence',
    file: 'tests/staging/staging-death.test.ts',
  },
  {
    id: 'S10-PRE',
    name: 'S10-PRE healthy staging release reports committed success once',
    file: 'tests/staging/staging-death.test.ts',
  },
  {
    id: 'S10-POST',
    name: 'S10-POST healthy staging release reports committed success once',
    file: 'tests/staging/staging-death.test.ts',
  },
] as const;
