// Deliberate inventory committed with the M2C design, before executable tests.
export const captureCases = [
  {
    id: 'IC01',
    name: 'IC01 atomic fresh and populated capture initialization',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC02',
    name: 'IC02 complete immutable transfer and staged acknowledgement',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC03',
    name: 'IC03 automatic selection captures late commits without a watermark',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC04',
    name: 'IC04 independent concurrent claimers own disjoint work',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC05',
    name: 'IC05 source clock renewal expiry and generation reject stale transitions',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC06',
    name: 'IC06 delayed old worker cannot overwrite a newer terminal acknowledgement',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC07',
    name: 'IC07 real kill after source claim commit recovers',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC08',
    name: 'IC08 real kill before pipeline commit leaves no partial stage or acknowledgement',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC09',
    name: 'IC09 real kill after pipeline commit restages before acknowledgement',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC10',
    name: 'IC10 real kill after source acknowledgement preserves the terminal receipt',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC11-CLAIM',
    name: 'IC11-CLAIM healthy release returns one committed capture result',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC11-PRE',
    name: 'IC11-PRE healthy release returns one committed capture result',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC11-POST',
    name: 'IC11-POST healthy release returns one committed capture result',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC11-ACK',
    name: 'IC11-ACK healthy release returns one committed capture result',
    file: 'tests/capture/capture-death.test.ts',
  },
  {
    id: 'IC12',
    name: 'IC12 actual pipeline outage persists delay and recovers automatically',
    file: 'tests/capture/capture-process.test.ts',
  },
  {
    id: 'IC13',
    name: 'IC13 pipeline instance identity is validated inside staging',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC14',
    name: 'IC14 bounded medium batches and oversized work preserve fairness',
    file: 'tests/capture/capture.test.ts',
  },
  {
    id: 'IC15',
    name: 'IC15 two capture processes reconcile finite workload kills and new work after idle',
    file: 'tests/capture/capture-process.test.ts',
  },
  {
    id: 'IC16',
    name: 'IC16 restricted roles immutable history and frozen obligations',
    file: 'tests/capture/capture.test.ts',
  },
] as const;
