// Independent completion requirements; these supplement, never replace, OP01-OP18.
export const recoveryCases = [
  {
    id: 'RC01',
    name: 'RC01 Atomic bounded replay admission and current-failure idempotency withstand concurrent keys.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC02',
    name: 'RC02 Correlated replay attempts and repeated errors remain immutable without cross-sink changes.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC03',
    name: 'RC03 Real higher remote/local witness resolves old obligation; invalid witnesses fail closed.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC04',
    name: 'RC04 Same-target verification is audited fenced and cannot adopt invalid credentials or identity.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC05',
    name: 'RC05 Original completed-with-errors run stays historical while present recovery improves.',
    file: 'tests/operational/upgrade.test.ts',
  },
  {
    id: 'RC06',
    name: 'RC06 Actual replay admission COMMIT SIGKILL boundaries and healthy controls preserve idempotency.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC07',
    name: 'RC07 Bounded lists and actual operator grants retain navigation and reject unsupported mutations.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC08',
    name: 'RC08 Current and last-known snapshots and useful rates match independent database evidence.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'RC09',
    name: 'RC09 Populated forward recovery installation preserves prior immutable audit and outcome evidence.',
    file: 'tests/operational/upgrade.test.ts',
  },
];
