// Independent acceptance inventory. Update deliberately when the contract changes; never derive at runtime from discovered tests.
export const requiredCases = [
  {
    "id": "PG",
    "name": "infrastructure: real PostgreSQL 18.6 connection with durable settings",
    "file": "tests/integration/smoke.test.ts"
  },
  {
    "id": "T01",
    "name": "T01 mutation lifecycle, exact BIGINT boundaries, no-op, and two committed revisions",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T02",
    "name": "T02 rollback after executed single, multiple, and inserted source revisions",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T03",
    "name": "T03 real narrowly scoped outbox INSERT error rolls back the source mutation",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T04",
    "name": "T04 concurrent writers wait on an observed row lock and create successive revisions",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T05",
    "name": "T05 historical after-images stay unchanged and evidence cannot be edited or deleted",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T06",
    "name": "T06 actual runtime login, owners, grants, protected metadata and unsupported SQL rejection",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T07",
    "name": "T07 known allocation-order risk: B commits first, late A remains queryable by identity",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "T10-SOURCE",
    "name": "T10 source tests leave no runtime database sessions or instrumentation",
    "file": "tests/integration/source.test.ts"
  },
  {
    "id": "M11-F01",
    "name": "M11-F01 caught SQL failure cannot turn rollback into caller success",
    "file": "tests/integration/transactions.test.ts"
  },
  {
    "id": "M11-F01-TAG",
    "name": "M11-F01-TAG actual COMMIT ROLLBACK tag is rejected by the owner",
    "file": "tests/integration/transactions.test.ts"
  },
  {
    "id": "M11-F02",
    "name": "M11-F02 nested and concurrent owners reject before SQL and outer rollback remains true",
    "file": "tests/integration/transactions.test.ts"
  },
  {
    "id": "M11-LIFETIME",
    "name": "M11-LIFETIME success, rollback, expired capability and sequential owner reuse",
    "file": "tests/integration/transactions.test.ts"
  },
  {
    "id": "M11-DEADLOCK",
    "name": "M11-DEADLOCK real 40P01 victim loses both mutations and survivor retains correct revisions",
    "file": "tests/integration/transactions.test.ts"
  },
  {
    "id": "M11-OBSERVE",
    "name": "M11-OBSERVE timeout disposes a real in-flight PostgreSQL query and session",
    "file": "tests/integration/wait.test.ts"
  },
  {
    "id": "T08",
    "name": "T08 actual pre-COMMIT SIGKILL rolls back source and outbox, then a fresh writer progresses",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "T09",
    "name": "T09 actual post-COMMIT SIGKILL retains both rows with caller outcome unknown",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "T10-FAULT",
    "name": "T10 fault writers were reaped and their database sessions ended",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "M11-HEALTHY-PRE",
    "name": "M11-HEALTHY-PRE ordinary release exits zero once and commits both rows",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "M11-ORPHAN-PRE",
    "name": "M11-ORPHAN-PRE parent loss fails and closes the owned session",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "M11-HEALTHY-POST",
    "name": "M11-HEALTHY-POST ordinary release exits zero once and commits both rows",
    "file": "tests/integration/writer-death.test.ts"
  },
  {
    "id": "M11-ORPHAN-POST",
    "name": "M11-ORPHAN-POST parent loss fails and closes the owned session",
    "file": "tests/integration/writer-death.test.ts"
  }
] as const;
