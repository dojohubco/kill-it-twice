# ADR 005: one owner and one level for source transactions

Status: accepted for M1.1 implementation; acceptance pending.
Origin: external adversarial review, followed by real PostgreSQL reproductions.

## Evidence and gap

At reviewed f8c2e06 the helper accepted arbitrary pg.Client instances. Reproduction run `m1-20260916175333240-599fdf32` observed F01: SQLSTATE 22012 was caught by the callback, COMMIT returned ROLLBACK, the helper returned `incorrect-success`, and independent entity/outbox counts were both zero. F02 nested and concurrent invocations both returned an inner success, then reported outer `rolled_back` although the source and outbox rows persisted. The three-table trigger contract still held; the helper's outcome claims did not.

## Decision

A Source owns connection configuration, an explicit active guard, and each transaction's private pg.Client lifetime. Each sequential transaction opens and closes its own session. The same Source rejects nested/concurrent calls synchronously before connection or transaction SQL. This is not nested atomicity: callers needing one atomic unit must use the existing work capability. No arbitrary client, raw SQL or manual transaction control is exposed on that capability. Low-level SQL tests remain explicitly separate.

A capability expires when its callback finishes. It provides only create/mutate and a fixed diagnostic read of its source revision and session. Await every operation; concurrent or unawaited capability operations fail the transaction, are drained before completion, and cannot race COMMIT. Record the first operation failure even if callback code catches it. Never retry mutations.

Check BEGIN, COMMIT and ROLLBACK command tags. A COMMIT response tagged ROLLBACK is known rollback, never success. A failed or interrupted COMMIT exchange is unknown even if a subsequent ROLLBACK succeeds. Preserve the primary cause and SQLSTATE separately from cleanup errors and completion evidence. Report not_started for connect/setup failure before work, rolled_back only with rollback evidence, committed if COMMIT was confirmed but connection cleanup failed, and unknown when completion cannot be established. A failed cleanup or unknown completion poisons the owner; subsequent attempts require an explicitly new owner.

Connections are bounded by connect/query/server statement timeouts, have an idle error listener, and are always closed. BEGIN/setup failures also go through cleanup. A callback itself must cooperate and settle; this API does not claim to preempt arbitrary JavaScript. Process death leaves the caller without a result, and only independent database observations establish persisted state. No command-idempotency or retry contract is added.

## Validation and alternatives

Real regression tests cover caught SQL errors, ownership rejection, expired capabilities, sequential reuse, rollback and a barrier-driven two-entity deadlock without choosing the victim. Existing real kill tests will execute this production owner; private test instrumentation may pause client completion/cleanup without exposing a production failpoint. Pure transport-failure tests supplement, never replace, PostgreSQL evidence.

Savepoints, SQL parsing, undocumented driver internals and accepting caller-owned sessions are unnecessary for this single-level contract. Connection per transaction trades connection overhead for a small, explicit ownership boundary; pooling/performance are outside M1.1.

References: [pg result command tags](https://node-postgres.com/apis/result), [pg connection lifecycle](https://node-postgres.com/apis/client), [PostgreSQL COMMIT](https://www.postgresql.org/docs/18/sql-commit.html).
