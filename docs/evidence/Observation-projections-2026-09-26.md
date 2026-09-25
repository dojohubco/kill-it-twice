# Transactional observation projection checks — 2026-09-26

Status: scoped server checks PASS; corrected full server acceptance pending. [Selected evidence](Observation-projections-2026-09-26.json) identifies the exact migration and query files. These checks used a development overlay on `690447c`; they are not a clean full-application acceptance run.

## Measured query behavior

The isolated PostgreSQL fixture contained one million run members, two million delivery obligations and one million consumer observations. It retained the 1 GiB memory, 256 MiB shared-memory and two-CPU limits. Autovacuum was disabled **only on the synthetic fixture relations**, followed by deliberate state churn; this is not the original runtime dataset or its missing historical execution plan.

The original run aggregate timed out in all three reads at the unchanged 8-second statement limit. The same aggregate over narrow state projections returned the exact expected million-member partitions in 5.793, 3.757 and 3.519 seconds, including client invocation. The 32-member and empty runs also returned exact counts. The separate snapshot aggregate exceeded its original 2.5-second limit on the wide relations and returned in 0.985 seconds on the projections. Diagnostic EXPLAIN execution times for those snapshot queries were 7.802 and 0.898 seconds; diagnostic plans use a separate 45-second allowance and cannot confer acceptance at the shorter application limit.

The projected run plan still read 173,333 shared blocks and 54,019 temporary blocks. This reduces work; it does not make arbitrary-size observations constant-time. The two prototype tables occupied 643,629,056 and 227,164,160 bytes excluding indexes. Six million original state updates also required six million projection updates. Churn took 742.604 seconds, compared with 457.525 seconds in the earlier separate memory diagnostic; those runs are not a controlled application-throughput comparison. The added writes and storage remain a tradeoff to measure in the real workload.

The first projection attempt failed before seeding because its readiness probe saw PostgreSQL's temporary initialization server. It was preserved as FAIL with cleanup PASS. The diagnostic was corrected to wait for and use TCP. The earlier count timeouts and shared-memory failure remain failures; increasing memory was not adopted.

## Correctness and retained upgrade

Real PostgreSQL checks passed for 144 combinations of delivery/receipt states and missing rows, exact old/new snapshot JSON, fresh installation, populated copy, concurrent independent sink updates, uncommitted visibility, commit/rollback, insert/delete propagation and missing-mirror failure. Twelve actual operator write attempts were denied. The migration rejects a REPEATABLE READ installation transaction before creating its tables, so its retained copy uses the required post-lock READ COMMITTED snapshot.

A separate real retained runtime loaded and fully processed 4,096 baselines on the original image before migration. Original events, obligations, receipts, memberships, batches, checkpoints and run rows remained byte-identical. Function ownership, grants and the unchanged terminal validators were preserved. The new projection rows matched the original tables using symmetric `EXCEPT ALL`; all seven existing runtime/operator roles lacked direct write privilege on all three projections. Existing rollback-only invalid-membership/checkpoint/batch tests still rejected their deliberately damaged evidence. The independent source-to-receiver oracle passed for all 4,096 entities/events. Cleanup passed, all 28 preexisting server container IDs remained present, and no owned runtime resources remained.

The full migration additionally stores event timestamps and installs foreign keys; its performance is not identical to the two-table diagnostic. The retained upgrade demonstrated 4,096 rows, **not a populated million-row migration**. The normal initializer's 30-second statement limit remains unchanged, and a larger retained upgrade needs its own maintenance-window validation.

## Acceptance boundary

Original terminal validation and reconciliation still read the original relations. Projection counts cannot authorize source acknowledgment, consumer effects or backfill completion. The correction keeps the existing 2.5-second/8-second observation limits and all fault/oracle/resource budgets. Historical million-record PASS remains tied to `1f2f3f2`, including its 505 nonfresh pipeline observations. New full server acceptance must also pass the recorded G1 status/metrics availability check on the corrected clean code before this limitation can be called resolved for that workload.
