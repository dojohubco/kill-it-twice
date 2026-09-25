# ADR 017: Transactional observation projections

Status: selected for an isolated prototype; implementation and server acceptance pending.

## Evidence and problem

The accepted million-record run recorded 505 nonfresh pipeline observations. A later server diagnostic reproduced three timeouts at the existing eight-second run-observation limit. Its synthetic, deliberately churned relations are not the original runtime data: the diagnostic plan read 789,899 shared blocks and 54,093 temporary blocks, taking 22.204 seconds with two parallel workers. A larger per-operation memory setting exhausted the diagnostic container's shared-memory allowance. Neither result identifies the original run's exact plan retrospectively.

Operational reads need event identity, state and a few timestamps. They currently visit relations containing canonical bodies, delivery evidence and consumer receipt bytes. Wider mutable rows and their obsolete versions make these reads compete with the processing they observe.

## Decision to verify

Keep narrow, transactionally maintained PostgreSQL projections of event metadata, delivery state and consumer-observation state. They contain one row per existing event/obligation, not aggregate counters or a separately refreshed cache. Original relations, constraints and evidence remain authoritative.

- Forward migration copies retained rows and installs maintenance triggers while holding write-conflicting locks on the original tables, all in one transaction. A concurrent writer cannot commit between the initial copy and trigger installation.
- Each original insert or relevant state transition updates its projection in the same transaction. A failed projection update aborts the original transaction; rollback and crash cannot leave a committed original change with an uncommitted mirror. No network call or second database is added to that transaction.
- Separate delivery and receipt rows retain the original locking identities. There is no shared aggregate-counter row serializing unrelated records. State-only updates can reuse space in the narrow relation without rewriting large receipt content.
- Runtime writer/operator roles receive no direct projection write privilege. Trigger functions use fixed table names and a fixed search path. Operators may read the same metadata already exposed by the existing contract.
- Snapshot and per-run reads retain their exact grouping, joins, run filter and missing/pending partitions. Missing observations remain pending; no cached value substitutes for unavailable current data. Keep the existing 2.5-second/8-second read limits and container limits.
- Terminal backfill validation and the independent content oracle continue reading the original evidence. A projection count cannot authorize completion, a broker ACK or a consumer effect.

This changes physical read organization, not I1–I10 or the meaning of a processed receipt. The prototype must demonstrate useful read savings before the runtime migration is adopted.

## Alternatives and tradeoffs

Covering indexes already exist, but updated rows can still require heap visits. Increasing memory consumes resources per operation and per concurrent reader; the measured resource failure rules out treating that as the correction. Raising timeouts or presenting stale results as fresh would hide the problem. Transactional aggregate counters would make reads smaller but add shared write locks and more difficult run-membership concurrency. Asynchronous refresh would need a separate, visible freshness contract.

Narrow projections add disk space, migration work and local writes. They still require bounded aggregate scans; they are not a claim of constant-time reads at arbitrary scale. Measure write overhead and concurrent reader latency under the existing resource limits. Do not assert a twofold pipeline speedup from a query microbenchmark.

## Required evidence

1. Preserve the failed server diagnostics and compare the same exact aggregates against churned projected rows with unchanged read limits.
2. Verify fresh install and a populated upgrade, exact old/new metadata equality, all supported state/missing-row combinations, rollback, uncommitted visibility, concurrent writers and denied direct writes.
3. Preserve original canonical rows, function ownership/grants and terminal validation. Check projection equality independently against original relations in one read snapshot.
4. Run real faults, exact large-data reconciliation, negative oracle controls and cleanup on clean committed code. Record availability and latency throughout active processing, not just at the final G5 sample.
