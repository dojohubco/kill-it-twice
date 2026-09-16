# ADR 004: Convergent backfill with an explicit completion boundary

Status: accepted design; implementation verification pending.
Origin: pre-implementation architectural review.

## Context
The source remains writable during a large resumable scan. Scan completion alone does not establish sink or consumer completion. Continuous later writes must not prevent a finite run from finishing forever.

## Decision
Use bounded keyset scanning plus reliable incremental capture, promising eventual convergence rather than one long point-in-time snapshot. Commit page progress with its staged work. Preserve late committed changes through the outbox, independently of scan position.

The selected completion design seals a finite source-snapshot set of committed event identities and combines it with backfill-observed revisions. Successful completion requires the relevant sink obligations and consumer receipts, not a maximum sequence value or a heartbeat.

## Alternatives
A scan-only cursor misses concurrent change semantics. A long-lived whole-run snapshot adds a guarantee not required here. Waiting for an eternally changing source to become empty is not a finite completion definition.

## Consequences and validation
Persist fence membership and distinguish scanning from draining. Before the backfill milestone, specify and test fence creation, crash-safe import, missing-obligation handling, and late commits. This design is known upfront but not yet implemented or proven.
