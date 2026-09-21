# Measured capacity continuation

Authorized continuation, 2026-09-21. Baseline: `caf1b19d9637f14ada73f60dbd6b74c47dcd79c9`. No performance result is implied by this plan.

The retained 8,192-baseline pilot completed seed/activation in 27.013 seconds but remained scanning at its 120-second observation boundary. Pipeline consumer receipt observations lagged far behind already committed inbox events. The baseline chunk-count plan scanned all source rows. These are measured small/intermediate observations, not permission to relax correctness or claim 2M capacity.

## First bounded changes

1. Add a forward source index on immutable `(bootstrap_key, chunk_first)`. Make baseline/current and historical command-result validation explicitly constrain their non-null identity keys before comparing the complete retained tuple. Preserve every check, trigger, owner, OID and privilege, and preserve populated rows exactly.
2. Validate all receipts in one existing bounded lookup before recording any of them, then record the validated set in one owned pipeline transaction in stable identity order. Retain the existing eight-record/byte ceilings and exact source/consumer/content validation. An error rolls back that observation batch; explicit safe re-observation resolves ambiguous COMMIT.
3. Do not impose a one-second admission sleep after committed observation progress. Keep positive bounded yielding after useful work, and the existing one-second delay for no progress/missing receipts. Persisted `next_check_at` and bounded identity lookup remain authoritative; absence never means processed.

No worker pooling, dependency changes, lossless protocol changes, broader permissions, trigger disabling, manual receiver success, unbounded requests or altered source ACK semantics are authorized by this change. Read-only history/metric work is not called constant-time merely because its result is small.

## Required evidence

- Before/after source validation plans and exact populated row/function/ACL preservation.
- Missing or wrong retained baseline/receipt evidence still rejects with the original error; rejected observation batch leaves no partial positive observations.
- Real consumer receipts, duplicates, mismatched identity/content, unavailable consumer, fair missing-receipt scheduling and restart behavior remain checked. Synthetic scheduler tests are labeled unit checks, not service throughput.
- Repeat the same 8,192-row pilot on separately owned services with the same fixed observation window. Record actual image/code, durations, progress, memory and cleanup; compare only observed values.
- Run the complete real integrated functional fault gate and its independent reconciliation on committed code. Preserve every actual failure and negative control.

If remaining hot-path history checks dominate, document a second explicit design refinement and evidence before changing them. Every run records actual scale and completion status. A partial intermediate observation does not pass the 2,000,000-row target.

References used for the change: PostgreSQL 18 documentation for multicolumn indexes, EXPLAIN and deferred trigger semantics. Index selection/loop count must be observed on the real server; documentation alone is not a benchmark.
