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

## Second bounded refinement after the repeated pilot

The same 8,192-row/120-second pilot at `42149b5` remained partial: seed/activation 21.557 seconds, 2,736 staged, 2,704 consumed and 1,506 recorded receipts at the final 119.025-second sample. Receipt progress improved relative to the earlier observation, but overall staging did not improve; these are single loaded-host observations, not a claimed universal speedup.

The scanner invokes the full accumulated event/witness/body-validation status twice per page. That reporting work is not needed for page admission: the claim, page and deferred COMMIT checks already enforce current range/batch identity, membership and lease validity, and `backfill_advance` performs the full retained-progress checks before sealing/draining/completion. Add a separate, explicitly marked admission-only snapshot of run/range control fields for the retained runtime. Keep the public/full status API and final completion validation unchanged. The default historical worker entry retains its full status, so old profile requirements do not become smaller by accident.

Baseline deferred validation currently verifies every row in a chunk for every newly inserted row. Preserve per-row validation of its own exact recipe/identity/time/chunk relationship and retain the full chunk-set validator once per inserted chunk. Progress increments must reference exactly the newly committed contiguous chunk under the existing lifecycle lock; full accumulated validation remains at phase boundaries and sealing. Verify all existing data once during upgrade. This is an inductive proof over immutable chunk receipts, not permission to delete integrity checks or trust a client counter.

Raise the explicitly configured retained-runtime receipt lookup to at most 32 events using forward owning-database bounds; historical defaults remain eight. The cap is 2 MiB for expected canonical bodies and at most 4.125 MiB for quarantined raw bodies/metadata before bounded validation. No body/hash comparison, source/consumer identity check, fair scheduling, atomic observation or COMMIT outcome rule is removed. Runtime configuration must select this only after both forward migrations exist.

Require new SQL rejection controls for wrong recipe/membership/progress, exact populated preservation, and all existing functional fault/reconciliation checks on the optimized runtime. If a partial pilot or dependency failure remains, report it rather than extending that observation to a 2M PASS.

## Bounded absent-receipt scanning refinement

At `479012d`, the repeated 8,192-row pilot seeded in 8.523 seconds and staged 8,160 records by the final 120.438-second observation. Receivers lagged and the observer had recorded only 80 receipts despite 3,280 processed inbox events. The saved live observer logs showed successive 32-ID pending cohorts with no receipts: sleeping a full second after each nonempty-but-absent selection kept older due identities behind the growing queue. This is a polling/admission issue, not a missing consumer effect or permission to infer processing from broker confirmation.

Keep the per-identity persisted one-second revisit delay. Yield 50 ms after a nonempty selection with no newly observed receipt so other due cohorts can be examined; yield 10 ms after committed observations and retain the one-second idle delay only when no work is selected. This bounds empty-response scanning to at most twenty cycles per second before database time and remains fair through `next_check_at`. No selection watermark, busy-loop or successful-observation shortcut is introduced. Real rollback/receipt proofs and the final functional gate must cover the resulting runtime.

## Complete intermediate validation

The indexed/chunk-local source changes passed the original eighteen real bootstrap/activation cases with the explicitly recorded forward migrations. A separate populated upgrade preserved exact source rows and function identity/configuration, while permitting only the documented PUBLIC privilege narrowing. The actual 32-receipt observer test rolled back an intentionally failing second observation, then committed a full validated batch with one shared transaction ID and passed independent whole-fixture reconciliation. The complete 1,024-baseline G1-G5 fault run also passed on the same code.

The next intermediate invocation requests convergence within a finite window and then freezes workers and runs the existing disk-backed independent source/receiver oracle. `scripts/capacity/pilot.py --reconcile` must fail if that requested convergence/reconciliation is not reached; ordinary fixed-window pilots still distinguish measured partial progress. Count/scale remains explicit, and no intermediate result substitutes for the two-million-row target.

## Third measured refinement — 2026-09-22

A diagnostic 8,192-row pilot at `c88ac7a` enabled PostgreSQL function timing only inside its owned databases. At its final 175.887-second sample all events were staged but only 5,113 ES obligations and 6,032 broker obligations were satisfied; 5,788 consumer observations were recorded. The pipeline database reported 30,508 sessions. Function timing also showed repeated full-body validation during draining and material per-item renewal/settlement work. The diagnostic adds timing overhead and is not a clean throughput benchmark; its exact reports are under `artifacts/final/kit-final-20260922092224-a12c1b6e/`.

The next correction batches only local database work already bounded by the receiver protocol. The retained runtime may read up to sixteen ES projections and settle or renew up to thirty-two claims per exclusively owned transaction, in deterministic identity order, by calling the existing validated SQL functions. A stale item remains stale without modifying its neighbors; an actual transaction error leaves that local group uncommitted/unknown and safely repeatable, not remotely rejected. Already committed groups remain settled. No database transaction spans receiver network I/O, no checkpoint moves separately from its page, and no connection pooling or broadened privilege is introduced.

The default historical API path remains one local item per transaction. Explicit retained-runtime configuration selects the bounded mode; new real rollback/stale/mixed-rejection tests and integrated fault gates must exercise it. A successful bulk response is still classified item by item before local settlement. Read caches are capped independently of the existing 4-MiB receiver request/response budgets. Positive short yielding after actual admitted progress removes a fixed one-second pause; empty/cooldown polls retain their bounded delay. No retry-count-based data loss, hidden client retries, event mutation or limit increase is allowed.

Full run completion validation is still required. If measurements justify avoiding it on every draining poll, the admission-only poll may first check for a pending required obligation and defer the expensive final proof until every required obligation is terminal. That precheck cannot complete a run or hide missing evidence: only the existing full advance/validation transaction may record completion. Public status remains an honest separate operation. Tests must preserve both successful and complete-with-errors paths and missing-evidence rejection.

Required evidence is actual SQL rollback of a group after a later item fails, unchanged settled state under stale repeats, mixed independently classified outcomes, exact per-item content/fencing, bounded read/selection rejection, and the existing real kill/outage/497-of-500/inbox oracle. Compare the same intermediate workload before claiming a speedup, then increase scale only with explicit finite limits and independent reconciliation.

The capacity driver now distinguishes a valid unavailable observation from a malformed contract. It retains the actual status response before deriving counters, preserves null/unknown rather than filling zeros, and requires fresh complete sink/consumer evidence before final export/reconciliation. An optional `--scanners` value from one to four changes only the test-owned backfill replica count; the regular runtime and functional-fault fixture remain single-scanner by default. Actual replica IDs are recorded. Compare the same 32,768-row fixture first; a larger count is not declared successful by extrapolation.

## Constraint-backed completion aggregate

The four-scanner 32,768-row attempt reached a stored complete run and 32,768 consumer inbox records, but the operator status consistently became unavailable. An actual restricted-role read hit SQLSTATE 57014 at its 2,500-ms statement deadline. The unchanged full status took 3,202.645 ms in a separate read-only plan. A rollback-only candidate removed only the repeated `valid_body(e)` parsing, retaining checksum, identity, witness, attempt and receipt checks; it produced exactly equal counts in 641.197 ms. The entire schema experiment rolled back. Its first TEMP-table attempt was denied; the successful comparison used a quoted psql value instead of granting additional permission. This is a diagnostic comparison, not completed capacity acceptance.

The authoritative `event_body_consistency` CHECK already evaluates that predicate on every insertion/change, is validated for all existing rows, and canonical rows remain immutable to runtime roles. The next forward migration reuses this database invariant rather than reparsing every immutable JSON body on every status/count. It must first verify the exact validated constraint and keep a constant-time catalog proof in the read; absent, unvalidated or altered constraint evidence makes required rows invalid, not complete. Existing checksum and remote/local evidence checks remain. Independent raw-source reconciliation still reconstructs and checks every canonical byte. No statement deadline increase, body-validity exemption, cached fake count or skipped final completion check is authorized.

Required regression: old and new aggregate outputs match on actual completed, pending and declared-failure fixtures; malformed bodies still fail the validated CHECK; runtime mutation is denied; removing the proof constraint in a rollback-only fixture makes aggregate validity fail closed. Preserve row/receiver data exactly across the migration and repeat functional G1-G5 plus the previously failing size.

## Indexed diagnostic navigation — 2026-09-22

The completed 65,536-baseline run exposed a separate verification bottleneck: keyset export queries used numeric entity order or explicit `COLLATE "C"`, while the available primary keys were epoch-prefixed or used the column's default collation. An actual rollback-only comparison returned identical selected rows and changed the observed source/pipeline/consumer page query times from 46.930/40.357/29.463 ms to 0.220/0.526/0.582 ms. These are individual read-only query timings, not end-to-end throughput or committed optimization results.

Add exactly three non-unique navigation indexes: source baseline `entity_id`, pipeline event `event_id COLLATE "C"`, and consumer processed-event `event_id COLLATE "C"`. Preserve query text, canonical identity, constraints, content, grants and runtime mutation semantics. No wide payload is included in an index. Installation uses the existing controlled transaction-based initializer; this is an ordinary blocking index build during setup/maintenance, not a zero-downtime production migration. A populated old-version fixture must show exact row/catalog preservation, complete export equality, changed actual plans and denied index administration by runtime roles.

Then run the existing real functional fault gate and a larger explicitly bounded capacity fixture with independent reconciliation. Record stage durations, actual worker/process memory limits and observed resource use. The final two-million-row target remains unrun until its own retained evidence proves completion; individual-query speedups and smaller totals cannot substitute for it.

Authoritative behavior references: PostgreSQL 18, [indexes and collations](https://www.postgresql.org/docs/18/indexes-collations.html), [multicolumn indexes](https://www.postgresql.org/docs/18/indexes-multicolumn.html), [CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html) and [EXPLAIN](https://www.postgresql.org/docs/18/using-explain.html). Planner choice and preservation are checked on the pinned server, not inferred solely from documentation.

## Pipeline shared-memory provisioning — 2026-09-22

The first indexed 131,072-row attempt used Docker's observed default 64-MiB `/dev/shm` inside a pipeline container still limited to 1 GiB total memory. Normal backfill status and a separate real read-only comparison raised PostgreSQL errors that dynamic shared-memory segments could not be resized (`No space left on device`). The source and receiver workers continued processing; the unavailable status was not a completed capacity result. The run was explicitly interrupted through its owned cleanup after preserving actual container IDs, limits, status samples and error logs.

Provision a 256-MiB shared-memory filesystem for the pipeline PostgreSQL container only, inside its unchanged 1-GiB container memory ceiling. This is a Compose service resource setting, not host sysctl tuning, a mount of host IPC, a PostgreSQL durability reduction or permission expansion. Record actual `HostConfig.ShmSize` with each capacity sample and require the configured bound before seeding. No success is inferred until the same workload completes with independent reconciliation; query deadlines and data checks remain unchanged.

A diagnostic read-only no-parallel/JIT comparison took over ten seconds and did not justify disabling those features in production. It changed no persistent setting. Avoid blindly changing plans or raising statement deadlines: the next run tests the identified shared-memory prerequisite first.

References: Docker Compose [service shm_size](https://docs.docker.com/reference/compose-file/services/#shm_size) and PostgreSQL 18 [dynamic shared memory/resource settings](https://www.postgresql.org/docs/18/runtime-config-resource.html). Resource samples and actual server errors remain the evidence, not these documentation links alone.
