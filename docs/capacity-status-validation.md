# Large-run observation and terminal proof

2026-09-23, investigation before correction. The recorded 262,144-baseline run at `26548e9` timed out without a fresh complete pipeline observation. Its consumer and broker diagnostics reached 262,144, but no independent final reconciliation executed. That run remains FAIL; counts alone cannot establish the missing proof.

The operator read path has a 2.5-second statement deadline and calls `backfill_status`, which recomputes the full canonical/attempt/witness/receipt proof. Ordinary owned queries also have a finite 12-second server deadline. These are candidate causes, not a claim that an unobserved SQL timeout has already been reproduced.

A separate isolated installation using the unchanged pinned application will measure the actual status, progress and terminal-evidence queries after normal source/receiver execution. Capture SQLSTATE, query plans and before/after data; do not increase deadlines blindly or relabel unavailable samples healthy.

The intended smallest correction keeps all canonical constraints, immutable identities, full terminal proof and corruption regressions. Interactive observation and successful run completion are different operations: an observational read may report state without claiming to have revalidated every byte. If separated, the response must name its evidence level and historical completion rather than invent an integrity result. Full successful completion still requires all required relations/content and receiver/consumer evidence.

A query rewrite must return identical required/pending/error/invalid partitions against the original function, including missing and malformed evidence controls. No receiver work or run membership may disappear to speed a count. Any bounded longer terminal-audit budget must be explicit, limited to that operation, and leave source/capture/lease/consumer timeouts unchanged.

Final verification will use the actually measured dataset and the same published command path. The original task does not prescribe exactly two million rows; any final count change from the planned target needs an explicit rationale demonstrating that the raw data substantially exceeds the worker memory budget. A smaller diagnostic run is not the final large-data result.

## Recorded baseline and chosen separation

The isolated run `kit-final-20260923085637-d5248b65` retained actual read-only plans on 262,144 entries before cleanup: full result counts took 6,834.932 ms, progress validation 10,840.846 ms, and full status 9,057.386 ms in those separate observations. That invocation was interrupted at its finite diagnostic boundary and remains FAIL; no reconciliation is inferred. These measurements exceed the interactive 2,500-ms statement budget, and combined terminal checks can exceed the ordinary 12-second transaction-operation budget.

The correction will retain the original full `backfill_counts` and terminal evidence requirements. Progress validation will expand immutable batch entries once instead of rescanning/parsing the same JSON for every member; old/new validation must agree on healthy and corrupted fixture states. A separate explicitly named observational status returns current state counts and cursor metadata without claiming full byte/attempt proof. Its invalid-evidence field is unknown, not zero. Existing full-audit entry points remain available.

Only backfill phase-advance operations receive an explicit bounded 180-second server statement / 185-second client-query budget. This is not a change to source transactions, sink leases, consumer COMMIT/ACK or ordinary query timeouts. It is scoped to the finite terminal proof (or a fast readiness return), with a separate SQL control statement on the same owned session before that query. The transaction owner and COMMIT outcome handling stay unchanged. Normal status observation keeps its short deadline; it does not mark a run complete.

The first 4,096-row populated proof (`77b5dcf`, `kit-final-20260923103238-79ccfdb0`) passed exact data/function-metadata preservation, all four old/new progress comparisons and restricted observation/denial checks, then failed while creating its temporary reference function inside a read-only EXPLAIN transaction (actual SQLSTATE 25006). That run remains FAIL with cleanup PASS. The reference is now created in the isolated session before starting its read-only measurement transaction; no production query, data or correctness assertion changed.
