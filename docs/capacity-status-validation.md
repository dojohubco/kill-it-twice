# Large-run observation and terminal proof

2026-09-23, investigation before correction. The recorded 262,144-baseline run at `26548e9` timed out without a fresh complete pipeline observation. Its consumer and broker diagnostics reached 262,144, but no independent final reconciliation executed. That run remains FAIL; counts alone cannot establish the missing proof.

The operator read path has a 2.5-second statement deadline and calls `backfill_status`, which recomputes the full canonical/attempt/witness/receipt proof. Ordinary owned queries also have a finite 12-second server deadline. These are candidate causes, not a claim that an unobserved SQL timeout has already been reproduced.

A separate isolated installation using the unchanged pinned application will measure the actual status, progress and terminal-evidence queries after normal source/receiver execution. Capture SQLSTATE, query plans and before/after data; do not increase deadlines blindly or relabel unavailable samples healthy.

The intended smallest correction keeps all canonical constraints, immutable identities, full terminal proof and corruption regressions. Interactive observation and successful run completion are different operations: an observational read may report state without claiming to have revalidated every byte. If separated, the response must name its evidence level and historical completion rather than invent an integrity result. Full successful completion still requires all required relations/content and receiver/consumer evidence.

A query rewrite must return identical required/pending/error/invalid partitions against the original function, including missing and malformed evidence controls. No receiver work or run membership may disappear to speed a count. Any bounded longer terminal-audit budget must be explicit, limited to that operation, and leave source/capture/lease/consumer timeouts unchanged.

Final verification will use the actually measured dataset and the same published command path. The original task does not prescribe exactly two million rows; any final count change from the planned target needs an explicit rationale demonstrating that the raw data substantially exceeds the worker memory budget. A smaller diagnostic run is not the final large-data result.
