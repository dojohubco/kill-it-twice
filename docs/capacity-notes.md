# Capacity notes

## Evidence status

**The 2,000,000-entity profile has not run.** The integrated fault fixture contains 1,024 roughly-1-KiB baselines plus 519 declared mutations. Its elapsed time includes cold image/service initialization, deliberate crashes, an explicit 60-second outage, paused workers, small polling defaults, exports, negative controls and cleanup. Dividing records by that whole duration would not be a meaningful throughput benchmark.

The target remains two million entities with bounded worker memory, an independent disk-backed reconciliation, and measured stage-specific elapsed time, CPU/RSS, PostgreSQL growth and Elasticsearch storage. No forecast is presented as a validated result.

## Direct small-fixture observations

A read-only `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` on the 1,024-row source showed the baseline chunk-count query scanning all 1,024 rows to match 64, rejecting 960 rows and visiting 172 shared blocks. That particular execution took 0.746 ms. The actual plan is retained at `artifacts/capacity/reconnaissance/baseline-chunk-plan.json`; it is not a large-data result.

The current deferred bootstrap checks invoke this chunk validation repeatedly. Progress checks also count accumulated baseline history, and backfill status validates/joins accumulated run evidence. A response with bounded size does not imply bounded database work. Those paths require measurement before increasing the dataset; integrity checks may not simply be removed to improve timing.

The receipt observer currently requests at most eight events and its CLI waits one second between iterations. Eight receipts per second is only a code-derived admission ceiling before database/processing time, not a measured sustained rate. It makes an unmodified two-million-row demonstration impractically slow even if the receivers are fast. Future batching/cadence changes must retain explicit identity/content validation, fair retry scheduling and atomic observations.

## Measurement procedure before optimization

Use separate run-owned source/state/receiver resources and capture exact code, recipe, counts and service limits. Start with a measured intermediate fixture to identify query plans and stage-specific progress; preserve any timeout/failure and cleanup. Add only evidence-backed indexes or bounded batching improvements, with forward migrations and invariant/upgrade regressions. Repeat the same workload before claiming an improvement.

For the final scale run, record seed/activation separately from scan/staging and sink/consumer drain; monitor actual process RSS/high-water marks and receiver/database storage. Reconcile exact identities, versions, payloads, tombstones and effects from bounded exports using disk-backed state. Logical throughput excludes repeated attempts and duplicates. If a stage is blocked, the recorded result is incomplete rather than extrapolated to completion.

Single-node services and retained local storage remain assumptions. Host power loss, independent database restoration, administrator replacement and unlimited backlog are not covered by small functional tests or resource limits.
