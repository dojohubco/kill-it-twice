# Capacity notes

## Evidence status

**The 2,000,000-entity profile has not run.** The integrated fault fixture contains 1,024 roughly-1-KiB baselines plus 519 declared mutations. Its elapsed time includes cold image/service initialization, deliberate crashes, an explicit 60-second outage, paused workers, small polling defaults, exports, negative controls and cleanup. Dividing records by that whole duration would not be a meaningful throughput benchmark.

The target remains two million entities with bounded worker memory, an independent disk-backed reconciliation, and measured stage-specific elapsed time, CPU/RSS, PostgreSQL growth and Elasticsearch storage. No forecast is presented as a validated result.

## Historical small-fixture observations — 2026-09-21 baseline

A read-only `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` on the 1,024-row source showed the baseline chunk-count query scanning all 1,024 rows to match 64, rejecting 960 rows and visiting 172 shared blocks. That particular execution took 0.746 ms. The actual plan is retained at `artifacts/capacity/reconnaissance/baseline-chunk-plan.json`; it is not a large-data result.

At that baseline, the deferred bootstrap checks invoked this chunk validation repeatedly. Progress checks also count accumulated baseline history, and backfill status validates/joins accumulated run evidence. A response with bounded size does not imply bounded database work. Those paths require measurement before increasing the dataset; integrity checks may not simply be removed to improve timing.

At that baseline, the receipt observer requested at most eight events and its CLI waited one second between iterations. Eight receipts per second is only a code-derived admission ceiling before database/processing time, not a measured sustained rate. It makes an unmodified two-million-row demonstration impractically slow even if the receivers are fast. Future batching/cadence changes must retain explicit identity/content validation, fair retry scheduling and atomic observations.

## Measurement procedure before optimization

Use separate run-owned source/state/receiver resources and capture exact code, recipe, counts and service limits. Start with a measured intermediate fixture to identify query plans and stage-specific progress; preserve any timeout/failure and cleanup. Add only evidence-backed indexes or bounded batching improvements, with forward migrations and invariant/upgrade regressions. Repeat the same workload before claiming an improvement.

For the final scale run, record seed/activation separately from scan/staging and sink/consumer drain; monitor actual process RSS/high-water marks and receiver/database storage. Reconcile exact identities, versions, payloads, tombstones and effects from bounded exports using disk-backed state. Logical throughput excludes repeated attempts and duplicates. If a stage is blocked, the recorded result is incomplete rather than extrapolated to completion.

Single-node services and retained local storage remain assumptions. Host power loss, independent database restoration, administrator replacement and unlimited backlog are not covered by small functional tests or resource limits.

## First intermediate observation — 2026-09-21

An isolated pre-optimization pilot used the exact accepted application image at `545b2c8dc8772da8f77c0c726eab20c42075dc38` and 8,192 genuine baselines. Root `6766a77` adds only documentation relative to that image. The source was initially empty, and explicit seed/activation/scheduling completed in 27.013 seconds. Workers then ran normally for a fixed 120-second observation window; no fault wrapper, extra worker, batch-size change or schema optimization was enabled.

The last fresh snapshot at 117.085 seconds after seed recorded 3,424 staged events, 2,763 satisfied ES obligations, 3,392 broker-confirmed events, 3,392 consumer inbox records and only 680 validated pipeline consumer observations. There were zero mutation effects and zero quarantine records, as expected for baseline-only data. Backfill was still scanning. These component observations are not one atomic global snapshot and are not full-state reconciliation.

The recorded outcome is **MEASURED_PARTIAL**, not correctness PASS, timeout-free completion or two-million-row acceptance. All run-owned resources were removed after observation. The experiment confirms that baseline copying and receipt-observation throughput need work before a full-size run; blindly multiplying a count or ignoring lag would be misleading. The host also retained its existing demo and unrelated workloads, so this is not an uncontended hardware benchmark.

The original report, timed commands, samples, actual image identity, read-only source query plan and cleanup are retained in `artifacts/final/kit-final-20260921195312-415a0b27/`. The exact pilot script is `artifacts/capacity/measure-pilot.py`. A later optimization must be compared with this same declared workload and retain the invariants and independent functional gates, not change the expected counts to fit the outcome.

## Subsequent measured improvements — 2026-09-22

[The bounded page/queue report](evidence/Capacity-page-and-queue.md) records the completed forward optimizations, actual upgrade/transaction proofs and independently reconciled 131,072-row measurements. The earlier partial pilot above is preserved as historical evidence, not presented as the current runtime rate.

On one shared-host run each, 131,072 baselines reached the complete observation after approximately 1,528 seconds with the prior sixteen-record page, 1,175 seconds with explicit sixty-four-record pages, and 660 seconds with the same page setting plus the equivalent due-index predicate. Exact code/configuration/seed timing, resource snapshots and independent receiver/source comparisons are in the report; these numbers are not a universal throughput promise. Normal page defaults remain sixteen and the 256-KiB wire-batch ceiling is unchanged.

Forty of the final run's 91 periodic pipeline observations were unavailable; the sampler kept them unknown, while other components continued to report. The run later produced a fresh complete observation and exact independent reconciliation. Summary-query latency under load therefore remains a real limitation before a two-million-row run or a dashboard scale claim. No timeout was lengthened or unknown response relabeled zero to obtain this result.

The two-million-row target is still NOT RUN. Full `make verify` remains nonpassing pending that separate final acceptance. `--page-records 64` is an explicit tested capacity/functional option, not a silent change to historical fixtures.
