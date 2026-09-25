# Capacity notes

## Current local acceptance, 2026-09-25

Tested `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685` passed the full local million-record workload with 519 mutations and real G1–G5 faults, exact independent reconciliation, five genuine negative controls and cleanup. Worker/page/heap bounds are unchanged. [Current scalar evidence](evidence/Final-submission-2026-09-25.json) records per-role sampled CPU/memory, service images and phase timings. Optional 2M remains NOT RUN.

| Measurement                                    |                                Current result |
| ---------------------------------------------- | --------------------------------------------: |
| Baseline payload bytes                         |                                 1,034,667,793 |
| Seed and activation                            |                                     679.172 s |
| Scan/delivery/receipt drain after fault        |                                   8,875.704 s |
| Receiver export                                |                                     882.155 s |
| Positive exact oracle                          |                                     318.470 s |
| Full command through supervisor                |                                  12,973.575 s |
| Largest observed worker process RSS high-water |                                 148,201,472 B |
| Largest observed worker cgroup memory peak     |                                  95,920,128 B |
| Resource samples                               |                                           833 |
| Minimum host available memory / free disk      |            8,664,481,792 B / 68,811,968,512 B |
| Source / pipeline / consumer database bytes    | 2,697,221,823 / 8,779,118,271 / 2,610,075,327 |
| Elasticsearch primary store bytes              |                                   349,479,582 |

Storage was observed at 16:52:20 UTC. RSS and cgroup counters use different accounting. Database process/cgroup values, container CPU sums and unrelated host workloads are not a controlled benchmark. Pipeline cumulative sampled CPU was 51,510.814 seconds; source was 1,693.469 seconds. There were 505 nonfresh pipeline observations. Final convergence does not establish continuous availability or the original timeout's exact cause. Controlled query experiments and the populated migration 022 upgrade are scoped evidence; old failed attempts remain failed. Optimization originally stopped after this accepted workload; the user subsequently authorized the separately tracked [server observation correction](observability-closure.md).

### Measured progress windows

The original run's fresh pipeline samples give the following cumulative-delta rates. The initial and final counters, exact observation timestamps and source-file SHA-256 are in [the selected window evidence](evidence/Capacity-observation-windows-2026-09-26.json).

| Quantity                    | First fresh count at 14:18:11.204740 UTC | First observed target            |         Window |  Delta / elapsed time |
| --------------------------- | ---------------------------------------: | -------------------------------- | -------------: | --------------------: |
| Durable staging             |                                    3,461 | 1,000,005 at 14:48:38.933826 UTC | 1,827.729086 s |   **545.24 events/s** |
| Validated consumer receipts |                                      763 | 1,000,005 at 16:43:56.367960 UTC | 8,745.163220 s | **114.26 receipts/s** |

All timestamps are September 25, 2026. The numerator excludes work already present at the first fresh sample. The denominator includes intervening stalls; an observation can follow actual completion. These windows end at different times, so they are neither simultaneous stage rates nor an isolated scanner/receiver benchmark. Dividing one million by the whole `make verify` duration would mix setup, deliberate outages, exports and negative controls into a misleading throughput number.

**Observed bottleneck:** actual sink delivery and consumer processing finished before pipeline receipt observation. Pipeline PostgreSQL also dominated sampled CPU. This identifies the slow completion path, not the precise execution plan responsible for the old timeouts.

**Concrete improvement hypothesis:** remove repeated full-history work from operational snapshot/run-observation reads while retaining exact current counts and independent terminal validation. First measure those exact statements on the server under concurrent changes, then compare a scoped query/index correction against the same fixture. A reduction in total limiting-stage service demand of roughly one half would target **228.52 receipts/s** in an equivalent window. Extra workers alone may worsen contention. Neither the doubling nor a particular SQL cause is currently claimed proven.

## Earlier completed local acceptance, 2026-09-24

The default **1,000,000 distinct approximately 1-KiB baselines**, with 519 declared mutations and real failures, passed at `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7`. Full `make verify` completed all phases, G1–G5, exact disk-backed reconciliation, five corruption controls and cleanup. Worker limits remained 256 MiB, with 128-MiB Node heaps and explicit 64-record / 256-KiB pages. `make verify VERIFY_COUNT=2000000` remains optional and **NOT RUN**. The earlier adoption of 1M was prospective; old results have not been relabeled.

| Measured quantity                                        |                        Local default-million result |
| -------------------------------------------------------- | --------------------------------------------------: |
| Baseline payload bytes                                   |                                       1,034,667,793 |
| Per-baseline bytes: minimum / maximum / average          |                           1021 / 1036 / 1034.667793 |
| Seed and activation                                      |                                           683.208 s |
| Scan, delivery and receipt drain after the scanner fault |                                          6857.363 s |
| Receiver export                                          |                                           887.299 s |
| Positive exact oracle                                    |                                           310.564 s |
| Full command through strict supervisor completion        |                                         10965.656 s |
| Largest observed worker process RSS high-water           |                                       147,918,848 B |
| Largest observed worker cgroup memory peak               |                                        94,896,128 B |
| Positive oracle process peak RSS                         |                                        29,229,056 B |
| Periodic resource samples                                |                                                 704 |
| Minimum observed host available memory / free disk       |                  7,751,229,440 B / 87,690,547,200 B |
| Source / pipeline / consumer database sizes              | 2,697,172,671 B / 8,246,441,663 B / 2,610,386,623 B |
| Elasticsearch primary store                              |                                       353,745,174 B |

Baseline payload is approximately 3.85 times one worker's 256-MiB budget. Process RSS and cgroup memory use different accounting and must not be treated as interchangeable. The independent oracle used disk-backed state. Samples can miss a process's final high-water value after exit; host memory includes unrelated workloads. Database and index sizes were observed at 19:25:38 UTC, before cleanup. Physical Lucene document counts are separate from the oracle's logical entity counts. Per-role CPU, memory, limits, image identities and exact timings are in the [selected JSON evidence](evidence/Final-submission-2026-09-24.json) and [final report](evidence/Final-submission-2026-09-24.md).

Pipeline PostgreSQL dominated sampled cumulative CPU: 36,401.628 seconds versus source PostgreSQL's 1458.119 seconds. These are sums of maximum observed cgroup counters, not exact whole-host CPU or an uncontended benchmark. Scan/drain includes delivery and receipt observation, so it is not isolated scan throughput. Receipts lagged behind delivery: sink/consumer completion preceded receipt completion, and G1 waited for all required fresh terminal evidence. Receipt progress resumed without runtime edits after an earlier stall. Operational pipeline reads intermittently timed out; the exact query cause was not isolated and continuous status availability is not claimed. No further optimization or bound changes were made to obtain this completed result.

The inherited sequence at `93eac82` completed 262144 genuine baselines with exact source/receiver reconciliation and cleanup. [Inspected results and resource/storage measurements](evidence/Observation-terminal.md) distinguish the populated corruption proof, 1024 fault test and 262144 baseline-only run. They do not establish faults at one million rows. Current final results belong in [the acceptance matrix](acceptance-matrix.md).

## Historical planning and measurements

The sections below preserve the earlier 2M plan and then-current limitations. The current default above supersedes that target prospectively. Historical placeholder statements describe the earlier entry point, which closure replaces.

## Evidence status

**The 2,000,000-entity profile has not run.** The integrated fault fixture contains 1,024 roughly-1-KiB baselines plus 519 declared mutations. Its elapsed time includes cold image/service initialization, deliberate crashes, an explicit 60-second outage, paused workers, small polling defaults, exports, negative controls and cleanup. Dividing records by that whole duration would not be a meaningful throughput benchmark.

The target at that historical stage was two million entities with bounded worker memory, independent disk-backed reconciliation and stage-specific measurements. The verified default above supersedes that plan. No forecast is presented as a validated result.

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

At the time of those historical measurements, full `make verify` had not passed. The current local 1M acceptance above now passes; optional 2M remains NOT RUN. `--page-records 64` is an explicit tested capacity/functional option, not a retroactive change to historical fixtures.
