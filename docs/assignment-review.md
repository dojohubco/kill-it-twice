# Assignment review guide

This guide maps the Optio **Kill It Twice** assignment, reread in the applicant portal on 2026-09-26, to the implementation and its recorded execution evidence. The assignment asks for a locally runnable system and a public repository. The chosen full fixture is one million roughly-1-KiB baselines; the justification and measured memory limits are in [README](../README.md#capacity-and-limits).

## Evidence identities

| Validation                                                                        | Tested revision                            | Recorded result                                                                       |
| --------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------- |
| Full local 1M, all G1–G5, exact oracle, five corruption controls and cleanup      | `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7` | [PASS, September 24](evidence/Final-submission-2026-09-24.md)                         |
| Full local 1M, strict 1024 repeat and retained R01–R08 including editable polling | `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685` | [PASS, September 25](evidence/Final-submission-2026-09-25.md)                         |
| Current application code: quality, real 1024 fault fixture and rendered UI checks | `812a1b7f3c8c57e698f02387e917bbef65b7cc05` | [Hosted CI PASS](https://github.com/dojohubco/kill-it-twice/actions/runs/36194928143) |
| Current observation projections: exact states, transactions and populated upgrade | `812a1b7f3c8c57e698f02387e917bbef65b7cc05` | [Scoped server evidence](evidence/Observation-projections-2026-09-26.md)              |

The dated million-record reports define the scope of the full-scale claims. Subsequent UI and observation changes have their own evidence. This documentation update changes no runtime, migration, assertion or workload.

## Requirement map

| Assignment requirement                                 | Where to review it                                                                                                                                 | Verification                                                                                      |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Concurrent backfill and incremental capture            | [Backfill contract](adr/013-backfill-checkpoints-fence.md), `scripts/backfill.ts`, `scripts/capture.ts`                                            | G1 and exact final reconciliation                                                                 |
| Search plus an independent event consumer              | [Delivery guarantees](../README.md#architecture-and-commit-boundaries), `scripts/deliver-es.ts`, `scripts/deliver-rabbit.ts`, `scripts/consume.ts` | G2–G4; independent receiver and consumer exports                                                  |
| Meaningful bounded-memory dataset                      | [Capacity notes](capacity-notes.md)                                                                                                                | 1,034,667,793 baseline bytes; 256-MiB worker limits                                               |
| One automatic G1–G5 command                            | [Gate contract](final-gates.md), `Makefile`, `scripts/verify-final.py`                                                                             | `make verify`; faults, oracle and cleanup affect the exit status                                  |
| Explicit delivery guarantee                            | [Architecture and commit boundaries](../README.md#architecture-and-commit-boundaries)                                                              | At-least-once transport; versioned projections; effectively-once database effects                 |
| Diagnostic DLQ and replay                              | [Operations](operations.md), [replay contract](adr/014-operational-control-replay.md)                                                              | G4 and retained R07; original event and history preserved                                         |
| UI status and data updates                             | [Records browser and API evidence](evidence/Records-live-updates-2026-09-25.md)                                                                    | G5, search/details, automatic refresh and last-known state                                        |
| UI start/pause/resume, replay and editable parameters  | [Polling contract](adr/016-operator-polling-settings.md)                                                                                           | R01–R08; durable polling values, bounds, concurrency and restart                                  |
| UI outage, invalid-record and source-change simulation | [Operator use](../README.md#operator-use-and-recovery), `apps/operator-ui/`                                                                        | Real retained controls and rendered fixture inventory                                             |
| Whole system startup and explicit seed                 | [Quick start](../README.md#prerequisites-and-quick-start), `compose.yaml`, `Makefile`                                                              | `docker compose up -d --build`, `make seed`, retained restart                                     |
| Pre-code specification and its evolution               | [SPEC](../SPEC.md), [SPEC history](https://github.com/dojohubco/kill-it-twice/commits/main/SPEC.md)                                                | Specification `251cb5a` precedes harness `7af0818` and implementation `5368de6`                   |
| Agent conventions and safety rules                     | [AGENTS](../AGENTS.md)                                                                                                                             | Commands, ownership, invariants and evidence discipline                                           |
| Four decisions, alternatives and tradeoffs             | [Decision table](../README.md#decisions-and-provenance)                                                                                            | ADRs 001–004, with implementation refinements linked below the table                              |
| Architecture, checkpoint and DLQ locations             | [README diagram](../README.md#architecture-and-commit-boundaries)                                                                                  | Components, data path and transaction-boundary table                                              |
| Throughput, bottleneck and doubling proposal           | [Measured progress windows](capacity-notes.md#measured-progress-windows)                                                                           | Timestamped counter deltas, resource observations and explicit hypothesis                         |
| Scope choices and two concrete AI deviations           | [README](../README.md#what-i-did-not-build-and-why)                                                                                                | Wire-byte accounting and independent-oracle corrections, with source commits and real regressions |
| Public repository and reproducible entry points        | [Public source](https://github.com/dojohubco/kill-it-twice)                                                                                        | Fresh-checkout quick start, seed and verification instructions                                    |

## Suggested review sequence

1. Read the README diagram and delivery-boundary table.
2. Follow the quick start, then exercise Overview, Records, Backfill, Failures, Simulations and Configuration.
3. Run `make verify-functional` and `make verify-ui` for the small real fault fixture and rendered UI inventory.
4. Run `make verify` on the documented capacity profile for the full workload; consult the dated manifests and checksums for the completed reference runs.

Historical evidence keeps its original identity. The separately scoped [server observation follow-up](observability-closure.md) records its own acceptance contract. Preparing the public repository and sending the application are separate actions.
