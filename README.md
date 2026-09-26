# Kill It Twice

A retained local replication application: PostgreSQL source and outbox, resumable backfill plus incremental capture, Elasticsearch search, RabbitMQ stream, independent consumer effects, and an Angular operator UI.

**Local final acceptance passed on 2026-09-25**, including UI parameter editing, at tested commit `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685`. Full `make verify` passed all five parent phases, G1–G5, independent exact million-baseline reconciliation, five corruption controls and both child cleanups. The separate 1024-baseline functional check and real retained-runtime R01–R08 also passed. See [current evidence](docs/evidence/Final-submission-2026-09-25.md), [the acceptance matrix](docs/acceptance-matrix.md) and [closure decision](docs/acceptance-closure.md). Later documentation identity and public hosted-CI results are recorded separately; this local result does not establish server acceptance or optional 2M.

## Start here as a reviewer

1. **Try the system:** follow the quick start below with 1,024 seed records. In the UI, open Overview and Records, connect operator access, generate a source change in Simulations, and watch the selected record update. Backfill, Failures and Configuration expose pause/resume, replay and polling controls.
2. **Check the fault scenarios quickly:** run `make verify-functional`, then `make verify-ui`. These execute real G1–G5 on a small fixture plus browser checks. The last hosted run took about ten minutes including installation; this is not a large-data claim. [Hosted checks](https://github.com/dojohubco/kill-it-twice/actions).
3. **Reproduce full acceptance:** run `make verify` from a clean checkout. The measured million-record run took **3 h 36 min**; allow at least **8 GiB available RAM and 80 GiB free disk**. It creates isolated resources and verifies cleanup. Time varies with hardware and other workloads.
4. **Inspect the engineering decisions:** the diagram and decision table below lead to the detailed ADRs; the gate table links to dated evidence. [SPEC history](https://github.com/dojohubco/kill-it-twice/commits/main/SPEC.md) shows the pre-code specification and subsequent changes.

For the complete requirement-to-evidence map, use the [assignment review guide](docs/assignment-review.md). Every report identifies its tested revision and execution environment.

## Prerequisites and quick start

Linux x64 with a local Docker daemon, Compose, Git, make, Python 3 with SQLite, Node **24.19.0**, npm **12.0.2**, and an installed Chromium/Chrome. `UI_CHROMIUM_PATH` selects an existing browser. The Docker host needs `vm.max_map_count >= 1048576`; checks do not change the host or install system packages. Full acceptance requires at least 8 GiB available memory and 80 GiB free on both evidence and Docker storage filesystems; it retains a 2 GiB memory / 10 GiB disk safety reserve. The existing shared-host observations are not dedicated-hardware benchmarks.

```sh
npm ci --no-audit --no-fund
npm run tools:provision          # pinned local actionlint, no privileged install
npm run runtime:preflight
# Select a distinct name/port when another installation exists.
export COMPOSE_PROJECT_NAME=kit-local-demo
export KIT_UI_PORT=4200
docker compose up -d --build
make seed SEED_COUNT=1024
make runtime-status
make operator-token             # private local token; do not save in a report
```

Open [the local operator UI](http://127.0.0.1:4200). Compose builds the image from this checkout. Initializers generate installation-local credentials/TLS material in named volumes; workers receive restricted roles, and the browser receives no database credentials. Only the gateway is published, on loopback. The token is held in browser page memory. No committed secret, ignored configuration or downloaded evidence archive is needed.

`make seed` is explicit, bounded and resumable under the same manifest. A conflicting count fails; activation and scheduled backfill do not mean receiver completion. Normal restart retains identity, data, credentials, checkpoints and historical outcomes:

```sh
docker compose down             # retain volumes
docker compose up -d
```

Keep the same Compose project name for this restart. Removing its volumes destroys that installation. Automated verification uses its own unique projects, ports and volumes and removes only those resources.

## Verification

```sh
make verify                      # default: 1,000,000 genuine ~1 KiB baselines
make verify-functional           # fresh 1,024-baseline development fault repeat
make verify-runtime              # root setup, seed, retained restart and real UI controls
make verify-ui                   # rendered browser fixture inventory, separate from live proof
# Optional, explicitly selected capacity target:
make verify VERIFY_COUNT=2000000
```

Run final acceptance from clean committed code after installation. `make verify` executes quality, browser fixture checks, the separate 257-baseline retained-runtime/control fixture, then real G1-G5 faults and 519 unique mutations on the **selected large installation**. It does not read an old summary to obtain PASS. [The final gate contract](docs/final-gates.md) describes the actual fault boundaries. A green fast CI job proves only its named small fixture; full acceptance is a separate optional manual job on an explicitly provisioned capacity runner. No hosted result is implied by local workflow validation.

| Gate | Required observed behavior                                                                                                                                                                                      | Local 1M result |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| G1   | Prior committed pages, actual page writes before COMMIT, independent open-transaction/cursor observation, scanner SIGKILL, unchanged durable checkpoint and resumed progress with overlapping source mutations. | **PASS**        |
| G2   | Actual redelivery after consumer COMMIT-before-ACK and duplicate publication after broker confirm-before-local-settlement; one effect per unique mutation.                                                      | **PASS**        |
| G3   | Real Elasticsearch service outage lasting at least 60 measured seconds, durable backlog, bounded attempts, independent broker/consumer progress and recovery.                                                   | **PASS**        |
| G4   | One actual 500-operation bulk, 497 successes and three independently predeclared mapping rejections with retained DLQ evidence.                                                                                 | **PASS**        |
| G5   | Real status/metrics/browser observations of progress, useful throughput, lag, failures and dependency/data health.                                                                                              | **PASS**        |

These results were observed on 2026-09-25 at tested commit `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685`; see the [full local acceptance report](docs/evidence/Final-submission-2026-09-25.md) for the exact run, reconciliation, negative controls, resources and cleanup. The [assignment review guide](docs/assignment-review.md) separately identifies the current source, UI checks and hosted validation.

Exports and the independent disk-backed oracle compare exact identity sets, versions, payloads, tombstones, event history, source ACKs, receipts and business effects. Five corrupt-export controls must be detected. Full terminal run validation remains mandatory; short status observations explicitly do not revalidate integrity, and unknown fields remain unknown. Failed phases and unattempted gates are distinct. Cleanup failure makes the entire command nonzero.

Reports, input hashes, source snapshots, commands, fault markers, exports, resource samples and screenshots are written to ignored `artifacts/final/`, `artifacts/runtime/` and `artifacts/ui/`. They are local files, not public download links. [Dated evidence](docs/evidence/) retains previous results and failures at their original code identities.

## Architecture and commit boundaries

Read the solid arrows from top to bottom: source changes and backfill pages join the same durable pipeline, then reach two independent receivers. The dotted line is the operator's control and observation path.

```mermaid
flowchart TB
  Source["Source PostgreSQL<br/>Rows + immutable outbox"]
  Transfer["Backfill + incremental capture<br/>Concurrent, bounded workers"]
  Pipeline["Pipeline PostgreSQL<br/>Events + sink obligations<br/>Checkpoints + Elasticsearch DLQ"]
  Search["Elasticsearch<br/>Versioned search + tombstones"]
  Broker["RabbitMQ<br/>Durable event stream"]
  Consumer["Consumer PostgreSQL<br/>Inbox + projection + business effects"]
  Operator["Angular operator UI<br/>Status, records, controls, simulations"]
  API["NestJS control API<br/>Restricted commands + metrics"]

  Source --> Transfer --> Pipeline
  Pipeline -->|per-item delivery| Search
  Pipeline -->|confirmed publication| Broker
  Broker -->|delivery and redelivery| Consumer
  Operator --> API
  API -.->|observe and control| Pipeline
```

The API observes source, pipeline, consumer, search and broker health; the dotted edge summarizes that control plane. It never carries replicated records between receivers. Elasticsearch DLQ records live in the **pipeline database**, alongside attempts and replay audit history.

| Boundary          | What becomes durable together                            | What happens afterward                         |
| ----------------- | -------------------------------------------------------- | ---------------------------------------------- |
| Source mutation   | Row version and immutable outbox after-image             | Incremental capture can claim it               |
| Backfill page     | Canonical events, obligations, membership and checkpoint | Resume begins from that committed checkpoint   |
| Captured mutation | Pipeline event and both sink obligations                 | The source event can be acknowledged as staged |
| Consumer delivery | Inbox deduplication, projection and business effects     | Individual RabbitMQ ACK                        |

An Elasticsearch item rejection retains its own DLQ entry while successful items settle. Audited replay reuses the original event and preserves the other receiver's progress. Publisher confirmation and consumer completion are deliberately separate boundaries.

Source and pipeline are separate PostgreSQL services/transaction domains. The consumer owns a separate database and credentials on the state service. Source mutations commit their immutable after-images atomically. Backfill pages commit staging and checkpoint evidence together. A finite source fence closes a run without preventing later incremental capture.

Delivery is **at least once**, with monotonic versioned projections and effectively-once defined consumer database effects. Each unique mutation contributes one audit effect and one aggregate unit; baseline observations do not. A source ACK means durable staging, a publisher confirm means broker acceptance, and a consumer receipt means database COMMIT. They are separate facts. There is no cross-sink atomic visibility or end-to-end exactly-once claim. Ambiguous outcomes remain unresolved until identity-based recovery proves them.

These guarantees assume intact retained storage, controlled source privileges, trusted runtime roles, no incompatible independent database restore, and eventual healthy service time/capacity. Finite storage cannot absorb unlimited writes. Persistent tombstones and exact canonical event bytes prevent stale resurrection and content substitution. Runtime never recreates registered receivers or resets another sink to replay Elasticsearch.

## Operator use and recovery

![Current dark operator overview](docs/images/operator-overview.jpg)

Current interface captured on 2026-09-26 against the retained local demo with 257 records.

All six operator pages use a dark workspace with compact navigation, controls and data tables. See [dark workspace verification](docs/evidence/Dark-workspace-2026-09-26.md) for desktop/mobile, accessibility and real-API checks.

The UI opens read-only. Connect the installation token only for an intended command. Overview separates staging, broker confirmation and consumer effects; missing/last-known observations are labeled. Records provides search, current details and exact identifiers. The current result page and open detail refresh every five seconds after the previous read cycle finishes, while the tab is visible and Auto-refresh is on. Search, pagination and keyboard focus are preserved; failed reads retain visibly last-known data. Refresh records also updates the open detail when automatic updates are paused. See [Records verification](docs/evidence/Records-live-updates-2026-09-25.md) for the scoped browser and real-API evidence. Backfill supports start, pause and resume; pause is admission control, so in-flight pages may still commit. Request acceptance is shown as scheduling, not completed delivery.

Failures exposes current and historical ES attempts, source blocks and consumer quarantine. ES replay targets the current terminal attempt, retains the original canonical event and all previous failures, and preserves broker/consumer success. Unchanged invalid content can fail again. Correcting source data creates a newer revision; explicit verified supersession is available through the existing [operations CLI](docs/operations.md). Consumer quarantine has **no public replay/purge control**.

Simulations operates sixteen named source fixtures (create/update/delete/restore), a real invalid mapped revision, and configured Toxiproxy disconnect/reconnect controls. It does not offer arbitrary SQL, arbitrary payloads or host commands. Configuration edits incremental capture and idle-backfill polling intervals (50–30,000 ms; 1,000 ms defaults). Connect operator access, review both values, then save. Settings are durable and revisioned; concurrent edits require a reload, and ambiguous responses retry the same request. Workers observe changes between iterations with a five-second read cache; an existing wait or in-flight operation finishes first. The page also displays immutable identities, actual API limits and restart-required connection settings. Automatic browser refresh is a separate local toggle. See [metric definitions](docs/metric-definitions.md), [UI contract](docs/ui-verification-contract.md) and [interface design](docs/ui-design.md).

## Decisions and provenance

The first commit `251cb5a` contains SPEC, AGENTS, ADRs 001-004 and the initial milestone before harness `7af0818` and implementation `5368de6`. Architecture was selected through AI-assisted review before implementation; it is not presented as a later agent discovery. [SPEC](SPEC.md) preserves that adoption context and subsequent deliberate refinements. [AGENTS](AGENTS.md) records workflow constraints.

| Decision                                                                                             | Alternatives considered                                                                    | Tradeoff                                                                                                              |
| ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| [Transactional source outbox](docs/adr/001-source-capture.md)                                        | Timestamp/cursor polling, application-only writes, WAL CDC                                 | Requires source schema privileges and write/storage overhead; retains late commits and exact after-images.            |
| [At-least-once transport with effectively-once database effects](docs/adr/002-delivery-semantics.md) | At-most-once, distributed transactions, end-to-end exactly-once                            | Duplicate transport is expected; durable deduplication is required, and the two receivers are not atomically visible. |
| [Source versions and persistent tombstones](docs/adr/003-revision-identity-and-tombstones.md)        | Timestamp ordering, new IDs on retry, physical deletion                                    | Retained metadata consumes disk; old deliveries cannot resurrect deleted or overwrite newer state.                    |
| [Convergent backfill with a finite completion boundary](docs/adr/004-backfill-consistency.md)        | A scan-only cursor, one long snapshot, waiting for a continuously changing source to empty | Stores checkpoint/fence membership; promises eventual convergence rather than a whole-run point-in-time snapshot.     |

Further decisions cover [atomic staging](docs/adr/008-atomic-pipeline-staging.md), [consumer COMMIT/ACK](docs/adr/011-rabbitmq-consumer.md), [finite backfill implementation](docs/adr/013-backfill-checkpoints-fence.md), [audited operations](docs/adr/014-operational-control-replay.md) and [retained runtime](docs/adr/015-retained-local-runtime.md). Earlier module setup/limitations are retained in [historical notes](docs/history/README-milestones.md).

## Capacity and limits

The verified default is one million distinct roughly-1-KiB entities, with each worker limited to **256 MiB**, Node heap **128 MiB**, each page at most **64 records / 256 KiB**, and per-event validation unchanged. The completed run contained **1,034,667,793 baseline payload bytes**, about 3.85 worker budgets; the largest observed worker process RSS high-water value was 148,201,472 bytes. These are sampled shared-host observations, not a dedicated-hardware benchmark. The earlier 2M plan remains historical and its optional profile is not claimed executed.

[Capacity notes](docs/capacity-notes.md) retain measured CPU, memory, storage, seed/drain/export/oracle duration and bottlenecks. At `93eac82`, 262,144 baselines passed exact reconciliation and cleanup with four scanners/two sink workers; this was a baseline-only capacity run. It does not prove faults at one million rows. On earlier single shared-host 131,072 runs, the same page64 configuration with an equivalent due-index predicate reduced observed completion time from 1,175 to 660 seconds, about **1.78x**, not an asserted 2x. Doubling effective throughput would require roughly halving the limiting stage's per-record service demand or independently doubling its processing capacity without database contention; that is an untested hypothesis, not a promised result.

The accepted million-record run's timestamped fresh observations give **545.24 newly staged events/s** over a 1,827.73-second window and **114.26 validated consumer receipts/s** over an 8,745.16-second window. These are separate cumulative-delta averages, including stalls, not instantaneous rates or rates derived from the entire test duration. Receipt observation lagged behind actual delivery; pipeline PostgreSQL dominated sampled CPU. The initial improvement target is the repeated full-history observation work, with actual plans and concurrent server measurements required before choosing a correction. About 228.52 receipts/s would double this measured receipt-window rate; it is a target, not a result. [Exact windows and limitations](docs/capacity-notes.md#measured-progress-windows).

**Known observation limitation:** the accepted run recorded 505 nonfresh pipeline observations. Final G5 and exact convergence passed, but continuous dashboard availability under load was not established. [Server observation work](docs/observability-closure.md) tracks the separately authorized correction and its required evidence.

## What I did not build and why

Intentionally excluded: cloud deployment, multi-node HA, multi-tenancy, a generic connector framework, arbitrary external effects, Kafka/Kubernetes, automatic retention/GC, live receiver replacement, generic retries, public quarantine replay and additional polished screens. They are unnecessary to demonstrate this assignment and would broaden its failure/authority contract.

## Two concrete AI deviations

1. **Consumer wire accounting.** Requested: admit at most 32 original messages / 1,048,576 canonical wire bytes consistently in application and SQL, with COMMIT before ACK. The generated SQL used a different charge, so a legal 32-message cohort totaling exactly 1,048,576 bytes was charged 1,049,056 and rejected with `P6002`. Real RabbitMQ redelivery and unchanged database effects were retained, not called success. Correction `0a375fd` aligned the forward SQL accounting; clean `bba6465` passed fresh and populated B02-B08/B06H checks. [Reproduction and correction evidence](docs/evidence/M4.1-development.md).
2. **Rejected-update oracle.** Requested: a rejected v2 must leave the previously admissible v1 document (or tombstone) intact, while unexpected missing/corrupt data fails. The generated oracle excluded the whole entity when its latest revision was rejected and falsely failed an unchanged valid v1 receiver. Diagnostic `94c0d74` reproduced the actual mapping rejection and exact old-oracle failure. Correction `d1b3baa` used verifier-owned expected admissible revisions and real negative controls; O01-O06 passed. [Reproduction and correction evidence](docs/evidence/M3.1-development.md).

Neither example is invented to meet a quota. The reports distinguish original review findings, diagnostic reproduction, implementation correction and later acceptance.

## Submission handoff

The source is public at [dojohubco/kill-it-twice](https://github.com/dojohubco/kill-it-twice). The [acceptance matrix](docs/acceptance-matrix.md) separates the exact million-record tested revision, later UI verification and [hosted checks](https://github.com/dojohubco/kill-it-twice/actions). The full local run is not a hosted million-record result. Application submission remains a separate manual action.
