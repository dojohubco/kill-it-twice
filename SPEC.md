# SPEC v1: Kill It Twice

Status at initial adoption: accepted direction; implementation evidence pending. Current execution status is recorded separately in docs/acceptance-matrix.md.
Evidence at adoption: no application code, benchmarks, or passing gates.

## 1. Origin and authority

This specification was prepared before implementation using AI-assisted architectural research and review. The initial decisions were made during that work; they must not later be presented as discoveries made by the implementation agent. The candidate remains responsible for understanding and reviewing the submission.

SPEC.md defines the required behavior. Accepted ADRs explain consequential decisions. A milestone task authorizes a bounded subset of the work; it does not silently override this specification. Conflicts require review. More detailed prior research is background, not an unseen repository contract. Introduce required details into the applicable milestone or repository documents before relying on them.

DECIDED means selected, not experimentally proven. ASSUMED states a condition on the guarantee. MUST VERIFY identifies required implementation evidence. OPEN TO MEASUREMENT identifies tuning rather than semantics. OUT OF SCOPE limits the submission. These labels are not mutually exclusive: a decided mechanism can still require verification.

## 2. Problem, goals, and boundaries [DECIDED]

Replicate a controlled relational source into a searchable current-state index and a change stream with an independent consumer. A bounded initial backfill and continuous incremental capture must run concurrently and recover after crashes without silently omitting required work.

The primary acceptance artifact is make verify: reproducible real failures, independent reconciliation, and honest G1-G5 results. Also provide usable operator controls, diagnostic replay, measured capacity notes, and chronological specification/decision history.

Selected stack: TypeScript/Node.js, NestJS, Angular, PostgreSQL, Elasticsearch, RabbitMQ, Docker Compose, Toxiproxy, and Prometheus. Introduce dependencies only when their milestone needs them. Use explicit SQL and protocol-aware adapters on correctness-critical paths. No framework supplies the delivery guarantee.

Source, pipeline, and consumer own separate logical databases and credentials. The source and pipeline must be separate PostgreSQL service/transaction domains when cross-boundary capture is introduced. Pipeline and consumer may share the second service, without a high-availability claim. No cross-database or cross-sink atomic transaction is assumed. M1 needs only the source service.

## 3. Failure model and source permissions [ASSUMED]

Assume permission to modify the source schema and install capture triggers. Runtime writers cannot disable capture, alter protected identity/version metadata, physically remove retained identities, or truncate source tables. These privilege restrictions must be verified during implementation.

Exercise process SIGKILL/restart, retained-volume service restart, temporary dependency outages, lost responses, unknown commit outcomes, duplicate and reordered delivery, concurrent source writes, item-level rejection, and expired ownership while requests remain in flight.

Assume intact retained durable storage, no independent rollback of databases to incompatible backups, cooperating admission controls, and eventual capacity and a sufficiently long failure-free interval for progress. Finite storage cannot buffer unlimited writes. Permanent invalid data requires explicit repair or disposition before claiming full convergence.

## 4. Intended guarantees and invariants [DECIDED; MUST VERIFY]

Target at-least-once transport, monotonic current-state projections, and effectively-once defined consumer database effects. Do not claim end-to-end exactly-once or atomic visibility across sinks. These are intended guarantees until implementation evidence exists.

I1. Every supported, state-changing source row mutation and its immutable versioned outbox after-image commit together or neither commits. Multiple changes in one transaction can produce multiple revisions.

I2. A source event is acknowledged as staged only after its canonical event and required independent sink obligations commit in the pipeline database. Ambiguous outcomes are resolved by durable identity, not assumed rollback.

I3. Backfill checkpoint advancement and the staged events, obligations, and required progress/membership evidence it represents commit together.

I4. One source revision has one stable identity and immutable canonical content. Conflicting content under the same identity is an integrity failure, not a harmless duplicate.

I5. Source versions increase per entity. A lower version cannot replace a higher projection version; equal versions cannot silently change content. Tombstones prevent stale resurrection.

I6. Each sink owns an independent durable obligation. One sink's failure never resets another sink's settled success.

I7. Local work results require current ownership. Lease expiry does not cancel remote requests; remote overlap requires versioning or deduplication.

I8. Consumer inbox deduplication, defined business effects, and projection changes commit in one consumer database transaction before RabbitMQ ACK.

I9. Required events, tombstones, deduplication state, and unresolved work do not expire during the supported verification/replay epoch.

I10. Batches, in-flight requests, memory, retries, and admission are bounded. Missing evidence, quarantined work, and unknown outcomes are never silently counted as success.

## 5. Source, event, and deletion principles [DECIDED]

Use a trigger-backed transactional outbox containing immutable after-images, not references to mutable current rows. The source owns entity versions and stable revision metadata. Meaningful insert/update/logical-delete/restore operations create revisions; no-op updates do not. Entity IDs are not reused within an epoch; a restore advances the retained version.

Logical event identity is (source_epoch, entity_id, entity_version). The epoch survives ordinary restarts. Source-change identity distinguishes mutations from seeded baseline observations. Capture path does not change revision identity or meaning.

Use deterministic canonical event content and JCS/SHA-256 for the later pipeline envelope. Exact schema, safe numeric representation, field coverage, and independent test vectors must be specified and verified before canonical staging is introduced. Identifiers must never be rounded through JavaScript Number.

Incremental capture selects unacknowledged committed events, not a live maximum sequence/timestamp cursor. Sequence allocation is not commit order. Source mutation API retries will require a transactional command idempotency key before an API or retrying generator is exposed.

Baseline seed data must exist before ordinary incremental capture is enabled, so backfill cannot be accidentally replaced by replaying all seed inserts. Implement a controlled activation boundary without disabling capture triggers. M1 does not implement baseline seeding or activation; its fixtures are ordinary captured mutations.

## 6. Backfill and completion [DECIDED; MUST VERIFY]

Backfill is a finite, resumable keyset scan plus continuous reliable capture. It promises eventual convergence, not one long point-in-time snapshot. Late commits behind the scan cursor must be handled by incremental capture. Updates and deletes may race with scanning without regressing sinks.

Distinguish scan completion, delivery drain, and successful completion through a finite completion boundary. The selected design records a durable set of committed source event identities visible at a defined source snapshot and combines it with observed backfill revisions. A maximum sequence value is not that boundary.

Fence creation, restartable membership import, missing-obligation detection, and consumer-receipt evidence must be verified during implementation. Freeze their detailed transactions before the backfill milestone. Later changes must not endlessly extend an already sealed run.

## 7. Sinks, consumer, and recovery [DECIDED; MUST VERIFY]

Elasticsearch uses deterministic entity IDs, strict external source versions, explicit mappings, and persistent tombstone documents. Inspect every bulk item. Equal-version retries require matching canonical content; an older obligation needs valid higher-state evidence before being satisfied as superseded. Treat unknown outcomes as retryable. Detect unexpected target replacement; never silently recreate or replace a target.

RabbitMQ uses durable topology, persistent mandatory publication, publisher confirms, and a quorum queue with an explicit redelivery policy supporting the repeated-kill tests. Preserve the selected no-silent-expiry policy, including an unlimited-redelivery setting paired with bounded consumption, outage cooldown, quarantine, and storage controls. A confirm is broker acceptance, not consumer processing. Validate routing and reconnect behavior against a real broker before asserting the guarantee.

The independent consumer maintains an inbox, a monotonic projection, and a demonstrable database business effect: one audit entry and one aggregate unit per unique captured source mutation. Baseline observations do not count as new actions. An unseen older mutation still gets its one effect without replacing newer state. Duplicate delivery gets no additional effect.

Retries are persisted, delayed, jittered, and sink-isolated. Infrastructure outages are not mass data errors. Use leased PostgreSQL work claiming with generation checks; verify stale results and requests that outlive leases. Per-sink cooldown/circuit behavior must prevent busy loops.

DLQ and replay are sink-specific, diagnostic, idempotent, and audited. Replay preserves historical content. Receiver repair may permit the same event; source-data correction creates a new version. Exact state tables, indexes, counters, and adapter calls are milestone designs, not proven here.

## 8. Verification and observability [DECIDED]

G1: real crash and durable resume, without omission or a full restart.
G2: observed duplicate transport and reordered revisions, without duplicate business effects or stale final projections.
G3: a real Elasticsearch outage, durable backlog, bounded retry activity, independent sink progress, and automatic recovery.
G4: a real 500-item Elasticsearch bulk with exactly three receiver rejections; 497 settle successfully and three retain diagnostic DLQ evidence.
G5: metrics, logs, and UI answer progress, useful throughput, lag, DLQ, and health without reading code.

Use deterministic barriers to locate dangerous windows, but real SIGKILL/service/network faults to cause failure. Test real databases/broker/search for their semantics. Isolate resources, impose deadlines, preserve failure artifacts, and distinguish partial milestone evidence from a full gate PASS.

The final oracle must be independently implemented, must not use production normalization as its only authority, and must compare identity, version, actual payload, tombstones, and mutation effects. Counts and application-generated hashes alone are insufficient. Journal expected mutations independently of capture. Use bounded-memory exports/streaming or disk-backed comparison and test the oracle with intentional corruption.

Expose source freshness, scan/drain state, attempts versus useful settlements, per-sink backlog, broker confirmations versus consumer receipts, retry/DLQ status, and resource pressure. Idle with a fresh empty observation differs from unreachable/unknown. Define metric denominators, clocks, and restart behavior before implementing the dashboard.

Observe consumer receipts by explicit event identity, not an allocation watermark.

The UI eventually covers status, entity search/details, start/pause/resume, configuration, replay, and allowlisted source/fault simulation. It is an operator tool, not a design-system showcase.

## 9. Open questions, measurement, and exclusions

[MUST VERIFY] Trigger/role enforcement; concurrent revisions; unknown commit recovery; canonical bytes; cross-database staging/ACK gaps; completion fence; lease fencing; actual bulk/confirm/ACK behavior; replay races; independent oracle; capacity admission. Must be verified during implementation.

[OPEN TO MEASUREMENT] Page/bulk rows and bytes, concurrency, prefetch, transaction batching, leases, request deadlines, retry/circuit thresholds, memory budgets, indexes, progress aggregation, disk margins, and capacity. Choose explicit bounded starter settings when needed and record them; none is a benchmark conclusion.

Plan a 2,000,000-entity full verification profile, with a varied roughly 1 KiB payload and an enforced worker memory budget that makes all-in-RAM loading impossible. Validate local feasibility and document any justified scale change. G4's 500/3 fixture is an acceptance requirement, not a throughput tuning result. No performance results exist at adoption.

[OUT OF SCOPE] Generic connector platform, custom WAL decoder, Kafka, Redis locks, Kubernetes, distributed transactions, arbitrary external side effects, multi-host HA claims, live target replacement, unproven retention/GC, sophisticated visual polish, and invented AI deviations.

## 10. Initial milestone sequence and change policy

M1: source mutation/version/outbox atomicity, role enforcement, commit-order characterization, and real writer SIGKILL tests. One PostgreSQL service; small fixtures; no capture acknowledgements or pipeline database.
M2: canonical staging, independent durable sink obligations, source command idempotency, and staging-before-source-ACK across two PostgreSQL services.
M2A is the separately reviewed source-command subset: caller-supplied epoch/command identity, atomic successful receipt/result retention and replay under the same normalized request. Only successful outcomes are retained; new keys denote new commands. ADR 006 defines this source-only boundary. Canonical staging, sink obligations and source acknowledgements remain later M2 work.
Later bounded milestones: Elasticsearch semantics; RabbitMQ plus consumer effects; concurrent backfill/completion; retry/lease/replay/admission hardening; independent full verifier; operator UI and measured capacity.

Later order may be refined by review, but correctness invariants are not optional. Each milestone records the exact scope/tests before implementation and reports code identity, commands, results, limitations, and actual deviations afterward.

Passing tests normally add evidence, not a new architecture version. Fix implementation bugs without rewriting the requirement they violated. Update SPEC/ADRs for deliberate decisions, assumption changes, or meaningful contract clarification; retain chronology. Never manufacture a failed attempt, a SPEC v2, or two AI deviations to satisfy an appearance of evolution. At adoption all full gates are unimplemented and not run.

### M1.1 source transaction ownership clarification

The managed source transaction API owns its session and permits one active transaction per owner. Work receives an expiring mutation capability, not an arbitrary SQL client; nested/concurrent owner use and manual transaction control are unsupported. Success requires confirmed COMMIT completion. Known rollback, unknown COMMIT and confirmed commit followed by cleanup failure remain distinct; cleanup cannot retrospectively prove an ambiguous COMMIT failed. Preserve the primary PostgreSQL error separately from cleanup errors. ADR 005 specifies this contract gap exposed by external review; source capture invariants and M2 scope remain unchanged.

### M2B lossless envelope and local staging refinement

M2B authorizes bounded committed source-revision reads and atomic staging in a separate PostgreSQL service. ADR 007 defines the exact v1 envelope: schema_version=1, source_epoch, entity_id, entity_version, deterministic event_id, source_change_id, source_recorded_at, kind, is_deleted, payload_encoding=pg18-jsonb-text/v1 and payload_json. JCS canonicalizes this envelope; payload_json is the source's exact PostgreSQL JSONB object text as an opaque string, never lossy JavaScript numeric JSON. The codec is fixed for the epoch. Hash canonical body bytes with SHA-256, then canonically wrap body and content_sha256 for the wire. Baseline shape is synthetic-only until seeding is independently authorized; all current source revisions are mutations.

ADR 008 requires immutable event bytes/metadata and exactly two pending sink intents plus one pending consumer observation in one confirmed pipeline COMMIT. Destinations remain unbound. Equal identity/content replays; conflicts or missing obligations roll back without overwrite or silent repair. There is no source acknowledgement, incremental completeness, delivery or consumer-success claim. This pre-implementation precision refinement is authorized from existing M2A fixtures, not an invented discovery. The future baseline/no-op command FK integration requirement is recorded in ADR 007 and intentionally unsolved here.

### M2C bounded capture and staged acknowledgement refinement

ADR 009 implements the planned incremental transfer boundary with three separate owned transactions: source claim COMMIT, identity-bound pipeline stage COMMIT, then current-claim source ACK COMMIT. A separate source work table retains every captured mutation through pending/leased/blocked/acknowledged states; source-clock expiry and exact owner/generation checks fence local results. No moving allocation watermark exists. Controlled initialization binds one immutable pipeline-instance UUID and transactionally enqueues all retained outbox evidence under a source write lock. Worker startup never registers a replacement pipeline automatically.

Canonical v1 content and source business-command semantics remain unchanged. Capture retries only safe staging, retains bounded jittered eligibility delays, renews through independent short sessions and explicitly reports blocked/delayed/unknown state. Stage-before-ACK is a trusted protocol invariant; source SQL cannot independently prove remote durability. ACK denotes complete local staging while both sinks and consumer observations remain pending and destinations unbound. Seed/activation, backfill and real sink/consumer delivery are later milestones; G1–G5 remain unimplemented.

### M2C.1 source mutation isolation clarification — 2026-09-17

Real PostgreSQL reproduction confirmed that registration's outbox lock alone cannot refresh a REPEATABLE READ writer snapshot established before registration began. Migration 004 allowed that legacy writer to commit an outbox revision without work when its enqueue binding SELECT saw the old empty state. The immutable outbox survived and the capture summary stopped the worker on missing work; this was not false acknowledgement.

The supported mutation contract with the capture extension is READ COMMITTED for every state-changing source mutation, including the legacy writer. Forward migration 005 enforces SQLSTATE 25001 in the common enqueue trigger before any binding lookup, aborting the mutation/outbox atomically. READ COMMITTED writes before registration remain retained for initialization, and writes afterward enqueue atomically. Read-only snapshot transactions remain allowed; a legacy no-op creating no revision does not reach this trigger. Command calls, including no-op receipt creation/replay, retain their existing READ COMMITTED precondition. The legacy exclusion from command idempotency never permits unschedulable mutation history. Earlier migrations and canonical/ACK history remain unchanged.

### M3 first durable Elasticsearch projection — 2026-09-17

ADR 010 adds real Elasticsearch delivery from the immutable pipeline ledger. Canonical v1 bytes/codec and source command/capture semantics remain unchanged. Search-v1 uses full-state indexing at epoch:entity_id with strict external BIGINT versions and indexed tombstones. PostgreSQL extracts only optional top-level name, country and loyalty_points into bounded precise JSON text; every source value remains in canonical_body_json even when unindexed. Rejected mapper values become retained sink-specific dead letters. A corrected source command creates a new revision; it never edits the original event or failure.

Controlled initialization binds the existing ES destination to a concrete unique index, real cluster/index UUIDs and a verified projection configuration. Runtime startup cannot create or adopt a receiver. Pre/post UUID checks detect replacement but are not atomic remote fencing; retained storage, restricted credentials and no concurrent administrator replacement are assumptions. One retained node with request translog durability provides no node-loss HA claim.

ES obligations progress through pending, leased, retry_wait, satisfied or dead_letter under exact owner/generation and database-clock leases. Claim COMMIT precedes network I/O. Local outcome settlement follows complete item parsing and target validation, with realtime GET and matching local ledger evidence for equal/higher conflicts. Unknown responses remain retryable; auth/configuration/integrity failures block target admission. Cooldown has one fenced recovery probe. Every bound applies before unbounded transfer, and all automatic transport retries are disabled.

Complete staging now means the two structural sink obligations and consumer observation exist; equal restaging preserves valid ES progress. RabbitMQ delivery and consumer observation remain pending. Source ACK still means staged, independent of ES settlement. Database functions enforce ownership, local witness and relational evidence; they cannot independently prove a remote HTTP write. The trusted adapter performs that protocol. Backfill, baseline seeding/activation (including the known no-op receipt FK obligation), RabbitMQ/consumer, UI and full G1–G5 acceptance remain outside M3.

### M3.1 finite degraded-fixture reconciliation — 2026-09-19

A later source revision rejected by Elasticsearch does not remove an already accepted receiver document or tombstone. Finite quiescent verification distinguishes latest source state from the highest independently expected admissible revision. It requires that exact older revision (or absence if none was admissible), reports the latest rejected state as unresolved/degraded, and requires the actual receiver error with its retained diagnostic. Expected rejections are declared by the verifier before delivery and bound to source command/revision/content; pipeline-owned failure state cannot authorize exclusions. A later valid correction advances normally without rewriting earlier errors. This clarifies the deterministic fixture oracle, not a new delivery state or product mapping engine.

## M4 authorized broker and consumer refinement — 2026-09-19

ADR 011 specifies the first real RabbitMQ and independent consumer implementation. Broker confirmation is acceptance, not consumer processing. The consumer's one audit row and one aggregate unit per unique captured mutation commit with its inbox and monotonic projection in a separate database before an individual AMQP ACK. Unseen older mutations retain effects without replacing newer state; synthetic baseline protocol observations have no business action. Publication/redelivery can repeat; canonical content never changes. Explicit matching consumer receipts advance independent observation evidence, never a confirm or a maximum allocation ID.

M4's application-owned topology registration cannot detect an identically recreated broker queue through standard AMQP. Intact storage, trusted runtime behavior and no unauthorized topology replacement/purge are explicit assumptions. Bounded quorum retention disables the delivery limit for repeated crash recovery; ready-body queue limits are not exact total-storage limits. This authorized design requires the independent MQ01–MQ16 and healthy-control acceptance before implementation success can be claimed. No seeding/backfill, replay API, UI, external side effects, HA or full G1–G5 claim is added.

## M4.1 consumer byte-accounting clarification — 2026-09-19

The normal consumer transaction limit is 32 original received messages and 1048576 bytes of their canonical wire-body buffers, including the unchanged body/hash wrapper. Equal boundary values are admitted. Deduplicating identical events reduces database work but never reduces original application count/byte retention accounting. Consumer migration 002 replaces the demonstrated body+108 SQL estimate with the octet length of the exact reconstructed canonical wire (93 wrapper bytes for a valid hash). The ceiling, protocol, effects and COMMIT-before-ACK semantics are unchanged. This byte limit is distinct from bounded metadata, prefetch retention, decoded copies and hex SQL parameter overhead; it is not a total heap-memory claim.

## M5A retained baseline bootstrap and activation — 2026-09-20

ADR 012 authorizes genuine retained version-1 source baselines with NULL change identity. Controlled bounded chunks commit ordinal membership, current entities, immutable baseline revisions and progress together, without mutation outbox/work/command receipts or automatic staging. A closed bootstrap seals complete recipe evidence, then atomically binds capture and becomes active. Ordinary mutations retain READ COMMITTED capture enforcement; baseline no-ops retain exact historical receipts against baseline evidence, while mutation receipts match outbox evidence. Existing active epochs cannot be reseeded or reclassified.

Explicit bounded baseline reads preserve the frozen eleven-field envelope/codec and source revision kind. Selected small fixtures use existing real sinks; baseline inbox/projection includes a zero-unit aggregate without a mutation effect. Mutation backlog zero does not mean baselines were replicated. No scanner, pipeline run checkpoint, completion fence or scale acceptance is added.

## M5B finite concurrent backfill refinement — 2026-09-20

ADR 013 refines the planned convergent model: bounded keyset reads of current entities through a retained upper key, with live mutation capture continuing independently. A scan observation retains its real baseline/mutation kind. Each pipeline page commits canonical staging, structural obligations, run membership, immutable batch evidence and matching checkpoint together under current range ownership. The separate source fence atomically retains all mutation identities visible in one source statement snapshot; its bounded import is checkpointed with staging. Allocation order is not commit order, and backfill never acknowledges source work.

The finite required set is observed scan revisions union the sealed mutation fence. It freezes after import. Delivery lag remains draining; all-terminal sink errors/quarantine produce complete_with_errors, while missing membership/content/integrity evidence blocks. Successful complete requires ES satisfaction, broker confirmation and a separately validated processed consumer receipt for every member. Post-fence mutations continue through capture without extending that run. Pause is durable admission control, not rollback of in-flight work. Source/read and remote-receipt truth retain their documented trusted adapter boundaries; no cross-database FK or distributed COMMIT is claimed. This authorizes small real-service backfill acceptance, not UI/G5, public replay, deployment or scale acceptance.

### Operational recovery completion — 2026-09-21

The pre-existing local M6 HTTP adapter is retained; this completion introduces no additional HTTP routes or frontend. Bounded direct domain/CLI operations add atomic ES replay selection (1..50), immutable request/item-to-attempt evidence, explicitly verified higher-version supersession, and fenced same-target verification/resume. Canonical events, source ACKs, other-sink outcomes and historical completed-run membership/outcome/time remain unchanged. A current recovery view is separately named; missing progress/obligation evidence cannot become a satisfied denominator.

Independent component observations retain timestamped last-known values separately from unavailable current data. Fixed-series useful rates use exact counter deltas, validated instance identity and monotonic sampling; warming/reset/unavailable are not fabricated zeroes. Source-recorded age is not commit lag. Query cost and full-scale behavior remain unmeasured. The actual new execution evidence belongs in the M6 completion report, not this prospective contract.

### Final acceptance dataset decision, 2026-09-23

Prospectively revise the default acceptance profile to 1,000,000 distinct baseline entities using the existing approximately 1-KiB recipe and retained 256-MiB worker limits, plus the declared concurrent mutation/failure workload. The original assignment requires demonstrably nontrivial bounded-memory replication, not exactly 2M rows. Preserve the earlier 2,000,000 plan and its optional explicitly selected profile; no smaller run proves either full profile. `make verify` now executes the actual selected workload and fails on missing evidence, failed reconciliation or cleanup. The closure note records resources, finite deadlines and stopping conditions before implementation. All invariants I1-I10 remain unchanged.

### Observation under sustained load, 2026-09-26

The accepted finite workload's final G5 PASS does not establish continuous observation availability: 505 pipeline observations were nonfresh. The separately authorized server correction must retain every failed observation and report active-processing status/metrics latency and availability. ADR 017 selects an isolated prototype of narrow PostgreSQL observation projections, maintained in the same transaction as each original row transition. They are a read optimization, not an asynchronous cache or a new completion authority. Original evidence, missing/pending semantics, permissions, read deadlines and independent terminal/content validation remain required. Adoption depends on measured benefit, populated-upgrade/concurrency evidence and new real server acceptance; the earlier PASS keeps its original tested identity.
