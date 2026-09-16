# SPEC v1: Kill It Twice

Status: accepted initial direction; implementation evidence pending.
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
Later bounded milestones: Elasticsearch semantics; RabbitMQ plus consumer effects; concurrent backfill/completion; retry/lease/replay/admission hardening; independent full verifier; operator UI and measured capacity.

Later order may be refined by review, but correctness invariants are not optional. Each milestone records the exact scope/tests before implementation and reports code identity, commands, results, limitations, and actual deviations afterward.

Passing tests normally add evidence, not a new architecture version. Fix implementation bugs without rewriting the requirement they violated. Update SPEC/ADRs for deliberate decisions, assumption changes, or meaningful contract clarification; retain chronology. Never manufacture a failed attempt, a SPEC v2, or two AI deviations to satisfy an appearance of evolution. At adoption all full gates are unimplemented and not run.
