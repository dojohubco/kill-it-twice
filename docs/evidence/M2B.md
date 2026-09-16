# M2B local acceptance evidence

Recorded 2026-09-17 Asia/Tbilisi; execution timestamps below and in artifacts use UTC (2026-09-16). **PASS for bounded local staging only. G1–G5 remain NOT IMPLEMENTED.**

## Identities and authorization

- Reviewed/baseline documentation HEAD: `735585c3ddb45fa9c2239943102dbc45b37ce710`.
- Accepted prior M2A application/test commit: `6be433da050a1ece890560511e9f957a44dbce16`.
- M2B tested application/test SHA: `f8d7728f1d1f17ff7fc16ae0c29e4b64946e1636`; branch main, clean tracked tree throughout all final commands and 12 isolated integration runs.
- Every final run has input manifest SHA-256 `23de9dd786b97d4f2374416fa0b194b6f33d273fd29b7167f6e3f37cb59dd26b`. Per-file hashes are in each run's inputs.json. No migration 001/002, lockfile, Node pin, TypeScript or ESLint policy change occurred.
- Final documentation HEAD is the later docs-only commit identified by the handoff and local bundle manifest.json. This report does not substitute that later HEAD for the tested code SHA.

Actual start matched the reviewed commit with no later commits or unrelated edits to reconcile. Read SPEC, AGENTS, ADRs 001–006, milestone/evidence files and implementation; inspected branch/status/tools/Docker. No reset, amend, rebase, squash, backdating, push, remote branch/PR, settings change or privileged install occurred. IDs/descriptions in docs/milestones/M2B.md and ADRs 007/008 were committed at 9231de2 before production changes. The executable inventory was present in the hashed developmental tree and committed with the tests; it was not inferred from test results.

Baseline `make quality` exited 0 with 28 unit checks, and `npm run test:integration:m2a` exited 0 with 36 required cases, restart and cleanup. Evidence: artifacts/m2b/baseline-735585c and artifacts/m2a/m2a-20260916195318413-5e3e8dc3. The historical M2A report now has a dated note for successful hosted run 35141624449 at the reviewed HEAD; the reviewer inspected the hosted artifact, not a local rerun. Its cb8cd256…547b artifact digest remains explicitly reviewer-supplied; intake independently checked run metadata. **M2B hosted CI: NOT RUN.** Local actionlint is not hosted acceptance.

## Implemented contract

A caller explicitly selects committed immutable `(entity ID, version)` outbox revisions. The read-only source credential exports the PostgreSQL 18 JSONB text and UTC microsecond timestamp from one coherent statement. All real revisions are mutations. The same revision read from current state and outbox produces identical canonical bytes, including numeric tokens 9007199254740993 and 0.123456789012345678901234567890, arrays, nulls and Georgian/emoji text. A no-op with different numeric formatting does not replace the stored source representation. Baseline envelope shape is tested only by synthetic unit vectors.

The fixed flat v1 envelope uses scoped RFC 8785 serialization; payload_json is an opaque string, never arbitrary numeric JSON decoded into JavaScript Number. All eleven fields participate in native SHA-256. Wire bytes canonically wrap body and content_sha256. Literal checked-in vectors and independent Python hashlib outputs supply expected bytes/hash/wire; PostgreSQL independently validates JSONB-object shape and exact codec round trip. Every body string, identity, timestamp, kind/deletion state, supplied UTF-8 byte representation and hash is checked. SQL also rejects unknown/missing fields, NULL loopholes and mismatched indexed metadata. Hashing does not authenticate source provenance.

Source migration 003 only adds source_reader grants. Pipeline migration 001 creates six tables in a **second actual PostgreSQL service**, with its own database, administrator/runtime credentials and retained volume: source binding, two logical destinations, events, delivery intents, pending consumer observations and sanitized integrity incidents. The pipeline binds one expected source epoch and codec. Its two local destination UUIDs identify unbound logical destinations; they are not claimed receiver identities.

One allowlisted Pipeline.stage call validates/deduplicates a batch, sorts unique event IDs, and uses the shared ADR 005 owner for one READ COMMITTED transaction. Unique-key arbitration happens before obligations; a conflicting inserter reads the winner in a fresh VOLATILE statement. It compares exact bytes and hash. A new event and its two pending sink intents plus one pending observation commit together, enforced with FKs, immutable guards and deferred completeness. Equal repeats return already_staged without changing original bytes, xmin, timestamps or identities. Conflicts or missing obligations roll back the whole attempted batch; existing evidence is never silently repaired. A separate sanitized incident attempt cannot turn the primary failure into success. Original PostgreSQL SQLSTATE/outcome and diagnostic failure remain separate.

The public result is returned only after a confirmed COMMIT tag and connection cleanup. Results inside an owned work callback are tentative until the owner returns. Expired capabilities and overlapping owners reject; callbacks expose no raw client or transaction-control SQL. Unknown completion stays unknown and poisons the owner; an explicit identical repeat using a new healthy owner resolves durable evidence. There is no automatic retry or source ACK.

Application-enforced limits are 16 input records, 64 KiB per canonical wire record and 256 KiB per batch, configured in src/limits.ts. Before transfer, source SQL measures escaped payload text plus a conservative 1024-byte allowance, and returns null payload projections for **all** selected rows if any record or total overflows. The real driver response test confirms this suppression. Actual wire sizes are checked too; SQL separately caps stored body bytes at 64 KiB. Batch/count enforcement assumes the trusted staging facade, which permits only one stage batch per owned transaction. These values are starter configuration, not benchmark conclusions. Oversized records remain durable and explicitly unstaged; absent/uncommitted selections are reported notVisible.

## Commands, profiles and exact results

Capture directory (local): `artifacts/m2b/acceptance-20260916203336043-7bbd3739`. Its commands.json retains UTC start/end times, exit codes, signals, timeout/overflow/cleanup flags and separate complete stdout/stderr logs. All final commands below ran from committed clean code.

| Command                                     | Exit | Result                                |
| ------------------------------------------- | ---- | ------------------------------------- |
| `npm ci --no-audit --no-fund`               | 0    | PASS                                  |
| `npm ls --depth=0`                          | 0    | PASS                                  |
| `npm run format:check`                      | 0    | PASS                                  |
| `npm run lint`                              | 0    | PASS                                  |
| `npm run typecheck`                         | 0    | PASS                                  |
| `npm run knip`                              | 0    | PASS                                  |
| `npm run validate:compose`                  | 0    | PASS                                  |
| `npm run validate:workflow`                 | 0    | PASS                                  |
| `npm run test:unit`                         | 0    | PASS                                  |
| `make quality`                              | 0    | PASS                                  |
| `npm run test:integration:m1`               | 0    | PASS                                  |
| `npm run test:integration:m2a`              | 0    | PASS                                  |
| `npm run test:integration:m2b`              | 0    | PASS                                  |
| `npm run test:integration:m2b -- --upgrade` | 0    | PASS                                  |
| `make verify-m2b`                           | 0    | PASS                                  |
| `make verify-m2b`                           | 0    | PASS                                  |
| `make verify`                               | 2    | Expected nonzero; G1–G5 unimplemented |

Quality passes **30 unit checks** with zero failures/skips/todos/cancellations. M1 requires 22 cases. M2A requires all 22 prior cases plus 14 command cases/controls (36). M2B requires 16 independently named cases: two envelope unit cases and fourteen real PostgreSQL/staging cases/controls. The two unit cases overlap quality; repeated profile counts are not counts of distinct guarantees. Every profile has zero failed/skipped/todo/cancelled cases and no missing/duplicate/malformed results. The acceptance checker retains the contradictory-summary failed-count rejection.

| Isolated run                     | Profile / migration mode    | Required passed | Restart / cleanup |
| -------------------------------- | --------------------------- | --------------- | ----------------- |
| `m1-20260916203402936-741c4a30`  | m1 / fresh                  | 22              | PASS              |
| `m2a-20260916203415906-2c841f72` | m2a / fresh                 | 36              | PASS              |
| `m2b-20260916203430910-1dc149fc` | m2b / fresh                 | 16              | PASS              |
| `m2b-20260916203446998-576696ae` | m2b / populated M2A upgrade | 16              | PASS              |
| `m1-20260916203515451-6d8e4428`  | m1 / fresh                  | 22              | PASS              |
| `m2a-20260916203528466-dc63acdc` | m2a / fresh                 | 36              | PASS              |
| `m2b-20260916203543710-2595e60a` | m2b / fresh                 | 16              | PASS              |
| `m2b-20260916203601540-68d6b3b9` | m2b / populated M2A upgrade | 16              | PASS              |
| `m1-20260916203628376-0e092fd2`  | m1 / fresh                  | 22              | PASS              |
| `m2a-20260916203639981-98a5c2c1` | m2a / fresh                 | 36              | PASS              |
| `m2b-20260916203656900-b711a558` | m2b / fresh                 | 16              | PASS              |
| `m2b-20260916203712819-f2a56037` | m2b / populated M2A upgrade | 16              | PASS              |

Each run has a different persisted source epoch, isolated project/ports/volumes, recorded image and initial empty-table observation before workload. Fresh M2B applies 003 to an empty M2A source. Upgrade M2B first commits one source entity, two revisions and three successful command receipts, then applies 003 and compares every source/receipt row before/after. The upgrade test subsequently queries retained receipts without parsing their numeric payloads in JavaScript. Both databases are restarted with retained volumes; independent full tiny-fixture snapshots, including original event bytes and pending/unbound state, remain identical. Runtime sessions are absent; owned container/volume/network lists are empty after cleanup. Unrelated resources are not cleaned.

Final environment: Node v24.19.0; npm 12.0.2; TypeScript 5.9.3; pg 8.23.0; Prettier 3.9.7; ESLint 10.10.0; typescript-eslint 8.70.0; Knip 6.36.0; actionlint 1.7.12; Docker 29.7.2; Compose 5.5.1. Both services use `postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. Observed server: PostgreSQL 18.6 (Debian 18.6-1.pgdg12+2), x86_64; UTF8; fsync, synchronous_commit and full_page_writes all on. Each manifest records exact container/image identities and settings separately.

## Fault and SQL observations

The following are exact observations from the final upgrade run `m2b-20260916203712819-f2a56037`; every M2B run has corresponding full sql-evidence.jsonl records. Row counts below mean event/delivery/consumer rows for the identified event, with full bytes/hash/metadata/relations independently checked, not a count-only oracle.

| Case | Source command key (outside canonical identity) | Event ID | OS PID / backend PID | Before kill session | Rows before → after kill | Actual exit | Explicit recovery |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S08 | `f0f3fc31-f51c-4808-b138-80f5a4546f63` | `c5bfd891-10db-49fe-830f-edc9a2293bc8:2:1` | 465642 / 112 | idle in transaction; xid 755 | 0/0/0 → 0/0/0 | SIGKILL, code null; 0 success bytes | new PID 465655: inserted |
| S09 | `21b70d42-94ec-41c0-8414-ce7fad9b915b` | `c5bfd891-10db-49fe-830f-edc9a2293bc8:3:1` | 465707 / 123 | idle; xid None | 1/2/1 → 1/2/1 | SIGKILL, code null; 0 success bytes | new PID 465740: already_staged |

S08 independently observed RowExclusiveLock on events, delivery_intents and consumer_observations plus the transaction's ExclusiveLock, with no visible rows. S09 independently observed idle session/COMMIT, no active transaction ID, and the complete committed 1/2/1 set before killing. Both killed the exact child and observed actual signal exit, zero ordinary success bytes and no surviving session. New-process replay re-reads the immutable source revision; post-COMMIT recovery preserves original canonical bytes, hashes, staging timestamps, destination IDs and obligations. Source state is unchanged by staging or recovery.

S10-PRE: process 465776, backend 126, exit {'code': 0, 'signal': None}, exactly one inserted caller result, then new-process already_staged replay; S10-POST: process 465807, backend 129, exit {'code': 0, 'signal': None}, exactly one inserted caller result, then new-process already_staged replay. Their private instrumentation surrounds the actual shared transaction owner and never substitutes raw mock BEGIN/COMMIT or simulated success.

S05/S06 observe actual pg_blocking_pids/Lock boundaries for duplicates, mismatches and rollback takeover. Mixed conflicting batches leave their new entries absent. Independent SQL verifies bytes and native SHA-256, source payload/time/metadata, and common xmin for newly inserted event/obligations. The database rejects each missing sink/consumer at deferred COMMIT (P3002), mismatched metadata/hash or malformed payload/NULL evidence (23514/23502), wrong sink FK (23503), duplicate sink (23505), and prohibited runtime access (42501). Content conflict P3001, missing existing obligation P3002, and deliberate incident-write privilege failure remain explicit failures. Privileged corruption setup is scoped and restored; runtime cannot perform it. Earlier 40P01 deadlock, rollback-tag, command and SIGKILL requirements continue to pass in their unchanged inventories.

S12 retains the over-limit source row, rejects a six-record aggregate byte overflow without partial stage, and stages 25 small explicit fixtures in four pages of at most seven. Page selection does not establish a live watermark or incremental completeness. All destinations remain unbound; all deliveries and consumer observations remain pending. The implemented operation uses only the two PostgreSQL endpoints; no receiver integration or simulated consumer success exists.

## Actual corrections and preserved evidence

The initial source baseline passed. Developmental PostgreSQL staging/upgrade runs passed; no SQL data-loss defect was reproduced. Typed lint initially rejected 13 newly registered native tests as floating promises, corrected with explicit void registration; Knip identified two unused exported types, corrected through an internal body type and the reused public source-config type. No quality rule was disabled globally or downgraded. These early terminal findings are described here, not claimed as retained failing PostgreSQL runs.

Implementation review improved primary SQLSTATE preservation: the staging classifier now retains the original driver error as the transaction cause, with a separate sanitized conflict classification. Further independent tests cover SQL epoch rejection, changed numeric text under the same identity, and shared insertion transaction IDs. A clean capture at c5f88580a7dd6a14d215e83ed127aaae1c366a30 passed but was **superseded** after inspection found that the upgrade diagnostic parsed whole receipt JSON merely to read its UUID. That lookup now occurs inside PostgreSQL, avoiding even discarded numeric conversion. This was a test-path correction, not evidence of altered canonical event bytes or a manufactured SQL failure. The complete final capture was repeated at f8d7728.

Dirty developmental runs remain labeled developmental with input hashes/patches; HEAD alone does not identify those trees. Their logs and the superseded clean capture remain in the local bundle. Final evidence here is exclusively the clean f8d7728 capture.

## Chronological commits and exact changed files

```text
9231de2 docs: authorize M2B lossless encoding and atomic staging boundary
adb3b57 feat: preserve lossless revisions and atomically stage local obligations
968be23 test: prove two-database staging contention crashes limits and source upgrades
df46cce fix: preserve staging SQLSTATE and strengthen independent boundary checks
c5f8858 build: gate M2B with prior profiles fresh upgrades and local review evidence
f8d7728 test: keep upgrade receipt payloads opaque during identity lookup
```

A later documentation-only commit adds this report/compact summary and links verification in ADRs 007/008. The bundle records its exact SHA and full chronological commit metadata.

SPEC adds only the authorized M2B precision/local-staging refinement. ADR 007 fixes lossless identity/encoding, scoped JCS and bounded reads, and records the future baseline/no-op command FK obligation. ADR 008 defines the separate pipeline schema, privileges, immutable/deferred invariants, sorted READ COMMITTED arbitration and outcome contract. No new SPEC version or invented deviation quota is claimed. Earlier ADRs and historical M1/M1.1 reports remain unchanged; M2A receives only its dated subsequent CI qualification.

Changed paths relative to reviewed HEAD, including this documentation handoff:

- `.github/workflows/m1.yml`
- `AGENTS.md`
- `Makefile`
- `README.md`
- `SPEC.md`
- `compose.m2b.yaml`
- `docs/adr/007-lossless-revision-envelope.md`
- `docs/adr/008-atomic-pipeline-staging.md`
- `docs/evidence/M2A.md`
- `docs/evidence/M2B-summary.json`
- `docs/evidence/M2B.md`
- `docs/milestones/M2B.md`
- `knip.json`
- `migrations/003-source-reader.sql`
- `migrations/pipeline/001-staging.sql`
- `package.json`
- `scripts/m1.ts`
- `scripts/migrate-staging.ts`
- `scripts/required-staging-cases.ts`
- `scripts/review.ts`
- `scripts/stage.ts`
- `scripts/validate-tools.ts`
- `src/envelope.ts`
- `src/internal/transaction.ts`
- `src/limits.ts`
- `src/pipeline.ts`
- `src/source-reader.ts`
- `src/source.ts`
- `tests/fixtures/envelopes.json`
- `tests/staging/staging-death.test.ts`
- `tests/staging/staging.test.ts`
- `tests/support/staging-child.ts`
- `tests/support/staging.ts`
- `tests/unit/envelope.test.ts`

## Local handoff and remaining scope

[Compact machine summary](M2B-summary.json) records required IDs, commands/exits, tested SHA/input hashes, profile states, signals and cleanup. Complete sanitized command logs, native reports, SQL/process observations, migration snapshots and restart evidence are under the local capture/run directories. They are not remotely accessible URLs.

`npm run review:bundle -- artifacts/m2b/acceptance-20260916203336043-7bbd3739 --m2b` creates artifacts/review/m2b-final-<documentation-head>/ with tracked.tar, since-reviewed.patch, since-spec.patch, commits.txt, changed-files.txt, manifest.json, prior/development/final evidence and SHA256SUMS. The final handoff supplies the concrete local archive and checksum paths; its manifest identifies the final documentation HEAD without a self-referential checksum in this tracked report.

Remaining: source acknowledgement/capture continuity, real target binding and sink delivery, consumer processing, seed/activation, backfill/fences, leases/retries/DLQ, API/UI, production deployment and large-scale behavior are unimplemented and unrun. The exact future regression remains: seed a protected baseline without mutation outbox, activate, issue unchanged update, retain a successful command receipt without inventing a mutation. Migration 002's existing result-to-outbox FK is deliberately unchanged. No baseline behavior was demonstrated by current source fixtures.

Trusted staging/source credentials and database ownership are explicit boundaries; hashes do not prove authenticity. Callbacks must cooperate and connection-per-transaction costs are not benchmarked. Process-group cleanup cannot guarantee descendants deliberately escaping that group; arbitrary host/power/storage failures were not exhaustively tested. No full G1–G5 guarantee follows from this milestone. **Stop at M2B; await independent review before M2C.**
