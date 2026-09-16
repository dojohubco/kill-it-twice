# M2A evidence — 2026-09-16

**Local M2A source-command acceptance: PASS. M2A remote CI: NOT RUN. G1–G5: NOT IMPLEMENTED.**

Reviewed/start baseline: `e5813a4bcd7857ba816ac6ad4537cc7a52f86963`. M1.1 application/test baseline: `7cb3dbc4da0dca4e7fa683d06bd5198a15c05cd0`. Initial specification: `251cb5a5b095c204dc99985b3eae1e21aac3c6ce`. Final tested code: **`6be433da050a1ece890560511e9f957a44dbce16`** on main, clean in all four final integration manifests. This report and its machine summary are later documentation; the final documentation SHA is in the handoff and bundle manifest, avoiding a self-referential SHA.

All four final runs recorded input-content SHA-256 **`6331c9651c1386c871732e83dc6dd5442b52aa2e3193fd66a26fe0fffb9118c3`**. Per-file hashes are in each run's inputs.json. The start matched the reviewed HEAD, with no later commits or unrelated edits to discard. Local history was preserved. No push, remote branch/PR, publication, settings change, privileged host install or global Git change occurred.

## Baseline and authority

M1.1 was accepted by the independent reviewer for source-only behavior after code review and inspection of subsequent hosted CI run/artifact 35135496960. The reviewer did not rerun PostgreSQL locally. Read-only GitHub CLI inspection during this task confirmed that run completed successfully at e5813a4, 18:36:53–18:37:50 UTC. Its later date is appended to [historical M1.1 evidence](M1.1.md); the original report has not been rewritten as if CI had already run. That hosted run is not M2A evidence.

At clean e5813a4, `make quality` exited 0 with 27 unit cases and `npm run test:integration:m1` exited 0 with all 22 required cases, retained restart and cleanup. Baseline run `m1-20260916191035872-55186fa0` and local logs/metadata under `artifacts/m2a/baseline-e5813a4/` are retained. There was no prerequisite/invariant blocker. The pre-implementation scope, independent inventory and ADR 006 were committed in 9e8cf25 before production changes. Command idempotency was already planned in SPEC; it is not represented as a newly discovered requirement.

## What the command key means

The same `(expected source epoch, caller-supplied command UUID)` and equal normalized request recovers the original committed result. A fresh key denotes a different command. A rolled-back attempt leaves no successful receipt; an explicit later same-key attempt can execute against then-current state. The owner never retries or generates a replacement key.

One command invocation creates, updates, logically deletes or restores one entity. Source, outbox and successful receipt commit atomically. PostgreSQL JSONB equality defines payload equality; whitespace/key order and insignificant numeric formatting replay, while array order, missing versus JSON null and different numeric values matter. Identifiers remain decimal strings and payload remains SQL JSON text, including numbers beyond JavaScript precision. No arbitrary payload JSON.parse, Number conversion or hash-only comparison is used.

The retained snapshot includes entity ID, epoch, version, change ID, source time, deletion state and payload. Command timestamps are UTC with six fractional digits. `replayed` describes an attempt, separately from its immutable result. Historical create/update/delete/restore replays precede current lifecycle checks; no-op receipts add no revision. Only successes are retained, for the supported epoch without TTL/GC. Stable replay of failed responses is not promised.

Migration 002 adds one table, three functions and three triggers. Unique reservation insertion precedes mutation. A competing insert waits for commit or rollback; a separate statement in the VOLATILE function reads a fresh READ COMMITTED snapshot. Completed rows cannot change, and a deferred constraint rejects an incomplete reservation at commit with P2003. P2001 / SourceTransactionError.kind idempotency_conflict identifies a request conflict; P2002 / source_epoch_mismatch identifies a wrong epoch. The original PostgreSQL error/SQLSTATE remains separate from cleanup and outcome evidence.

The existing Source owner now starts explicit READ COMMITTED and offers a small command operation/facade. It still checks COMMIT tags, expires capabilities, rejects ownership overlap and poisons ambiguous/broken sessions. The restricted source_command login can execute only execute_command, has no table reads/writes, and cannot call legacy mutations or assume owner roles. The old source_writer remains for M1 SQL scope; arbitrary use of that non-idempotent interface is outside the new guarantee. This is source-command deduplication, not end-to-end exactly-once.

## Commands and independent runs

Exact command arguments, times and exits: `artifacts/m2a/acceptance-20260916192849933-98dfcb19/commands.json`. Numbered `<NN>.stdout.log` and `<NN>.stderr.log` files retain full sanitized output. The [compact machine summary](M2A-summary.json) records required case IDs, input hashes, versions, actual signals and cleanup. Paths are local to this checkout, not public evidence URLs.

| Log | Command                        | Exit | Result                         |
| --- | ------------------------------ | ---: | ------------------------------ |
| 01  | `npm ci --no-audit --no-fund`  |    0 | PASS                           |
| 02  | `npm ls --depth=0`             |    0 | PASS                           |
| 03  | `npm run format:check`         |    0 | PASS                           |
| 04  | `npm run lint`                 |    0 | PASS                           |
| 05  | `npm run typecheck`            |    0 | PASS                           |
| 06  | `npm run knip`                 |    0 | PASS                           |
| 07  | `npm run validate:compose`     |    0 | PASS                           |
| 08  | `npm run validate:workflow`    |    0 | PASS                           |
| 09  | `npm run test:unit`            |    0 | PASS                           |
| 10  | `make quality`                 |    0 | PASS                           |
| 11  | `npm run test:integration:m1`  |    0 | PASS                           |
| 12  | `npm run test:integration:m2a` |    0 | PASS                           |
| 13  | `make verify-m2a`              |    0 | PASS                           |
| 14  | `make verify-m2a`              |    0 | PASS                           |
| 15  | `make verify`                  |    2 | Expected G1–G5 NOT IMPLEMENTED |

Every unit invocation passed **28/28**, with zero failures, skips, cancellations or todos. The original M1 profile passed **22/22**. Standalone M2A and both fresh `make verify-m2a` runs passed **36/36** each, with zero failed/skipped/cancelled/todo cases. Full `make verify` ran once in this task and exited **2** as expected (recipe exits 1), reporting all G1–G5 NOT IMPLEMENTED.

| Run / isolated Compose project   | Passed cases | Host port before → after restart | Persisted epoch                        |
| -------------------------------- | -----------: | -------------------------------- | -------------------------------------- |
| `m1-20260916192915549-e08c5d9f`  |           22 | 32812 → 32813                    | `5762bfde-92ec-42e2-aaf4-2a28ca2bebe9` |
| `m2a-20260916192928782-42aec31e` |           36 | 32814 → 32815                    | `828c5682-633e-420a-ba7e-175a5b1c66f7` |
| `m2a-20260916192956993-88c12abd` |           36 | 32816 → 32817                    | `1bc511f6-0f74-4dd6-81a1-c54c4bbdca70` |
| `m2a-20260916193022603-47481ae3` |           36 | 32818 → 32819                    | `32bfe4cf-7187-4603-8ea2-b916bf4e1f39` |

Each invocation observed empty source/outbox tables before any workload; M2A also observed zero receipts. Orchestration owns this assertion, so test filename order cannot remove it. The M1 run retained 16 entities/33 revisions. Every M2A run retained 31 entities/61 revisions/30 completed receipts. Counts supplement independent field/history comparisons. Entire epoch/entity/outbox/receipt snapshots matched across retained-volume restart. Runtime sessions and test instrumentation ended; all run-owned containers (including stopped ones), volumes and networks were absent after cleanup. Temporary credentials were removed. Other resources were not targeted.

## Required case observations

| Case                      | Actual assertion/evidence                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C01                       | Create/update/delete/restore versions 1/2/3/4; entity ID 9007199254741993 and numeric values 9007199254740993 and 12345678901234567890.1234567890123456789 preserved in text; snapshots match independent receipt and outbox reads.                                                                                                                                                                                       |
| C02                       | Twelve same-owner sequential replays with reordered/whitespace-varied JSON and 1.00 versus 1; identical original result, unchanged database counts/state, expired command capability rejected.                                                                                                                                                                                                                            |
| C03                       | Changed operation, target, numeric value, array order, missing/null field and contract version reject with P2001; wrong epoch gives P2002; invalid shape/version requests fail without a receipt or mutation. Both target states and total counts remain unchanged.                                                                                                                                                       |
| C04                       | Original create/update results survive later deletion; delete/restore results survive restoration and later update. Replaying successful restore while live succeeds from the receipt.                                                                                                                                                                                                                                    |
| C05                       | Unchanged update and repeated delete retain successful receipts with no extra revision; replay after restoration still returns each original result.                                                                                                                                                                                                                                                                      |
| C06                       | Real waiter backend 189 blocked by 188, transaction 780, before release in the final repeat. Commit causes replay with identical result; total entities/outbox/receipts each increase by exactly one.                                                                                                                                                                                                                     |
| C07                       | Real differing-request waiter 191 blocked by coordinated winner 190, transaction 781. Winner commits, loser reports P2001, only one committed entity/event/receipt.                                                                                                                                                                                                                                                       |
| C08                       | Waiter 193 blocked by first owner 192, transaction 782. Explicit callback failure yields confirmed ROLLBACK with its original cause. Waiting attempt then executes once; failed entity and event are absent, only the survivor receipt remains.                                                                                                                                                                           |
| C09                       | Real SIGKILL after finalized command work before COMMIT; independently invisible receipt/source/outbox and held transaction/write locks; no persisted partial state; new-owner same-key retry executes once.                                                                                                                                                                                                              |
| C10                       | Independently visible completed receipt/result/source/outbox and idle COMMIT session before SIGKILL; explicit new-owner retry returns original result with replayed=true and no extra event.                                                                                                                                                                                                                              |
| C11                       | Real scoped outbox trigger failure P9002 removes the request reservation and source mutation together. After instrumentation removal the same key executes successfully. A failed restore is also retried successfully after a later delete, illustrating success-only retention.                                                                                                                                         |
| C12                       | 26 actual command-login bypass attempts denied with 42501; function ACL/owner/volatility, role capabilities, fixed search paths and expanded catalogs checked. Completed receipt UPDATE/DELETE/TRUNCATE rejected; privileged test insertion of an incomplete reservation fails COMMIT with P2003 and leaves no row. Non-READ-COMMITTED raw command rejected with 25001. Timezone/search-path changes do not alter replay. |
| C09-HEALTHY / C10-HEALTHY | Each private barrier released normally: exit 0, exactly one ordinary success, completed receipt/source/outbox, ended session and harmless subsequent replay.                                                                                                                                                                                                                                                              |

All 22 original cases remain mandatory, including both original T08/T09 SIGKILLs, both healthy controls, both orphan controls, real 40P01 deadlock, commit-tag/outcome regressions and database observation disposal. T06 now enumerates the exact nine functions and nine triggers for M2A (six each for M1), retains every legacy grant check, and verifies owner/search-path/PUBLIC execution restrictions for additions. Full source-history inspection remains limited to tiny diagnostic fixtures.

## Actual final-repeat command faults

Run `m2a-20260916193022603-47481ae3`; epoch `32bfe4cf-7187-4603-8ea2-b916bf4e1f39`.

| Case | Command UUID                         | OS PID / backend PID | Transaction observation               | Entity / version | Actual exit    |
| ---- | ------------------------------------ | -------------------- | ------------------------------------- | ---------------- | -------------- |
| C09  | ec7e202d-13b8-41b4-8af8-69428dca85c6 | 342316 / 104         | Active xid 755                        | 1 / 1            | null / SIGKILL |
| C10  | 50cf0498-68fb-4183-8294-ed9d0dab348a | 342357 / 115         | Idle, no active xid; receipt xmin 757 | 3 / 1            | null / SIGKILL |

Both had zero ordinary-success bytes before/after kill and independently absent backend sessions afterward. C09 source change fa50852e-b6ec-486f-bc6b-0c05c2496a6b did not persist; explicit same-key retry created entity 2/version 1/change c7c3abf3-c281-46db-8ee7-1f089fab09e6, replayed=false. C10 recovered entity 3/version 1/change 22b76ce1-911f-4223-9c8b-e754a41a0ab0 and original time 2026-09-16T19:30:28.093958Z, replayed=true. Further explicit replays added nothing.

The original T08/T09 kills also occurred in this repeat: OS/backend PIDs 342739/269 and 342775/274, xids 845/847, actual SIGKILL exits with no caller success. New healthy-control PIDs 342393/119 and 342408/123 each exited 0 and emitted one success. Full observations, command payload text, locks, receipt/source/outbox before/after values and replay results are in sql-evidence.jsonl, with private telemetry distinct from success output. Process identities from all other runs are retained separately; the summary never substitutes one run's evidence for another.

## Development, quality and limits

Developmental PostgreSQL runs `m2a-20260916192245778-10df61e3` and `m2a-20260916192705121-b2b8e857` both passed 36/36 with restart/cleanup. They were dirty runs identified by content hashes/patches, not final acceptance. The first exploratory run overlapped private test typing cleanup and is not promoted as a uniquely committed snapshot. No M2A PostgreSQL defect was demonstrated during these runs. The new private fixtures initially failed TypeScript checks for a too-narrow inventory tuple, returning undefined as string, and narrowing a mutable unknown-valued object across a callback; typed ESLint also rejected unknown rejection reasons, an unbound-method truthiness check and unsafe native once() array destructuring. Corrections use explicit inventory types, narrowed Error/string values and an unknown-array adapter, without blanket any, double casts, non-null assertions or global disables. The small explicit receiver-binding exception matches existing private pg instrumentation. Developmental logs/notes remain in the local bundle.

All existing exact pins and quality tools remain; no dependency or tool was added. npm ci and all seven quality subcommands passed, including Prettier, typed ESLint with zero warnings, strict TypeScript (skipLibCheck false), Knip with explicit new private child entry, Compose configuration and pinned actionlint. A contradictory summary fixture now explicitly requires failed=0; this is defensive hardening, not a claim that accepted CI run 35135496960 contained failed cases. Its reported native failed count was zero according to the reviewed artifact.

Environment: Fedora Linux 7.2.5-200.fc44.x86_64; Git 2.55.0; GNU make 4.4.1; Node 24.19.0; npm 12.0.2; Docker 29.7.2; Compose 5.5.1; PostgreSQL 18.6 (Debian 18.6-1.pgdg12+2), fsync/synchronous_commit/full_page_writes on. Image remains postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af. Existing pins: pg 8.23.0, TypeScript 5.9.3, ESLint 10.10.0, typescript-eslint 8.70.0, Prettier 3.9.7, Knip 6.36.0 and actionlint 1.7.12. Full package inventory is log 02.

The minimal workflow now selects make verify-m2a and sanitizes its evidence on failure, retaining the previously reviewed action SHAs/toolchain/image, permissions and timeout. Local workflow validation passed. **No M2A hosted CI run exists from this task; nothing was pushed.** No requested local check remains unrun. Separate security/advisory and targeted property/mutation work remains future scope.

## Chronology, exact changes and review bundle

| Commit  | Change                                                                                                          |
| ------- | --------------------------------------------------------------------------------------------------------------- |
| 9e8cf25 | Authorized M2A scope/inventory, ADR 006, narrow SPEC clarification and dated subsequent M1.1 CI note            |
| 563313d | Forward receipt migration, command facade/category and explicit READ COMMITTED using the existing owner         |
| f697f6d | Real command behavior/concurrency/fault tests, extended acceptance inventory selection and failed-summary check |
| 6be433d | Same-command local workflow and reproducible M2A capture/summary/bundle tooling; final tested code              |

The subsequent documentation commit adds this evidence/summary and records verification after the original ADR decision. The complete chronological commit metadata, exact changed-file list and full binary-capable diff from e5813a4 are in the local bundle, together with a diff from the initial specification. Exact changed paths:

- .github/workflows/m1.yml; AGENTS.md; Makefile; README.md; SPEC.md; knip.json; package.json.
- docs/adr/006-source-command-receipts.md; docs/milestones/M2A.md; docs/evidence/M1.1.md; docs/evidence/M2A.md; docs/evidence/M2A-summary.json.
- migrations/002-source-commands.sql; src/source.ts.
- scripts/acceptance.ts; scripts/m1.ts; scripts/migrate.ts; scripts/required-command-cases.ts; scripts/review.ts.
- tests/commands/commands.test.ts; tests/commands/command-death.test.ts; tests/support/command-child.ts; tests/support/commands.ts; tests/support/db.ts; tests/support/writer-child.ts; tests/integration/source.test.ts; tests/integration/writer-death.test.ts; tests/unit/acceptance.test.ts; tests/unit/transaction-errors.test.ts.

SPEC adds only the M2A split/retention boundary to the already planned M2 sequence. ADR 006 adds the concrete identity, success-only retention, request/result, SQL isolation/reservation/grant and replay contract. ADRs 001–005, migration 001, package-lock, PostgreSQL Compose pins and quality configurations apart from Knip's private entry are unchanged. No SPEC v2 or invented deviation count is introduced.

`scripts/review.ts` creates `artifacts/review/m2a-final-<documentation-head>/` containing tracked.tar, since-reviewed.patch, since-spec.patch, commits.txt, changed-files.txt, manifest.json, baseline/development/final sanitized logs and SHA256SUMS. Those paths are local, not remotely accessible URLs; the final handoff supplies the concrete directory and checksum link. The manifest records tested code, final documentation HEAD and clean state. Historical local M1 logs are retained with the bundle.

Limits: successful source commands only, retained epoch/storage, trusted migration owner, cooperative callbacks and connection-per-transaction cost not benchmarked. Failed responses do not have stable replay. Known response loss is still unknown to the original caller; only an explicit same-key attempt recovers a retained commit. Old mutation credentials are outside command deduplication. Process-group cleanup cannot guarantee termination of deliberately escaped descendants; arbitrary host/power/storage failures are not exhaustively tested. No pipeline database, canonical ledger, staging/acknowledgement, sink obligation/delivery, seeding/activation, backfill, broker/search, consumer, API/UI, production deployment or benchmark was added. **Stop after M2A and await independent review.**
