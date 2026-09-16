# M2A: durable source-command idempotency

Authorized 2026-09-16, before production changes. This is the already planned source-command portion of M2, split for independent review. M1.1 was accepted for source-only behavior after code review and inspection of later hosted CI evidence; the reviewer did not independently rerun PostgreSQL locally. No full G1–G5 guarantee follows.

## Actual start

HEAD is the reviewed documentation commit `e5813a4bcd7857ba816ac6ad4537cc7a52f86963`, on main with a clean tracked tree and no later commits to reconcile. M1.1 tested application/test code was `7cb3dbc4da0dca4e7fa683d06bd5198a15c05cd0`. Read SPEC, AGENTS, ADRs 001–005, M1/M1.1 scope/evidence and implementation. Docker works. Node 24.19.0, npm 12.0.2, Docker 29.7.2 and Compose 5.5.1 match the installed baseline. Existing exact pins and quality tools remain.

Before changes, `make quality` exited 0 (27 unit cases), and `npm run test:integration:m1` exited 0: 22 required cases, no failures/skips/cancellations/todos, retained-volume restart and owned-resource cleanup. Run: `m1-20260916191035872-55186fa0`. Local logs: `artifacts/m2a/baseline-e5813a4/`. Read-only `gh run view 35135496960 --repo dojohubco/kill-it-twice --json databaseId,headSha,status,conclusion,url,createdAt,updatedAt` confirmed completed/success at the reviewed HEAD. That is subsequent M1.1 CI, not M2A evidence.

## Implementation boundary

ADR 006 records the transaction, normalization, result and retention contract before implementation. Add only migration 002, one command-receipt table, restricted command role, SQL entry point and a small operation on the existing owned transaction capability. Keep migration 001 unchanged. Use explicit READ COMMITTED and fresh statement snapshots after unique-key contention. A deferred completion constraint prevents incomplete reservations from committing. Successful rows are immutable; only successful commands are retained for the epoch.

One invocation creates, updates, logically deletes or restores one entity. The caller supplies the expected persisted epoch and UUID command ID. Same normalized request/key returns the original snapshot; a changed request conflicts. No-op success also retains a receipt. Failed/rolled-back attempts retain no successful receipt. There are no implicit retries or replacement keys. The old source_writer interface remains for M1 SQL scope and is outside command deduplication.

## Required inventory recorded before execution

Preserve all 22 M1/M1.1 cases. The independent executable M2A inventory is `scripts/required-command-cases.ts`; none is optional.

| ID          | Required real PostgreSQL evidence                                                                                                |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------- |
| C01         | Create/update/delete/restore snapshots, exact BIGINT and numeric JSON text, source/outbox state                                  |
| C02         | Many sequential replays including harmless JSON whitespace/key-order differences                                                 |
| C03         | Changed operation/target/significant payload conflict; wrong epoch rejects before mutation                                       |
| C04         | Historical snapshot after later updates/deletion; successful restore replay while live                                           |
| C05         | Unchanged update and repeated delete receipts without revisions; replay after later changes                                      |
| C06         | Independent owners, observed database lock before winner release, one execution and equal results                                |
| C07         | Coordinated differing requests compete; one binding, one conflict, no extra mutation                                             |
| C08         | Waiting duplicate takes over after first reservation/mutation rolls back                                                         |
| C09         | Exact child SIGKILL before COMMIT; invisible receipt/source/outbox, ended backend, explicit same-key retry                       |
| C10         | Exact child SIGKILL after COMMIT before caller success; independently visible receipt/source/outbox, new-owner same-key recovery |
| C11         | Real source/outbox SQL failure rolls back receipt; explicit later attempt succeeds                                               |
| C12         | Actual restricted-role bypass denials, immutable receipts, deferred incomplete-receipt rejection                                 |
| C09-HEALTHY | Healthy release at pre-COMMIT barrier: exit 0, exactly one success, complete durable state                                       |
| C10-HEALTHY | Healthy release at post-COMMIT barrier: exit 0, exactly one success, complete durable state                                      |

Use tiny fixtures, independent SQL observations and deterministic barriers, with command key, entity/revision, session/process identity, actual exit signal and replay evidence. Initial empty-table observation belongs to orchestration before any workload. Inspect full histories only for these tiny diagnostic fixtures. Expanded catalog assertions must enumerate new objects without weakening existing role checks.

## Validation and handoff

Keep quality read-only and all existing checks. Add bounded `make verify-m2a` including M1/M1.1 and M2A inventories. Explicitly reject a contradictory nonzero native summary failed count; this defensive fixture does not imply the previously accepted CI report had failures. Final acceptance runs use clean committed code on fresh resources; capture npm ci, individual quality checks, quality, existing M1 acceptance, new acceptance and a fresh repeat. Full `make verify` remains deliberately nonzero with G1–G5 NOT IMPLEMENTED.

Preserve actual developmental failures and corrections. Record input hashes, commands/exits, versions, cleanup and signals, baseline/tested/documentation SHAs, exact changed files and chronological local commits. Produce sanitized compact machine evidence and a checksummed local review bundle. No push, publication, PR, privileged host install or global Git changes are authorized.

Stop after M2A. No canonical pipeline staging, second database, acknowledgements, sink obligations/delivery, activation/seeding, backfill, Elasticsearch, RabbitMQ, consumer, API/UI, deployment, benchmark, general idempotency library or automatic retry scheduler. Receipts have no TTL or GC. Raw database-owner bypass and stable replay of failed responses are outside the guarantee.

## Acceptance recorded after implementation

At clean `6be433da050a1ece890560511e9f957a44dbce16`, npm ci, every quality subcommand, quality, the 22-case M1 profile and three fresh 36-case M2A runs passed. Full verify exited 2 with G1–G5 NOT IMPLEMENTED. [M2A evidence](../evidence/M2A.md) records exact commands, identities, input hashes and cleanup; the later documentation commit does not change tested application or harness code. All local required cases ran; M2A remote CI was not run or pushed.
