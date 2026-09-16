# M2C.1: capture-registration isolation boundary

Authorized 2026-09-17, from external code/evidence review. Actual start is clean main at reviewed documentation HEAD 6a908f46d3c690023d204b0fdb785e53d471d207, with no later commits or unrelated edits. Prior tested M2C application/test SHA: 445f268709fad070eb020b5642a49772f7b5f58f. Preserve history, source migrations 001–004, pipeline migrations, envelope/codec, command receipts, owned transactions and toolchain pins. Local chronological commits only; no remote publication or privileged installation.

Read SPEC, AGENTS, ADR 009 and earlier source/command ADRs, migrations and initialization/tests. Node v24.19.0, npm 12.0.2, Docker 29.7.2, Compose 5.5.1 and the pinned PostgreSQL environment are available. Baseline `make verify-m2c` exited 0: 40 unit checks and separate 22/36/16/16/19/19 integration profiles, zero failures/skips/cancellations/todos, retained restart and owned cleanup. Complete logs and exit code: local artifacts/m2c.1/baseline-6a908f4. No prerequisite blocker occurred.

## Prediction, not yet a reproduced result

Migration 004 installs enqueue_capture before source registration. It silently skips work when its binding SELECT returns no row. An actual source_writer can BEGIN REPEATABLE READ and SELECT source_identity, establishing a snapshot before another session begins/commits register_capture. This initial read does not take an outbox write lock. The old writer may then call its already-granted create_entity. The registration lock cannot refresh that transaction's snapshot; predicted outcome is a committed entity/outbox revision but no work, a fresh missing=1 summary, and captureOnce stopping on integrity failure. Retained outbox destruction and false ACK are not predicted. The reviewer has not run this counterexample.

Before production changes, execute that exact schedule on isolated real PostgreSQL through migration 004, a real migrated pipeline identity, independent restricted writer and privileged registration sessions, and a fresh observer. Record roles, isolation, snapshots, transaction/backend identities, binding visibility/order, statements/tags/errors/SQLSTATE, exact entity/outbox/work membership, real summary and captureOnce result. Keep this intentionally flawed reproduction separate from acceptance. If it does not reproduce, retain the preventing mechanism and stop for review rather than redesign.

## Conditional correction

Only if reproduced, add source migration 005 replacing enqueue_capture with an unconditional current_setting('transaction_isolation') = 'read committed' precondition before its first binding lookup. Raise 25001 otherwise, aborting the state change/outbox together. Preserve the existing trigger, SECURITY DEFINER owner, fixed pg_catalog,pg_temp search_path and effective grants, with explicit PUBLIC revocation. Do not rewrite migrations 001–004 or duplicate the check in TypeScript mutation callers. Keep execute_command's existing READ COMMITTED check.

After this capture correction, every state-changing source mutation requires READ COMMITTED, including the supported legacy source_writer route. Read-only snapshots remain allowed. Legacy no-op update/repeated-delete operations that create no revision need no enqueue check; command invocations, including no-op receipts, retain their existing READ COMMITTED contract. The writer's exclusion from command deduplication does not exempt its mutations from capture. Normal READ COMMITTED writes before registration remain durable and are initialized; writes after registration enqueue atomically. Append dated SPEC/ADR 009 clarifications only after confirmation, without claiming the old implementation enforced this.

## Deliberate real regression inventory

| ID  | Required independent proof                                                                                                                                                              |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R01 | Exact pre-registration REPEATABLE READ snapshot schedule now rejects at mutation with 25001; COMMIT cannot persist entity/outbox/work; fresh missing=0                                  |
| R02 | READ COMMITTED transaction reads before registration then writes after its confirmed COMMIT; one revision/work and actual stage/ACK                                                     |
| R03 | Preserve both existing real registration lock controls, exact work membership and later stage/ACK                                                                                       |
| R04 | Actual writer create/update/delete/restore, valid changed fixtures, at REPEATABLE READ and SERIALIZABLE reject with unchanged original data/history/work; read/no-op treatment explicit |
| R05 | Guard rejects unsupported mutation before registration, before the invisible/absent-binding branch; READ COMMITTED pre-registration mutation is retained and initialized                |
| R06 | Post-registration command lifecycle/no-op/replay and pending/leased/ACK transitions, stale/wrong tokens unchanged; all prior faults remain required                                     |
| R07 | Exact data/identity snapshots before/after migration 005 on registered populated M2C and unregistered installation; no data/history/receiver rewrite                                    |
| R08 | Runtime replacement/trigger-disable/owner-assumption/bypass/metadata attempts denied; definer ownership/search_path/PUBLIC ACL preserved                                                |

Use the existing two-service orchestrator and private harness. A deliberately named reproduction profile stops at migration 004 and tests the predicted defect only. Acceptance retains historical M1/M2A/M2B/M2C profiles on their original schemas, then adds guarded capture profiles. Fresh guarded initialization runs R01/R02/R03/R05 before/around registration; the populated registered-M2C upgrade applies 005 after actual staging/ACK and compares exact before/after state. Both guarded profiles run all 19 original capture cases plus common R04/R06/R07/R08. Explicit inventories distinguish these fixture modes; no skipped test substitutes for a required result. This permits sharing the original lock/kill/outage harness without inventing a second implementation or changing immutable registration for tests.

## Validation and handoff

Run the targeted real regression and populated upgrade, all quality subcommands and complete prior-plus-new bounded acceptance with a fresh repeat from committed code and a clean tracked tree. Keep quality service independent. Full make verify stays nonzero with G1–G5 NOT IMPLEMENTED. Retain genuine failures/corrections, exact input hashes/commands/exits/SQL observations and scoped cleanup. Produce compact machine evidence, tested code SHA, later documentation HEAD, chronological commits/full diff and a sanitized local checksummed bundle. Local paths are not published artifacts; no M2C.1 remote CI claim without an actual run.

No sink delivery, polling/summary optimization, receipt/FK redesign, seed/activation, backfill, delivery state machine, payload duplication, new transaction manager, dependency/tool rollout, infrastructure service, API/UI, production deployment or benchmark. Stop at M2C.1 for independent review.
