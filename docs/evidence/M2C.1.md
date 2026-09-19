# M2C.1 evidence — 2026-09-17

The counterexample was **reproduced on real pinned PostgreSQL before the production correction**. An old REPEATABLE READ source_writer committed one entity/outbox revision without capture work. The retained outbox survived, missing-work detection stopped capture, and no false source ACK occurred. Forward migration 005 closes this boundary by rejecting revision-producing mutations outside READ COMMITTED before the enqueue function looks for a binding.

Corrected local acceptance passed: all 21 captured commands had their expected exit, including **two fresh complete `make verify-m2c1` runs**. The 24 isolated integration profiles produced 534 passing case executions, zero failed/skipped/cancelled/todo results, and 78 individually recorded named-boundary SIGKILL exits. These are repeated executions of the explicit inventories, not 534 independent guarantees or a full Optio PASS. Forty unit checks passed in the standalone unit command and each quality invocation. Full `make verify` intentionally returned 2 with G1–G5 NOT IMPLEMENTED.

## Identity, authorization and history

- Actual starting/reviewed documentation HEAD: `6a908f46d3c690023d204b0fdb785e53d471d207`, clean main, no later/unrelated work to reconcile.
- Previously accepted M2C application/test SHA: `445f268709fad070eb020b5642a49772f7b5f58f`.
- Clean counterexample code: `8ab152e5b687ca59528876faa773b921855ca17b`; production still used source migration 004.
- Clean corrected tested code: `4f1e8b5943cdf556e0c72cc3f7bda3754f667051`. Every final profile recorded this SHA, an empty tracked git status and `developmental: false`.
- Final documentation HEAD is the later evidence-only commit, recorded by the post-commit local bundle's manifest and the handoff response; it is deliberately not substituted for the tested SHA.
- All final runs share tracked-input SHA-256 `903af71e8141d6671457b6fdddea2a83b84624604a2811bebde2020ac2bd0d64`. Full per-file inputs, native reports, SQL evidence and process/cleanup logs are retained under each run directory.

Chronological local commits before this evidence-only commit:

```text
1414037 docs: scope M2C.1 registration isolation reproduction and correction
8ab152e test: add isolated real PostgreSQL registration snapshot counterexample
691429c fix: require READ COMMITTED before source capture binding lookup
fec2ee9 test: prove guarded registration schedules and populated M2C upgrade
4f1e8b5 ci: require prior and guarded capture profiles in M2C.1 handoff
```

The behavioral R01–R08 design was committed in 1414037 before reproduction and production changes. The dedicated B01 executable reproduction followed in 8ab152e, and was executed before migration 005 in 691429c. Executable corrected regressions were committed afterward in fec2ee9. This is not a fabricated test-first execution history. No amend, reset, rebase, squash, authorship change, push, PR, repository setting change or privileged host installation occurred.

The later successful [M2C hosted run 35157202674](https://github.com/dojohubco/kill-it-twice/actions/runs/35157202674) is appended as a dated observation to [the original M2C report](M2C.md). Its reviewer-supplied artifact digest is `1d7286a923797953c442566d97e1ec3421ec3574868e9443e46ea00911c77029`. Intake checked hosted run metadata, not a fresh download/digest of that artifact. The reviewer inspected recorded snapshots offline, not through reviewer-local PostgreSQL. That run passed the older inventory, which did not include this counterexample. **M2C.1 remote CI: NOT RUN.** Local actionlint is not remote CI evidence.

## Baseline and actual counterexample

Before implementation, `make verify-m2c` exited 0 at the clean reviewed baseline: 40 unit checks and separate 22/36/16/16/19/19 profiles, zero skipped/cancelled/todo/failed cases, retained restart and owned cleanup. Baseline log, exit code and hosted metadata are local under `artifacts/m2c.1/baseline-6a908f4/`.

`node scripts/m1.ts m2c1-repro` exited 0 for run `m2c1-repro-20260916223953613-99f59788`; this explicitly means **the historical defect was demonstrated**, not accepted capture correctness. Input hash: `119170942731d7d5850e53785f7c6c90f871c5dbcd7d0d27abe9b0870b58c96c`. The separate run applied source 001–004 and the real pipeline identity, leaving source.capture_binding empty. No guard/trigger/grant was disabled or rewritten for reproduction.

| Observation                                     | Actual result                                                                                                        |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| A's actual login role/isolation                 | source_writer / repeatable read                                                                                      |
| A snapshot, xid, backend PID, source time       | `757:757:`, `757`, `111`, `2026-09-16 22:39:59.397667+00`                                                            |
| Outbox locks held by A before B began           | none                                                                                                                 |
| B role/isolation, xid, backend PID, source time | m1_admin / read committed, `758`, `112`, `2026-09-16 22:39:59.400974+00`                                             |
| B visibility in A snapshot                      | false, independently observed with pg_visible_in_snapshot                                                            |
| Registration                                    | actual COMMIT; fresh observer binding xmin `758` before A mutated                                                    |
| A mutation/COMMIT                               | create_entity succeeded; no SQLSTATE error; command tag COMMIT                                                       |
| Durable result                                  | entity/version `1:1`, change `f1532a7c-a073-4e3e-95d8-f89082a13278`; one exact entity and outbox, zero matching work |
| Fresh capture summary and real captureOnce      | missing `1`, acknowledged `0`; fatal `Required capture work is missing`, no cleanup error                            |
| Pipeline/ACK                                    | no event, no ACK                                                                                                     |
| Retained restart/owned cleanup                  | PASS; no owned container, volume or network remains                                                                  |

[Machine reproduction evidence](M2C.1-reproduction.json) retains full role/snapshot/identity/result observations. The original SQL evidence and logs remain in `artifacts/m2c1-repro/m2c1-repro-20260916223953613-99f59788/`. This intentionally flawed database was removed after observation; no missing-work row was silently repaired or left in accepted fixtures.

## Correction and required regressions

The production change is [migration 005](../../migrations/005-capture-mutation-isolation.sql): CREATE OR REPLACE of the existing enqueue_capture, with a current_setting('transaction_isolation') check before the INSERT/SELECT from capture_binding. Unsupported isolation raises **25001 at mutation time**, so the entity change and outbox insertion abort together. A later COMMIT on that failed transaction returns ROLLBACK. The original trigger/function identity, source_owner ownership, SECURITY DEFINER, fixed pg_catalog/pg_temp search_path and effective ACL remain intact; PUBLIC EXECUTE is explicitly revoked.

All revision-producing legacy and command mutations now have an explicit READ COMMITTED contract. Ordinary READ COMMITTED work before registration remains durable and is filled by registration; work afterward enqueues atomically. A long-lived READ COMMITTED transaction that read before registration still works. Read-only REPEATABLE READ/SERIALIZABLE transactions are allowed. Legacy unchanged-update/repeated-delete no-ops produce no revision and do not reach enqueue_capture. The command API retains its existing READ COMMITTED restriction for all invocations, including no-op receipts/replays. Its deduplication exclusion for legacy writers never meant permission to create unschedulable capture history.

Only dated clarifications were appended to SPEC and ADR 009; the earlier incomplete lock-only argument was not retrospectively represented as enforced. Source migrations 001–004, all pipeline migrations, src/ implementation, command semantics, eleven-field envelope, codec, quality tools and dependency pins are unchanged. No new transaction manager or capture-state redesign was introduced.

| ID  | Observed proof                                                                                                                                                                                                                                                                                                                                                      |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R01 | Real restricted old-snapshot writer rejects at enqueue with 25001; COMMIT returns ROLLBACK; exact marker entity/outbox/work remain absent and missing=0. Old reader has no outbox lock, registration begins afterward and commits before release.                                                                                                                   |
| R02 | Real restricted READ COMMITTED reader established before registration commits one later revision/work, which the actual automatic worker stages/ACKs.                                                                                                                                                                                                               |
| R03 | Original command writer blocks registration and a during-registration writer blocks on registration; pg_blocking_pids records actual dependencies, exact work membership and subsequent canonical stage/ACK comparison pass.                                                                                                                                        |
| R04 | Create/update/delete/restore with valid changed fixtures at REPEATABLE READ and SERIALIZABLE all reach enqueue and reject with 25001 (8 attempts/profile). Original entities/history/receipts/work remain unchanged. Read-only transactions and no-revision legacy no-ops commit.                                                                                   |
| R05 | Both unsupported isolations reject before any binding exists. Normal READ COMMITTED pre-registration outbox survives, receives work during initialization and is captured.                                                                                                                                                                                          |
| R06 | Actual command role keeps its isolation checks; create/update/delete/restore, identical retries and two no-op receipts retain six receipts for four revisions. Pending→leased→ACK, wrong/stale token denial and immutable terminal ACK retry/hash behavior pass.                                                                                                    |
| R07 | Exact independent SQL row text before/after 005 matches for source identity/entities/outbox/receipts, capture binding/work, pipeline binding/events/destinations/intents/observations/incidents. Function/trigger OIDs, owner, path and ACL match; the function body deliberately changes. Both unregistered installation and populated registered M2C upgrade run. |
| R08 | Actual writer, command, reader and capture sessions make 44 denied replacement/disable/owner-assumption/replication-bypass/protected-metadata calls per profile, all 42501. Catalog and protected data are unchanged. Reader session read-only default is disabled only locally to prove privilege denial rather than an incidental read-only error.                |

The first final guarded fresh run, `m2c1-20260916225915300-63793036`, records A snapshot `761:761:`, xid `761`, backend `118` at `2026-09-16 22:59:23.738651+00`. B records xid `764`, backend `121` at `2026-09-16 22:59:23.768452+00`. A sees B as not visible, then gets 25001/ROLLBACK; observer missing=0. The READ COMMITTED companion commits entity/version `7:1`, change `240204f3-7505-4186-83b2-ea997429a160`, and reaches acknowledged. The registration-only schedule shares the real lock controls around B; it does not substitute sleeps for ordering.

The registered upgrade's pre-005 fixture already had 3 current entities, 4 outbox/work revisions, 5 command receipts, 4 staged events, 8 delivery intents and 4 observations, all capture work acknowledged. The two original M2B events had been staged before capture initialization. Every existing row, byte/hash, staging timestamp, ACK and binding is unchanged across 005. Fresh installation applies 005 with an empty binding, then exercises actual pre-registration writes. There is no fake binding reset. R01/R02/R03/R05 belong to that unregistered profile; R04/R06/R07/R08 run in both profiles. Earlier requirements are all retained explicitly.

## Final commands and profile results

Capture command: `npm run review:capture -- --m2c1`, exit 0. Local directory: `artifacts/m2c1/acceptance-20260916225613702-1ba95a7f`. Commands were run against committed code with a clean tracked tree throughout; ignored artifacts/dependencies were allowed. Each of the following records timestamps, exit, signal, timeout/overflow and cleanup status in commands.json:

| #   | Command                                      | Exit | Assessment |
| --- | -------------------------------------------- | ---- | ---------- |
| 1   | `npm ci --no-audit --no-fund`                | 0    | expected   |
| 2   | `npm ls --depth=0`                           | 0    | expected   |
| 3   | `npm run format:check`                       | 0    | expected   |
| 4   | `npm run lint`                               | 0    | expected   |
| 5   | `npm run typecheck`                          | 0    | expected   |
| 6   | `npm run knip`                               | 0    | expected   |
| 7   | `npm run validate:compose`                   | 0    | expected   |
| 8   | `npm run validate:workflow`                  | 0    | expected   |
| 9   | `npm run test:unit`                          | 0    | expected   |
| 10  | `make quality`                               | 0    | expected   |
| 11  | `npm run test:integration:m1`                | 0    | expected   |
| 12  | `npm run test:integration:m2a`               | 0    | expected   |
| 13  | `npm run test:integration:m2b`               | 0    | expected   |
| 14  | `npm run test:integration:m2b -- --upgrade`  | 0    | expected   |
| 15  | `npm run test:integration:m2c`               | 0    | expected   |
| 16  | `npm run test:integration:m2c -- --upgrade`  | 0    | expected   |
| 17  | `npm run test:integration:m2c1`              | 0    | expected   |
| 18  | `npm run test:integration:m2c1 -- --upgrade` | 0    | expected   |
| 19  | `make verify-m2c1`                           | 0    | expected   |
| 20  | `make verify-m2c1`                           | 0    | expected   |
| 21  | `make verify`                                | 2    | expected   |

`make verify-m2c1` wraps the unchanged earlier `make verify-m2c` and adds both guarded profiles. The local workflow selects the same wrapper and uploads sanitized guarded evidence. Original profile schemas/catalogs and inventories were not weakened. Profile runs 1–8 are standalone; 9–16 are the first complete repeat; 17–24 are the second complete repeat. Every row below has status PASS, zero failed/skipped/cancelled/todo cases, retained restart equality and successful scoped cleanup.

| #   | Isolated run                      | Installation mode                | Passed |
| --- | --------------------------------- | -------------------------------- | ------ |
| 1   | `m1-20260916225645527-14aefcfa`   | fresh                            | 22     |
| 2   | `m2a-20260916225659363-37617cac`  | fresh                            | 36     |
| 3   | `m2b-20260916225715545-6e6c1da7`  | fresh                            | 16     |
| 4   | `m2b-20260916225734195-78a9c780`  | populated M2A upgrade            | 16     |
| 5   | `m2c-20260916225750946-9012daf7`  | fresh                            | 19     |
| 6   | `m2c-20260916225833484-69a9f1d9`  | populated M2B upgrade            | 19     |
| 7   | `m2c1-20260916225915300-63793036` | fresh                            | 27     |
| 8   | `m2c1-20260916230001857-c6e1e6d1` | populated registered M2C upgrade | 23     |
| 9   | `m1-20260916230105220-424587e2`   | fresh                            | 22     |
| 10  | `m2a-20260916230118467-3b739caa`  | fresh                            | 36     |
| 11  | `m2b-20260916230134717-ca83d5a0`  | fresh                            | 16     |
| 12  | `m2b-20260916230154223-daca526d`  | populated M2A upgrade            | 16     |
| 13  | `m2c-20260916230211639-3f664ff4`  | fresh                            | 19     |
| 14  | `m2c-20260916230254278-574a65f7`  | populated M2B upgrade            | 19     |
| 15  | `m2c1-20260916230336811-ae77fba9` | fresh                            | 27     |
| 16  | `m2c1-20260916230421988-956b63d2` | populated registered M2C upgrade | 23     |
| 17  | `m1-20260916230520150-148e2b09`   | fresh                            | 22     |
| 18  | `m2a-20260916230535362-4a3483f2`  | fresh                            | 36     |
| 19  | `m2b-20260916230551287-1e224a98`  | fresh                            | 16     |
| 20  | `m2b-20260916230608043-ee6d31ca`  | populated M2A upgrade            | 16     |
| 21  | `m2c-20260916230627110-4c647e90`  | fresh                            | 19     |
| 22  | `m2c-20260916230711558-024d0f19`  | populated M2B upgrade            | 19     |
| 23  | `m2c1-20260916230754925-0ca1f953` | fresh                            | 27     |
| 24  | `m2c1-20260916230839922-5fe92b5e` | populated registered M2C upgrade | 23     |

All six guarded snapshots have **75 outbox/work revisions, 74 acknowledged/staged events, one blocked oversized revision, 148 pending sink intents and 74 pending consumer observations**. Original M2C snapshots retain 66/65/1 (fresh) and 68/67/1 (upgrade). These fixture counts describe actual records, not throughput or catch-up with blocked work. Independent SQL reconciliation compares exact identities, source payload text, canonical bytes/hash, metadata, relations and ACK hashes; counters alone are not the oracle.

The clean runs include all original T08/T09, C09/C10, S08/S09 and IC07–IC10 SIGKILLs with their healthy controls, plus actual SIGSTOP/SIGCONT stale-owner recovery and pipeline service outages. Across 24 profiles the summary includes 78 named-boundary process exits with actual signal SIGKILL and no ordinary success before killing. Each run retains its own process/session identity, SQL boundary, before/after state, claim generations/source times and explicit replay evidence; none is borrowed from another run. Representative guarded records from the first final fresh profile:

| Case               | Child PID | Event identity                              | Source backend | Pipeline backend | Actual exit |
| ------------------ | --------- | ------------------------------------------- | -------------- | ---------------- | ----------- |
| IC07-actual-signal | 868012    | `711b3a1a-75f8-4b6e-b8ea-8808ca9b4043:8:1`  | 143            | none             | SIGKILL     |
| IC08-actual-signal | 868178    | `711b3a1a-75f8-4b6e-b8ea-8808ca9b4043:9:1`  | 173            | 131              | SIGKILL     |
| IC09-actual-signal | 868300    | `711b3a1a-75f8-4b6e-b8ea-8808ca9b4043:10:1` | 203            | 150              | SIGKILL     |
| IC10-actual-signal | 868462    | `711b3a1a-75f8-4b6e-b8ea-8808ca9b4043:11:1` | 235            | 169              | SIGKILL     |

All destinations remain unbound and all delivery/consumer obligations pending. No receiver requests or consumer effects were added. Each two-service restart preserves exact source/pipeline snapshots and identities. Cleanup queries report no run-owned containers, volumes or networks and no surviving owned database sessions; unrelated resources are not pruned. No automatic gap-repair routine was added.

A supplemental offline audit after acceptance independently rebuilt the fixed canonical envelope from each recorded source export using Python's standard JSON/UTF-8/SHA-256 operations. Payload text remained opaque; arbitrary payload numbers were never parsed. All six guarded snapshots reconciled exactly: 444 events/ACKs, 888 pending intents, 444 pending observations and six blocked revisions, with unchanged restart snapshots and receiver identities. This is a recorded-evidence check, not another PostgreSQL execution. Command: `python artifacts/m2c.1/audit-recorded-snapshots.py`, exit 0. The script, environment version, report and log are included locally in the bundle; report SHA-256 `cbc6e07d8b7b36f7ab2894ffc0f9d71ef217d3bda2334d71f265a3ccc8aaf7ee`. It is not a new quality dependency or gate.

## Environment, hashes and genuine development history

Node v24.19.0, npm 12.0.2, Docker 29.7.2, Compose 5.5.1, actionlint 1.7.12; PostgreSQL 18.6 on x86_64, both services on the pinned `postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. fsync, synchronous_commit and full_page_writes are on. Existing UTF-8 codec checks and precision vectors remain required. No packages, tools or major versions changed. `make quality` stays read-only and independent of live services; provisioning/npm ci is separate.

Migration 005 SHA-256: `8b913433bcde661bbdaf65a78125c745bbe1cdb66910f8b018830ea3d4a42a6b`. Unchanged root lockfile SHA-256: `c5bf22c0f2c279f2c41b82e4194d522fef19e55996f59934e49893c341860e43`.

The first dirty guarded development runs passed, with no unexpected quality/integration failure: fresh `m2c1-20260916225126534-1873eb52`, input hash `1a43920b85fee4297544cfd6c4601664b94213649631cb46a7b7a9e024bb6d0d`; registered upgrade `m2c1-20260916225330062-618fa01f`, input hash `daa8c42cac2a10eeb34c68d495fc70e9865288e6a101613f03f00d2842faf0a5`. Each used a dirty worktree based on 691429c, not merely code identified by that HEAD. Their patches/input manifests and logs remain alongside final evidence. Subsequent instrumentation records explicit registration BEGIN/COMMIT tags; that final version was exercised in every clean run. The reproduced defect is the real pre-correction failure; original M2C passing READ COMMITTED schedules remain genuine. The only expected nonzero acceptance command is the full unimplemented verifier.

## Review files, bundle and limits

[Compact machine summary](M2C.1-summary.json) retains all required IDs, commands/exits, run/input identities, actual signals, corrected snapshot/SQLSTATE observations, preservation hashes and cleanup. [Reproduction summary](M2C.1-reproduction.json) remains distinct. Complete sanitized logs, inputs, native reports, SQL evidence and restart/upgrade observations are in the local artifact paths above. They are not publicly accessible URLs.

After committing this report, `npm run review:bundle -- artifacts/m2c1/acceptance-20260916225613702-1ba95a7f --m2c1` produces a local `artifacts/review/m2c1-final-<documentation-HEAD-prefix>/` containing the tracked snapshot, full binary-capable diffs since reviewed HEAD and initial SPEC, full chronological commit metadata, exact changed-file list, baseline/reproduction/development/final sanitized evidence, manifest with tested/final documentation SHAs, and per-file SHA256SUMS. The handoff includes its actual path and archive/checksums after creation. No publication is implied.

Exact changed files from the reviewed baseline, including this final documentation:

```text
.github/workflows/m1.yml
AGENTS.md
Makefile
README.md
SPEC.md
docs/adr/009-incremental-capture-acknowledgement.md
docs/evidence/M2C.1-reproduction.json
docs/evidence/M2C.1-summary.json
docs/evidence/M2C.1.md
docs/evidence/M2C.md
docs/milestones/M2C.1.md
migrations/005-capture-mutation-isolation.sql
package.json
scripts/m1.ts
scripts/migrate-staging.ts
scripts/required-isolation-cases.ts
scripts/review.ts
tests/capture/isolation-registration.test.ts
tests/capture/isolation-reproduction.test.ts
tests/capture/isolation.test.ts
tests/support/capture-upgrade.ts
tests/support/isolation.ts
```

M2C.1 fixes only this isolation boundary. The supported guarantee remains at-least-once immutable staging before source ACK, with ACK meaning STAGED. Database-owner bypass and independent rollback of retained databases remain outside scope. The SQL guard does not repair hypothetical older missing work; missing-work detection continues to stop capture. Read-only summaries/history are not optimized. Known seed/no-op receipt-FK and settled-obligation replay integration requirements remain separately documented and unimplemented. No sink delivery, seed/activation, backfill, API/UI, new service/tool/dependency, benchmark or full G1–G5 claim was added. All requested local checks ran; remote M2C.1 CI and later sink/consumer work did not. **Stop after M2C.1 for independent review.**

## Subsequent hosted observation — 2026-09-17

After the historical report above, [GitHub Actions run 35162006369](https://github.com/dojohubco/kill-it-twice/actions/runs/35162006369) completed successfully at documentation HEAD `c05f3dcf9d21ac7e3523038c902d1e2de3412cfb` (2026-09-16 23:26 UTC). Its status/head were checked read-only during M3 inspection. The supplied review identifies downloaded artifact SHA-256 `23aa90049cf578363af9e581a6261797b6a8415c4484179242125166b800ae5d`, 40 unit checks and profile counts 22/36/16/16/19/19/27/23, including offline reconstruction of 74 staged/acknowledged events in each guarded snapshot. That artifact inspection was not a reviewer-local PostgreSQL rerun. It covered the corrected M2C.1 inventory, not M3 sink delivery. The original NOT RUN statement remains an accurate account of the earlier report's timing.
