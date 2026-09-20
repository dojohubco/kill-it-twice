# M5B local evidence

2026-09-20: bounded local acceptance PASS; independent review pending. Both complete gates used committed code and clean tracked trees. Implementation and verification were performed with Codex; the original architectural decisions were already authorized.

Reviewed baseline: ab097aa95b961d0b02eee29eda706ba910bd5493. Tested application/test code: 92b1b26bbd7dc63e38fe494bd52653a1263d5724, on main with a clean tracked tree. The later documentation HEAD is recorded in the local bundle's identity.json and final handoff. No push, workflow dispatch, remote publication or host tuning occurred.

## Implemented boundary

The scanner reads coherent current revisions in bounded keyset pages through a fixed upper key. Each page's events, existing sink/consumer obligations, run membership, immutable batch record and checkpoint commit together in one owned pipeline transaction. After scanning, a separate source transaction retains the complete set of mutation identities visible in one statement snapshot. Bounded imports stage those exact revisions and advance import progress atomically. The finite required set is the union of scan observations and that retained cut; later capture continues without extending it.

Completion requires retained successful Elasticsearch evidence, broker confirmation and processed consumer receipts for every required event. Pending work stays draining; terminal receiver errors yield complete_with_errors; missing/corrupt evidence or an oversized unresolved boundary blocks progress. Backfill emits no source ACK. Empty and populated legacy-active epochs are supported without inventing a bootstrap key.

The implementation adds source migration 007, pipeline migration 006, restricted source_backfill/pipeline_backfill facades and start/status/pause/resume/once/follow CLI operations. Existing migrations, dependencies, canonical bytes, baseline receipts, capture semantics and consumer byte limits remain intact. SPEC has one authorized M5B refinement; ADR 013 records the SQL, ownership, snapshot and trusted-adapter boundaries. The design/inventory commit preceded production implementation. BF01E later separated an existing empty/legacy requirement before that profile ran; BF09's actual process-restart assertions were strengthened after inspection, not retrospectively described as original test-first execution.

## Commands and profiles

The initial npm ci and unchanged make verify-m5a baseline exited 0. Its 18 profiles are baseline evidence, with design/unreferenced development work during the run documented in [M5B-development.md](M5B-development.md). Final capture invokes npm ci --no-audit --no-fund, two independent make verify-m5b gates, then make verify. It checks clean status and unchanged HEAD after every command.

Final commands: npm ci --no-audit --no-fund exited 0; make verify-m5b exited 0 twice (2261.788 and 2311.558 seconds); make verify exited 2 and printed G1–G5 NOT IMPLEMENTED. Gate intervals were 10:09:15.312Z–10:46:57.100Z and 10:46:57.111Z–11:25:28.669Z on 2026-09-20. Every required result passed with zero failed/skipped/cancelled/todo cases. Resource cleanup and evidence finalization passed for all 42 profiles; no secondary cleanup errors were reported. Each complete gate requires quality plus the following profiles once, using fresh owned resources:

| Profile                                                     | Required passing cases per gate |
| ----------------------------------------------------------- | ------------------------------: |
| M1 / M2A                                                    |                         22 / 36 |
| M2B fresh / populated upgrade                               |                         16 / 16 |
| M2C fresh / populated upgrade                               |                         19 / 19 |
| M2C.1 guarded fresh / upgrade                               |                         27 / 23 |
| M3 fresh / upgrade; rejected-update oracle                  |                      15 / 15; 6 |
| M4 fresh / populated upgrade                                |                         22 / 22 |
| M4.1 historical diagnostic; corrected fresh / upgrade       |                        1; 8 / 8 |
| M5A bootstrap / active upgrade                              |                          18 / 1 |
| M5B fresh / populated upgrade / empty-then-populated legacy |                      22 / 1 / 1 |
| Total                                                       |          318 across 21 profiles |

Quality includes read-only formatting, typed ESLint with zero warnings, TypeScript, Knip, Compose configuration, actionlint and 57 native unit checks. Repeated earlier profiles are repeated tests, not additional independent guarantees. The M4.1 historical profile expects its old P6002 rejection and does not claim successful processing at that boundary.

## New real-service observations

The final fresh runs are m5b-20260920104139881-fe881c24 and m5b-20260920111959597-f8bdd32d. Each main run retained 297 required events from observed baselines and a 41-mutation source fence and completed through both sinks and consumer receipts. Each separate declared rejection run retained 299 events and a 45-mutation fence and ended complete_with_errors. Each oversized-record run remained blocked and unsealed. Both snapshot audits independently found 257 retained baselines, 54 outbox revisions, 53 ACKed mutations plus one explicitly blocked oversized mutation, 309 events/inbox records, 53 mutation-effect rows and 53 aggregate units, 618 intents and 309 processed observations. These totals accompany exact identity/content comparisons; they are not the oracle alone.

The independent fixture oracle checks the recipe/ordinal map, declared mutation journal, exact source fence membership, canonical bytes/hash, current versions/tombstones/search fields, inbox/effects/totals and ACK evidence. It excludes the original baseline superseded before observation, preserves baselines actually observed before concurrent mutations, and rejects missing/extra membership, altered content/version/tombstone/projected fields and duplicate effects. A separate Python snapshot audit imports no production normalizer or completion query. The two audited restart snapshots have SHA-256 56eac3124b8273e4f06e919d3f0d490ac0132c885960d4675fe9d409c399adc9 and f509a9d9616f1b1c4f98152d58d445a43bb64f57f11af4d1de1cdf81c545e270. The audit script and separate first/repeat results are included under development/development/ in the bundle. The quarantine-only completion control is explicitly a rolled-back privileged fixture with zero ES errors and one quarantined observation, not a fabricated remote quarantine; prior real consumer poison/crash cases remain required.

| Required real kill                                    | First / repeat process PID | Actual exit |
| ----------------------------------------------------- | -------------------------- | ----------- |
| BF03 before page COMMIT                               | 1082809 / 1326124          | SIGKILL     |
| BF04 after page COMMIT, before success                | 1083032 / 1326385          | SIGKILL     |
| BF11 before source fence COMMIT                       | 1087020 / 1330478          | SIGKILL     |
| BF11A after source COMMIT, before pipeline attachment | 1087096 / 1330550          | SIGKILL     |
| BF12 after import COMMIT, before success              | 1087158 / 1330626          | SIGKILL     |

All have zero ordinary-success bytes before kill and observed session cleanup, with healthy BF03H/BF04H/BF11H/BF11AH/BF12H controls. The first page pre-COMMIT observer saw backend 581, xid 788, idle in transaction; the source-fence observer saw backend 970, xid 842. The repeat independently observed backend/xid 602/789 and 1000/842 for those respective open transactions. Exact keys, batches, snapshots and before/after states are in sql-evidence.jsonl. BF05 used actual SIGSTOP/SIGCONT, pipeline database-clock expiry and generation 7 -> 8. BF09 used distinct process triples 1084671/1084753/1084770 and 1328110/1328153/1328187, each advancing retained cursor 112 -> 128 after pause/resume. BF05 observed expiry at 10:42:50.233620Z and 11:21:12.774518Z respectively before reclaim; the old workers could not overwrite the new generation. Populated migration/restart checks compare actual before/after rows and receiver identities, without rewriting prior history.

## Environment, chronology and limits

Node 24.19.0; npm 12.0.2; Docker 29.7.2; Compose 5.5.1. Both databases report PostgreSQL 18.6 with fsync, synchronous_commit and full_page_writes on. Existing vm.max_map_count was 1048576 and was only read. The local Linux/Docker-host assumption remains explicit. Existing image references and actual container identities are retained per run. Pins remain:

- PostgreSQL 18.6-bookworm: sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af.
- Elasticsearch 9.5.4: sha256:82ac14f43fe701992e601f4cc81e1c0d7dbc5a2576d8cd736006452925df4026.
- RabbitMQ 4.3.6-management: sha256:de62d9901fb73aaa8767b9c1e9ea32f2938f69d1b83e430f7c5ca854081d2128.
- Toxiproxy 2.12.0: sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e.

No dependency or service was added.

Actual development failures and corrections are preserved in [M5B-development.md](M5B-development.md) and the bundle: new-profile project-name admission, new SQL syntax/alias errors, an early cross-case test teardown, and a supplemental audit's incorrect projection-field assumption. Membership-replay and isolated quarantine checks were strengthened through explicit negative controls. An initial complete capture was deliberately stopped only after its active broker profile cleaned up so BF09 could prove a replacement process; that incomplete capture is retained as nonzero, not application fault evidence or a final PASS.

The initial reviewed M5A hosted run subsequently completed successfully; its dated observation and verified artifact digest were appended to M5A.md. No M5B hosted run was executed or observed. Local workflow validation is not hosted acceptance. The wrapper explicitly allows 75 minutes for verify-m5b, with an 80-minute workflow job and the unchanged 10-minute fallback for unknown commands. The slower local gate took 38 minutes 31.558 seconds, leaving 36 minutes 28.442 seconds inside the 75-minute per-gate command limit. The outer job also allows setup/artifact handling. These measurements justify a finite bound; they are not throughput measurements or proof of hosted execution.

Known limits remain: one finite source-side fence copy has storage/transaction cost; existing seed validation and all-history diagnostic/status queries need later scale measurement. Small fixtures do not establish two-million-row capacity, high availability or distributed exactly-once effects. No UI/API, replay engine, benchmark, production deployment or full G1-G5 completion is claimed.

Chronological implementation commits (the later evidence-only commit is additionally included in bundle chronology.txt):

```text
565ee99 docs(backfill): define atomic page progress and finite completion fence
c73b611 feat(backfill): stage bounded pages with atomic checkpoints and retained fences
ffbf86a test(backfill): exercise current-state races and durable page and fence recovery
0b3f5b5 ci(verify): include bounded backfill profiles with an explicit gate deadline
facff50 fix(verify): permit owned backfill service project names
bfe3e30 fix(pipeline): disambiguate the page phase expression
44c1ca9 fix(backfill): remove SQL alias ambiguity and verify exact fault sessions
f15079c test(backfill): retain late writers and verify concurrent fence arbitration
9670847 fix(backfill): require retained membership for batch replay
c0636e9 test(backfill): isolate quarantine from receiver rejection classification
92b1b26 test(backfill): resume paused runs in replacement scanner processes
```

## Local handoff

Capture: artifacts/m5b/review-20260920100913274. Its capture.json records exact command exits/times, tested SHA, input digest and all profile paths; the compact input-array digest is 0405508f4263514325821edbb35b4a033582c790c8fc9ef3485c65b19545d840; inputs.json records tracked file hashes. The compact tracked [M5B-summary.json](M5B-summary.json) references each sanitized local manifest and required inventory. Exact changed files and the full reviewable patch are listed in [changed-files.txt](../../artifacts/m5b/review-20260920100913274/bundle/changed-files.txt) and [full.diff](../../artifacts/m5b/review-20260920100913274/bundle/full.diff). The bundle contains successful and failed evidence, full.diff, changed-files.txt, chronology.txt, tested/final source archives, identity.json and SHA256SUMS, with an archive checksum beside it. These are local files, not published download URLs. Final documentation is committed after acceptance and is distinguished from the tested application/test SHA.

## Later hosted observation, 2026-09-20

Read-only inspection at 2026-09-20T12:16Z found GitHub Actions run 35508282031 completed with conclusion success at documentation commit ab00bdd7409a3d65269a864925878ee5a12deddb; its acceptance job also concluded success. GitHub reports updatedAt 2026-09-20T12:11:34Z. This appends the later hosted result to the original report, whose earlier no-hosted-observation statement was true when written. This is observation of hosted execution, not reviewer-local service execution or a scale/capacity claim. Sanitized run metadata is retained locally at artifacts/m6/hosted-m5b-observation.json. No run was dispatched or rerun during this observation.
