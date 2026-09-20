# M5A local acceptance and review handoff

Date: 2026-09-20 (Asia/Tbilisi). M5A only. Baseline documentation HEAD: `685b37158fae5dd19e44425a00a553bce979e158`. Tested application/test SHA: `4178e2112452881a708cba5e0a5689f57073df96`. Final documentation HEAD is recorded separately in the bundle identity and handoff, avoiding a self-referential commit hash. Final commands used clean committed code; ignored artifacts are not source changes.

## Result and evidence boundary

Two fresh complete `make verify-m5a` invocations passed on the same clean committed code. Each includes 54 passing unit checks and all 18 required profiles. All 36 profile cleanups passed, with no missing/failed/skipped/cancelled/todo required evidence. `make verify` deliberately exited 2 with G1–G5 NOT IMPLEMENTED. This is M5A acceptance, not backfill completion or full replication acceptance.

The fresh fixture retains 257 genuine source baselines in nine bounded chunks. Only ten explicitly selected baselines reach the existing sinks. Seven fitting captured mutations also reach them; one oversized mutation remains explicitly blocked/unacknowledged. The resulting 17 canonical/inbox events retain exact content and produce seven mutation effects. Baseline-only entities have zero-unit aggregate rows; receiving a baseline adds no mutation effect or aggregate unit. The other 247 baselines deliberately remain unstaged. Zero mutation backlog is not baseline replication completion.

Hosted M4.1 [run 35467226835](https://github.com/dojohubco/kill-it-twice/actions/runs/35467226835) actually completed FAILURE at the baseline documentation SHA. The command wrapper omitted verify-m4-1 from its long-deadline allowlist and used 600000 ms. Nine completed profile manifests passed; complete hosted acceptance was not established. Artifact 10592025694 SHA-256: `328c3f89eab1d0bffe45faad374a82bafd53f6b9918517e588cc3fb56ee6376b`. This agent inspected the later metadata/log/artifact read-only; the external reviewer did not execute these services or inspect that completed artifact. The original M4.1 local report remains historical. M5A hosted CI is **NOT RUN**. No push, dispatch or publication occurred.

## Implemented contract

Forward source migration 006 adds immutable baseline revisions, an immutable chunk membership receipt and one protected bootstrap lifecycle/recipe. Committed ordinal mappings retain their allocated entity IDs and stable source time across retries/restarts. Rolled-back chunks leave no mapping and may consume sequence values; no sequence reset conceals those gaps. A short owned chunk transaction commits current rows, retained baselines and progress together. Dedicated bootstrap authority uses a private database definer identity; triggers stay enabled and no caller-settable flag bypasses capture. Existing used epochs become legacy-active without relabeling history and cannot be seeded.

Ordinary source calls check the lifecycle before mutation, no-op or command replay. READ COMMITTED writers take a compatible shared lifecycle lock; chunks/seal/activation take the exclusive row lock first. Activation verifies the expected pipeline outside the source transaction, then commits capture registration and active state together. Remote identity checking is not a distributed transaction or authentication of remote durability. Old closed snapshots cannot bypass admission; the existing state-changing isolation guard remains enforced.

The known baseline/no-op receipt obligation is now closed with conditional immutable-history validation. A baseline result has a real NULL change ID and must match retained baseline epoch/entity/version/time/deletion/payload; a mutation result must match its outbox revision. Incomplete, NULL-loophole or mismatched evidence fails. Historical receipts remain immutable. No-op replay after later update/delete returns the original baseline without a fabricated mutation.

SourceReader adds bounded explicit baseline selection and correct current kind. The eleven-field envelope, PG18 opaque payload codec, numeric tokens, timestamps, hashes, consumer wire-byte ceiling and all prior migrations remain unchanged. No new dependency or service was added. SPEC gains a scoped clarification, ADR 012 documents the actual design, and AGENTS lists the CLI/gate. The local workflow selects the same gate; an explicit timeout helper corrects the observed CI-wrapper omission without tuning this workstation.

Implementation entry points: [source migration 006](../../migrations/006-source-baselines.sql), [owned bootstrap facade](../../src/bootstrap.ts), [bounded reader](../../src/source-reader.ts), [fresh acceptance](../../tests/bootstrap/bootstrap.test.ts), [upgrade preservation](../../tests/bootstrap/upgrade.test.ts) and [independent required inventory](../../scripts/required-bootstrap-cases.ts).

## Commands and profiles

| Command                       | UTC start → finish                                  | Exit |
| ----------------------------- | --------------------------------------------------- | ---- |
| `npm ci --no-audit --no-fund` | 2026-09-19T23:20:15.790Z → 2026-09-19T23:20:21.116Z | 0    |
| `make verify-m5a`             | 2026-09-19T23:20:21.140Z → 2026-09-19T23:58:50.339Z | 0    |
| `make verify-m5a`             | 2026-09-19T23:58:50.359Z → 2026-09-20T00:34:16.187Z | 0    |
| `make verify`                 | 2026-09-20T00:34:16.202Z → 2026-09-20T00:34:16.218Z | 2    |

`npm run review:m5a -- capture` exited 0. Each complete gate ran read-only formatting, typed zero-warning lint, TypeScript, Knip, Compose/actionlint validation and 54 unit checks, then the profiles below. Supplemental `python3 artifacts/m5a/development/audit-bootstrap-snapshot.py <fresh-run-directory>` exited 0 for both final fresh M5A snapshots. These offline audits are this implementation agent’s additional checks, not external reviewer execution or substitutes for the real service tests.

| Profile   | Installation                         | First passed | Repeat passed |
| --------- | ------------------------------------ | -----------: | ------------: |
| m1        | fresh                                |           22 |            22 |
| m2a       | fresh                                |           36 |            36 |
| m2b       | fresh                                |           16 |            16 |
| m2b       | populated M2A upgrade                |           16 |            16 |
| m2c       | fresh                                |           19 |            19 |
| m2c       | populated M2B upgrade                |           19 |            19 |
| m2c1      | fresh                                |           27 |            27 |
| m2c1      | populated registered M2C upgrade     |           23 |            23 |
| m3        | fresh                                |           15 |            15 |
| m3        | populated guarded M2C.1 upgrade      |           15 |            15 |
| m3-oracle | fresh                                |            6 |             6 |
| m4        | fresh                                |           22 |            22 |
| m4        | populated M3.1 upgrade               |           22 |            22 |
| m41-repro | fresh                                |            1 |             1 |
| m41       | fresh                                |            8 |             8 |
| m41       | populated M4 consumer upgrade        |            8 |             8 |
| m5a       | fresh closed bootstrap               |           18 |            18 |
| m5a       | populated M4.1 active source upgrade |            1 |             1 |

Every profile has zero failed/skipped/cancelled/todo cases and PASS cleanup. The machine summary retains each exact required-case ID, run path, manifest hash and receiver cleanup result. Each complete invocation has 294 integration cases; repeating them is repeat evidence, not 588 distinct guarantees.

Ordered tracked-input manifest digest: `fa06372a092ad9e8e74492389ed0c447549aed5a31d208a374a5d0781d3d80c4`. Per-file input hashes are in `inputs.json`. Independent final fresh restart-snapshot SHA-256 values: `a68195c99b6013ec18a40c5b816cb0f188ad5cb15f35f408987b7c3043b340f7` and `83e56a6f16b232f955323c8a731e0d2f34ec0d08fd3aa5858e31b14797133541`. Frozen-contract audit confirms all prior migrations, the envelope and lockfile are byte-identical to the review baseline.

All profile inventories are independently checked; missing, skipped, cancelled, todo or duplicate required results fail. Earlier profiles repeat existing guarantees, not new independent milestones. Historical M4.1 B01 remains an expected P6002 application failure against migration 001, separate from corrected processing acceptance.

## New observations

BS01–BS14 include four real SIGKILL boundaries and four healthy release controls: before/after seed-chunk COMMIT and before/after activation COMMIT. Independent SQL records exact process/session/transaction identities, tentative invisibility, committed mapping and subsequent identical retry. Healthy activation controls use isolated sealed source copies on the same owned service; no capture/sink worker adopts those copies. The READ COMMITTED activation-race write succeeds after the row lock releases and is deliberately rolled back; ordinary committed mutations are separately journaled. Old-snapshot attempts report actual P7001/25001 evidence.

Real no-op command contention, immutable receipt replay, NULL-reference rejection, source-clock/locking evidence and actual runtime credentials are exercised. BS08 injects a labelled work-insert SQL failure P7099 inside a rollback-only privileged fixture and verifies atomic rollback. This is not described as an organically observed production failure.

Selected baseline/current and mutation/outbox exports match byte-for-byte. Higher mutation/tombstone delivery before an older real baseline preserves both receiver projections and every distinct inbox/publication record. Independent recipe and mutation-journal assertions check identities, payloads, canonical bytes/hashes, ACKs, obligations, effects and aggregate values. Missing/extra/altered baseline, wrong progress/chunk and missing/wrong receipt controls reject. ES and RabbitMQ actually restart with retained state; orchestration separately restarts both PostgreSQL services and compares source, pipeline and consumer snapshots. BS13 applies migration 006 over populated M4.1 data and compares exact prior evidence; only the declared new lifecycle metadata appears.

| Run    | Case  | Process PID | Observed source backend / XID before COMMIT | Exit    |
| ------ | ----- | ----------: | ------------------------------------------- | ------- |
| First  | BS03  |     4102825 | 678 / 784                                   | SIGKILL |
| First  | BS04  |     4102899 | 692 / 786                                   | SIGKILL |
| First  | BS06  |     4103166 | 743 / 812                                   | SIGKILL |
| First  | BS06A |     4103187 | 755 / 813                                   | SIGKILL |
| Repeat | BS03  |      136084 | 623 / 784                                   | SIGKILL |
| Repeat | BS04  |      136141 | 636 / 786                                   | SIGKILL |
| Repeat | BS06  |      136470 | 688 / 812                                   | SIGKILL |
| Repeat | BS06A |      136534 | 693 / 813                                   | SIGKILL |

First fresh run: `m5a-20260919235500992-0cf0462d`. Healthy BS03H/BS04H/BS06H/BS06AH each exited 0, emitted exactly one ordinary success and closed their session. Killed children emitted no ordinary success. Post-COMMIT cases additionally observed committed state before the kill; their earlier transaction identity is shown above, not claimed to remain open after COMMIT.

Repeat fresh run: `m5a-20260920003044455-eec6c7d6`. Healthy BS03H/BS04H/BS06H/BS06AH each exited 0, emitted exactly one ordinary success and closed their session. Killed children emitted no ordinary success. Post-COMMIT cases additionally observed committed state before the kill; their earlier transaction identity is shown above, not claimed to remain open after COMMIT.

## Chronology, environment and limitations

Design/inventory commit 1e52216 preceded production changes and new-case execution. c408069 implemented the source extension; 22ecb33 and 5b056c7 corrected test ordinal ordering, distinguished read-only/ACL failures and used actual passive queue observation. f807e2b added missing receiver-restart coverage. The exact chronology and every correction are retained in [M5A-development.md](M5A-development.md), input snapshots and bundle patches.

The initial unchanged baseline ended 143 after fifteen finalized profile passes; its last native eight-case report lacked complete finalization. Scoped cleanup and a fresh standalone upgrade passed. An early capture was deliberately stopped after eight clean profiles to add receiver restarts. Neither incomplete invocation is called PASS.

A complete gate at f807e2b passed, but its repeat failed IC06 (18 passed/1 failed). Later attempts failed MQ09 (21/1) and IC11-PRE (18/1). All three failed profiles cleaned up successfully. They exposed private harness coordination/observation gaps: renewal quiescence before SIGSTOP, the positive-confirm barrier's placement outside conditional settlement, and distinguishing renewal transactions from claim/ACK owners. Original pre-assert diagnostics were incomplete, so exact original causes are not invented. fd69bed/babd001, f24ad76 and 4178e21 preserve the assertions, add observations, and change no production capture/delivery behavior. Their targeted repeats passed 19, 22 and 19 cases respectively. The final acceptance uses 4178e21 throughout. No demonstrated baseline data-loss failure was concealed.

Node 24.19.0, npm 12.0.2, Docker 29.7.2, Compose 5.5.1; PostgreSQL 18.6-bookworm, Elasticsearch 9.5.4, RabbitMQ 4.3.6 and Toxiproxy 2.12.0 use unchanged pinned image identities recorded per run. Existing local vm.max_map_count was 1048576; no host configuration changed. The 60-minute gate timeout and 65-minute disposable-host workflow timeout are finite bounds, not performance claims.

The CLI requires an explicit positive seed count (up to the configured two-million limit), recipe/key/epoch and chunk size (1–128). Only the small 257-row fixture was exercised; no two-million-row scale claim. Seed progress is not a pipeline checkpoint. No automated backfill/range scanner, pipeline run/fence, baseline delivery scheduler, replay API, UI or deployment was implemented. Full G1–G5 remain unfinished. Stop for independent review before M5B.

## Local review artifacts

The compact tracked result is [M5A-summary.json](M5A-summary.json). The completed local capture is `artifacts/m5a/review-20260919232015704/capture.json`; full sanitized run logs, SQL/message evidence, restart snapshots and command hashes are in that directory and its referenced run directories. They are local checkout paths, not public download URLs.

`npm run review:m5a -- summary artifacts/m5a/review-20260919232015704` regenerates the tracked summary. After committing this report, `npm run review:m5a -- bundle artifacts/m5a/review-20260919232015704` builds the local archive and its SHA-256 sidecar. The bundle includes `identity.json` (tested and final documentation SHAs), `changed-files.txt`, `full.diff`, `chronology.txt`, both tracked snapshots, all final runs, historical/failed evidence, committed input snapshots, the read-only hosted artifact and `SHA256SUMS`. The archive filename includes the final documentation HEAD. No receiver credentials or private key files belong in this handoff.

## Later hosted observation — 2026-09-20

During M5B work, read-only GitHub metadata showed run [35498634663](https://github.com/dojohubco/kill-it-twice/actions/runs/35498634663) completed successfully at ab097aa95b961d0b02eee29eda706ba910bd5493. Downloaded artifact 10600659769 matched SHA-256 9790b3a522952de8d7f129d182ca99b89fe1f4022f399ab89e775396d2986401 and contained the 18 passing prior profile manifests. This later observation preserves the original report's historical NOT RUN statement. It covers the M5A inventory, not new M5B behavior; the external reviewer had not executed services or inspected this completed artifact at the earlier review. Local inspection records are under artifacts/m5b/hosted-35498634663.
