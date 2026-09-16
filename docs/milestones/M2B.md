# M2B: lossless canonical revisions and atomic local staging

Authorized 2026-09-16 before production changes. Reviewed/current documentation HEAD is 735585c3ddb45fa9c2239943102dbc45b37ce710, clean main, with no later work to reconcile. Prior tested code is 6be433da050a1ece890560511e9f957a44dbce16. Preserve all history; local chronological commits only, no push/publication/PR/settings/privileged host install.

Read SPEC, AGENTS, ADRs 001–006 and prior milestone/evidence documents. Node 24.19.0, npm 12.0.2, Docker 29.7.2 and Compose 5.5.1 remain available. Baseline quality passed 28 unit cases; fresh M2A run m2a-20260916195318413-5e3e8dc3 passed 36 required cases, restart and cleanup. Logs are local under artifacts/m2b/baseline-735585c. Hosted CI 35141624449 was subsequently successful at reviewed HEAD, confirmed through read-only metadata inspection. The reviewer inspected its artifact (reported SHA-256 cb8cd256d925e1a6b3d697c7d5e9cf0cfd2b1fdafd50108c826cb4a6e882547b), not a local PostgreSQL rerun. No M2B result is inferred from it.

## Planned implementation

ADR 007 fixes the exact v1 fields, lossless pg18-jsonb-text/v1 payload codec, scoped JCS/SHA-256 implementation, strict validation and bounded source adapter. ADR 008 specifies actual columns, types, constraints, grants and transaction statements for the separate pipeline service. This is an authorized refinement of known precision requirements. Source migrations 001/002 and receipt semantics remain unchanged. Source migration 003 grants restricted reads; pipeline migration 001 installs only binding, two unbound destinations, events, pending deliveries/consumer obligations and sanitized incidents.

A bounded explicit selection is read from committed immutable outbox revisions and atomically staged in one owned pipeline transaction. Identical repeats return already_staged; differing content fails without overwriting/repairing history. No source row is acknowledged or marked complete. No outgoing receiver request occurs. The existing transaction owner is shared through a small internal abstraction, never copied into a second subtly different helper.

## Independent required cases before execution

The checked-in staging inventory will require these exact IDs and explicit names, independently of discovered results. S01 consists of golden and rejection unit cases, also selected by the staging profile. No skips, cancellations, todos, duplicate/malformed/missing evidence or contradictory failed summaries pass.

| ID         | Required proof                                                                                                                         |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| S01-GOLDEN | Literal independently prepared bytes/hash/wire for live mutation, tombstone, synthetic baseline, key order and Unicode                 |
| S01-REJECT | Invalid Unicode/UTF-8, fields, IDs, timestamp, kind/deletion, hash and noncanonical body bytes reject                                  |
| S02        | Real current/outbox same-revision precision parity, exact high numbers/decimals/arrays/nulls/Unicode and no-op formatting              |
| S03        | Historical outbox after later update/delete, distinct out-of-order versions retained                                                   |
| S04        | One event, two pending sink intents, one pending consumer obligation, independent bytes/hash/metadata SQL                              |
| S05        | Intra-batch/repeated/concurrent duplicate safety, observed lock boundary, immutable original timestamps                                |
| S06        | Recomputed-hash conflicts, concurrent mismatch, mixed-batch rollback, incident success/failure and unchanged source                    |
| S07        | Privileged missing/wrong/duplicate obligations and indexed-body constraint tests; actual runtime role denial and immutable surface     |
| S08        | Exact stager SIGKILL before COMMIT, independently invisible rows/observed transaction, explicit repeat                                 |
| S09        | Exact stager SIGKILL after COMMIT before ordinary success, independently complete rows, new-process/session replay                     |
| S10-PRE    | Healthy pre-COMMIT release: exit 0 and exactly one expected success                                                                    |
| S10-POST   | Healthy post-COMMIT release: exit 0 and exactly one expected success                                                                   |
| S11        | Selected uncommitted source revision not visible/staged, visible after commit, immutable content despite later current changes         |
| S12        | Real count/record/transfer/serialized byte bounds; several small pages; durable oversized source; restart and cleanup in orchestration |
| S13        | Unbound destinations and pending obligations only; no receiver request, source ACK or simulated consumer success                       |
| S-UPGRADE  | Fresh/additive read migration and preserved populated M2A receipts/history, independently recorded in orchestration                    |

Keep 22-case M1 and 36-case M2A profiles explicit. make verify-m2b runs quality and both prior profiles, then both fresh and populated-upgrade variants of the two-service staging profile. The latter installs source 001/002, commits two revisions and three successful command receipts, and only then applies 003; the fresh variant applies 003 before any workload. Both require the complete S inventory. Keep the existing pinned workflow and tools, extending only its selected acceptance command/evidence. Final clean committed runs include all relevant commands and a fresh repeat; full make verify remains honestly nonzero. Record real failures/corrections, exact source/test and later documentation SHAs, input hashes, versions, commands, signals, SQL states, scoped cleanup and sanitized local bundle/checksums.

## Deferred work

Record the exact future seed-without-outbox/no-op-command FK regression in ADR 007; do not claim it was demonstrated or fix it now. No M2C, continuous poller/source ACK, pipeline worker, leases/fencing, sink delivery, consumer, seed/activation, backfill/checkpoints/fences, retry/DLQ/replay, API/UI, deployment, large benchmark or full G1–G5 claim. Stop after M2B for independent review.
