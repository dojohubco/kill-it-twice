# M2C: bounded incremental capture and source acknowledgement

Authorized 2026-09-17. Actual start: clean main at reviewed documentation HEAD 642925bf66fc0755f4a845bab2a55350957475c4, no later changes. Accepted M2B application/test SHA f8d7728f1d1f17ff7fc16ae0c29e4b64946e1636. Preserve existing history, pins, original migrations, envelope vectors and transaction/command contracts. Local chronological commits only; no publication or privileged installation.

Read SPEC, AGENTS, ADRs 001–008, milestone/evidence documents and implementation. Node 24.19.0, npm 12.0.2, Docker 29.7.2 and Compose 5.5.1 available. Baseline `make verify-m2b` exited 0: quality (30 unit cases), M1 22, M2A 36, M2B fresh 16 and populated-upgrade 16, zero skips/failures with retained restarts and cleanup. Runs m1-20260916210441876-9cb2154a, m2a-20260916210455016-61107e18, m2b-20260916210510077-388ddcc5 and m2b-20260916210526293-1872f4c6. Logs: local artifacts/m2c/baseline-642925b. No prerequisite blocker observed.

Read-only hosted metadata confirms run 35148507444 succeeded at reviewed HEAD on 2026-09-16 (20:45:53–20:47:22 UTC). Reviewer supplied artifact SHA-256 b13b7681199201c1f0d445ddc4d266cee09fa4c951abe4cfc8b0dac5c8ce29f2 and offline reconciliation of 40 staged events per M2B snapshot. This is artifact inspection, not reviewer-local PostgreSQL execution or full-scale reconciliation. No M2C hosted execution is implied.

## Design and independent inventory before production changes

ADR 009 specifies additive pipeline identity, source binding/work columns, keys/checks/indexes/grants, lock-protected initialization, source-clock transitions, separate A/B/C transactions, independent renewal, bounds, transient delays and all four private failpoints. Capture/ACK was planned, not newly discovered. The executable inventory scripts/required-capture-cases.ts is committed with this document before M2C tests exist or run. M2B's earlier descriptive design was committed before implementation; its executable inventory was committed with tests after initial dirty developmental execution. Do not retroactively change that chronology.

| ID                      | Required proof                                                                                                                                                  |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| IC01                    | Fresh and populated M2B upgrade, concurrent registration commit, exactly one work row, actual work-insert SQL failure atomicity                                 |
| IC02                    | All lifecycle/intermediate revisions, exact source/event/ACK evidence, no-op/replay, unchanged pre-staged evidence                                              |
| IC03                    | Earlier allocated open A, later committed/captured B, then automatic capture of committed A                                                                     |
| IC04                    | Real independent disjoint claims, finite duplicate/restart convergence                                                                                          |
| IC05                    | Renewal, expired/wrong/stale token denial for every transition, higher generations, clock after row lock, overflow                                              |
| IC06                    | A staged then delayed beyond real expiry; B reclaims/stages/ACKs; resumed A cannot overwrite terminal evidence                                                  |
| IC07–IC10               | Exact SIGKILL at claim COMMIT, before pipeline COMMIT, after pipeline COMMIT, after source ACK COMMIT; independent state and recovery                           |
| IC11-CLAIM/PRE/POST/ACK | Healthy control at each new barrier, one successful result, exit 0, no orphan session                                                                           |
| IC12                    | Real pipeline shutdown during capture, no false ACK, persisted bounded retry, automatic recovery after restart                                                  |
| IC13                    | Different immutable pipeline ID despite same epoch rejects inside actual staging; retained restart preserves identity                                           |
| IC14                    | Individual oversized blocked/unacknowledged, valid medium records partition before payload transfer, later small progress, delayed states visible               |
| IC15                    | Two real capture processes, hot finite lifecycle workload, several kills, idle then new writes; independent full identity/content/obligation/ACK reconciliation |
| IC16                    | Restricted role bypass denials, immutable ACK/history, original command guarantees, pending/unbound obligations; restart and cleanup in orchestration           |

No test may use sleep alone as lock/expiry proof or count-only reconciliation. All waits are bounded, signals actual, failures nonzero with primary/cleanup distinction. Preserve earlier 22/36/16/16 profiles on their original schemas. make verify-m2c additionally runs fresh and populated M2B-upgrade capture profiles with this entire inventory, then a fresh complete repeat from clean committed code. Quality stays read-only and service independent. Full make verify remains nonzero, G1–G5 NOT IMPLEMENTED.

## Handoff and explicit stop

Retain commands/exits/input hashes/source-clock generations/per-event ACKs and bytes, fault session/process identities, retries/receiver checks/permissions and owned cleanup. Produce compact sanitized summary and local checksummed bundle with tracked snapshot, full diff and commit chronology. Tested code and later documentation HEAD are distinct. Remote CI only if actually run; local workflow edits do not authorize pushing.

No baseline seed/activation/backfill/checkpoint/fence, real sink delivery/consumer, general retry/DLQ/replay system, API/UI, production deployment or large benchmark. Record the future settled-obligation replay migration and baseline/no-op receipt FK regression without implementing either. ACK means staged only. Stop after M2C for independent review.
