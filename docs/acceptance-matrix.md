# Final acceptance matrix

Historical status: **READY for local submission** at tested commit `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7`. Full default-million `make verify` exited zero and the strict supervisor completed at 2026-09-24 20:09:21 UTC. All five parent phases, G1–G5, exact independent reconciliation, five corruption controls and both child cleanups passed. A separate 1024-row functional run also passed at the same clean commit. See the [final report](evidence/Final-submission-2026-09-24.md) and [selected evidence](evidence/Final-submission-2026-09-24.json).

**Server acceptance remains NOT RUN / interrupted.** Local success does not relabel the preserved server attempts. Hosted CI, optional 2M and publication were not performed. Four previously failed workers in the recreated server demo remain preserved without repair.

| Criterion                                                      | Verified local result                                                             | Entry point / evidence                                                                |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Relational source, concurrent backfill and incremental capture | PASS at 1M baselines plus 519 declared mutations                                  | `make verify`, G1 and exact oracle                                                    |
| Search receiver and event stream with independent consumer     | PASS                                                                              | G2–G4 and independent reconciliation                                                  |
| Real G1–G5 through a single command                            | PASS, all five parent phases                                                      | `make verify`, submission and child manifests                                         |
| UI status, metrics, search and exact details                   | PASS, real browser, zero intercepted responses or page errors                     | G5; `scripts/runtime-browser.ts`                                                      |
| Controls, replay, configuration and simulations                | R01–R07 PASS in the separate retained-runtime fixture                             | `make verify-runtime`; this does not assert controls at 1M scale                      |
| Root Compose, explicit seed and retained restart               | PASS                                                                              | Isolated cold startup, G1 seed, retained-runtime cases                                |
| Nontrivial dataset and bounded worker memory                   | 1,034,667,793 baseline payload bytes; 256-MiB worker limits; 704 resource samples | [Capacity notes](capacity-notes.md), final evidence JSON                              |
| Honest delivery, DLQ and supported replay                      | PASS, real duplicates, one effect; three declared ES rejections                   | G2/G4/oracle and retained R07; no exactly-once transport claim                        |
| Five corrupted copies rejected                                 | PASS: missing, extra, payload, version and effect                                 | Each exit 1 with AssertionError; no OperationalError, timeout or overflow             |
| Ownership and cleanup                                          | Both children PASS; preexisting containers preserved                              | Strict supervisor and post-run zero-owned-container query                             |
| SPEC before code and chronological evolution                   | Verified in local Git without rewriting history                                   | SPEC `251cb5a`, harness `7af0818`, implementation `5368de6`                           |
| Diagram, ADRs, measurements and two actual AI deviations       | Documented, with execution boundaries                                             | README; ADRs 001–015; wire accounting `0a375fd` / B02–B08; oracle `d1b3baa` / O01–O06 |
| Public repository handoff                                      | NOT PUBLISHED                                                                     | Local source/history bundle only; no push, upload or PR                               |

Current addition: the operator polling editor in ADR 016 is implemented but awaits its own fresh clean acceptance. The historical result below is not evidence for later runtime changes.

## Candidate and evidence identity

Tested code: `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7`; tested tree: `20dcf981b4ef4bef0c5dfe4040e75e1d2af35e7d`. The later commit introducing this report changes documentation only. Its exact identity is recorded in the external bundle provenance alongside the tested commit; it is not a second runtime test result. The package retains a separate archive of the exact tested source.

The earlier million-row attempt at `f0f98d8` failed G1 on its operational snapshot deadline; G2–G5 and final reconciliation did not run, and cleanup passed. Its 12-second SQL cancellation, 18.078-second proof and subsequent scoped correction remain in [closure development evidence](evidence/Acceptance-closure-development.md). The server `afee415` attempt failed in Compose configuration during quality; the server `ecdfdb5` attempt was interrupted during lint to protect unrelated production. Its interruption sidecar is authoritative over the old wrapper's misleading zero exit. None of these results is retroactively passed.

## Limits and hygiene

Operational pipeline reads intermittently timed out during G1. Final fresh completion and exact reconciliation passed, but continuous dashboard availability under load was not established. This shared-host run is not an uncontended performance benchmark. Optional 2M, HA, independent database restoration and unlimited backlog remain outside this acceptance.

The pre-closure read-only scan inspected 1188 reachable blobs (12,228,056 bytes) and 500 tracked filenames against four signature families, with no matches or skipped blobs. Final package validation repeats that bounded check after the documentation commit and records its counts separately. These scans are not exhaustive secret detection. Raw credentials, private configuration, database exports, Docker environments and application payload logs are excluded from the sanitized package; original evidence and failures remain private and retained.

No separate original-assignment file was found among tracked filenames. The retained pre-code SPEC and current accepted requirements provide the mapping above.
