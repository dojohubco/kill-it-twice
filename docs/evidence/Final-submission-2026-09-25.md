# Operator polling and final local acceptance — 2026-09-25

**PASS at tested commit `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685`**, tree `2ac270a7d5973d0f7b80aa2b94eec291e543653b`. A fresh immutable local clone completed `make verify` from 14:02:07 to 17:38:20 UTC in 12973.575 seconds. All five parent phases passed: quality, UI build, browser fixtures, retained runtime and actual million-record faults. The separate strict 1024-baseline functional check passed at 14:02:06 on the same clean commit. These are local results; server acceptance and optional 2M remain unproved. Hosted publication CI is a separate small-fixture result, recorded in the publication receipt.

[Selected full evidence](Final-submission-2026-09-25.json) and [separate functional evidence](Functional-1024-2026-09-25.json) retain exact identities and hashes. The following documentation-only commit does not replace the tested identity; its exact SHA and changed paths are recorded in the external bundle provenance and publication receipt. No additional runtime optimization followed this PASS.

## What was verified

- Actual G1 scanner SIGKILL before page COMMIT, independently unchanged durable checkpoints, recovery and finite terminal proof after all 1,000,005 baseline/concurrent-event obligations completed.
- Actual G2 consumer COMMIT-before-ACK redelivery and publisher confirm-before-local-commit duplicate publication, identical wire bytes and one defined business effect.
- G3 real Elasticsearch outage for 60.657 seconds, broker/consumer progress, 15 attempts within the declared 150 bound and recovery.
- G4 one actual 500-operation bulk: 497 applied items and three predeclared mapping rejections with retained dead letters.
- G5 independently fresh status and metrics with actual staged sample 1,000,519, source pending zero and three expected ES dead letters; real rendered browser PASS without intercepted responses or page errors. The paired status was observed at 16:51:55.465 UTC and the independent metrics at 16:52:03.030 UTC.
- Independent exact oracle: 1,000,000 baselines, 1,000,515 entities, 1,000,519 historical events, 519 unique mutation effects and three expected rejected revisions. Actual stdout matched the manifest and recorded hash; command arguments selected this run's real exports, journal and rejections.
- Missing, extra, payload, version and effect corrupt-export controls each exited 1 with a genuine AssertionError, without OperationalError, timeout or output overflow.
- Both service-owning child cleanups passed with no errors. All 27 preexisting containers were independently found by exact ID after completion, both owned projects had zero containers, and tested/source checkouts were clean at the tested SHA.

## Real UI parameter editing

R01–R08 passed on a separate retained root-Compose installation at the same tested commit. R08 exercised the actual browser/API, operator authorization, input bounds, cancellation, keyboard confirmation, draft preservation, revision conflicts, concurrent edits with one winner, same-key original receipts and old retries that cannot overwrite newer settings. Reload and restart retained settings. The actual capture and backfill workers both observed revision 4, respectively 400 ms and 900 ms, before and after restart. Restricted worker access and immutable receipt/history checks passed. Axe reported zero violations at 1280 px and 320 px; 200-percent text reflow passed.

The supported controls are capture polling and idle-backfill intervals, 50–30,000 ms with 1,000 ms defaults. Changes are durable, revisioned and observed between worker iterations with a five-second cache; in-flight work or an existing wait finishes first. This does not expose arbitrary SQL, all runtime limits or connection credentials. See [ADR 016](../adr/016-operator-polling-settings.md).

## Corrections and preserved failures

The original failures remain failed and their raw local evidence is preserved:

| Candidate | Actual result                                                                                                                 | Later scoped correction                                                                                       |
| --------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `ef5f633` | Million-record G1 settle deadline; later gates/oracle not run                                                                 | Covering delivery index in migration 021; no claim that this alone fixed the timeout                          |
| `cbf7bf5` | Million-record G1 PASS, G2 settle deadline; later gates/oracle not run                                                        | Migration 022 executes the original exact aggregate as a fixed bound PLpgSQL EXECUTE                          |
| `91d606b` | Strict 1024 G2 trace assertion before genuine redelivery finished; full 1M never started                                      | Bounded 15-second wait for actual redelivery and already_processed evidence before unchanged exact comparison |
| `2588af8` | Million-record G1–G4 PASS, G5 assertion against an independently unavailable metrics scrape; browser/oracle/negatives not run | Pair a fresh status and independent exact metrics inside the existing single 360-second deadline              |
| `1f2f3f2` | Strict 1024 and full local 1M acceptance PASS                                                                                 | Documentation/evidence closure only                                                                           |

The [query-planning evidence](Observation-count-planning-2026-09-25.md), [redelivery test review](Consumer-redelivery-wait-2026-09-25.md) and [G5 test review](G5-metrics-observation-2026-09-25.md) explain the measured corrections. Rejected query drafts and diagnostic experiments remain retained. Migration 022 passed a real populated 4096-row forward upgrade with durable bytes, owner/ACL and terminal-function preservation, rollback negatives and exact reconciliation. Controlled dirty million-row measurements establish only their own scope; the original real slow-query plan and precise cause remain unproved.

## Measurements and limits

The run retained the original 43,200-second outer bound plus 300-second cleanup grace, 36,000-second fault-child bound, 21,600-second G1 drain, 360-second incremental settles, 3,600-second exports, 7,200-second oracles and the original lease/query/fault bounds. Worker limits remained 256 MiB, Node heap 128 MiB, page ceiling 64 records / 256 KiB. Baseline payload was 1,034,667,793 bytes; the largest sampled worker process RSS high-water was 148,201,472 bytes. These are shared-host observations, not an uncontended benchmark.

There were 833 resource samples and no sampling errors. Minimum available host memory was 8,664,481,792 bytes and minimum free evidence disk was 68,811,968,512 bytes. Seed/activation took 679.172 seconds; scan, delivery and receipt drain after the scanner fault took 8,875.704 seconds. Receiver export took 882.155 seconds; positive exact reconciliation took 318.470 seconds. [Capacity notes](../capacity-notes.md) distinguish these phases and retain earlier measurements.

There were 505 nonfresh pipeline observations. Final fresh terminal convergence and exact reconciliation passed, but continuous dashboard availability under load was not established. Delivery finished before receipt observation; neither counter alone was used as final proof. No original timeout cause or universal speedup is claimed.

## Provenance and excluded material

| Manifest         | SHA256                                                             |
| ---------------- | ------------------------------------------------------------------ |
| submission       | `d8e0cedb5899152062e63d448c39d9e7d2ee8348398d3f7b146c1a9b2c1b727a` |
| retained-runtime | `495ca93b91c4eaa20b7c08a7466f1d383646f0e3e83be6d3ead95ac60b73bed6` |
| large-faults     | `7fdedcc6aea24b079ce60340aa8c1c6aa734be71fc39289fb116f69607e5dc1f` |

Raw installation configuration, generated credentials, Docker environment, database exports, payload logs and fault tokens remain outside the public source/package. Selected reports contain scalar measurements and artifact hashes, not private installation data. The bounded history scan covers four recognizable signature families and selected filenames; it is not exhaustive secret detection. The final documentation SHA is scanned again and recorded in package provenance.

Earlier `ecdfdb5` acceptance and all earlier ZIPs remain historical and unchanged. Shared-server attempts remain interrupted/failed, and its demo is preserved without repair. Public GitHub publication is authorized separately from this local evidence; sending a careers application remains outside the task.
