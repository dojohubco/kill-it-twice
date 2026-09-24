# Final local submission acceptance — 2026-09-24

**READY for local submission.** An independent clean clone completed full default-million `make verify` and the strict supervisor at 20:09:21 UTC, with zero exit status. Tested commit: `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7`; tested tree: `20dcf981b4ef4bef0c5dfe4040e75e1d2af35e7d`. Source remained clean and unchanged throughout execution. The later commit introducing this report changes documentation only; its exact SHA is recorded separately in the delivery provenance, not substituted for the tested SHA.

This is local execution evidence. **Shared-server acceptance remains interrupted / NOT RUN**, and optional 2M, hosted CI and publication were not performed. No server deployment or healthy retained-server-demo claim is made.

## Execution

The separate 1024-row functional run passed first at 17:04:08 UTC, including G1–G5, exact 1024+519 reconciliation, five corruption controls, 50 resource samples and cleanup. Full default-million execution then ran from 17:06:35 to 20:09:21 UTC: 10,965.656 seconds including the supervisor's final checks.

| Parent phase     | Result |   Seconds |
| ---------------- | ------ | --------: |
| quality          | PASS   |    47.319 |
| ui-build         | PASS   |     4.427 |
| ui-fixtures      | PASS   |    10.412 |
| retained-runtime | PASS   |   156.205 |
| large-faults     | PASS   | 10746.827 |

Quality included 88 passing tests without skips. Retained-runtime R01–R07 passed in its own service-owning fixture and cleaned up. The large child separately exercised the real million-row dataset. Controls/replay in the retained fixture are not presented as controls/replay tests at million-row scale.

## Fault and exact-data results

- **G1 PASS:** seeded one million distinct baselines with concurrent changes; actually killed the scanner with SIGKILL (exit 137, no OOM), restarted it, preserved checkpoint evidence and drained all 1,000,005 then-required sink deliveries and consumer receipts. Backfill completed at 19:15:34 UTC; a fresh terminal proof was accepted at 19:16:40. Seed/activation took 683.208 seconds; post-fault scan/delivery/receipt drain took 6857.363 seconds.
- **G2 PASS:** actual consumer ACK and publisher-confirm ambiguity produced duplicate deliveries, while the intended mutation effect remained singular.
- **G3 PASS:** Elasticsearch was unavailable for 60.686 seconds; ten mutations progressed independently through RabbitMQ and its consumer. ES attempt delta was 15 within the 150 bound; recovery was observed at 19:23:18 UTC.
- **G4 PASS:** an actual 500-operation bulk produced 497 applied revisions and three declared mapper rejections/dead letters.
- **G5 PASS:** real Chromium 153.0.8010.52 exercised the Compose gateway, Overview, Records and exact details, with zero intercepted responses and no page errors.

All ten bounded data exports completed. The receiver export took 887.299 seconds. The positive independent disk-backed oracle then passed in 310.564 seconds, proving exact identities, revisions, bytes, relations and values for **1,000,000 baselines, 1,000,515 entities, 1,000,519 historical events, 519 mutation effects and three expected rejected revisions**. Counts alone were not used as reconciliation.

Five separately corrupted copies were correctly rejected. Every control exited 1 with AssertionError; none failed with OperationalError, timeout or output overflow:

| Corruption | Expected rejection observed | Seconds |
| ---------- | --------------------------- | ------: |
| missing    | 1 / AssertionError          | 189.765 |
| extra      | 1 / AssertionError          | 189.796 |
| payload    | 1 / AssertionError          | 216.384 |
| version    | 1 / AssertionError          | 220.693 |
| effect     | 1 / AssertionError          | 220.058 |

## Resources and cleanup

The baseline payload contained 1,034,667,793 bytes (1021–1036 bytes per entity), approximately 3.85 times one 256-MiB worker budget. Worker/Node-heap/page limits remained unchanged. Across 704 periodic samples, the largest observed worker process RSS high-water was 147,918,848 bytes and the largest worker cgroup peak was 94,896,128 bytes. These use different accounting. The positive oracle process peak RSS was 29,229,056 bytes. Minimum observed host available memory was 7,751,229,440 bytes and free disk was 87,690,547,200 bytes.

Source, pipeline and consumer databases measured 2,697,172,671, 8,246,441,663 and 2,610,386,623 bytes at 19:25:38 UTC; Elasticsearch primary storage was 353,745,174 bytes. The [capacity notes](../capacity-notes.md) explain timing and CPU measurement limits. The [JSON evidence](Final-submission-2026-09-24.json) provides per-role observations and recorded service-image digests. Closure tool versions were Node 24.19.0, npm 12.0.2, Docker 29.7.2, Compose 5.5.1, Python 3.14.7 and Git 2.55.0.

Both service-owning children reported cleanup PASS. The strict supervisor verified all five parent phases, exactly three new manifests, both child manifest hashes and tested commit identities, unchanged clean source and preservation of every preexisting local container. A separate closure query found zero containers for both child projects and the earlier 1024 project. No competing acceptance run was launched.

## Evidence identity and reproduction

The original manifests remain locally retained with their private runtime evidence. The sanitized JSON selects reviewed fields; it is not a replacement raw manifest. Original manifest references:

- `artifacts/final/kit-final-20260924170635-842aed69/run.json` — SHA-256 `1a596041481fd7e9abb8e717ac6f02d155f58f678d867716744903248a5e0dd8`.
- `artifacts/runtime/kit-verify-20260924170737-0fe1e919/run.json` — SHA-256 `e90d488712bf8adb02ed0e1b1ad45f2c3c11cbf55ce356083b07fa106ef1d11c`.
- `artifacts/final/kit-final-20260924171013-ca3956cb/run.json` — SHA-256 `12373f993e0a00818b8077b385cb15d552f62345d2237adb2ece4a9e5c3b41d5`.

Strict supervisor result SHA-256: `43cf9cb38e0f38fa2074ecd48b2d4d0a045508b4553a06111b3320e61d9f2290`. Earlier 1024 manifest SHA-256: `f4be3b3184a2291acdb3b60d325aa70c562bd8581027d2eadaaba9c3eb8a3f9e`.

To reproduce in a fresh dedicated local clone with Docker and the documented prerequisites:

```sh
git checkout ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7
npm ci --no-audit --no-fund
npm run tools:provision
make verify-functional
make verify
```

This is the executed order: separate small functional acceptance followed by full default-million submission acceptance. The full command includes quality, UI build/fixtures, retained runtime and large faults. Preserve failures and inspect actual child manifests; an outer launcher being alive or exiting zero is insufficient proof. Provision sufficient local memory and disk for the database exports and disk-backed corrupted copies. The measured peak usage is an observation, not a universal minimum hardware guarantee.

## Preserved failures and remaining boundaries

Earlier failed local and server attempts remain retained. The `f0f98d8` million-row run failed G1 and did not execute later gates; the subsequent correction and actual counterexample remain in [closure development evidence](Acceptance-closure-development.md). Server `afee415` failed a 30-second Compose configuration bound during quality. Later 60-second Compose and 3600-second aggregate quality bounds were based on observed server durations; correctness, service and fault limits were not weakened. Server `ecdfdb5` was interrupted during lint to protect unrelated production, before service acceptance. Its interruption sidecar overrides the old signal-reporting wrapper's misleading zero exit. The corrected future wrapper was separately checked against six termination cases.

Production-host CPU throttling was confirmed and its removal was explicitly authorized. Heavy tests remained stopped there and completed locally under the user's distributed-check authorization. No reboot or additional provider reset was performed. Four earlier-failed workers in the recreated server demo remain preserved without restart or task deletion; this closure does not certify that demo healthy.

Operational pipeline reads intermittently timed out during G1. Receipt observation temporarily lagged behind sink and consumer delivery, then resumed without runtime changes. G1 waited for all required receipts and fresh terminal evidence; exact reconciliation passed afterward. The query cause was not isolated, and continuous status availability or an uncontended performance rate is not claimed. A read-only log inspection raced the scenario's planned replica scale-down; its missing-container diagnostic is retained separately and did not replace acceptance evidence.

Optional 2M, hosted CI, HA, independent database restoration, unlimited backlog and remote publication remain outside this result. No post-pass runtime optimization was made. Documentation and the sanitized local package complete this accepted local scope. Raw credentials, private configuration, database exports, Docker environments and payload logs are excluded; failures and original evidence remain preserved locally.
