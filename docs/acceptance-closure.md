# Acceptance closure, 2026-09-23

Prospective scope: finish the existing submission; no architecture expansion, publication, host tuning or new service. The original assignment is represented by the retained pre-code SPEC and the user's final acceptance requirements; no separate assignment file was found among tracked filenames.

## Remaining gaps and decision

Replace the placeholder `make verify`; execute large faults and exact independent reconciliation; exercise real operator controls and fresh root commands; consolidate README and CI; produce an honest matrix and sanitized local evidence package. Existing tests are evidence only at their recorded identities. Preserve every failed run separately.

The default final fixture is **1,000,000 distinct baseline entities**, recipe `local-runtime-v1`, with measured representative payload near 1 KiB, plus the existing 519 unique concurrent/fault mutations. This prospectively revises the earlier 2,000,000 target because the assignment requires nontrivial bounded-memory replication, not that exact count. The explicit 2M profile remains available and unproved. Never reduce a failed selected fixture silently.

Use the normal retained application and existing independent SQLite oracle. Page ceiling 64 and byte ceiling 256 KiB remain enforced. Start one scanner for deterministic healthy/SIGKILL boundaries; after the observed kill, use the already supported four scanners and two ES/publisher/consumer/observer processes for the large drain. Return to one of each for individually observed G2/G4 barriers and the single 500-operation bulk. Each worker retains 256 MiB (128 MiB Node heap); source/state PostgreSQL 1 GiB each, ES 2 GiB, RabbitMQ 1 GiB. No database or delivery semantics change.

## Commands and finite budgets

After the inherited sequence has exited and cleaned its own project, commit this note before implementation. Then commit only necessary verifier/runtime-test changes and reviewer documentation, in chronological subsystem commits.

In a fresh local clone of one clean committed candidate:

```sh
npm ci --no-audit --no-fund
npm run tools:provision
make verify
make verify-functional
```

`make verify` must run quality, complete UI fixture inventory, fresh root Compose/explicit `make seed`/retained restart and real operator checks, then `scripts/verify-final.py --count 1000000 --page-records 64` against another run-owned installation. It prints G1-G5 and fails on missing cases, failed reconciliation or cleanup. The separate functional repeat uses 1024 baselines and is labeled development evidence. No old evidence file supplies a passing result.

Admission requires at least 8 GiB available host memory and 80 GiB available storage for 1M, with continuous 2 GiB memory / 10 GiB disk reserves. Record actual process/cgroup peaks, CPU, payload bytes, database/index storage, seed and drain durations. Avoid six simultaneous SQLite scratch copies: retain primary exports and negative result logs/hashes; remove only each completed negative oracle's own reconstructible scratch database.

Audit all inherited deadlines before execution. Keep transaction/lease/remote timeouts unchanged. Seed retains 7200 seconds; large G1 scan/drain gets 10800 seconds (262144 observations around 26 minutes imply about 100 minutes at 1M before margin, not a performance promise); small incremental settles keep 360 seconds; each streamed export keeps 3600 seconds; each disk-backed oracle keeps 7200 seconds. Existing 24-second fault barriers, per-query/row/output bounds, 60-second outage minimum, browser failure propagation and cleanup remain mandatory. An enclosing finite acceptance budget and CI timeout are recorded in the implementation; budget expiry remains failure, with cleanup separately recorded.

## Stop conditions

Stop optimization once the selected workload passes. Only a reproducible blocker permits a minimal correction with preservation/correctness regression and a new clean candidate. Stop with NOT READY if required evidence remains failing or unavailable. A later documentation-only commit names the tested SHA separately. No push, dispatch, PR, upload, credential rotation or public submission is authorized here.

Inherited sequence inspected: `93eac82044068e697c42545fb4f71041f8e8baef`; populated/corruption proof `kit-final-20260923103508-0f6d63b3` PASS, functional `kit-final-20260923103702-d8f04bd1` PASS, capacity `kit-final-20260923104314-5aee3dd1` RECONCILED_COMPLETE, all cleanup PASS. The capacity fixture has 262144 baselines and zero mutations; it is not final assignment acceptance. Original failed attempts remain retained.

## Closure outcome, 2026-09-24

The local clean candidate `ecdfdb5063aa7977af6bceccdaa6b1ece3fdece7` passed the complete default-1M `make verify` and the separate 1024 functional check. [The final report](evidence/Final-submission-2026-09-24.md) records exact identity, phases, oracle, five negative controls, measured resources and cleanup. The user's later authorization allowed independent local execution after shared-server production contention required stopping the server series. This local result does not relabel the interrupted server attempt.

The original 10,800-second prospective drain allowance above is historical. The subsequent [restart decision](acceptance-restart.md) established 21,600 seconds for 1M and 36,000 seconds for the fault child before this run. Seed, incremental settle, export, oracle, lease, query and fault limits remain as specified by that later decision and the final gate contract. No bound was changed during the passing run. Optimization stops here; the follow-up commit changes documentation/evidence only. Publication and the optional 2M profile remain unperformed.
