# M5A: retained source baselines and controlled activation

Authorized 2026-09-20. Baseline inspected: clean main at 685b37158fae5dd19e44425a00a553bce979e158; no later commits or unrelated edits. M4.1 tested code remains bba64653878ac1dadc7a7fe1ea5d2e4ea9c3860e. Read SPEC, AGENTS and ADRs 001–011. No remote write, history rewrite, privileged workstation configuration, new dependency or service is authorized.

The known baseline/no-op receipt incompatibility is planned integration work, not a new discovery. M5A adds genuine version-1 baseline evidence, bounded resumable seed chunks, an explicit closed-to-active boundary, and selected small real sink exercises. It does not add a backfill scanner, pipeline run/checkpoint/fence, replay API, UI, scale acceptance or G1–G5 PASS.

## Actual starting evidence

Read-only GitHub metadata shows run 35467226835 completed FAILURE at the reviewed documentation SHA. Job 105961691450 hit scripts/ci-step.ts's 600000 ms fallback: its longer timeout allowlist omitted verify-m4-1. The wrapper recorded actual SIGKILL/timedOut=true; this is not a failed byte-boundary assertion. Downloaded artifact 10592025694 SHA-256 328c3f89eab1d0bffe45faad374a82bafd53f6b9918517e588cc3fb56ee6376b contains nine completed passing profile manifests, including the first ES profile; later profiles are not established. Full logs/inspection are local under artifacts/m5a/hosted-35467226835. The external reviewer did not execute local services or inspect this completed artifact. Preserve the failure, and correct the explicit finite gate timeout when adding verify-m5a.

Local Node 24.19.0/npm 12.0.2, Docker 29.7.2/Compose 5.5.1 and existing vm.max_map_count=1048576 are available. No pins or kernel values changed. The unchanged npm ci / make verify-m4-1 baseline is running with exact outputs under artifacts/m5a/baseline-685b371; its result will be recorded when obtained, not anticipated here.

## Design and independent inventory before implementation

ADR 012 specifies the proposed source migration, database authorization, shared/exclusive lifecycle locking, chunk/activation transactions, reader and conditional receipt evidence. scripts/required-bootstrap-cases.ts is the independent inventory. The design precedes production implementation and service execution of these cases.

- BS01: 257 actual baselines across bounded chunks, independent recipe/ordinal comparisons and zero mutation/outbox/work/receipt/downstream seed effects.
- BS02: manifest/chunk replay and real independent-session contention, stable IDs/time/content, conflicts reject.
- BS03/BS03H: real pre-chunk-COMMIT SIGKILL and healthy release, independent transaction/invisibility and safe resume.
- BS04/BS04H: real post-chunk-COMMIT/pre-success SIGKILL and healthy release, retained mapping recovered exactly.
- BS05: closed writes including legacy/no-op/replay paths; incomplete seal/activation reject; actual lock races; identical binding resume and replacement rejection.
- BS06/BS06H: activation kills before/after source COMMIT plus both healthy controls, closed-or-active/bound recovery; old-snapshot rejection and READ COMMITTED control.
- BS07: real unchanged-baseline successful command receipt, historical replay after mutation/deletion, concurrent key arbitration and conflict.
- BS08: genuine higher mutation revisions, new post-activation mutation-v1 entities, outbox/work atomic failure, isolation guard.
- BS09: bounded baseline/current/outbox parity, immutable historical reads, exact Unicode/numeric/time metadata, missing/oversize rejection.
- BS10: explicitly selected source-backed baselines through real ES/RabbitMQ/consumer; zero-unit aggregate and no mutation effect; repeat/restaging preserve outcomes.
- BS11: real newer mutation/tombstone before baseline, no regression/resurrection, independent mutation effects/history.
- BS12: actual role restrictions, immutable history/lifecycle, exact conditional receipt checks including NULL loopholes.
- BS13: populated M4.1 upgrade preserves source and downstream evidence, explicit active compatibility, never retroactive baseline.
- BS14: retained restart and independent baseline-map/mutation-journal reconciliation, negative controls and scoped cleanup.

Use separate fresh bootstrap and populated upgrade profiles; historical profiles keep their own migrations/assertions. New profile cases are deliberately selected by applicability, never skipped. Every new process fault uses private instrumentation around the actual owned transaction, exact PID/signal/session observations and its healthy control. All waits are bounded. Final complete gates include the unchanged earlier inventory once, with a fresh complete repeat on committed clean code. Unknown commit outcomes stay unknown; explicit identical retry recovers retained evidence.

## Actual pre-implementation baseline observations

On 2026-09-20 local time, `npm ci --no-audit --no-fund` exited 0. The unchanged `make verify-m4-1` executable baseline completed fifteen profiles before its outer execution ended with 143 during final-profile finalization. The final test report itself contained eight passing cases, but that incomplete orchestration is not called PASS. Only its identified run-owned resources and private files were removed. A fresh `npm run test:integration:m41 -- --upgrade` then exited 0 with restart and cleanup, run `m41-20260919211359560-b0247535`. Logs are under ignored `artifacts/m5a/baseline-685b371/`.

The baseline started at 685b371. The authorized design/inventory commit 1e52216 was made while it ran; it changed no executed production or test code. Later baseline manifests therefore identify that clean documentation state. Final M5A acceptance will require one committed application SHA throughout each complete invocation.

Read-only hosted inspection found M4.1 run 35467226835 failed: the wrapper used its 600000 ms fallback because verify-m4-1 was absent from its longer-deadline allowlist. Nine completed profiles passed; complete hosted acceptance was not established. The additive M5A gate includes an explicit finite deadline and a unit regression for command selection. This changes neither local host settings nor service safety checks.

## Final local execution observation

On 2026-09-20 local time, two complete `make verify-m5a` invocations passed at clean application/test commit `4178e2112452881a708cba5e0a5689f57073df96`. Each ran 54 unit checks and the 18 explicit profiles (294 required integration cases); every profile cleaned up. Both new fresh profiles passed 18 cases, and both populated-source upgrade profiles passed BS13. Supplemental independent offline checks reconciled each new final snapshot. `make verify` exited 2 with G1–G5 NOT IMPLEMENTED. See [M5A.md](../evidence/M5A.md), its compact machine summary and the preserved development record for exact commands, failures/corrections, input identities and artifacts. M5A hosted execution remains NOT RUN; the reviewed M4.1 run was refreshed read-only and remains completed/failure. Stop before M5B.
