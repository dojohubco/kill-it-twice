# Server observation follow-up — 2026-09-26

Status: correction implemented with [scoped server query/upgrade evidence](evidence/Observation-projections-2026-09-26.md); clean full server acceptance pending.

[ADR 017](adr/017-transactional-observation-projections.md) records the selected narrow-state prototype before runtime implementation. The first server count diagnostic timed out at the unchanged limit; the memory experiment also failed and was rejected. Neither failed experiment is acceptance evidence.

The accepted `1f2f3f2` million-record run converged exactly and passed G1–G5, but recorded 505 nonfresh pipeline observations. A final fresh sample does not prove that a dashboard remains useful during load. The original slow-query execution plan was not captured; the specific cause must not be invented retrospectively.

The user authorized correcting this limitation on the existing server, clarifying the reviewer documentation, committing and publishing completed changes, and keeping the local checkout current. The server checkout was first fast-forwarded from `ecdfdb5` to `5b0643c`. This did not change existing running images or retained data.

## Required work and acceptance

- Measure the current snapshot and per-run observation reads separately. Preserve before/after query text, plans, timing, resource limits and any failed experiment. Synthetic relation benchmarks are diagnostics, not full-service acceptance.
- Choose a scoped correction from measured evidence. Keep exact counts, run membership, missing/pending semantics, restricted roles and the independent terminal proof. Never label cached/old data fresh, omit failures, increase correctness deadlines to pass, or use a success fallback.
- Verify fresh and populated-upgrade behavior, rollback/concurrency and relevant negative controls. Exercise concurrent ongoing delivery and repeated operator status/metrics reads; report every unavailable/stale sample and latency distribution. The new G1 drain check requires zero unavailable/stale pipeline samples, independent metrics scrapes, at most 60 seconds between samples and coverage from active staging to final drain; the million-record check must start before 25% is staged. This is a finite sampled claim, not proof of continuous availability between requests.
- Run the real fault verifier and exact large-data reconciliation on the corrected clean code. Historical million-record PASS remains attached to its original revision. A new availability claim needs the new observation interval and sample count, including observations during active processing.
- Use only isolated, explicitly owned validation resources. Preserve unrelated services, retained demo data and old failures. A later rollout must identify the exact code/image and preserve installation identities and volumes.
- Publish chronological commits, follow required hosted CI, and finish with matching local/server source identities and explicit runtime identity. No careers submission is authorized.

## Reviewer documentation

Separate a quick real small-fixture check from expensive full acceptance. Keep at least four decisions, alternatives and tradeoffs directly visible in README. Publish measured progress windows with denominators and timestamps; describe the observed bottleneck and a falsifiable improvement target. Mark adoption-era pending statements as historical without rewriting their original evidence.
