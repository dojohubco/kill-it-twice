# Operational recovery completion

Authorized 2026-09-21. Starting local HEAD: 421b31ae04ab953fe83f14f83de0987b882e0bc5, clean main. This continuation implements the outstanding bounded recovery/read-model requirements from the supplied M6 task. It preserves all pre-existing commits and the existing local Nest API; that API predates this continuation and is not newly authorized UI work. No frontend, new HTTP surface, deployment, dependency or schema rewrite is planned.

## Baseline and scope

`make quality` and the existing real-service `test:integration:m6` passed at the starting HEAD (17 required fresh operational cases). Logs: local `artifacts/m6-completion/baseline-*.log`. Full earlier gates have not yet been rerun in this continuation. The latest published backfill run 35508282031 was observed completed/success at ab00bdd; no claim of a published M6 result is made.

Add a bounded CLI/domain facade alongside the retained API. Complete atomic multi-selection ES replay (1..50), explicit replay-to-attempt history, verified supersession and same-target verify/resume, timestamped current/last-known observations, reset-aware useful rates, and bounded navigation for replay/run/attempt metadata. Retain canonical history, source ACKs, RabbitMQ state, consumer effects and historical run outcomes.

Forward pipeline migration 008 adds request/item/check evidence and current replay correlation. Existing migration 007 and its single-event API remain historical and compatible. The implementation plan is in ADR 014's dated completion refinement; metric meanings are in docs/metric-definitions.md.

## Independent acceptance additions

| ID   | Required proof                                                                                                                               |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| RC01 | Batch admission is atomic, idempotent and current-failure-specific; concurrent different keys cannot both schedule the same failure.         |
| RC02 | Failed replay preserves original and new errors, retains every correlated attempt, and does not reset the other sink or source ACK.          |
| RC03 | Verified higher remote/local revision can satisfy an old ES obligation as superseded; missing/changed/unknown evidence cannot.               |
| RC04 | Operator verification of a blocked target is fenced and audited; invalid binding/credentials leave it blocked.                               |
| RC05 | Historical complete_with_errors remains immutable while the fixed run's current recovery view improves without multiplied counts.            |
| RC06 | Real pre/post replay-admission COMMIT SIGKILL and healthy controls preserve zero/one admitted operation under the same key.                  |
| RC07 | Bounded operation/attempt/run/failure navigation, stale selection and permissions fail closed without hidden writes.                         |
| RC08 | Fresh/idle/unreachable observations, last-known timestamps, warming/reset-aware rates and broker/consumer distinction match independent SQL. |
| RC09 | Populated extension preserves previous event/failure/control evidence and reloadable recovery state across retained restart.                 |

Existing OP01-OP18 and earlier profile inventories remain mandatory. Pure sampling/validation fixtures supplement real receiver/SQL/fault evidence. Final complete gates run sequentially from committed code and clean tracked trees; missing prerequisites, failures and incomplete runs remain explicit. No future milestone or UI work is authorized.
