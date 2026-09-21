# Integrated fault-gate contract

Status: implementation/verification in progress; no result is implied by this design. The retained runtime smoke is distinct from these faults and from the planned scale profile.

## One installation, real workers

A verifier owns a fresh explicitly named Compose project and uses the normal initializer, baseline seed, mutation commands, capture, backfill, receiver workers, consumer, observer and browser gateway. Fault-only overrides expose no business API: they pause actual production methods or record actual protocol responses in a run-owned volume. The external verifier sends real container SIGKILL or stops the owned Elasticsearch service.

No normal runtime service mounts a Docker socket, verifier volume or administrator configuration. Source/pipeline remain separate PostgreSQL transaction domains. A private observer can independently read its isolated databases and receivers but cannot manufacture the tested production results. The original immutable event and source-command contracts remain unchanged.

## Required gates

| Gate | Fault and independent evidence                                                                                                                                                                                                                                                                                                      |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1   | Pause after actual page/event/obligation/checkpoint SQL but before COMMIT, after earlier pages committed. Observe the old durable cursor and open transaction, SIGKILL the scanner, verify rollback and resume beyond the retained checkpoint. Concurrent real source commands target already-scanned entities and a new insertion. |
| G2   | Kill a consumer after its database COMMIT before ACK, then a publisher after real confirm before local settlement. Observe actual redelivery/duplicate publication of identical event content and exactly one retained consumer business effect.                                                                                    |
| G3   | Stop the actual Elasticsearch service for at least 60 measured seconds. Source capture, broker confirmation and consumer effects progress; ES work remains durable and attempts are bounded. The same worker recovers after service restart.                                                                                        |
| G4   | Stop ES admission, create/capture 500 declared small revisions, then send one real 500-operation bulk. Exactly three predeclared bad integer values produce actual mapper rejections; 497 settle. Every canonical event and independent stream effect remains retained.                                                             |
| G5   | Read real HTTP status/metrics and render the native Angular interface through its normal gateway. Assert distinct stage, broker and consumer values, explicit failure count, backlog-aware lag and historical backfill phase. No response interception or fixture fallback.                                                         |

Each private fault barrier also supports explicit healthy release. Missing evidence, skipped gates, a malformed response, timeout or incomplete cleanup fails the run. A gate's local observations do not excuse failed final reconciliation.

## Independent reconciliation

Expected baseline recipe and mutation requests/rejection declarations are written before execution. The Python/SQLite oracle does not import production normalization or completion logic. It compares exact source revisions, original command results, canonical bytes/hash, source ACKs, all sink/consumer relationships, mutation effects/aggregates, current receiver values/versions and tombstones. Receiver rejection exclusions come only from the verifier's declared workload, never from the worker's observed dead-letter set.

An explicit bounded functional fixture is not the required million-row capacity evidence. Functional output names its actual count and memory assumptions. The full-size invocation must be separately executed and measured before claiming assignment-wide scale acceptance; unrun or failed capacity stays nonpassing. The documented two-million-row target is not inferred from smaller tests.
