# Retained runtime and integrated functional gates

Recorded 2026-09-21. **Two complete functional G1-G5 executions passed** on clean committed code `545b2c8dc8772da8f77c0c726eab20c42075dc38`. Each uses 1,024 genuine source baselines, not a million-row fixture. Full-size capacity and final assignment-wide acceptance remain unrun. The later documentation commit is not substituted for the tested code SHA.

## Execution and independent evidence

| Invocation                      | UTC start       | UTC finish      | Outcome             |
| ------------------------------- | --------------- | --------------- | ------------------- |
| First fresh integrated fixture  | 19:25:21.627629 | 19:34:59.992322 | PASS                |
| `make quality` on the same code | 19:35:01.316963 | 19:35:46.672958 | PASS; 64 unit cases |
| Independent fresh repeat        | 19:35:46.844927 | 19:44:47.976828 | PASS                |

Both service runs execute `python3 -B scripts/verify-final.py --count 1024`. Their distinct projects are `kit-final-20260921192521-49c86bcd` and `kit-final-20260921193546-a245b0f9`. Both input inventories have SHA-256 `41c3db52dcf84cc6f6323bf379314b3e4977baa13972760bfe7efea8e5d1befb`. Each checks HEAD, working-tree state and every input hash after execution, then removes only its run-owned containers, volumes and networks. Both cleanup results are PASS.

| Gate | Actual result, in both runs                                                                                                                                                                                                                                                                   |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1   | The real scanner is SIGKILLed while page writes/checkpoint changes are inside an uncommitted PostgreSQL transaction. The prior durable cursor is 128 and does not advance on death. The same run resumes while real source updates/deletes/restore/insertion are captured.                    |
| G2   | A consumer is killed after effects COMMIT before ACK; a publisher is killed after actual broker confirm before local settlement. Recorded identical-wire redelivery and duplicate physical publication leave one effect per distinct mutation. Both boundaries have healthy release controls. |
| G3   | The actual Elasticsearch service stays stopped for 61.785 and 61.378 seconds. Capture, RabbitMQ and consumer progress; each outage creates 15 bounded ES attempts. The same runtime recovers after restart.                                                                                   |
| G4   | One actual 500-operation bulk returns 497 applied items and exactly three predeclared mapper rejections. Those three diagnostics remain durable; all stream events/effects remain processed. No local mock creates the rejected results.                                                      |
| G5   | Real gateway status/metrics report zero pending source work and three ES failures separately from healthy receiver connectivity. The actual Angular interface reads and inspects a record without intercepted responses, CSP errors or mobile page overflow.                                  |

## Exact reconciliation, not only counts

The frozen-fixture oracle uses declared baseline recipe/ordinals and source command requests. It reconstructs exact canonical bodies/hashes independently with Python integer/Decimal handling and SQLite, without importing production normalizers or completion queries. It compares source/receiver identities, current versions/payloads/tombstones, retained history, source ACKs, sink obligations, consumer inbox/effects/totals and declared ES rejections.

Each run retains 1,539 current source entities and 1,543 canonical/inbox events from 1,024 baselines and 519 distinct mutations. All 1,543 RabbitMQ and consumer obligations are satisfied; 1,540 ES event obligations are satisfied and three are dead-lettered. The three rejected new entities are absent from the receiver, leaving 1,536 actual current receiver documents. Historical backfill completion remains distinct from the later rejected mutations.
Five separate corrupted-export controls (missing document, extra identity, altered projected payload with copied hash, incorrect version and missing business effect) all fail the independent checker. Their deliberately altered snapshots never replace healthy receiver evidence. Unexpected failures are not converted into declared exclusions.

A supplemental offline inspection checked both complete reports, input identities, every command-output hash, actual container kill commands/exit 137, same-wire duplicate evidence, outage bounds, real 497/3 results, browser assertions and cleanup. This is artifact inspection, not a third service run. Its script/result are `artifacts/final-draft/audit-completed.py` and `completed-audit.json`. The compact tracked summary reproduces those checked identities without copying every log.

## Packaging and browser corrections

The root Compose smoke at `6e3194d717d469e111bedb995e55dde03f2ec780` separately passed R01-R06: cold setup, repeated seed/conflicting seed rejection, ongoing mutations, actual consumer kill, retained full down/up with exact exported state equality, real browser inspection and negative reconciliation controls. That fixture used 257 baselines and five mutations, not the later integrated fault workload.

The actual runtime browser uncovered a production-only issue: generated critical-CSS loading used an inline onload handler blocked by the gateway's script policy. Disabling only critical-style inlining made the full minified stylesheet load without weakening `script-src 'self'`. Subsequent failures were new verifier mistakes: assuming the inline record panel closes with Escape, using SQL `kind` rather than API `sink`, and expecting an ungrouped number instead of the UI's exact `1,543` display. Each failed run remains FAIL with scoped cleanup; assertions were corrected to the actual contracts, not removed. [Runtime observations](Runtime-development.md) and [fault-gate observations](Final-gates-development.md) preserve that chronology.

## Local artifacts and limits

These are local checkout paths, not published URLs:

- `artifacts/final/kit-final-20260921192521-49c86bcd/`: first complete command/protocol/SQL/browser and oracle evidence.
- `artifacts/final/kit-final-20260921193546-a245b0f9/`: independent repeat.
- `artifacts/final-draft/repeat-commands.json`: quality/repeat command results.
- `artifacts/runtime/kit-verify-20260921183802-05c339f9/`: retained-runtime smoke.
- `artifacts/capacity/reconnaissance/`: explicitly small-fixture query plan and container samples, not a capacity benchmark.

All service/tool pins and source/event/acknowledgement semantics remain unchanged. Normal runtime has no test barriers or Docker socket. Verification adds private observation wrappers only in its run-owned override; those wrappers forward actual operations and results. The external launcher performs the real faults.

No hosted workflow was dispatched or claimed successful. No remote publication, history rewrite, privileged workstation tuning, fake throughput or two-million-row result is included. `make verify-functional` executes this finite functional contract. Full `make verify` remains nonpassing until large-data/final acceptance is implemented and demonstrated; the old NOT IMPLEMENTED labels are not aliases for these new functional passes.
