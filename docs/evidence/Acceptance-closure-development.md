# Closure development evidence, 2026-09-23

The note `0be5d2a` preceded implementation `6f6281a`; documentation followed at `2ee4cee`. No production replication invariant changed.

Initial static checks caught an over-narrowed TypeScript request-array length assertion and the undeclared custom optional CI runner label. Both were corrected without removing assertions. The next quality run exited zero. Logs remain under `artifacts/submission-closure/`.

The first new runtime check at clean `2ee4cee`, `kit-verify-20260923113230-064d2dad`, passed R01-R03, exact before/after exports and both independent oracles, then failed R04's exact summary equality. Inspection compared `052-state.stdout.log` with `068-state.stdout.log`: every value matched, but the grouped sink rows changed order from RabbitMQ/Elasticsearch to Elasticsearch/RabbitMQ. The query lacked ORDER BY. The correction orders this diagnostic summary by kind,state; the exact summary assertion and all ten byte-for-byte export comparisons remain unchanged. Cleanup PASS; R04 and later cases are not called passing for that run.

G1's closure audit also makes overlap explicit: resume the killed scanner before issuing the existing five unique commands, observe a nonclosed scan beforehand and retained page progress afterward. This adds evidence without changing the journal count, page/checkpoint transaction or independent oracle. The repository browser runner now describes its actual execution path rather than asserting that the interactive Browser plugin is unavailable.

## Million-row sealing failure and bounded correction

At clean `f83149b67bb963063f6b98b92a702f199324afc4`, fresh `make verify-functional` passed G1-G5, exact reconciliation of 1024 baselines and 519 mutation effects, all five negative controls and cleanup: `kit-final-20260923114410-4fcee440`. Fresh `make verify` passed quality, 10 UI fixture cases and all seven retained-runtime cases, then failed its million-row child `kit-final-20260923115421-9bdaaa8f` during sealing after committing all 1,000,000 baselines. G1 FAIL, G2-G5 NOT RUN; cleanup PASS. Parent `kit-final-20260923115015-b35f8ee1` returned nonzero. These are not full acceptance passes.

The initial deadline audit missed the bootstrap transaction owner's ordinary 12-second SQL limit. Runtime diagnostics also omitted TransactionError SQL state and outcome. An isolated unchanged-code reproduction, `kit-final-20260923121101-50cd86eb`, reproduced SQLSTATE `57014` at 12.017 seconds with `rolled_back` outcome, then executed the exact same `source.seal_bootstrap` proof in 18.078 seconds under a finite diagnostic budget. It retained every per-chunk content check and committed the sealed phase; cleanup PASS. This diagnostic is not replication/fault acceptance.

The correction gives only full baseline sealing a 60-second SQL / 65-second client budget, about 3.3 times this measured million-row query. This is a finite allowance for the declared proof, not a 2M performance claim. Seed chunks, status, activation, source commands, leases and all SQL invariants retain their prior behavior. Safe runtime errors now retain SQL state, transaction phase/outcome and cleanup-error count without private cause text. Unknown outcomes remain unknown. No automatic transaction retry was added.

Quality passed with 85 unit cases. The current-capacity bootstrap profile `m5a-20260923122623880-f61fe1c9` passed its real 18-case inventory, including crash/healthy barriers, sealing, privileges and corrupted baseline detection. These targeted checks used the recorded working-tree patch; final acceptance still requires a new clean committed candidate.

The populated-upgrade preservation profile `m5a-20260923122924619-6df9e039` also passed, with cleanup PASS. No historical event, receipt, source identity or migration assertion was removed.

## Evidence-directory portability and hosted failure visibility

Read-only inspection observed hosted run [35861611483](https://github.com/dojohubco/kill-it-twice/actions/runs/35861611483) at `b2e56aa` fail G1 after quality passed; G2-G5 and the UI job were not run. Its upload contained the CI wrapper logs but omitted the detailed final-run report, so the precise hosted G1 cause cannot be asserted from those artifacts.

A separate real unprivileged-container reproduction established a concrete portability defect: the normal worker UID 1000 could not write fault evidence in a 0755 directory owned by UID 1001 (`EACCES`). The directory owner enabling group write (0770), plus adding only that directory's existing GID to the fault container, made the same write succeed. No root container, chown, global group change or privileged host operation was used. This is a reproduced verifier defect, not proof of the unseen hosted error's exact cause.

The owned million-row attempt `kit-final-20260923124357-3c5d7731` was deliberately interrupted through its SIGTERM cleanup handler while seeding (last retained progress 999424) before a verifier correction would invalidate its code identity. The manifest records KeyboardInterrupt / verifier termination, G1 FAIL and G2-G5 NOT RUN; cleanup PASS. It is not a product-integrity counterexample or completed acceptance. The prior first million-row sealing failure remains a separate genuine failure.

Fault containers now retain UID 1000 and gain only the run directory's existing group. An early actual write/unlink probe checks all four fault roles before seeding. Normal application Compose roles are unchanged. On failure the verifier captures bounded worker diagnostics, preserves the primary error and cleanup separately, and writes a curated final report under its public evidence directory. The existing fast-CI upload includes that directory. Quality passed with 85 unit cases; a new clean fast and full run are still required.

## Fault-wrapper memory retention at the final workload

At clean `6662839642d07f4bb7dbc272adc9b3093ccb4816`, the default command passed quality, ten UI fixture cases and all seven fresh retained-runtime cases. Child `kit-final-20260923130711-5dbd24f2` seeded, sealed and activated all 1,000,000 baselines in 723.071 seconds. During G1 drain, all four scanner processes, both ES workers and both consumers exited 139 after `FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory`. Their limits remained 128 MiB Node heap / 256 MiB container; Docker did not report a cgroup OOM kill. The last observed staging count was 146373. The verifier was deliberately terminated through its owned handler after preserving identities and crash logs, rather than waiting out the three-hour drain budget with dead workers. G1 FAIL, G2-G5 NOT RUN; cleanup PASS. Parent `kit-final-20260923130318-80412146` returned nonzero.

The private fault wrapper used `node:test` method mocks, whose call history retained every request/result even with trace recording disabled. An isolated diagnostic using the same interception mechanism processed 32768 distinct 1-KiB payloads and retained all 33554432 payload bytes in 512 calls; live heap after collection grew from 5261960 to 41921600 bytes. Direct forwarding processed 1000000 such payloads with live heap moving from 5256200 to 5306464 bytes. These are interception diagnostics, not replication acceptance. Raw probes and the actual failed worker logs remain in `artifacts/submission-closure/large-worker-failure/`.

The correction replaces only the private mock-recording mechanism with direct method interception in the dedicated verifier process. It retains the same real SQL, broker and ES calls, barriers, trace bounds and assertions; no production worker behavior or memory limit changes. G1 drain also checks owned worker counts/states every thirty seconds and fails promptly on an unexpected exit. Initial typecheck rejected an incomplete AMQP overload signature; the wrapper now forwards both actual channel overloads. A new clean fast fault run and full million-row run remain mandatory.

At clean `79ad5926d14885994eeb9198965f6f0c3560b172`, fresh `make verify-functional` passed G1-G5, exact 1024-baseline/519-effect reconciliation, all five negative controls and cleanup (`kit-final-20260923133532-18de7eda`). The next default invocation passed quality and ten UI fixture cases, then failed the real operator R07 check before starting the large dataset. R01-R06 passed; cleanup passed. `kit-verify-20260923134342-a58b75af` retains the Playwright `Network.getResponseBody: No data found for resource with given identifier` failure during pause confirmation. Parent `kit-final-20260923134234-1d1c5a59` correctly reports every large gate NOT RUN.

The browser test previously waited for click completion before reading the response body. It now reads immediately when the exact visible idempotency-key response arrives, while the click completes concurrently. The 202/body-shape assertions and all backend/UI state assertions remain required; there is no retry, swallowed error, fixture response or success fallback. The CDP error does not establish a production operation failure or an actual page navigation. Fresh execution must establish the corrected test outcome.

## Million-row operational snapshot timeout

At clean `f0f98d8e8ccf2b8f2688ae1a898fd3ac774ef7a7`, parent `kit-final-20260923135055-e2db0798` passed quality (85 unit cases), all ten UI fixture cases and fresh retained-runtime R01-R07 (`kit-verify-20260923135206-667ea70f`). Child `kit-final-20260923135640-99be3525` seeded all 1,000,000 baselines in 1092.736 seconds, exercised the scanner fault and five concurrent mutations, then failed `Deadline: settle-G1` after its unchanged 10800-second drain budget. G1 FAIL; G2-G5, final independent reconciliation and negative controls NOT RUN. Cleanup PASS. The parent returned nonzero at 17:17:28 UTC.

Failure-time worker logs report 1000005 satisfied deliveries for each sink and backfill `completed_at=2026-09-23T17:01:27.807527+00:00`. Fresh consumer status reports 1000005 processed events, five effects and no quarantine; source reports five acknowledgements. Those observations are not the missing independent final reconciliation and do not turn the failed gate into PASS.

The operator pipeline snapshot stopped returning fresh data after 14:31:23 UTC. A read-only live session observation at 17:14:38 identified backend 851992 executing the unchanged `queries.pipeline.snapshot`, started at 17:14:36.967692. PostgreSQL logged that same backend's statement timeout at 17:14:39.467 UTC, matching the operational read's 2500-ms limit. The bounded preceding log sample contains repeated cancellations. Exact diagnostic records are in `artifacts/submission-closure/pipeline-active-query-observation.json`, `pipeline-snapshot-timeout-confirmed.json` and `failed-million-status-summary.json`. No running inputs or deadlines were changed.

The next bounded investigation measures the snapshot and its component queries in a separate owned PostgreSQL installation. A diagnostic-only copy may expand normally replicated fixture rows to reproduce relation size; such copied rows are explicitly not a million-entity replication result. Any correction requires preserved exact query semantics and relevant real-service regression before a new clean full acceptance run.

The isolated diagnostic `kit-final-20260923173551-816a9d62` normally replicated 4096 baselines, quiesced its workers and expanded copies in a separate diagnostic schema to one million keys. Copied canonical bodies were deliberately not presented as valid million-entity replication evidence. The first snapshot plan took 16777.799 ms; after VACUUM the unchanged plan still took 3836.594 ms, with repeated wide-table scans. A covering consumer-observation index and a partial index containing every unresolved ES state reduced a later quiescent snapshot observation to 911.207 ms. Cache state differs across these observations, so this is not a controlled throughput multiplier. Removing those two indexes inside a rollback-only transaction reproduced SQLSTATE 57014 under the unchanged 2500-ms snapshot limit. A third trial delivery-covering index was not used by the plans and was removed rather than shipped.

The same exact backfill state-count function measured 4725.141 ms before indexes, 3585.514 ms with the trial indexes and 2804.719 ms in the final selected-index observation. Only the operational backfill read receives an 8000-ms statement budget, about 2.2 times the larger indexed observation. The normal snapshot and other reads retain 2500 ms; the existing 12000-ms database default, 15000-ms query-client limit, 12000-ms browser request limit, 30-second freshness window and separate 180-second full terminal proof remain unchanged. Status and backfill are sequential, so their SQL ceilings total 10.5 seconds; failures still produce unknown/unavailable.

All quiescent snapshot values and exact state counts matched before/after, excluding only the observation timestamp and ordering of grouped arrays with no ordering contract. Rollback-only diagnostic controls retained 1000 pending ES rows, three terminal ES errors, two quarantined consumer observations and one missing consumer observation, with independently asserted exact partitions and `invalid=null`. The snapshot passed its 2500-ms limit and the backfill observation its 8000-ms limit. This does not replace the existing full-content corruption tests or final real-service run. Plans, SQL, exact comparison and the intentionally excluded overlapping index-build measurement are retained in that diagnostic directory.

The populated upgrade/corruption check `kit-final-20260923175335-d5798f2a` passed on 4096 normally replicated baselines, including byte-for-byte preservation of events, delivery intents, consumer observations and backfill evidence; unchanged function metadata; exact operational snapshot equality; healthy/missing-member/wrong-checkpoint/wrong-batch-hash comparisons; restricted-role reads and DDL denial; independent exact reconciliation; and cleanup. It used the retained pre-016 image and the recorded working-tree migration patch, not a clean final-code claim. The index migration SHA-256 is `a3d79b38fe286c0766292431c28d6e5e41205d6ceda174ce3f2f87ad503ed894`.

The first quality attempt after the scoped budget change failed two unit assertions expecting the previous 2500-ms backfill budget. The final implementation retains 2500 ms for capability discovery and the legacy full-status fallback, extending only the discovered observational function to 8000 ms. The existing tests now assert those exact separate budget sequences and confirm that a subsequent ordinary pipeline snapshot still receives only 2500 ms; no assertion was deleted to hide a failure. The failed quality log is preserved separately from the corrected rerun.

The corrected `make quality` exited zero with all 85 unit tests passing, no skips, and successful formatting, lint, type, Knip, Compose and actionlint checks. Fresh clean-code fault and million-row acceptance remain required.

## Server relocation and browser response capture

The user explicitly moved Kill It Twice development and verification to the existing server, accepted shared-resource slowdown, and separately authorized the documented Node/npm/make/browser prerequisites and a non-root developer account. The retained 1024-baseline demo is isolated as `kit-server-dev`; this is not large acceptance. The local clean `3ab2df1` million run (`kit-final-20260923182506-b71a93f1`, parent `kit-final-20260923182122-7d8a9be6`) was interrupted at the user's request, using a revalidated process identity and SIGTERM. It ended at 2026-09-23 20:49:38 UTC with G1 FAIL (`KeyboardInterrupt: Verifier termination requested`), later gates NOT RUN, and cleanup PASS. This was an intentional relocation interruption, not a passing run or evidence of spontaneous application failure. Its local artifacts remain preserved.

The first server `make verify` at clean `3ab2df1` (`kit-final-20260923210345-eeb1dc45`) passed quality, the UI build and all UI fixture checks. Retained runtime `kit-verify-20260923210531-dd6f7a95` passed R01-R06, including exact before/after-restart reconciliation. R07 failed on Chrome 154.0.8037.57 while reading the first real control response through Playwright's `Network.getResponseBody`: `No data found for resource with given identifier`. Cleanup PASS; the million-row phase and G1-G5 were NOT RUN. The earlier immediate-read correction did not eliminate this failure. The error alone proves neither an actual navigation nor a failed backend operation. The original stderr and reports remain under the server acceptance checkout.

The targeted verifier correction observes a bounded clone of the actual browser fetch stream instead of retrieving its body again through DevTools. It returns the original response to the UI unchanged, sends no second request, and uses no HTTP fixture, route fulfillment or success fallback. The same visible idempotency key must match the real HTTP 202 response and captured body; envelope/data object assertions remain, with an added exact envelope/header request-ID comparison. Capture errors fail the test. The existing UI's 4 MiB response cap and 12-second request budget bound this observation. All nine real commands, accepted-versus-completed wording, backend state checks and immutable replay assertions remain required. No production UI or replication code changes. Fresh real runtime regression and clean-code final checks remain pending.

The targeted working-tree run `kit-verify-20260923212538-b8e0518a` passed R01-R07, including all nine real browser writes, the exact request-ID/body assertions and replay preservation. Its overall result is nevertheless **FAIL**, cleanup PASS: during the run the assistant appended this development note, changing the working-tree status and violating the verifier's unchanged-input guard. That workflow mistake is preserved, not relabeled as acceptance or fixed by weakening the guard. The tested browser-script SHA-256 is `90b4cb7285d0e0ef12e75bdd002cde66222873a5ce226b9616a72d74d60bcd75`; the patch is retained in `artifacts/server-setup/operator-response-candidate.patch`. All subsequent acceptance runs use a clean committed isolated checkout that is left untouched.

## Server restart after user-requested interruption

Candidate2738166 full run `kit-final-20260923215128-2b601a2a` was stopped at the user's explicit request using validated PID/start-time/cwd/command and pidfd SIGTERM at2026-09-23T23:33:46Z. Its G1 result remains FAIL/KeyboardInterrupt, G2-G5 and final oracle NOT RUN, cleanup PASS23:34:01. The parent series returned2. Evidence lives in `artifacts/server-acceptance/candidate-2738166`, including user-requested-stop.json and bounded read-only diagnostic snapshots. This was not a spontaneous application crash or a passing workload.

Both PostgreSQL and Elasticsearch had no configured CPU quotas. Their measured252.51% and1.54% CPU use was demand, not assigned capacity. The real run recorded repeated statement timeouts; the activity snapshot alone did not correlate a specific recovery query with cancellation.

The separately owned `kit-final-20260923233559-3fe27d3a` normally replicated4096 rows, then used isolated schema copies expanded to1M mixed keys for SQL diagnosis. These are explicitly not1M replication evidence. Original snapshot22093.080ms, repeat26850.963ms; the repeat included7637.021ms JIT compilation. The receipt-state count scanned422233 heap blocks and took8609.341ms. A two-index/ordered-LIMIT trial still took8585.771ms and was not selected. Its all-settled diagnostic bulk UPDATE exceeded a60-second fixture deadline; the raw failure remains in artifacts/server-restart/query-trial.log and its diagnostic outputs.

Selected: three narrow metadata indexes, one shared exact delivery grouping, original MIN/join semantics for oldest unresolved event, and session-local JIT-off only for operational pipeline snapshot/backfill reads. The selected plan took1536.497ms and executed twice under the unchanged2500ms snapshot limit. Exact normalized response comparisons passed mixed, missing-event, receiver-rejected, missing-observation and real4096-all-settled cases. Cache/autovacuum state varied; this is a query comparison, not a throughput multiplier. The diagnostic finished DIAGNOSTIC_RECORDED with cleanupPASS. Plans and exact comparisons are retained in index-jit-comparison.json and selected-comparisons.json under its owned directory.

`python3 -B scripts/capacity/observation-check.py --metadata-upgrade` then passed on a real4096 populated273 image (`kit-final-20260923235236-384e633a`): exact source/pipeline/receipt/batch snapshots preserved, original/new operational outputs equal, function metadata preserved, healthy/missing-member/wrong-checkpoint/wrong-hash controls, restricted-role denial and independent exact4096 oracle PASS; cleanupPASS. This is developmental preservation evidence at the recorded working-tree input, not clean final acceptance. `make quality` also passed85 tests with no failures/skips; log artifacts/server-restart/quality.log.

The prospective restart note records six hours for1M G1 based on measured65-80events/s, ten hours for the whole fault child and consistent enclosing deadlines. Ordinary SQL, transaction/lease/remote ambiguity, full terminal proof, worker256MiB limits and per-page/event byte bounds remain unchanged. The next clean candidate must still pass fresh fast and default million-row acceptance before submission readiness.

## Resource sampler lifecycle correction, 2026-09-24

Clean `83a49ec` fast run `kit-final-20260924000041-a54aeb07` failed in negative-controls with `Resource sampling failed`. G1-G5 and the independent 1024-baseline/1539-entity/1543-event/519-effect/three-rejection reconciliation passed; the five negative controls did not complete. Cleanup passed. The series correctly withheld the million-row run. Its original resource summary retained only RuntimeError, so the exact original Docker stderr is unknown, not retrospectively proved.

A bounded real-Docker reproduction (`artifacts/server-restart/sampler-before/result.json`) removed one owned ephemeral container between the sampler's list and inspect. Docker returned exit1, valid inspection of the remaining owned container, and `error: no such object` for precisely the removed ID; the old sampler failed. The corrected sampler accepts only that exact missing-ID outcome, records disappeared IDs explicitly without invented peaks, and checks that returned plus missing identities exactly match the requested set. Daemon/permission failures, unexpected IDs, mixed errors, incomplete responses and other exit codes remain failures. It retains command/output/sample bounds and now records a bounded error message. This is observation lifecycle handling, not a replication or resource-budget change.

The same real two-container reproduction passed after correction and both reproductions cleaned only their owned containers. Six targeted Python cases cover units, foreign ownership, explicit failure, exact disappearance, unrelated errors and incomplete responses. The failed clean fast report remains unchanged; a new clean candidate must rerun the fast gate and then the default million-row workload. No unchanged passing candidate is rerun to inflate totals.

After the sampler correction, `make quality` passed all 85 unit tests with zero failures/skips (`artifacts/server-restart/quality-sampler.log`).

## Million fixture worker exit and missing diagnosis, 2026-09-24

Clean code `213ab3d11712e60bd261cc7cb7ba11e798592044` passed the same-code
1,024-baseline functional gate, all five oracle negative controls, resource
sampling and cleanup. The fresh full run passed quality, UI build/fixtures and
retained runtime including real controls. Its genuine million-baseline child
`kit-final-20260924002742-61c8a803` failed G1 at 02:20 UTC: the health check
required two running Elasticsearch workers and observed one running and one
exited. G2–G5, final reconciliation and the large negative controls were NOT RUN.
Cleanup passed at 02:20:43 UTC. The full series exited 2. No timeout expiry or
out-of-memory cause has been established.

The worker's retained log contains only `es-delivery-failure` with classification
`transaction_or_cleanup`; it discarded the underlying transaction cause. The
shared diagnostic command also exceeded its 1 MiB output budget because it
combined large successful batch reports from every replica. Raw failed evidence
remains under `artifacts/server-acceptance/candidate-213ab3d/fresh/`; the summary
is `artifacts/server-restart/failed-million-213ab3d-summary.json`. Consumer counts
and the unavailable pipeline observation do not establish reconciliation.

The next correction is diagnostic only. Elasticsearch CLI failures retain bounded
transaction phase/outcome, SQLSTATE, cause/cleanup relationships and repository
code locations, without exception messages, SQL, parameters or credentials.
Failure capture records each owned replica's state and last five log lines
separately; database diagnostics stay private. Every command and the overall
60-second diagnostic stage are bounded, and capture errors remain errors.
Delivery semantics, retry behavior, budgets, resource limits and all acceptance
assertions are unchanged. The original worker-exit root cause remains UNKNOWN;
this correction must not be described as a demonstrated replication fix.

An isolated real PostgreSQL reproduction exercises division-by-zero (`22012`)
and statement cancellation (`57014`), confirming that the new diagnostic retains
the real SQLSTATE, work phase and rolled-back outcome without printing its private
credential. Artifacts are in `artifacts/server-restart/failure-diagnostics-repro/`.
The first diagnostic procedure had an argv-index error before application work;
its failed result and successful cleanup remain in the sibling
`failure-diagnostics-repro-argument-error/`. An initial unit-test lint failure
(unawaited test promises) remains in `quality-failure-diagnostics-lint-failed.log`.
These development failures are separate from the million fixture failure.

Validation of the diagnostic correction: `make quality` exited 0, including
87 unit tests with no failures or skips; log
`artifacts/server-restart/quality-failure-diagnostics.log`. The real PostgreSQL
reproduction exited 0 and cleanup passed. These checks validate diagnostic
retention; they do not resolve or supersede the failed million-entity run.

## ES claim target contention, 2026-09-24

The clean `8d5a9f3` million run failed G1 at 06:01 UTC. Both ES workers exited 1 with OOMKilled false. The bounded error summary identified `es_claim`, SQLSTATE 57014 during work, and a confirmed rollback. Cleanup passed; later gates, reconciliation and negative controls did not run. The older `213ab3d` failure still has no established SQL cause.

An isolated 4,096-baseline auto_explain experiment did not reproduce cancellation. ES 500-row claims took 5,592 ms with JIT on and 6,412 ms with JIT off under nested analyzed logging. Instrumentation and cache effects prevent attributing the failure to JIT. No JIT or timeout change followed.

The subsequent instrumented million diagnosis `kit-final-20260924062516-d6a7115c` failed G1 and cleaned up at 07:36:51 UTC. It is not acceptance: nested slow-plan logging and bounded activity sampling added overhead. Backend 69680 was observed waiting on another `es_claim` backend 69646 at 07:35:40; at 07:35:46 it was executing without a blocker, with total query age 8.144 seconds. It hit the unchanged 12-second statement limit at 07:35:50, inside `guard_delivery` during the claim's intent update, and rolled back. Completed target-lock waits in the retained plans reached 10.411 seconds. These observations establish target contention consuming the claim's work budget; they do not isolate CPU/cache/per-row costs or establish that every prior cancellation had this cause.

A controlled real PostgreSQL reproduction on 4,096 staged baselines held the same target row for 14 seconds. Original claim: SQLSTATE 57014 after 12.440 seconds. The isolated candidate returned zero claims in 0.367 seconds with the target still locked. After release it claimed the full 500 rows; rollback preserved intent/attempt fingerprints, an absent target still raised P0002, and cleanup passed. Evidence: `artifacts/final/kit-final-20260924074157-5651c5c6/run.json`. This induced contention checks the narrow failure mechanism, not million-entity acceptance or an identical timing replay.

Forward migration 019 keeps the exclusive target lock and all admission, single-probe, per-intent locking, generation, evidence and lease checks. Only its initial target acquisition uses NOWAIT; only `lock_not_available` returns an empty claim. The existing worker status/report/idle-poll path handles that result without remote delivery or a new transaction retry. Missing targets and other errors remain errors. Transaction, lease, retry, memory and acceptance budgets stay unchanged. `scripts/capacity/claim-contention-check.py` tests the old populated image and actual migration, restricted-role contention, 500-row claims after release, preserved metadata/data, blocked/cooldown admission and generation/role rejection. Its final result and the new clean-candidate acceptance remain pending at this commit.
