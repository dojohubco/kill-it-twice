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
