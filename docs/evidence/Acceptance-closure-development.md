# Closure development evidence, 2026-09-23

The note `0be5d2a` preceded implementation `6f6281a`; documentation followed at `2ee4cee`. No production replication invariant changed.

Initial static checks caught an over-narrowed TypeScript request-array length assertion and the undeclared custom optional CI runner label. Both were corrected without removing assertions. The next quality run exited zero. Logs remain under `artifacts/submission-closure/`.

The first new runtime check at clean `2ee4cee`, `kit-verify-20260923113230-064d2dad`, passed R01-R03, exact before/after exports and both independent oracles, then failed R04's exact summary equality. Inspection compared `052-state.stdout.log` with `068-state.stdout.log`: every value matched, but the grouped sink rows changed order from RabbitMQ/Elasticsearch to Elasticsearch/RabbitMQ. The query lacked ORDER BY. The correction orders this diagnostic summary by kind,state; the exact summary assertion and all ten byte-for-byte export comparisons remain unchanged. Cleanup PASS; R04 and later cases are not called passing for that run.

G1's closure audit also makes overlap explicit: resume the killed scanner before issuing the existing five unique commands, observe a nonclosed scan beforehand and retained page progress afterward. This adds evidence without changing the journal count, page/checkpoint transaction or independent oracle. The repository browser runner now describes its actual execution path rather than asserting that the interactive Browser plugin is unavailable.
