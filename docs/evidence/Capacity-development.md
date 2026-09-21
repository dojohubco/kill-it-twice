# Capacity continuation: development observations

2026-09-21. These observations are not completed scale acceptance.

The first measured change (`42149b5`) batched exact consumer receipt observations and added indexed source-history predicates. The 8,192-row fixed-window pilot still ended MEASURED_PARTIAL; receipt observations increased, but total staging did not improve relative to the earlier loaded-host sample. No universal speedup is claimed from those two observations.

The first populated source migration check failed its function ACL equality assertion. Data, function OIDs, owner and fixed search path matched exactly, but the earlier `require_current_baseline` trigger function had an implicit PUBLIC execute grant and the forward migration explicitly revoked it. That is a deliberate restriction, not an expanded permission or data change. The corrected preservation check must name that exact narrowing while requiring every other property to remain equal; the original failed report remains preserved. Its later negative SQL cases had not executed before failure.

One quality run failed the pre-existing one-millisecond observation test: the deadline expired before its first observation, producing `last observation: undefined` instead of the test's expected string. Production waiting code and that test were unchanged. The failure remains recorded in `artifacts/capacity/continuation/p2-quality.log`; a later retry is separate evidence, not a relabeling of this run.

The next change applies chunk-local deferred proof and an explicitly admission-only scanner snapshot. It retains full phase/final completion checks and historical profile defaults. New source migrations are additionally selected by `node scripts/m1.ts m5a --capacity`, which must execute the same eighteen real baseline/activation/receiver cases rather than substitute a reduced test list. Its manifest explicitly records the new source migration selection.
