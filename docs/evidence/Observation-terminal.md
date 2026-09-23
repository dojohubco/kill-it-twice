# Observation / terminal proof, inspected 2026-09-23

Code `93eac82044068e697c42545fb4f71041f8e8baef` completed the existing finite sequence without checkout changes. This records newly inspected results; it does not attribute execution or approval of unseen evidence to the earlier reviewer.

- Populated 4096-baseline proof `kit-final-20260923103508-0f6d63b3`: PASS. Exact table/function metadata preservation; old/new healthy, missing member, wrong checkpoint and wrong batch hash results agree. Restricted observation returns `invalid: null` and an explicit non-revalidation evidence scope. Runtime DDL denied. Independent source/receiver reconciliation and cleanup PASS.
- Functional `kit-final-20260923103702-d8f04bd1`: G1-G5, 1024 baselines / 519 mutations, exact reconciliation, five negative controls and cleanup PASS.
- Capacity `kit-final-20260923104314-5aee3dd1`: 262144 distinct baselines, zero mutations, 4 scanners, page64, two sink processes per role. RECONCILED_COMPLETE and cleanup PASS. [Machine summary](Observation-terminal-summary.json) records exact timings, per-container memory/CPU, physical storage and manifest hashes.

The original attempt at `77b5dcf` failed with SQLSTATE 25006 while defining a temporary reference inside a read-only measurement transaction. Its original report remains FAIL. `93eac82` creates the reference before that transaction; no production assertion or query changed to make the reproducer pass. Earlier 262144 observation timeouts remain failures, not corrected retroactively.

Short observation is not proof: the retained complete transition still calls full terminal checks. The corrected small proof does not establish one-million query latency. The completed 262144 run does not execute large-dataset failure gates. Local raw artifacts remain under `artifacts/final/` and `artifacts/capacity/continuation-20260923/`; these paths are not public URLs.
