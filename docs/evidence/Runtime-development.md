# Retained runtime development observations

2026-09-21. These observations precede committed-code runtime acceptance. They are not final G1-G5 or capacity results.

The continuation inherited an uncommitted root Compose/runtime draft at `0302d205fb38421db5446e9e0597bccf5286314b`. Its files, hashes and tracked diff were recorded before edits under local `artifacts/runtime/continuation-20260921/`. The earlier development installation was preserved; verification uses different explicit project identities.

Initial quality checks found formatting in the existing inspector, then four redundant double-quote escapes in a SQL template. Both were corrected without disabling rules. Subsequent quality passed with all 64 existing unit checks.

The first real runtime fixture `kit-verify-20260921180015-7b8a3f97` initialized both databases and authenticated receivers, seeded/replayed 257 baselines, rejected a conflicting seed count, and delivered five declared mutations with a harmless command retry. A real consumer SIGKILL was recorded. Independent export reconciliation then failed: the baseline export contained only 162 rows, although the source summary and delivered receiver contained all 257 baseline entities. All owned resources were cleaned; the run remains FAIL.

The new read-only inspector's keyset query filtered a native BIGINT but ordered by an output alias cast to text. The observed first identifiers were `1, 10, 100, 101, ...`, so the numeric continuation skipped keys. Source, pipeline and consumer replication had not lost those rows. Four numeric entity-key exports now explicitly order by the qualified native database column. The independent expected count/content assertions remain; the fixture spans decimal digit boundaries to retain this regression. The original failed exports/logs are not rewritten.

The fixture oracle is deliberately independent of production normalization: declared commands, the fixed seed recipe, exact PostgreSQL-exported payload text, Python Decimal/int semantics and canonical metadata determine expected history/current state. Its bounded 257-row workload is not a production scale export claim. SQL snapshots are separately consistent after quiescence, not a cross-database atomic snapshot.

The next real fixture `kit-verify-20260921180632-99c2391d` exported complete numeric-key collections after that correction. Its new offline checker then failed because the generic record-key selector indexed consumer projections by their event ID rather than by their entity cursor. The checked receiver/source state was not missing: collection-specific key selection corrected the oracle, and its independent recheck of the retained exports passed 257 baselines, 258 current entities, 262 historical events and five effects. The original failed SQLite file and FAIL run remain retained. This recheck is not a new service run or a successful retained-restart test; a complete fresh run is still required.
