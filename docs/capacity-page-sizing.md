# Bounded backfill page-size experiment

2026-09-22. The latest retained 131,072-baseline run at `ab0b8e4` fully reconciled after a 1,528.378-second post-seed completion observation. Its four scanners still admitted only sixteen records per page and opened independent short source/claim/renew/page/status transactions. The existing two-million-row target is not established by this result.

The next measured experiment permits an explicitly selected page-record ceiling of **64**, preserving the 256-KiB canonical-wire batch ceiling, 64-KiB individual-record ceiling, deterministic keyset ordering, source snapshot semantics and atomic page/checkpoint/obligation transaction. Historical and ordinary defaults remain sixteen until measurements justify a deliberate runtime default change. Capture batches and all receiver/consumer caps are unchanged.

This requires matching forward source/pipeline SQL bounds and TypeScript reader/ledger limits. Batch evidence permits at most 64 identity/hash entries and at most 32 KiB; the canonical transfer and 600,000-byte serialized SQL-input ceilings stay unchanged. A byte-limited page can contain fewer than 64 rows and is not EOF. No connection pool, persistent transaction, weaker body validation or skipped membership proof is introduced.

The experiment must first prove exact source and pipeline boundaries, a true local rollback, replay/ownership behavior, missing evidence rejection and actual shared-transaction identity on a 64-row page. Existing sixteen-row consumers remain valid after a populated migration; historical migrations are not edited. More-than-64 and over-byte inputs still fail. Immutable old rows, function identity and grants remain preserved.

A private real-service fixture exercises actual source reads and page commits, followed by both receivers and independent reconciliation. The complete functional fault gates must then pass with the explicitly selected new setting, including their normal healthy controls. Compare a separately owned intermediate workload with both settings before claiming a speedup. Larger pages can increase per-page membership-check CPU; an improvement is a hypothesis, not a conclusion.

The capacity launcher records selected page size, actual scanner count, byte ceilings, image/code identity and resource pressure. Host resource limits and unrelated work are not changed to manufacture performance. If resource headroom is insufficient or a required read times out, retain the nonpassing evidence instead of changing the expected state.
