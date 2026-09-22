# Bounded page and due-queue measurements

Recorded 2026-09-22. The tested page contract is `ff7e115d6d39b2ad04da58e54b001fb6525c6c33` and the tested final selector code is `9c63b8bd95bb001307a6bbc78bacee2112a190e3`. Later documentation has a distinct identity. All successful invocations were committed/clean, and every owned cleanup passed. Implementation and investigation used the authorized development tools; the required identity/transaction guarantees predate this optimization.

## Actual measurements

| Baselines | Page/selector setting       | Seed and activation, seconds | Post-seed completion observation, seconds | Code      |
| --------: | --------------------------- | ---------------------------: | ----------------------------------------: | --------- |
|    131072 | 16 / before page tuning     |                      121.591 |                                  1528.378 | `ab0b8e4` |
|      8192 | 16 / original selector      |                        7.420 |                                   104.115 | `ff7e115` |
|      8192 | 64 / original selector      |                        7.269 |                                    73.973 | `ff7e115` |
|    131072 | 64 / original selector      |                       97.019 |                                  1175.391 | `ff7e115` |
|    131072 | 64 / explicit due predicate |                       95.766 |                                   659.835 | `9c63b8b` |

The 8,192 one-scanner pair used the same code and settings except page count. The 131,072 runs use four scanners. Each is a single shared-host observation, not an uncontended hardware benchmark or a universal rate guarantee. Reported completion time excludes later export/oracle/cleanup; successful final reconciliation separately checks the exact frozen source and both receiver histories. The earlier 131,072/page64 run included small read-only queue-plan diagnostics; its runtime data/functions were not changed during measurement.

The final run recorded 40 unavailable pipeline observations out of 91 samples. They remained unknown rather than zero. A final fresh completion observation and exact reconciliation are required; intermittent read unavailability was not hidden or converted into success. This remains a limitation to consider before larger-scale dashboard polling.

## Page contract and real upgrade proof

Normal and historical defaults remain 16. An explicitly selected 1..64 record ceiling preserves the existing 64-KiB individual wire and 256-KiB aggregate wire limits. Source and pipeline forward functions agree, while bounded batch-identity evidence permits the additional entries. Capture batches, source receipts, event identity/bytes, per-item sink outcomes, consumer effects and fencing are unchanged. No framework, dependency, service or connection pool was added.

The real populated proof commits an old 16-record page, applies the normal forward migrations and compares every existing source/pipeline row plus function OID/owner/ACL/path. It commits one actual 64-record page, verifies a single event/batch/member transaction identity, proves full rollback after a callback failure and recovers the same committed batch on replay. Old 16-record reads remain an exact prefix. Source/pipeline 65-item requests and runtime DDL are rejected. Six externally declared real mutations demonstrate byte-limited 5+1 reads without false EOF, then reconcile through both sinks and the consumer.

## Equivalent indexed due selection

The new selector only adds the already-implied pending/leased/retry-wait state set so PostgreSQL can recognize its existing partial-index predicate. The OR eligibility, database time, ordering, SKIP LOCKED, target/probe locking, generations and historical attempt retention are unchanged. No index was forced and no eligible work was removed.

A populated 4,096-row fixture compares actual old/new rollback claim calls with identical ordered 128-event cohorts, exact retained data and unchanged function metadata. A 54-case real SQL truth table including NULL/due/expired values has zero differences; restricted-role ALTER FUNCTION is denied with 42501. Actual delivery and independent whole-fixture reconciliation pass afterward. Small SELECT plan times are approximately 5.791 to 0.209 ms for ES and 7.635 to 0.236 ms for RabbitMQ; these are not end-to-end publish/settlement throughput.

## Faults, failures and evidence separation

The integrated real G1-G5 gate passed with page64 before and after the selector change. It retains actual pre-COMMIT scanner SIGKILL, consumer COMMIT-before-ACK and publisher confirm ambiguity, a real 60-second ES outage, actual 497/3 bulk results, actual browser reads and independent corrupted-export controls. These faults use a 1,024-baseline fixture, not the capacity dataset.

The first page64 gate passed G1-G4 but failed Chromium screenshot capture during G5. The underlying compositor cause was not established. The failed report and successful cleanup are retained; a read-only demo browser check and an unchanged-code fresh complete gate subsequently passed. No UI assertion, CSP setting or receiver data was modified to manufacture a green result.

The final quality floor passed 79 unit cases plus formatting, typed lint, TypeScript/templates, dead-code analysis and existing configuration checks. This report does not claim every historical standalone milestone suite was rerun. Populated targeted migrations, integrated faults and exact capacity reconciliation are the new executed evidence.

## Local handoff and remaining scope

`docs/evidence/Capacity-page-queue-summary.json` provides exact code/run identities and compact results. Full reports, recorded commands, declared workloads, exported values, actual faults, query plans, resource samples and cleanup live under `artifacts/final/<project>/`. The supplemental scripts and checked audit results are in `artifacts/capacity/continuation-20260922T1542/`. These are local paths, not public download links.

A local review package records tested and later documentation identities, complete diff/chronology, streamed diagnostic exports, preserved failures and SHA256SUMS. Credentials/private key volumes and SQLite scratch databases are excluded. The archive and sidecar checksum are generated after committing this report; their actual identity is supplied in the handoff, not fabricated here.

**The 2,000,000-row profile remains NOT RUN.** Normal defaults are not silently raised; full `make verify` is still nonpassing until its separate large-data/final contract is demonstrated. These results do not prove high availability, unlimited buffering or distributed exactly-once transport. No host tuning, remote push/publication, history rewrite, unrelated cleanup or new dependency is part of this continuation.

## Handoff workspace qualification

After the completed selector run and its audit, the root advanced with a separate capacity-planning commit and uncommitted `consumer/006-projection-navigation.sql` registration work. Those later changes are not part of the tested `9c63b8b` application and are left intact. This handoff commits only its selected evidence documents. The package archives exact committed identities and records any live working-tree difference separately; it does not present later source changes as tested, reset them or substitute their code for the completed runs.
