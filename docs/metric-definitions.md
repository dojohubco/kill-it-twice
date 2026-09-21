# Operational snapshot and rate definitions

A snapshot contains independently observed source, pipeline, consumer and receiver sections. It is not an atomic global transaction. Each section exposes current freshness/time, safe failure classification and a separately labeled bounded last-known value. Unavailable current data is null, never a reused zero. Worker liveness is unknown without worker heartbeat evidence.

| Fact                       | Definition                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Scan observations          | Revisions in committed scan batches; not source mutation count.                                                             |
| Required run events        | Unique sealed scan/fence union; denominator is null before sealing.                                                         |
| Staged total               | Distinct immutable pipeline event identities.                                                                               |
| ES useful settlement total | Distinct satisfied obligations, split by applied/already_applied/superseded.                                                |
| Broker-confirmed total     | Distinct satisfied RabbitMQ obligations; not physical messages or processed consumer events.                                |
| Consumer processed total   | Distinct committed inbox event identities, including baselines.                                                             |
| Consumer effect total      | Distinct committed mutation effects; excludes baselines and duplicates.                                                     |
| Attempts                   | Actual finished attempts, separately from logical successful work; retained outcome aggregates survive diagnostic pruning.  |
| Open ES failures           | Current dead-letter obligations; a repeated failure does not duplicate an obligation.                                       |
| Failure history            | All immutable rejection records, including resolved/replayed historical failures.                                           |
| Replay scheduled           | An admitted recovery with no correlated final attempt yet; not a success.                                                   |
| Due/delayed                | Current retry eligibility evaluated using the owning database clock.                                                        |
| Oldest pending source age  | Source observation time minus earliest unresolved source-recorded time; includes pre-commit duration, not exact commit lag. |
| Pipeline delivery age      | Pipeline observation time minus the earliest unresolved event staged_at.                                                    |
| Historical run outcome     | Original terminal phase/fence/completion timestamp, retained after repair.                                                  |
| Current run recovery       | Present evidence-backed dispositions within that fixed required set.                                                        |

The CLI sampler retains one previous valid monotonic counter sample per fixed series. Rate = exact counter delta divided by monotonic elapsed time, represented as a bounded decimal string. Initial sample, identity change, counter regression or missing/stale observation yields warming/unknown with a reason. No historical rate continuity is claimed after sampler restart. An unavailable sample invalidates that rate baseline; recovery requires a new pair of fresh samples. No event IDs or free-form reasons become metric labels.

Replay failure increases attempts/failure history, not useful throughput. Terminal successes are never reset by replay. Canonical staging duplicates and receipt re-observation do not increase totals. Source pending excludes acknowledged work but includes blocked and missing-work conditions; zero mutation backlog does not prove baseline backfill completion.

Counts are exact decimal strings. Database timestamps drive backlog age; monotonic process time drives measured rate intervals. Cross-service wall clocks are not assumed identical. Read pages default to 50, max 100, with an explicit 256-KiB response budget and navigation cursors; cursors are not completeness watermarks. Summary query costs remain to be measured at scale.
