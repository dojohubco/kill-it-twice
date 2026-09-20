// Independent operational requirements committed before production implementation.
export const operationalCases = [
  {
    id: 'OP01',
    name: 'OP01 Actual HTTP and generated OpenAPI paths/methods/statuses; UUID/BIGINT/cursor/limit/body validation; deterministic errors, idempotency and secret exclusion.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP02',
    name: 'OP02 Fresh idle source has zero backlog and null pending age; unreachable is unknown; delayed/leased/blocked and real sealed/unsealed backfill state remain distinct.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP03',
    name: 'OP03 Durable backfill start/pause/resume; same key replay; process death after COMMIT before response; completed run cannot restart.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP04',
    name: 'OP04 Real ES current/tombstone search, bounded server pagination/query, exact string versions, degraded current receiver evidence.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP05',
    name: 'OP05 Bounded event metadata/hash/source ACK/sink attempts/consumer observations match direct SQL; malformed and missing identities fail closed.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP06',
    name: 'OP06 Parse real metrics; exact HELP/TYPE; fixed label sets; durable totals survive process restart; gauges decrease, counters do not; true timestamps and idle/unavailable distinction.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP07',
    name: 'OP07 Real mapper rejection caused by temporary receiver configuration, controlled repair, SAME immutable event replay, new attempt, satisfaction and unchanged original failure evidence.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP08',
    name: 'OP08 Unchanged invalid mapped data rejects again; two immutable failure rows/distinct attempts, latest terminal identity, no cross-sink/source reset.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP09',
    name: 'OP09 Replay key repetition, concurrent competing keys, stale terminal attempt refusal, post-COMMIT process death and original-result retry.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP10',
    name: 'OP10 Source business correction produces a new version; old event/failure/replay remain historical.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP11',
    name: 'OP11 Real consumer poison quarantine is inspectable/non-replayable; no raw-body replay endpoint; consumer DB outage is not poison.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP12',
    name: 'OP12 Named deterministic fixture create/update/delete/restore through source command authority; corrupt simulation reaches real ES rejection and normal broker/consumer effect.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP13',
    name: 'OP13 Actual configured ES/Rabbit Toxiproxy disconnect/reconnect affects workers; no arbitrary names/URLs, Docker socket or shell execution.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP14',
    name: 'OP14 Bounded structured outcomes/audit; request correlation; no secrets/bodies/raw quarantine; primary/cleanup error distinction.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP15',
    name: 'OP15 Actual API process restart preserves status/metrics/replay and independent worker progress; startup causes no mutations.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP16',
    name: 'OP16 Frozen mixed failure fixture pages without duplicate/omitted identities; bounded contexts and histories.',
    file: 'tests/operational/operations.test.ts',
  },
  {
    id: 'OP18',
    name: 'OP18 JSON, metric samples and logs independently locate backfill, useful settlement throughput, incremental pending state, failure counts and unhealthy dependencies. Backend G5 preparation only.',
    file: 'tests/operational/operations.test.ts',
  },
];
export const operationalUpgradeCases = [
  {
    id: 'OP17',
    name: 'OP17 populated M5B migration preserves exact retained evidence',
    file: 'tests/operational/upgrade.test.ts',
  },
];
