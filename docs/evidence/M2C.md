# M2C local acceptance evidence

Recorded 2026-09-17 Asia/Tbilisi; execution timestamps use UTC (2026-09-16). **PASS for bounded incremental source-to-pipeline capture only. G1–G5 remain NOT IMPLEMENTED.**

## Identities, authorization and actual start

- Reviewed documentation/baseline HEAD: `642925bf66fc0755f4a845bab2a55350957475c4`.
- Accepted prior M2B application/test SHA: `f8d7728f1d1f17ff7fc16ae0c29e4b64946e1636`.
- Final tested M2C code: `445f268709fad070eb020b5642a49772f7b5f58f`; main, clean tracked tree throughout all final commands and 18 isolated integration runs.
- Input-manifest SHA-256 for every final run: `3fa65fbcc4e4a75c26736eebfa4f22ba74948c418b9c2b9597f8648664c28943`. Each inputs.json retains individual tracked input hashes.
- Final documentation HEAD is the later documentation-only commit named in the handoff and bundle manifest. It is distinct from the tested code SHA.

Actual start was clean main at the reviewed documentation HEAD, with no later commits or unrelated edits to reconcile. SPEC, AGENTS, ADRs 001–008, milestone/evidence documents and implementation were inspected, along with branch/status, tools and Docker access. History and original migration/envelope/command/transaction contracts were preserved. No reset, amend, rebase, squash, backdating, push, remote branch/PR, repository-setting change, publication or privileged host installation occurred.

Baseline `make verify-m2b` exited 0, including quality with 30 unit cases and separate 22/36/16/16 M1/M2A/M2B-fresh/M2B-upgrade profiles, retained-volume restarts and cleanup. Logs: local artifacts/m2c/baseline-642925b. Its run IDs are recorded in docs/milestones/M2C.md. No prerequisite blocker occurred.

The historical M2B report now appends the later successful hosted run 35148507444 at baseline HEAD (2026-09-16 20:45:53–20:47:22 UTC), independently checked through read-only run metadata during intake. The reviewer supplied artifact SHA-256 b13b7681199201c1f0d445ddc4d266cee09fa4c951abe4cfc8b0dac5c8ce29f2 and an offline reconciliation of 40 staged events per M2B snapshot. This was artifact inspection, not reviewer-local PostgreSQL execution or full-scale reconciliation. Its original historical NOT RUN claim remains intact. **M2C hosted CI: NOT RUN.** Local actionlint is not a hosted CI result.

The M2C milestone, ADR 009 and executable independent inventory were committed together at ac79a20 before production changes or M2C execution. Test implementation was committed later at 8013e4e after recorded dirty developmental runs. M2B's descriptive design preceded implementation, while its executable inventory was committed with tests after initial dirty execution; the appended historical note preserves that distinction.

## Implemented boundary

A capture worker uses three separate, exclusively owned transactions:

1. Source: claim due or expired work, assign the worker incarnation and a higher generation, confirm COMMIT and close the session.
2. Pipeline: stage the unchanged canonical revisions and complete local obligations, validate the configured pipeline-instance UUID inside this transaction, then confirm COMMIT and normal owner return.
3. Source: acknowledge each matching revision/hash under its still-current claim and confirm COMMIT.

No source claim transaction stays open during pipeline work. No ACK runs inside the pipeline work callback. An unknown COMMIT or confirmed COMMIT followed by a connection-cleanup error takes no ACK path; a later safe repeat resolves durable staging evidence. Business command mutations are never automatically retried. An acknowledged revision means **staged**, with both sink intents and the consumer observation still pending and both destinations unbound.

Pipeline migration 002 adds one immutable, retained instance UUID to the existing binding. Source migration 004 adds one immutable capture binding and one work table without duplicate business payload. The source work revision has a unique composite FK to its immutable outbox evidence and a composite FK to the configured binding. Runtime source_capture and pipeline_capture roles cannot replace bindings, directly write scheduling/ACK state or history, call business mutations, or use the old identity-unbound staging entry point. Safe owner/search_path and PUBLIC revocation are enforced in the migration transactions. Original source migrations 001–003, pipeline migration 001, lockfile, canonical eleven-field envelope, golden vectors and transaction owner are unchanged.

Initialization is controlled administration. The pipeline identity exists before source registration; incomplete setup rejects claims. Source registration takes SHARE ROW EXCLUSIVE on outbox, waits for prior writers, registers the exact identity, fills every retained revision and commits before later writers proceed. The installed AFTER INSERT trigger then creates work in each source mutation transaction. The upgrade tests hold real writers on both sides of this boundary and observe their PostgreSQL blockers. Existing manually staged M2B events begin unacknowledged and restage without changing their original evidence. This is initialization of captured mutation history, not seed/backfill activation or a live sequence watermark.

Work has only pending, leased, acknowledged and blocked states. Native BIGINT work IDs order scheduling but never filter by a moving maximum. READ COMMITTED claims lock due pending or expired leased rows with FOR UPDATE SKIP LOCKED. Every owner/generation transition reads clock_timestamp after acquiring the row lock. Expired claims cannot renew, defer, block or ACK even before another worker reclaims them. Generations remain exact decimal strings and overflow raises 22003. Identical terminal ACK retries preserve their original timestamp and receipt; wrong tokens raise P4002, conflicting hashes P4003, and binding mismatch P4001. SQL enforces local ownership and shape; it cannot prove a remote pipeline COMMIT. Staging-before-ACK is the trusted capture protocol invariant tested here with real failures, not a source-to-pipeline FK or cryptographic authenticity claim.

The default worker claims at most 16 records, with one staging batch in flight, 64 KiB per record and 256 KiB per batch, 30-second source-clock leases, approximately five-second independent renewal and one-second idle polling. These are starter configuration, not capacity measurements. SQL size metadata precedes payload transfer; fitting records are partitioned before bounded immutable outbox reads. Oversized work becomes explicitly blocked and unacknowledged; later fitting work continues. Missing claimed evidence or missing work is an integrity error, not an empty success. The complete returned unique event-ID set must match the submitted set before ACK.

Renewal uses separate short source owners while pipeline work is outstanding. Lost ownership stops local ACKs but does not claim to cancel an already-running remote request. Transient failures leave persisted eligibility delays with positive jitter (one-second base, 30-second cap); no arbitrary attempt threshold permanently blocks infrastructure failures. Lost connectivity can leave leases to expire. Binding or canonical conflict stops the worker with bounded diagnostics. A fresh source summary distinguishes due, delayed, current/expired lease, blocked, acknowledged and missing work; unreachable source is unknown. An empty claim is never described as complete catch-up.

`npm run capture -- once` and `npm run capture -- follow` use explicit restricted credentials, expected epoch and pipeline ID. Startup never registers a replacement receiver. SIGTERM/SIGINT stop admission while bounded work settles or remains recoverable; SIGKILL needs no shutdown handler. Follow waits after idle and admits later mutations. Each CLI metadata report is at most 64 KiB, awaits the actual write callback, and fails/disposes blocked output at a five-second deadline. It does not accumulate an unbounded console queue. Output failure cannot undo an already committed ACK.

## Commands, profiles and environment

Local capture directory: `artifacts/m2c/acceptance-20260916220749290-aec6e789`. commands.json retains UTC starts/ends, exact exit/signal/timeout/overflow/cleanup results and separate complete stdout/stderr logs.

| Command                                     | Exit | Result                                  |
| ------------------------------------------- | ---- | --------------------------------------- |
| `npm ci --no-audit --no-fund`               | 0    | PASS                                    |
| `npm ls --depth=0`                          | 0    | PASS                                    |
| `npm run format:check`                      | 0    | PASS                                    |
| `npm run lint`                              | 0    | PASS                                    |
| `npm run typecheck`                         | 0    | PASS                                    |
| `npm run knip`                              | 0    | PASS                                    |
| `npm run validate:compose`                  | 0    | PASS                                    |
| `npm run validate:workflow`                 | 0    | PASS                                    |
| `npm run test:unit`                         | 0    | PASS                                    |
| `make quality`                              | 0    | PASS                                    |
| `npm run test:integration:m1`               | 0    | PASS                                    |
| `npm run test:integration:m2a`              | 0    | PASS                                    |
| `npm run test:integration:m2b`              | 0    | PASS                                    |
| `npm run test:integration:m2b -- --upgrade` | 0    | PASS                                    |
| `npm run test:integration:m2c`              | 0    | PASS                                    |
| `npm run test:integration:m2c -- --upgrade` | 0    | PASS                                    |
| `make verify-m2c`                           | 0    | PASS                                    |
| `make verify-m2c`                           | 0    | PASS                                    |
| `make verify`                               | 2    | Expected nonzero; G1–G5 NOT IMPLEMENTED |

All final commands used committed clean code. Quality passes **40 unit checks**, zero failed/skipped/todo/cancelled. This includes ten added capture/report checks; protocol-error fixtures supplement the real PostgreSQL faults. Each complete verify-m2c selects M1 22, M2A 36, M2B fresh 16, M2B populated M2A upgrade 16, M2C fresh 19 and M2C populated M2B upgrade 19. Prior inventories and their original schemas remain separate. Repeating profiles repeats tests; these totals do not represent new independent guarantees. The M2B envelope cases overlap unit quality.

The deliberate new inventory is IC01–IC10, IC11-CLAIM/PRE/POST/ACK and IC12–IC16 (19 executable cases). Missing/skipped/cancelled/todo/empty/malformed/duplicate results and contradictory failed counts still fail acceptance. Per-run small summaries are additionally checked against actual profile, run ID, tested HEAD and acceptance inventory during handoff capture.

| Isolated run                     | Profile / migration mode    | Passed | Restart / cleanup |
| -------------------------------- | --------------------------- | ------ | ----------------- |
| `m1-20260916220819027-3ed32e0b`  | m1 / fresh                  | 22     | PASS              |
| `m2a-20260916220830654-c2c01d86` | m2a / fresh                 | 36     | PASS              |
| `m2b-20260916220848852-d11a094c` | m2b / fresh                 | 16     | PASS              |
| `m2b-20260916220905444-c88eb8de` | m2b / populated M2A upgrade | 16     | PASS              |
| `m2c-20260916220922599-bd023cc1` | m2c / fresh                 | 19     | PASS              |
| `m2c-20260916221006723-6cbf56ef` | m2c / populated M2B upgrade | 19     | PASS              |
| `m1-20260916221101640-23783667`  | m1 / fresh                  | 22     | PASS              |
| `m2a-20260916221114878-2c7371dc` | m2a / fresh                 | 36     | PASS              |
| `m2b-20260916221129062-9bc28c96` | m2b / fresh                 | 16     | PASS              |
| `m2b-20260916221145583-42e106d8` | m2b / populated M2A upgrade | 16     | PASS              |
| `m2c-20260916221202265-7cd8a7b3` | m2c / fresh                 | 19     | PASS              |
| `m2c-20260916221244711-8fe5436b` | m2c / populated M2B upgrade | 19     | PASS              |
| `m1-20260916221337685-bd2f2a1c`  | m1 / fresh                  | 22     | PASS              |
| `m2a-20260916221350736-5dc0a9a1` | m2a / fresh                 | 36     | PASS              |
| `m2b-20260916221406357-56c055e0` | m2b / fresh                 | 16     | PASS              |
| `m2b-20260916221422464-193d9b42` | m2b / populated M2A upgrade | 16     | PASS              |
| `m2c-20260916221441336-4925d188` | m2c / fresh                 | 19     | PASS              |
| `m2c-20260916221521422-518cfce6` | m2c / populated M2B upgrade | 19     | PASS              |

All 18 isolated runs have distinct persisted source epochs and owned resources. Each records the initial empty source observation before workload. Both two-database migration modes preserve full source/receipt evidence; the M2C upgrade also preserves pre-existing pipeline event/obligation history while adding the instance identity and work queue. Full tiny-fixture snapshots remain identical across retained-volume restart of both services. Runtime sessions are absent afterward, and owned container/volume/network lists are empty after cleanup. No global Docker prune or unrelated resource cleanup occurs.

Final environment: Node v24.19.0; npm 12.0.2; TypeScript 5.9.3; pg 8.23.0; Prettier 3.9.7; ESLint 10.10.0; typescript-eslint 8.70.0; Knip 6.36.0; actionlint 1.7.12; Docker 29.7.2; Compose 5.5.1. Both services use `postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`, observed PostgreSQL 18.6 (Debian 18.6-1.pgdg12+2), x86_64, UTF8, fsync/synchronous_commit/full_page_writes on. Exact image/container identities and server settings are retained in each run.json. No dependency or toolchain pins changed and no new tools were added.

## Fault, ownership and independent SQL observations

These are exact observations from final populated-upgrade run `m2c-20260916221521422-518cfce6` with pipeline instance `205e6912-e60f-4dc4-bc66-621dc8d147ab`. The source epoch is part of each event ID below. Rows mean event/delivery/consumer rows for that identity, accompanied by full content/relations checks. Source PID is the child's last observed control session; independent session lists establish whether it has already closed.

| Case | Command key                            | Event ID                                   | OS / PostgreSQL identity before kill                     | Source state and pipeline rows before → after kill | Actual exit                         | Recovery                                          |
| ---- | -------------------------------------- | ------------------------------------------ | -------------------------------------------------------- | -------------------------------------------------- | ----------------------------------- | ------------------------------------------------- |
| IC07 | `c46d1a0c-2b39-4783-8cea-a88eeec404f9` | `8730abe8-34a8-4f26-ab2e-617b7a566da8:4:1` | 777473 / source 153; no pipeline session                 | leased → leased; 0/0/0 → 0/0/0                     | SIGKILL, code null; 0 success bytes | PID 777576: inserted; terminal generation 2       |
| IC08 | `b9b2aac9-c9de-415c-9500-a3c3f51f3153` | `8730abe8-34a8-4f26-ab2e-617b7a566da8:5:1` | 777600 / source 183; PID 133 idle in transaction xid 759 | leased → leased; 0/0/0 → 0/0/0                     | SIGKILL, code null; 0 success bytes | PID 777700: inserted; terminal generation 2       |
| IC09 | `45462d08-bb39-40e6-8a60-6492fa18d1be` | `8730abe8-34a8-4f26-ab2e-617b7a566da8:6:1` | 777744 / source 211; PID 152 idle xid None               | leased → leased; 1/2/1 → 1/2/1                     | SIGKILL, code null; 0 success bytes | PID 777875: already_staged; terminal generation 2 |
| IC10 | `1044e104-7d41-4be2-ab93-20651b7a5144` | `8730abe8-34a8-4f26-ab2e-617b7a566da8:7:1` | 777903 / source 236; no pipeline session                 | acknowledged → acknowledged; 1/2/1 → 1/2/1         | SIGKILL, code null; 0 success bytes | PID 777938: no fresh claim; terminal generation 1 |

| Healthy control | Process PID | Exit         | Ordinary success       | Ended source / pipeline sessions |
| --------------- | ----------- | ------------ | ---------------------- | -------------------------------- |
| IC11-CLAIM      | 777963      | 0, no signal | exactly one; 621 bytes | both empty                       |
| IC11-PRE        | 778021      | 0, no signal | exactly one; 622 bytes | both empty                       |
| IC11-POST       | 778173      | 0, no signal | exactly one; 624 bytes | both empty                       |
| IC11-ACK        | 778269      | 0, no signal | exactly one; 624 bytes | both empty                       |

At the claim boundary, the source lease is independently visible and its transaction/session has completed; no pipeline rows exist. Before pipeline COMMIT, an actual open transaction and locks are observed while event/intent/observation rows remain invisible. After pipeline COMMIT, the complete immutable 1/2/1 set is independently visible with source work still unacknowledged. After ACK COMMIT, its terminal source receipt is independently visible before ordinary success. All killed processes have actual signal exits, zero ordinary success bytes before kill and no surviving owned database session. Healthy releases at all four boundaries exit zero and emit one normal result. Private instrumentation surrounds the production owner and control calls; it does not replace transactions or simulate success.

Post-pipeline-COMMIT recovery uses a new process and actually returns already_staged before ACK, preserving original event bytes, hashes, staging timestamps, destination IDs and obligation rows. Post-ACK-COMMIT restart claims no fresh work for that event. Identical terminal ACK retry is harmless, while wrong token/hash retries reject without rewriting it. Primary transaction failures and cleanup/diagnostic failures remain separate. The prior T08/T09, command kills and S08/S09 still execute in their original profiles; they do not substitute for these integrated source-ACK assertions.

IC05's claim generation 1 renewed from 2026-09-16T22:15:56.585415Z to 2026-09-16T22:15:57.104459Z. A separate lock blocked renewal (backend 840, blockers [839]); source clock 2026-09-16 22:15:57.126285+00 was beyond persisted deadline 2026-09-16 22:15:57.104459+00. The waiting renewal rejected after acquiring the lock. Reclaim returned generation 2 at 2026-09-16T22:15:57.184836Z; terminal ACK was recorded at 2026-09-16T22:15:57.240467Z. IC11-PRE independently observed source time 2026-09-16 22:15:39.593715+00 beyond original deadline 2026-09-16T22:15:39.589776Z, while the same owner/generation held a renewed deadline 2026-09-16 22:15:41.045369+00 during the open pipeline transaction.

IC06 suspended process 778352 with SIGSTOP after staging. Source time 2026-09-16 22:15:42.411006+00 proved its generation-1 lease expired at 2026-09-16 22:15:42.389205+00. New process 778410 restaged and acknowledged generation 2. The old process resumed with SIGCONT and exited 1 with zero success bytes; independent comparisons found its newer terminal receipt and prior pipeline content unchanged.

IC12 stopped the actual owned pipeline container `973964094edc33ee51a156ef36add40ece0365744ee033ad87d78a6eadc67914` during a real transaction. The source retained pending generation 1, reason transient_failure, null ACK hash, and next eligibility 2026-09-16 22:15:46.087328+00 at source observation 2026-09-16 22:15:44.120551+00. Generation 2 was observed while the service remained down; the test bounds admission to at most three attempts at that observation. Restart automatically recovered a generation-3 ACK, followed by graceful SIGTERM exit 0. Exact failed/recovery reports and stop/start identity checks are retained.

IC15 additional kills: PID 779491 at capture.after_claim_commit.before_stage, actual SIGKILL; PID 779550 at capture.before_pipeline_commit, actual SIGKILL; PID 779587 at capture.after_pipeline_commit.before_source_ack, actual SIGKILL. The two finishing follow processes were 779542, 779622; both exited zero on SIGTERM. Source idle observation 2026-09-16 22:15:52.909842+00 preceded new revision 14:24, which was automatically captured.

Receiver identity comparison used registered `205e6912-e60f-4dc4-bc66-621dc8d147ab` and replacement `b1fdc9e4-ec9c-43b6-9def-049072ce3346` under the same epoch; replacement staging rejected.

IC01 injects a real P9004 work-insertion error through privileged test setup. Independent queries find no receipt, entity, outbox or work from that attempt; a later explicit same command key succeeds. The test setup is removed and capture guards restored. IC03 holds an earlier allocated outbox revision uncommitted, commits and acknowledges a later one, then commits the earlier transaction; the automatic claim selector captures it too. IC04 observes disjoint independent claims and an actual held work-row lock skipped by another claimer. IC05 independently observes expiry using the source clock, tests every stale transition, exact generation 9007199254740993 and real BIGINT overflow 22003.

IC13 creates a distinct empty pipeline database in the owned test service with the same source epoch and a different instance UUID. The actual identity-bound staging transaction rejects it; no automatic registration or source ACK occurs. The owned replacement database is removed, and recovery through the registered receiver succeeds. Its actual identities and rejection evidence are retained in IC13-identity. Retained service restarts preserve the original receiver ID.

IC14 blocks the oversized revision without payload transfer/ACK, stages six valid medium records plus a small record in fitting batches of five and two, and keeps later small work processable. Delayed and blocked states remain visible when a claim is empty. IC15 runs a finite hot-entity lifecycle workload with two real capture processes, three additional real kills, then an idle interval and a new mutation. Independent SQL compares every expected outbox revision identity and precise payload against the entire event set, body bytes, native SHA-256, metadata, two required pending intents, one pending observation and source ACK hash/generation/binding. It rejects missing, extra or mismatched rows; counters and maximum IDs are not the oracle. IC16 separately checks runtime privilege denials, source catalog additions, immutable terminal/history state and the actual once CLI.

Final fresh profiles each retain 66 outbox/work revisions: 65 staged and acknowledged, one deliberately oversized blocked revision with no event or ACK. Populated M2B upgrades each retain 68 revisions: 67 staged and acknowledged plus that one blocked revision. No due, delayed, leased or missing work remains at retained restart. These finite fixture totals are not completeness or capacity claims for arbitrary workloads.

- `m2c-20260916220922599-bd023cc1`: 65 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `46c844271213fca8513812191bc1ef7d4e9a3dfa60131442d8100c9cc1a03d92`.
- `m2c-20260916221006723-6cbf56ef`: 67 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `c57eeaa73b25220ab70ba05701dc1d6c97f661a6dfc6ead476a9961f355f723f`.
- `m2c-20260916221202265-7cd8a7b3`: 65 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `dda444833311b71d05ab3d00556805db1c9af51fc790d95db84b1ce2e5f2fcc3`.
- `m2c-20260916221244711-8fe5436b`: 67 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `d2cec93f86fd3846ebb27169971e04e1e7370db5668bf560c36c7640c3fafb16`.
- `m2c-20260916221441336-4925d188`: 65 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `1d6cb6a713720a3cc8ba1dcacf0b4cb693986deb8db173aa920caa66a274bccc`.
- `m2c-20260916221521422-518cfce6`: 67 acknowledged events, 1 blocked revision; SQL evidence SHA-256 `bb02deb47d3fad9e8133e269e595172ba6694af308530fcadb8ddfa21fc242bd`.

An additional offline evidence check ran with exit 0: `python3 artifacts/m2c/acceptance-20260916220749290-aec6e789/reconcile-recorded-evidence.py artifacts/m2c/acceptance-20260916220749290-aec6e789`. Its script and offline-reconciliation.json are retained with the capture. Python standard-library JSON string encoding and hashlib independently reconstruct the fixed envelope from recorded SQL source exports while leaving payload numbers inside opaque text. All six final capture profiles match exact bytes/hash/indexed metadata/obligations/ACKs, actual fault exits and retained snapshots. This checks recorded evidence; it is not another PostgreSQL execution or a new quality-tool rollout.

Full per-event records, canonical byte hex, SQL-computed hashes, command keys, worker incarnations, source clocks, generations, process/backend IDs, locks, replacement receiver identities, service stop/start commands and cleanup observations are in each run's sanitized sql-evidence.jsonl, capture-upgrade.json and restart-evidence.json. Only tiny synthetic diagnostic fixtures use full-history reconciliation; this is not a new unbounded source reader API or large-scale result.

## Actual development corrections

The first dirty PostgreSQL capture run m2c-20260916212653830-1fe497f3 passed 13/19 and failed six, with cleanup complete. Five failures were an incorrect oracle comparison between PostgreSQL locale ordering and JavaScript ordinal order, despite equal identity membership and matched record bytes. Both lists now use the same ordinal order without removing membership/content assertions. The sixth was a private stale-worker child retaining IPC after correctly reporting its stale error, so it did not exit. Its error path now closes IPC after session cleanup. All four required SIGKILL cases and the actual outage case passed in that failed run. These are harness corrections, not a claimed source-loss reproduction.

Dirty m2c-20260916213033061-4417b4bb and populated-upgrade m2c-20260916213300493-93c0af4f then passed 19/19 with restart and cleanup. Their input hashes/diffs identify tested dirty content; HEAD alone does not. Initial type checking exposed closure narrowing, lint exposed boundary typing/unnecessary casts, and Knip exposed unused exports. These were corrected without weakening rules or blanket suppressions.

Clean candidate 39a323547baa47055692f0f3355f4cc490c05529 passed a complete 19-command capture but was superseded after code inspection found follow's unbounded console-output queue. Awaited, bounded output and four real Writable tests now cover backpressure, deadline disposal, callback/late-error handling and overflow. This was a code-inspection finding, not a reproduced PostgreSQL failure. Clean candidate 179dd2f25bda5e7cc1186287abba742f71687379 also passed all 19 commands, but handoff inspection found its per-run small M2C summaries incorrectly labeled M2B. Its actual run manifest/inventory and aggregate capture correctly selected M2C. The label was fixed and independently cross-checked before the final complete capture. Both superseded captures remain in the bundle; their historical artifacts were not rewritten.

Clean summary-label candidate 6f8dcf3bda756e0b1816ce1b013f66fc50f55876 also passed the complete capture. A final bounded diagnostic-path review replaced fallible cleanup in new tests with the already-tested withCleanup helper, covering service restart, temporary receiver disposal, trigger restoration, initialization session and private child end instrumentation. If both work and cleanup fail, the primary cause and cleanup errors remain distinct; temporary receiver removal is attempted even if session end fails. No paired runtime/cleanup failure was observed or manufactured. Full review diffs also export directly to owned patch files, retaining bounded subprocess diagnostics and explicit overflow failure; the since-spec diff was already 878593 bytes before final evidence was added. Final acceptance was repeated after these corrections.

The M2C outage profile selects an available loopback pipeline port before provisioning so a real stop/start retains the worker endpoint. The release-to-bind race fails explicitly if another process acquires it. Prior profiles retain Docker-assigned ports. No new runtime service or infrastructure stack was introduced.

## Chronology, specification changes and exact files

```text
ac79a20 docs: authorize M2C capture ownership protocol and required inventory
e8921ae feat: claim source revisions and acknowledge confirmed identity-bound staging
8013e4e test: exercise capture clocks contention crashes outage and reconciliation
39a3235 build: gate M2C and capture reproducible local review evidence
179dd2f fix: bound capture progress output and verify the real CLI
6f8dcf3 fix: identify capture profiles consistently in acceptance summaries
445f268 fix: preserve fault cleanup diagnostics and export complete review diffs
```

A later documentation-only commit adds this report/compact summary and links completed local verification in ADR 009 and the milestone. The bundle manifest records that exact final documentation HEAD separately from tested code.

SPEC adds only the authorized M2C capture/ACK refinement. ADR 009 defines exact additive columns/types/keys/checks/indexes/grants, initialization locking, source-clock transitions, identity binding, three transaction boundaries, retry bounds and private failpoints. Capture/ACK was already planned. No new SPEC version, invented requirement discovery, benchmark or deviation count is claimed. Earlier ADRs and reports remain historical; M2B receives only the dated subsequent CI observation and inventory-timing clarification.

Changed paths relative to the reviewed HEAD, including this documentation handoff:

- `.github/workflows/m1.yml`
- `AGENTS.md`
- `Makefile`
- `README.md`
- `SPEC.md`
- `compose.m2b.yaml`
- `docs/adr/009-incremental-capture-acknowledgement.md`
- `docs/evidence/M2B.md`
- `docs/evidence/M2C-summary.json`
- `docs/evidence/M2C.md`
- `docs/milestones/M2C.md`
- `knip.json`
- `migrations/004-source-capture.sql`
- `migrations/pipeline/002-capture-instance.sql`
- `package.json`
- `scripts/capture.ts`
- `scripts/m1.ts`
- `scripts/migrate-staging.ts`
- `scripts/required-capture-cases.ts`
- `scripts/review.ts`
- `scripts/validate-tools.ts`
- `src/capture.ts`
- `src/internal/capture-report.ts`
- `src/pipeline.ts`
- `src/source-capture.ts`
- `tests/capture/capture-death.test.ts`
- `tests/capture/capture-process.test.ts`
- `tests/capture/capture.test.ts`
- `tests/support/capture-child.ts`
- `tests/support/capture-process.ts`
- `tests/support/capture-upgrade.ts`
- `tests/support/capture.ts`
- `tests/unit/capture-report.test.ts`
- `tests/unit/capture.test.ts`

## Local handoff and remaining limitations

[Compact machine summary](M2C-summary.json) records commands/exits, tested SHA/input hashes, independent case IDs, profile states, actual signals, source claims, observations and cleanup. Full sanitized logs and native reports remain in ignored local artifact directories. These paths are local, not remotely accessible evidence links.

`npm run review:bundle -- artifacts/m2c/acceptance-20260916220749290-aec6e789 --m2c` creates local artifacts/review/m2c-final-<documentation-head>/ with tracked.tar, since-reviewed.patch, since-spec.patch, chronological commits.txt, changed-files.txt, manifest.json, prior/development/final evidence and SHA256SUMS. The final handoff supplies the concrete archive/checksum paths. This avoids a self-referential tracked documentation SHA or archive checksum.

This is at-least-once source-to-pipeline staging with immutable local deduplication, not publication, searchability, consumer processing or end-to-end exactly-once. All destinations remain unbound; all sink intents and consumer observations remain pending. No receiver calls or simulated consumer effects exist.

Before real delivery, M2B assert_obligations must be deliberately migrated to permit replay of valid settled obligations without resetting success; its current pending/unbound requirement is retained. The known seed/activation regression also remains: seed a protected baseline without mutation outbox, activate, issue unchanged update, and retain a successful command receipt without inventing a mutation. Migration 002's command-result FK is unchanged; current real fixtures are captured mutations, not a demonstration of that future baseline path.

Independent rollback of retained databases, malicious owner bypass, arbitrary host/storage failures and unlimited process descendants escaping their owned group remain outside this evidence. Trusted capture code must stage before ACK; a hash cannot authenticate remote durability. Clock-based leases assume the configured source clock behavior. Callbacks must cooperate; connection costs, polling cost, throughput and large data behavior were not benchmarked. Blocked oversized work is visible but has no recovery/replay API or sink DLQ in this milestone.

All applicable local checks listed above ran. Remote M2C CI, backfill/runs/checkpoints/fences, seed/activation, real receiver registration/delivery, consumer implementation, general sink retry/DLQ/replay, production API/UI/deployment and large-scale guarantees remain unrun/unimplemented. The local workflow now selects the same verify-m2c command, without being pushed. **Stop after M2C and await independent review before real sink delivery.**

## Subsequent hosted CI observation — 2026-09-17

After the historical report above, [run 35157202674](https://github.com/dojohubco/kill-it-twice/actions/runs/35157202674) succeeded at documentation HEAD 6a908f46d3c690023d204b0fdb785e53d471d207 (2026-09-16 22:20:40–22:23:16 UTC). M2C.1 intake independently checked read-only run metadata. The reviewer supplied downloaded artifact SHA-256 1d7286a923797953c442566d97e1ec3421ec3574868e9443e46ea00911c77029 and independently reconciled its recorded snapshots: fresh 66 outbox/work revisions, 65 staged/acknowledged events and one blocked oversized revision; populated upgrade 68, 67 and one. Recorded bytes, hashes, metadata, relations, ACKs and restart snapshots reconciled offline. This was evidence inspection, not reviewer-local PostgreSQL execution. That CI passed the existing inventory; it did not execute the newly proposed old-snapshot registration counterexample, which the reviewer had not run. The historical NOT RUN statements retain their original timing.
