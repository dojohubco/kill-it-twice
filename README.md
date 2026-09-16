# Kill It Twice — source M1, M1.1 and M2A

This repository implements the controlled source mutation/version/outbox contract with real PostgreSQL, restricted runtime credentials and actual writer SIGKILLs. M1.1 hardens transaction ownership and the acceptance harness after external adversarial review. M2A adds durable source-command receipts using the accepted transaction owner. G1–G5 remain **NOT IMPLEMENTED**. Read [SPEC v1](SPEC.md), [M1.1 scope](docs/milestones/M1.1.md), [transaction decision](docs/adr/005-managed-source-transactions.md), and [historical M1 evidence](docs/evidence/M1.md).

Prerequisites: Linux x64, Node **24.19.0**, npm **12.0.2**, Docker with Compose, Git, make and tar. Node's native runner executes TypeScript; TypeScript **5.9.3** checks it separately. Keep one root lockfile and exact package pins.

```sh
npm ci --no-audit --no-fund
npm run tools:provision
make quality
npm run test:integration:m1
make verify-m1
make verify-m2a
make verify
```

Provisioning downloads actionlint **1.7.12** into `.tools/cache/`, verifies the published archive checksum and a pinned binary checksum, and checks its version. No privileged system install is used. Missing tools fail clearly. `make quality` only reads existing tools and inputs: Prettier check, typed ESLint with zero warnings, TypeScript, Knip, Compose configuration with nonsecret interpolation, actionlint and pure/unit tests. It does not install, consult advisories, start PostgreSQL, edit files or invoke the integration/full verifier. Disposable process fixtures are exercised by the unit tests and cleaned up.

`make verify-m1` runs quality followed by fresh real PostgreSQL acceptance. Every integration invocation owns a unique Compose project, temporary credentials, volume and loopback port. It verifies a checked-in inventory of 22 required cases using native structured test events, retains JUnit and direct SQL/process evidence, checks retained-volume restart, and removes its own resources. Missing, skipped, todo, cancelled, duplicate or malformed results fail. `make verify` deliberately prints G1–G5 NOT IMPLEMENTED and exits nonzero (GNU make exit 2).

PostgreSQL remains pinned to `18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. Durability settings remain on. The unchanged original migration defines epoch, entities and immutable outbox. Forward migration 002 adds only command receipts; the M1 profile applies 001 and the M2A profile applies both. Real migration execution, trigger/constraint/role and lifecycle branch tests establish SQL behavior; ordinary SQL parsing is not a PL/pgSQL proof. No SQL formatter or static SQL extension is a blocking dependency.

## Transaction contract

Create a `Source` from connection configuration, then call `source.transaction(async (tx) => ...)`. Each transaction opens and closes a private session; callbacks receive allowlisted create, mutate, command and fixed diagnostic inspection operations. Full-history inspection is only for tiny test diagnostics, not a future bounded incremental reader. Await each operation. The capability expires at callback completion. The owner rejects nested/concurrent transactions before connection/transaction SQL; use the same existing capability for one atomic unit. Manual SQL transaction control is not exposed. Independent low-level SQL fixtures remain under tests/.

Confirmed COMMIT permits success; a ROLLBACK command tag never does. A caught mutation error still fails the transaction and retains the original PostgreSQL cause/SQLSTATE. An interrupted COMMIT is unknown even if a subsequent ROLLBACK succeeds. Setup failure before work, known rollback, unknown completion and confirmed commit followed by cleanup failure are distinct. Cleanup errors remain separate, and unknown outcomes or broken cleanup poison the owner. There are no automatic mutation retries. Deadlock acceptance verifies the actual 40P01 victim and survivor without choosing which writer must lose.

Callbacks must settle cooperatively; arbitrary JavaScript cannot be preempted by this API. IDs/versions are checked decimal strings. Live payloads are JSONB objects; deletion uses SQL NULL, and restore advances the retained identity's version. The legacy source_writer mutation interface is outside M2A command deduplication; it remains for the original M1 SQL scope.

## Source command contract

Use the restricted `source_command` credential with `source.command({ sourceEpoch, commandId, contractVersion: 1, operation, entityId, payloadJson })`. The caller supplies the expected persisted epoch and UUID command ID. Create requires `entityId: null`; update/delete/restore require a positive BIGINT decimal string. Live payloads are JSON object text; delete requires `payloadJson: null`. The role can only execute the command function and cannot read or edit tables, call legacy mutations, alter capture or assume owner privileges.

The same key and equal PostgreSQL JSONB request recovers the original committed `result`; `replayed` describes only the attempt. The immutable result contains string IDs/version, change ID, epoch, UTC microsecond timestamp, deletion state and JSON payload text. It remains the original snapshot after later mutations. A fresh key denotes a different command. A rolled-back attempt leaves no successful receipt, so an explicit same-key retry can execute later. No-op commands retain receipts without adding revisions. There is no automatic retry or failed-response retention; receipts do not expire within the supported epoch.

The owner starts READ COMMITTED, and SQL reserves the unique key before mutation. Contenders wait for the actual transaction outcome, then replay, conflict or take over after rollback. SQLSTATE P2001 / `SourceTransactionError.kind === 'idempotency_conflict'` identifies differing content under a successful key; P2002 / `source_epoch_mismatch` rejects an unexpected epoch. Completion checks prohibit committed partial reservations. [ADR 006](docs/adr/006-source-command-receipts.md) specifies the boundary and [M2A scope](docs/milestones/M2A.md) defines C01–C12 and both healthy fault controls. This is source deduplication, not end-to-end exactly-once or pipeline delivery.

`make verify-m2a` runs unchanged quality checks plus all 22 M1/M1.1 cases and 14 M2A cases/controls, on fresh isolated PostgreSQL. Inventories are explicit, not inferred from discovered results. The checker also rejects a contradictory nonzero summary failed count. Initial empty-table assertions run before any workload; suites use distinct fixtures and known IDs rather than filename ordering.

For a clean M2A handoff, use `npm run review:capture -- --m2a`, then `npm run review:summary -- <capture-directory> --m2a`, and after committing documentation, `npm run review:bundle -- <capture-directory> --m2a`. This captures the existing M1 profile, standalone M2A, two fresh verify-m2a runs, all quality commands and the expected nonzero full verifier. Remote M2A CI is unrun unless a hosted run is explicitly recorded. The later successful M1.1 run is dated separately in its historical evidence.

## Harness limits and evidence

SIGKILL tests pause the production transaction owner at both existing private boundaries. They require exact process/session identities, independent row/lock observations, actual SIGKILL exit and no caller success. Healthy release controls require exit zero and exactly one success. Unexpected parent IPC loss closes the session and exits 72; it never counts as a SIGKILL result.

Polling uses a monotonic deadline and observes success only before it. Resource-bearing database observations have a 500 ms server statement timeout; expiry closes the owned connection, bounds disposal, consumes late rejection, and verifies backend disappearance independently. Socket closure alone need not immediately interrupt a server query. Pure promises have no external resource to cancel. Subprocess capture is bounded to 1 MiB combined output and explicitly fails on overflow; complete chunks and cutoff prefixes are credential-redacted. Timeout cleanup targets only the process group created by that spawn. Tests terminate a real child/grandchild while an unrelated sentinel survives. Descendants deliberately escaping the owned group, harness SIGKILL, host crash and malicious database owners are outside these cleanup guarantees.

Local sanitized artifacts live in ignored `artifacts/m1/<run-id>/` or `artifacts/m2a/<run-id>/`. Public upload copies are created only through explicit sanitization; failed finalization invalidates stale PASS summaries. Primary and cleanup failures are separate. Dirty developmental runs record tracked/untracked input hashes and a patch; final acceptance requires committed clean code. Paths in summaries identify local files, not publicly accessible evidence URLs.

`npm run review:capture` records all quality subcommands, quality, standalone integration, two fresh verify-m1 runs and expected failing verify from clean committed code. `npm run review:bundle -- <capture-directory>` creates a local final tracked archive, full diffs from the reviewed and initial specification commits, chronological history, development/acceptance logs and SHA-256 checksums.

## Tool choices and CI

Prettier **3.9.7** is the sole formatter for supported formats; SQL is excluded. ESLint retains its pinned major and uses typescript-eslint recommended type-checked rules with projectService, explicit floating/misused promise, await-thenable and unsafe-value rules. Narrow public-method test instrumentation exceptions explain the explicit receiver binding. TypeScript retains strict, noUncheckedIndexedAccess and erasableSyntaxOnly; adds exact optional properties, explicit returns, switch fallthrough, override, index-signature access, side-effect import and casing checks. `useUnknownInCatchVariables` is already included in strict. `skipLibCheck: false` is retained after checking the pinned dependencies.

Knip **6.36.0** discovers npm scripts and the custom reporter; explicit entries cover native test files and the explicitly listed private process fixtures. Source files are reached through imports. No unused-code/dependency suppression is configured. Discarded formatting, promise/type and unused code/dependency canaries must be detected before handoff.

The push/PR workflow now uses the same `make verify-m2a` command on Ubuntu 24.04 with read-only repository permission, exact Node/npm, no persisted checkout credentials, no project secrets and a 15-minute timeout. Action references are full release commit SHAs; their official release metadata and action inputs/runtime were reviewed locally. Actionlint validation is local evidence, **not a remotely run CI PASS**. Sanitized evidence upload runs even on failure. This task neither pushes the workflow nor changes repository settings.

Separate future work includes advisory/security review and targeted property/mutation tests. No extra security scanner ecosystem, large CI benchmark, pipeline, acknowledgements, backfill, broker/search, consumer, UI or production deployment is introduced.

Sources: [native reporters](https://nodejs.org/docs/latest-v24.x/api/test.html#custom-reporters), [typed linting](https://typescript-eslint.io/getting-started/typed-linting/), [Knip configuration](https://knip.dev/overview/configuration), [actionlint release](https://github.com/rhysd/actionlint/releases/tag/v1.7.12), and [pg lifecycle](https://node-postgres.com/apis/client).
