# AGENTS.md

## Before work

Read SPEC.md, relevant accepted ADRs, and the current milestone task. Inspect git status and repository/ancestor instructions. SPEC defines behavior; this file defines workflow. Report contradictions instead of silently choosing another guarantee.

## Scope and correctness

Work only on the authorized milestone. Use explicit transactions on one checked-out database connection. Introduce only dependencies/components needed now.

Never silently change delivery semantics. Never advance a checkpoint outside its required staging transaction. Never acknowledge a source event before durable staging, or ACK RabbitMQ before consumer database COMMIT. Never treat Elasticsearch bulk HTTP success as success for every item, or publisher confirmation as consumer processing. A timeout does not prove rollback or remote rejection. Never mutate historical canonical content to make replay succeed.

Never weaken/delete assertions to make code pass. Never replace required real PostgreSQL, RabbitMQ, Elasticsearch, SIGKILL, or outage tests with mocks. A genuinely incorrect test needs an explicit explanation and review, not a quiet rewrite. Never mark an unrun, skipped, or incomplete gate PASS.

## History and evidence

Commit the initial specification before implementation. Make meaningful, chronological commits. Do not backdate, invent authorship, amend/rebase/squash away evidence, fabricate discoveries, or label prior architectural knowledge as an agent discovery. Do not commit secrets or raw chat transcripts.

Run applicable checks and report exact commands, exit codes, skips, tool/service versions, and artifact paths. An environmental blocker is not a passing test. Preserve failed evidence; report actual deviations or "none observed."

Change SPEC/ADRs only for a deliberate decision, assumption change, or substantive clarification. New evidence belongs in milestone reports; routine bug fixes do not require rewriting SPEC. Stop for review before changing a central invariant. Update this file when real repository commands become available.

## Safety and handoff

Use isolated test project names, ports, databases, and volumes. Cleanup only resources created by the current test. Never prune global Docker resources, kill unrelated processes, install privileged host software, change global Git identity, or publish/push without authorization.

Finish with tested commit SHA, changes, reproducible results, relevant SQL/code, unresolved risks, and unimplemented scope. Stop after the requested milestone. Do not opportunistically build the next one.

## Available commands

Use `npm ci`, `npm run typecheck`, `npm run lint`, `npm run test:unit`, and `npm run test:integration:m1`. `make verify-m1` runs only the implemented M1 checks. `make verify` deliberately reports G1-G5 NOT IMPLEMENTED and exits nonzero. Integration artifacts are under ignored `artifacts/m1/<run-id>/`; integration resources are managed by `scripts/m1.ts`. Do not invoke Compose with a shared/default project name.

## M1.1 commands and boundary

Provision with `npm ci --no-audit --no-fund` and `npm run tools:provision`. `make quality` is read-only and local: formatting check, typed lint, typecheck, Knip, Compose configuration, actionlint and unit tests. `make verify-m1` adds fresh real PostgreSQL acceptance against the independent inventory in scripts/required-cases.ts. `npm run review:capture` requires clean committed code; `npm run review:bundle -- <capture-directory>` packages local evidence. Source callbacks use the owned expiring capability described in ADR 005; never pass arbitrary pg clients or expose manual transaction control through that API. M1.1 authorizes no M2 work or remote publication.

## M2A commands and boundary

M2A is separately authorized source-command idempotency only. `npm run test:integration:m2a` and `make verify-m2a` select both source migrations and all M1/M1.1 plus M2A inventories. The command role is source_command, never source_writer for future retrying callers. Supply the caller's expected epoch and UUID; the transaction owner never retries or generates a replacement key. ADR 006 defines success-only retention and original result replay. `npm run review:capture -- --m2a` captures clean acceptance; summary/bundle modes take `<capture-directory> --m2a`. No later M2 pipeline, acknowledgement or remote publication is authorized.

## M2B commands and boundary

`make verify-m2b` runs quality, the 22-case M1 and 36-case M2A profiles, then both fresh and populated-upgrade variants of the 16-case two-database staging profile. `npm run test:integration:m2b` selects fresh installation; append `-- --upgrade` for forward source-reader grants after committed M2A receipts/history. Every run owns its two Compose services/volumes. ADRs 007/008 define precise opaque PG18 payload text, bounded explicit source selection, immutable canonical bytes and atomic pending obligations. `npm run stage -- <entity-id>:<version> ...` uses explicit SOURCE_EPOCH, SOURCE_READER_HOST/PORT/PASSWORD and PIPELINE_STAGER_HOST/PORT/PASSWORD; no default or owner credentials. `npm run review:capture -- --m2b` captures clean final evidence; summary/bundle accept `<capture-directory> --m2b`. No poller, source ACK, sink/consumer success, seeding, backfill or full G1-G5 guarantee is implemented.

## M2C commands and boundary

`make verify-m2c` retains every earlier profile/upgrade, then requires fresh and populated M2B-upgrade capture profiles. `npm run test:integration:m2c -- --upgrade` selects the latter. ADR 009 defines source claim COMMIT, pipeline stage COMMIT and then source ACK COMMIT, with source-clock ownership and explicit immutable pipeline identity. `npm run capture -- once` or `-- follow` requires SOURCE_EPOCH, PIPELINE_ID, SOURCE_CAPTURE_HOST/PORT/PASSWORD and PIPELINE_CAPTURE_HOST/PORT/PASSWORD. Runtime startup never registers or replaces a binding. Source capture and pipeline capture are distinct restricted credentials; business commands are never automatically retried. Keep eleven-field canonical content unchanged. `npm run review:capture -- --m2c` and summary/bundle with `<capture-directory> --m2c` retain clean local evidence. ACK means staged; sinks and observations remain pending/unbound. Stop before real sink delivery, seeding or backfill.

## M2C.1 commands and boundary

`make verify-m2c1` is the complete prior-plus-isolation gate: unchanged `verify-m2c`, guarded fresh 27-case and registered populated-upgrade 23-case profiles. `npm run test:integration:m2c1 -- --upgrade` selects the latter. Historical `m2c1-repro` deliberately stops at migration 004 and demonstrates the defect; never treat it as corrected acceptance. Source migration 005 enforces READ COMMITTED for revision-producing writes at the common enqueue boundary before binding visibility matters. Read-only/legacy no-revision no-ops remain allowed; command isolation rules are unchanged. Review capture/summary/bundle use `--m2c1`. Stop before sink work.

## M3 commands and boundary

`make verify-m3` runs quality and each earlier profile once, then real Elasticsearch fresh and populated guarded M2C.1-upgrade profiles. Each new profile owns two PostgreSQL services plus authenticated TLS Elasticsearch and test-only Toxiproxy. `npm run test:integration:m3 -- --upgrade` selects the latter. `npm run deliver:es -- once|follow|status` requires explicit restricted pipeline/ES credentials; setup is controlled and separate. ADR 010 defines the fixed search projection, durable item outcomes, fenced leases and exact external versions. Runtime never creates/rebinds indexes or sends RabbitMQ/consumer effects. `npm run review:m3 -- capture` requires committed clean code and captures two complete gates plus honest nonzero full verify; `summary` and `bundle` take its local capture directory. No remote execution/publication is authorized. ES startup requires existing vm.max_map_count >=1048576; the harness changes no host settings. Stop after M3 review.

## M3.1 commands and boundary

`make verify-m3` now checks the read-only Linux/local-Docker prerequisite before all heavy profiles, then runs the earlier profiles once, both M3 profiles (including O07) and `npm run test:integration:m31` (O01–O06). `test:reproduction:m3-oracle` preserves the historical oracle counterexample, not healthy acceptance. Rejection expectations are verifier-owned declarations made before delivery; latest-source degradation never authorizes arbitrary stale/missing receiver content. Only the GitHub-hosted ubuntu-24.04 workflow may raise a lower vm.max_map_count on its ephemeral VM; never execute that privileged change locally. Phase-aware reporting keeps pre-test NOT RUN separate from missing attempted evidence and genuine cleanup failures. No production delivery redesign or RabbitMQ/M4 work is authorized.

`npm run review:m31 -- capture` captures two complete 11-profile M3.1 gates from clean committed code plus honest nonzero full verify. `summary`/`bundle` take the local capture directory. The shared M3 review script still recognizes historical M3 capture inputs for their original bundles; new captures target M3.1. The separate oracle profile has six cases; original M3 profiles add O07 while retaining ES01–ES14.

## M4 commands and boundary

`make verify-m4` runs quality and all eleven earlier profiles once through `verify-m3`, then fresh and populated M3.1-upgrade 22-case broker/consumer profiles. `npm run test:integration:m4 -- --upgrade` selects the latter. ADR 011 defines separate broker-confirm and consumer-database-COMMIT/individual-ACK boundaries. RabbitMQ is retained, TLS authenticated, uses a quorum queue and application-owned registration; runtime never declares or recreates topology. The independent `consumer_m4` database is on the state PostgreSQL service, with no consumer access to source/pipeline databases. `npm run deliver:rabbit -- once|follow|status`, `npm run consume -- once|follow|status`, and `npm run observe:consumer -- once|follow|status` require the explicit credentials in scripts/rabbit-cli-config.ts. Consumer status includes bounded quarantine metadata; no replay/purge API. `npm run review:m4 -- capture` captures two complete clean committed gates and nonzero full verify; `summary`/`bundle` require its local directory. No remote execution/publication is authorized. No baseline activation, backfill, UI, deployment, distributed exactly-once or full G1–G5 claim. Stop for review after M4.

## M4.1 commands and boundary

`make verify-m4-1` runs the complete existing M4 gate once, historical `test:reproduction:m41` on consumer migration 001, then fresh and populated-consumer `test:integration:m41` profiles (append `-- --upgrade` for the latter). B01 is expected failed application processing, not consumer success; corrected B02–B08 and B06H require exact original wire-byte accounting, real ACK/crash behavior and preservation through forward consumer migration 002. The normal bound stays 32 original messages/1048576 canonical wire-body bytes; deduplication reduces only unique SQL work. No source/event/broker protocol change. `npm run review:m41 -- capture` captures two complete gates; `summary`/`bundle` take the local capture directory. No push/dispatch, host tuning, seeding, backfill, replay API, UI or M5. Stop for independent review.

## M5A commands and boundary

`make verify-m5a` retains the full verify-m4-1 gate once, then fresh bootstrap and populated M4.1 source upgrade profiles. `npm run test:integration:m5a -- --upgrade` selects the latter. Source migration 006 and ADR 012 define immutable baselines, dedicated bootstrap authority, bounded chunk receipts and sealed-to-active capture registration. `npm run bootstrap -- seed <epoch> <key> <recipe-version> <seed> <count> <chunk-size>` explicitly resumes a matching manifest using SOURCE_BOOTSTRAP_HOST/PORT/PASSWORD; status, seal and activate are separate modes. Activation also requires explicit PIPELINE_ID and PIPELINE_CAPTURE_HOST/PORT/PASSWORD for remote identity validation before the source transaction. No automatic seed staging, backfill scanner/checkpoint/fence, replay API, UI, scale claim or remote publication. Stop after M5A.
