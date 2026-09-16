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
