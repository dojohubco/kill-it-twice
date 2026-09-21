# 015 — Retained local runtime and independent acceptance

Date: 2026-09-21. Status: accepted implementation direction; execution evidence pending.

## Decision

Compose assembles the existing source, pipeline, consumer and receiver implementations rather than introducing another replication engine. Source and pipeline remain separate PostgreSQL services. Consumer remains a separate database and role on the state service. Each capture, backfill, Elasticsearch, publisher, consumer and receipt observer runs in its own bounded process/container.

A one-shot provisioner creates installation-local random credentials and TLS material in named volumes. A separate initializer applies the unchanged ordered migrations with per-database checksum receipts and registers the existing receiver identities. Administrative credentials are absent from worker/API volumes. Initializer credentials remain available only to explicit setup/verification tools; no process receives a Docker socket. Compose dependency completion controls startup, not distributed durability.

Existing registered receiver loss or configuration mismatch blocks initialization instead of recreating state. Migration receipt and SQL changes commit together. A restart validates stored hashes and retains credentials. The local single-node receiver/no-independent-database-rollback assumptions remain unchanged. Partially completed initial setup may resume; an initialized installation is never reclassified as empty.

`make seed` is explicit. It uses the existing deterministic roughly 1-KiB baseline recipe, bounded chunks and immutable bootstrap key. A changed requested count/recipe conflicts. Confirmed activation precedes a convenience worker-start marker and creation of a stable initial backfill run. The marker is not a durability oracle: workers still validate their own registered database/receiver identities. Worker restarts do not call seed, migrate, register or reset.

A bounded backfill dispatcher discovers existing nonterminal runs using the granted read surface, then invokes the existing worker with its normal lease/checkpoint transactions. It does not create runs or replace their state machine. Other workers reuse their existing CLI loops. Local public access is through one loopback-bound same-origin gateway; the private operational API receives only its restricted configuration. No browser code receives service credentials.

The final acceptance launcher owns a separate explicit Compose project and verifies resource labels before destructive fault/cleanup operations. Real failures happen from that external launcher, never through a Docker socket in the application. Independent reconciliation runs from bounded exports and verifier-owned expected mutation/failure declarations. Browser fixtures are not integrated service proof. Missing prerequisites and unfinished gates remain nonpassing.

## Consequences and limits

Named volumes intentionally persist across ordinary `docker compose down`/up. Removing volumes destroys this local installation; no automatic reset command is needed for normal operation. Setup and test tools may have broad authority within their own isolated installation; runtime identities do not. Single-host service networking is a trusted local demonstration boundary, not production multi-tenant authentication or HA.

Resource ceilings make retained work visible rather than promising unlimited buffering. The planned capacity profile needs direct elapsed time, memory and storage observations. Small correctness gates and build sizes cannot establish 2M-row capacity. No schema or batch-size optimization is selected before actual evidence motivates it.

References: Docker Compose startup ordering and `service_completed_successfully` (https://docs.docker.com/compose/how-tos/startup-order/); Elasticsearch file realm and role files (https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/file-based). These describe orchestration/authentication mechanisms, not the application delivery guarantee.
