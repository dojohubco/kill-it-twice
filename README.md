# Kill It Twice — M1 only

M1 proves the controlled source mutation/version/outbox contract with real PostgreSQL, restricted runtime credentials and actual writer SIGKILLs. It does not complete any full Optio G1–G5 gate. Read [SPEC v1](SPEC.md), [M1 scope](docs/milestones/M1.md) and the [actual evidence and limitations](docs/evidence/M1.md). The initial specification/ADRs came from pre-implementation AI-assisted architectural review.

Prerequisites: Linux, Node **24.19.0**, npm **12.0.2**, Docker with Compose, Git and make. The harness installs no system software. Node's built-in test runner executes TypeScript directly; TypeScript 5.9.3 checks it separately. One root lockfile pins dependencies.

```sh
npm ci
npm run typecheck
npm run lint
npm run test:unit
npm run test:integration:m1
make verify-m1
make verify
```

`verify-m1` runs M1-only checks: typecheck, lint, 6 unit tests and 12 real integration cases, followed by retained-volume service restart and owned-resource cleanup. Every integration invocation creates fresh resources. `verify` deliberately prints **G1–G5 NOT IMPLEMENTED** and returns nonzero (GNU make exit 2). No missing gate is counted as passed.

The integration command creates one source PostgreSQL service, a unique Compose project, temporary credentials and an ephemeral loopback port. It records sanitized command logs, direct SQL observations, JUnit results and actual signal/session evidence under ignored `artifacts/m1/<run-id>/`. Tests fail on missing barriers/timeouts; cleanup removes only that run's resources. Keep logs when a check fails. The harness needs an accessible Docker daemon and network access if the pinned image is not cached. SIGINT/SIGTERM request cleanup after the bounded active command; SIGKILL of the harness itself is outside its cleanup guarantee.

PostgreSQL is pinned to `18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. The harness also records the actual container image identity and server version. `fsync`, `synchronous_commit` and `full_page_writes` remain on. Readiness requires TCP, the temporary password mount uses a private SELinux label, and the host port is resolved again after restart.

`migrations/001-source.sql` defines only epoch, entities and immutable outbox tables. The real writer login can execute two source mutation functions and read rows; it cannot directly mutate tables, metadata or sequences, disable capture, truncate/delete retained identities or assume the owner role. `src/source.ts` supplies checked decimal-string identifiers, explicit client transactions and mutation helpers without retries. JSONB objects are live payloads; deletion stores SQL NULL and restore advances the same identity's version. Private process barriers exist only under tests/.

No pipeline, canonical wire envelope, acknowledgement worker, source command-idempotency API, backfill, sinks, consumer, UI, benchmark or full gate harness is implemented. Post-COMMIT writer death intentionally leaves caller outcome unknown; do not blindly repeat the mutation.

Version sources: [Node releases](https://nodejs.org/en/about/previous-releases), [exact Node distribution index](https://nodejs.org/dist/index.json), [PostgreSQL version policy](https://www.postgresql.org/support/versioning/), and [official image definitions](https://github.com/docker-library/official-images/blob/master/library/postgres). Node 24.19.0 is the installed LTS-family patch used and pinned here, not a claim to be the latest Node patch.
