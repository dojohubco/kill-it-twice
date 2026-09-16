# Kill It Twice — M1

The initial SPEC and ADRs were prepared through AI-assisted architectural review before implementation. This repository currently contains the M1 PostgreSQL harness. Source contract and process-death acceptance are pending; no full G1-G5 gate exists.

Prerequisites: Node **24.19.0**, npm **12.0.2**, Docker with Compose, Git, and make. No system software is installed by the harness. Node's built-in test runner executes TypeScript directly; TypeScript 5.9.3 checks it separately. The root lockfile pins dependencies.

```sh
npm ci
npm run typecheck
npm run lint
npm run test:unit
npm run test:integration:m1
make verify-m1
make verify
```

`verify-m1` is M1-only (currently infrastructure smoke verification). `verify` deliberately prints G1-G5 NOT IMPLEMENTED and returns nonzero. The integration command creates one isolated source PostgreSQL service, binds an ephemeral loopback port, creates temporary credentials, records sanitized artifacts in `artifacts/m1/<run-id>/`, and removes only its own Compose resources. It requires a working Docker daemon and network access if the pinned image is absent. Failures are not skips.

PostgreSQL is pinned to `18.6-bookworm` and manifest digest `sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. Each run also records the actual container image ID and server version. Normal PostgreSQL durability remains enabled.

Version basis: [Node releases](https://nodejs.org/en/about/previous-releases), [exact Node distribution index](https://nodejs.org/dist/index.json), [PostgreSQL version policy](https://www.postgresql.org/support/versioning/), and [official image definitions](https://github.com/docker-library/official-images/blob/master/library/postgres). Node 24.19.0 is the existing local LTS-family patch used and pinned here, not a claim to be the latest Node patch.

Initial development check: typecheck/lint and 3 unit tests passed. The first real service startup failed on this SELinux-enforcing host with permission denied reading the temporary secret mount. This is an unresolved harness failure in this commit, not a PostgreSQL test pass. Sanitized evidence: `artifacts/m1/m1-20260916155946793-d667e1e0/`; its owned resources were removed.
