# Kill It Twice — M1

The initial SPEC and ADRs were prepared through AI-assisted architectural review before implementation. This repository contains the M1 PostgreSQL harness, source mutation functions, version/capture triggers, restricted runtime role, source contract tests, and private real writer SIGKILL tests. Full M1 acceptance is pending a fresh successful complete run; no full G1-G5 gate exists.

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

`verify-m1` is M1-only (currently infrastructure and source contract checks). `verify` deliberately prints G1-G5 NOT IMPLEMENTED and returns nonzero. The integration command creates one isolated source PostgreSQL service, binds an ephemeral loopback port, creates temporary credentials, records sanitized artifacts in `artifacts/m1/<run-id>/`, and removes only its own Compose resources. It requires a working Docker daemon and network access if the pinned image is absent. Failures are not skips.

PostgreSQL is pinned to `18.6-bookworm` and manifest digest `sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`. Each run also records the actual container image ID and server version. Normal PostgreSQL durability remains enabled.

Version basis: [Node releases](https://nodejs.org/en/about/previous-releases), [exact Node distribution index](https://nodejs.org/dist/index.json), [PostgreSQL version policy](https://www.postgresql.org/support/versioning/), and [official image definitions](https://github.com/docker-library/official-images/blob/master/library/postgres). Node 24.19.0 is the existing local LTS-family patch used and pinned here, not a claim to be the latest Node patch.

Initial development check: typecheck/lint and 3 unit tests passed. The first real service startup failed on this SELinux-enforcing host with permission denied reading the temporary secret mount. Sanitized failure evidence remains at `artifacts/m1/m1-20260916155946793-d667e1e0/`. The corrective mount uses a private `:Z` label on only the run-owned password file; it does not change host security settings. The subsequent real PostgreSQL smoke test passed at `artifacts/m1/m1-20260916160048270-b6cae620/`. Both runs removed their owned resources.

Source development check: typecheck/lint and 5 unit tests passed. The first source run (`artifacts/m1/m1-20260916160939467-d8ba6031/`) passed 8/9 integration tests. Review of the T06 statement and its real SQL error showed that assigning a GENERATED ALWAYS identity produces `428C9` before the privilege check; the original `42501` expectation was incorrect. The correction asserts exactly `428C9` for that statement and retains rejection/state assertions. The corrected run (`artifacts/m1/m1-20260916161134422-72859062/`) passed 9/9. SQL function/grant details were recorded in `docs/milestones/M1.md` before migration execution.

A later fresh startup (`artifacts/m1/m1-20260916161741493-e9afb166/`) exposed a readiness race: the socket-based health check accepted the image's temporary initialization server. No integration tests ran in that attempt. The health check now requires the TCP listener, which the temporary server does not expose. The SQL connection/settings check still runs before migration; no source mutation is retried on an unknown outcome.

The first complete source/fault test attempt (`artifacts/m1/m1-20260916161845688-0f974b51/`) passed all 12 tests, including actual SIGKILL exits at both private barriers. The later retained-volume restart check failed connecting to the original ephemeral host port. The overall run remains recorded as FAIL. The corrected harness resolves the published port again after restart. Run `artifacts/m1/m1-20260916162005276-60c805f5/` passed all 12 tests, verified unchanged epoch/entities/outbox through restart, and removed all owned resources. Separate final acceptance and a fresh repeat are still required.
