# Local operational backend

M6 adds a Nest HTTP process; existing capture, backfill, ES, Rabbit and consumer workers remain separate processes. There is no frontend, Swagger UI, background source generator or automatic startup mutation. ADR 014 defines the contract. This is a local demonstration boundary, not a production authentication deployment.

## Prepare and run

Use the pinned root runtime/package and lockfile:

```sh
npm ci --no-audit --no-fund
npm run build:api
CONTROL_CONFIG_FILE=/absolute/private/control.json npm run control:api
```

The process listens on 127.0.0.1:3000 unless CONTROL_PORT specifies another port. Use a private file readable by the API user only. The JSON object has these keys; placeholders below must be replaced with the existing run's restricted connection details. Do not commit a filled configuration or pass passwords on command lines.

```json
{
  "source": {
    "host": "127.0.0.1",
    "port": 15432,
    "password": "<source_operator password>"
  },
  "pipeline": {
    "host": "127.0.0.1",
    "port": 15433,
    "password": "<pipeline_operator password>"
  },
  "consumer": {
    "host": "127.0.0.1",
    "port": 15433,
    "password": "<consumer_operator password>"
  },
  "sourceEpoch": "<registered source UUID>",
  "pipelineId": "<registered pipeline UUID>",
  "es": {
    "node": "https://<run-owned ES proxy>",
    "username": "<existing restricted ES reader/worker>",
    "password": "<password>",
    "ca": "<PEM CA>"
  },
  "rabbit": {
    "url": "https://<run-owned Rabbit management endpoint>",
    "username": "<existing metadata observer>",
    "password": "<password>",
    "ca": "<PEM CA>"
  },
  "proxyApi": "http://<run-owned Toxiproxy API>/",
  "proxies": { "elasticsearch": "es", "rabbitmq": "m6-rabbit" },
  "token": "<random run-local operator token, at least 32 characters>"
}
```

Database names and operator roles are fixed by the loader: source_m1/source_operator, pipeline_m2b/pipeline_operator, consumer_m4/consumer_operator. Before launching, the controlled provisioner applies source migration 008, pipeline migration 007 and consumer migration 003, after all accepted prior migrations. Each migration runs in one explicit transaction on its corresponding administrator connection; scripts/migrate-operations.ts is the shared implementation used by acceptance. Provisioning supplies dedicated random operator passwords. API startup never applies migrations, registers receivers, seeds or activates the source.

A container definition is provided in compose.m6.yaml with an exact Node image digest, read-only filesystem/config mount, no capabilities and loopback-only published host port. Use an explicit unique Compose project and a private config readable by the container's node user. Container connection addresses must address the existing run-owned services from that network; host loopback addresses inside a container do not address host services. The application gets no Docker socket. Cleanup only the named project created for that run. The acceptance harness uses real API child processes against real services. A separate container smoke check proves image build/startup, loopback binding, non-root/read-only operation and token enforcement with deliberately unconfigured dependencies; it is not a full retained demo deployment.

## Read and control

GET /api/v1/openapi.json returns controller-generated OpenAPI including response schemas. GET /metrics returns Prometheus text. All other routes are under /api/v1. Successful JSON responses wrap data with a request_id; errors carry a stable error.code and the same X-Request-ID response header. IDs and versions stay decimal strings. Timestamp/count observations describe the observed databases/receivers, not a globally atomic distributed snapshot.

Mutations require Authorization: Bearer <token> and Idempotency-Key: <UUID>. An optional X-Request-ID UUID provides correlation independent of the durable key. 202 means committed/scheduled; 200 with already_applied returns the original receipt. A 409 rejects conflicting key input or a stale replay attempt. 422 identifies an integrity/configuration block. 503 is currently unavailable; a timeout is never evidence of rollback. Retry the same immutable command key/input or read its state. Changing a key may create another action.

Backfill starts require run_id equal to the idempotency key and ranges 1..16. Pause/resume sets the accepted durable admission flag; it does not suspend a process. Before required membership is sealed, the denominator is null. Historical complete/complete_with_errors phases remain unchanged even if a later ES replay repairs obligations.

An ES replay body contains the currently displayed attempt_id, destination_id, generation and bounded reason. Replay schedules only the exact ES obligation, retains every original failure and leaves the source/Rabbit/consumer untouched. Repeating its key does not reset later progress. A repeated mapping rejection creates another historical failure. Business-data correction is a new source command/version. Consumer quarantine is inspectable and always non-replayable here.

Source simulation accepts fixture-01 through fixture-16 and deterministic create/update/delete/restore. Create must precede update/corrupt/delete/restore for that named fixture. Corrupt-record produces source-valid but ES-invalid loyalty_points through the real source command function. It remains normal consumer data. Network PUT accepts only connected/disconnected for the two immutable configured proxy names. An unresolved network intent blocks a different key until the original request is reconciled/retried; no generic proxy, shell or container control is exposed.

## Interpret observations

Fresh idle source observation means pending 0 and oldest pending null. Failed/stale source observation means unknown, with no zero-filled pending metric. Status separates dependency reachability, data health and combined health. A fresh observation can correctly report degraded data. Source timestamps come from PostgreSQL, and ES search is explicitly near realtime; detail uses realtime receiver GET and never manufactures a current row after receiver failure.

Useful throughput is the difference/rate of successful pipeline_delivery_settled_total dispositions or pipeline_consumer_effects_total over an observation interval. Attempt totals include rejected/retried work and are not useful throughput. Durable totals survive API restart; historical_unknown admits outcomes pruned before instrumentation. Pending counts are gauges and may fall. Missing dependency samples must not be treated as zero. Direct scrape tests suffice for M6; Prometheus server packaging is deferred.

JSON logs contain static operation, request_id, outcome, timing and sanitized error_class, never request/remote bodies, tokens, DSNs or quarantined bytes. A process killed after COMMIT may not emit an HTTP completion line; immutable source/control/replay receipts supply the durable correlation evidence. API restart does not own/reset worker claims.

## Reproduce acceptance

```sh
make quality
npm run test:integration:m6
npm run test:integration:m6 -- --upgrade
make verify-m6
npm run review:m6 -- capture
npm run review:m6 -- summary <capture-directory>
npm run review:m6 -- bundle <capture-directory>
```

verify-m6 runs all 21 accepted prior profiles once plus fresh operations and populated upgrade. Review capture requires clean committed code and runs two complete gates sequentially in independent local checkouts with separate builds and fresh isolated resources; it also records the intentionally nonzero full make verify. Evidence, OpenAPI and metric samples are under ignored artifacts/m6. M6 prepares backend G5 evidence; final UI/scale/full G1-G5 acceptance remains outside scope.
