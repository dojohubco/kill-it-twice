# ADR 008: one owned local pipeline staging transaction

Status: accepted; verified locally at f8d7728 in [M2B evidence](../evidence/M2B.md). This decision establishes local staging only, without source acknowledgement or sink success.

## Separate ownership and minimal tables

A second PostgreSQL 18.6 service owns pipeline_m2b, its own administrator/runtime credentials and retained volume. It is not another schema in the source transaction. No FDW/dblink, distributed transaction or consumer database. Pipeline migration 001 is separate from forward source migration 003; source migrations 001/002 remain unchanged.

Proposed pipeline schema, owned by a NOLOGIN/non-superuser pipeline_owner:

- source_binding: singleton boolean PK constrained true, source_epoch uuid NOT NULL UNIQUE, payload_encoding text NOT NULL constrained pg18-jsonb-text/v1. Migration binds the supplied source epoch; runtime cannot rebind it.
- destinations: destination_id uuid PK (stable local identity), kind text NOT NULL UNIQUE restricted to elasticsearch/rabbitmq, generation bigint NOT NULL constrained 1, state text NOT NULL constrained unbound, receiver_identity text constrained SQL NULL; UNIQUE(destination_id,kind). Exactly two migration-created logical destinations, with no claimed receiver IDs/topology.
- events: event_id text PK; source_epoch uuid NOT NULL FK source_binding; entity_id/entity_version positive bigint NOT NULL with UNIQUE(epoch,entity,version); source_change_id uuid nullable only for baseline; source_recorded_at timestamptz NOT NULL finite; kind text and is_deleted boolean NOT NULL; body_bytes bytea NOT NULL; content_sha256 text NOT NULL; staged_at timestamptz NOT NULL default clock_timestamp(). Deterministic ID, codec/body shape, exact canonical bytes, native sha256(bytea), indexed metadata and payload JSONB-object/text-round-trip constraints must all agree.
- delivery_intents: event_id FK events, kind restricted to the two required kinds, destination_id with composite FK to destinations, state NOT NULL constrained pending, created_at timestamptz; PK(event_id,kind), UNIQUE(event_id,destination_id).
- consumer_observations: event_id PK/FK events, state NOT NULL constrained pending, created_at timestamptz. This is an obligation, not a consumer receipt.
- integrity_incidents: generated UUID PK, bounded event_id text, allowlisted integrity code, optional validated observed SHA-256 and recorded_at timestamptz. No raw payload, credentials or general incident-management state.

Immutable guards forbid UPDATE/DELETE/TRUNCATE of these retained tables. Runtime has CONNECT/USAGE and EXECUTE only on stage_event and record_incident, no raw table DML, receiver rebinding, premature success or owner membership. All functions have schema-qualified references, fixed pg_catalog, pg_temp search_path, explicit PUBLIC EXECUTE revocation and safe non-superuser SECURITY DEFINER ownership where required, in the migration transaction. Constraints independently reject malformed stored evidence. Deferred constraint triggers on events/intents/consumer rows verify the complete two-sink/one-observation set at transaction completion; a partially initialized event cannot commit.

## Transactions and conflicts

Extract the accepted ADR 005 lifecycle into one small internal owner used by Source and Pipeline. Preserve active/expired operation guards, primary SQLSTATE versus cleanup failures, BEGIN/COMMIT/ROLLBACK tags, conservative unknown COMMIT, poisoned ownership and no automatic retry. Public work capabilities expose allowlisted operations only; no raw client or control SQL. Existing lifecycle tests plus pipeline-specific real fault/ownership tests protect the extraction.

Each work capability accepts one stage batch only, preventing repeated calls from bypassing the transaction bound. Before effects, validate count/bytes/envelopes and deduplicate the batch by deterministic identity; differing content for one identity fails. Sort unique events by ordinal event_id to avoid avoidable inverse lock order. BEGIN ISOLATION LEVEL READ COMMITTED; for each event call the restricted stage_event entry point. Its INSERT ON CONFLICT DO NOTHING arbitrates identity before obligations. A fresh separate VOLATILE statement reads a conflicting winner after its actual commit/rollback, comparing BOTH exact body bytes and hash. No stale CTE fallback or dummy UPDATE. Inserted events get exactly two pending intents and one pending observation. Existing events must already have their required obligations; do not repair corrupt evidence as an ordinary replay. COMMIT must be confirmed before returning inserted/already_staged results.

P3001 means conflicting retained identity/content; P3002 means invalid/missing staging invariant. Roll back the complete attempted batch on either, preserving all original data. Attempt a sanitized incident in a separate owned transaction after rollback. If diagnostics fail, preserve the primary integrity failure and a separate diagnostic error; never report staging success. Unknown COMMIT remains unknown; an explicit identical repeat in a new healthy owner resolves from durable evidence.

## Verification and exclusions

Required inventory S01–S13 is committed before execution, including independent SQL/crypto oracle, real observed unique-key contention, mixed-batch rollback, privileged constraint checks, actual runtime permissions, pre/post-COMMIT SIGKILL and healthy controls, visibility, count/byte/record bounds, pages, both retained-volume restarts and scoped cleanup. Fresh installation and source 003 applied after a small committed M2A command/history fixture must preserve every original row. Prior M1/M1.1/M2A profiles remain separately required.

All destinations stay unbound and all obligations pending. No source ACK, continuous capture, live watermark, seeding, backfill, retries/leases/DLQ/replay, receiver worker or consumer implementation is authorized. No full Optio gate is established. A digest proves byte consistency, not authenticity. Trusted database owners can bypass constraints and remain outside the runtime guarantee.

Reference: [VOLATILE statement snapshots](https://www.postgresql.org/docs/18/xfunc-volatility.html).

Implementation naming note: the retained destination and delivery columns use kind and state for the proposed sink-kind and binding-state fields. Their types and constraints are unchanged. The work facade returns one result per unique identity, sorted by event_id. SQLSTATE 23514/23502/22xxx can also report invalid input rejected by PostgreSQL checks/parsing; P3001/P3002 classify retained-content conflicts and integrity failures. IntegrityError retains the transaction outcome, original PostgreSQL error, and separate incident-write failure.
