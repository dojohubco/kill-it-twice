# ADR 001: Trigger-backed source capture

Status: accepted design; implementation verification pending.
Origin: pre-implementation architectural review.

## Context
Supported source mutations must not commit without recoverable change evidence. The exercise permits a controlled source schema. Live allocation IDs and timestamps are not accepted as completeness watermarks.

## Decision
Store a source-owned version and an immutable outbox after-image in the same transaction as every meaningful supported row mutation. Use triggers and restricted runtime privileges. Incremental capture will select unacknowledged committed work and acknowledge only after durable pipeline staging. Baseline seeding will have a controlled activation boundary.

## Alternatives
Timestamp polling and a moving primary-key/outbox cursor do not meet the selected concurrency/deletion contract. Application-only outbox writes leave supported direct SQL writers dependent on application discipline. WAL CDC remains an alternative for a source that cannot be modified, not an additional adapter to build now.

## Consequences and validation
Accept source write/storage overhead and the schema-permission assumption. M1 must verify atomic rollback, retained after-images, protected metadata, concurrency, late commits, and writer SIGKILL. M2 must verify staging before acknowledgement. No source-capture completeness result is claimed yet.

## Reference basis

https://www.postgresql.org/docs/current/trigger-definition.html
https://www.postgresql.org/docs/current/transaction-iso.html
https://www.postgresql.org/docs/current/sql-createfunction.html
