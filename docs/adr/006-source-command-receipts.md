# ADR 006: source command identity and successful result retention

Status: accepted for M2A implementation; verification pending.
Origin: planned source idempotency, explicitly authorized as a bounded M2 submilestone on 2026-09-16.

## Contract

The caller supplies `(expected source_epoch, command_id UUID)`. Contract version 1 supports one create/update/delete/restore operation per invocation. Create requires SQL NULL target; other operations require a positive signed BIGINT decimal string. Live payloads arrive as JSON text, cast by PostgreSQL to JSONB objects. Delete requires SQL NULL. No arbitrary numeric payload is parsed through JavaScript JSON.parse or Number.

Equality includes epoch, contract version, operation, target and JSONB payload, with SQL NULL compared explicitly. PostgreSQL JSONB equality ignores object-key order/whitespace and insignificant numeric representation; array order, missing versus JSON null and different numeric values remain significant. This is not JCS canonical pipeline serialization or hash-only equality.

A successful command retains the original entity ID, epoch, version, change ID, source-recorded time, deletion state and payload. Replies use decimal strings for BIGINTs, SQL JSONB text for payload and UTC `YYYY-MM-DDTHH:mm:ss.ffffffZ` for timestamps, independent of session timezone. A separate `replayed` boolean describes the attempt, not the immutable result. Replays read the receipt before entity lifecycle checks, including old successful restores and no-op commands.

Application SQLSTATE `P2001` means idempotency conflict: a successful key is already bound to different request content. `P2002` means expected source epoch mismatch. The existing SourceTransactionError retains the PostgreSQL cause/SQLSTATE and exposes those categories as `idempotency_conflict` and `source_epoch_mismatch`; outcome/cleanup classification remains ADR 005. Structural validation uses existing PostgreSQL error classes. Only successful outcomes are retained; validation errors and rollback leave no successful receipt and may be explicitly attempted later against then-current state. Receipts live for the supported epoch, without TTL or GC.

## Transaction and SQL boundary

Migration 002 adds `source.command_receipts`, keyed by epoch/command UUID, with normalized request fields and initially absent result fields. A completion flag and shape checks distinguish reservation from completed snapshot. A deferred constraint trigger looks up the final row at transaction completion and rejects any still-incomplete reservation. It must inspect current row state, not the INSERT event's old NEW image. A guard permits only incomplete-to-complete finalization with unchanged request; completed rows cannot be updated, deleted or truncated.

The sole command entry point is `source.execute_command(uuid,uuid,integer,text,bigint,jsonb)`, a VOLATILE SECURITY DEFINER function owned by source_owner, with fixed `search_path = pg_catalog, pg_temp`, fully qualified objects and no dynamic SQL. It validates epoch before work, then reserves via INSERT ON CONFLICT DO NOTHING before calling existing mutation functions. A winner finalizes its result in the same transaction as source/trigger outbox changes. On conflict, a separate SQL SELECT gets a fresh statement snapshot and compares retained content, returning replay or raising P2001. No same-statement CTE fallback, dummy UPDATE, mutate-before-deduplicate or check-then-insert is used.

Use explicit READ COMMITTED in the existing Source owner; the command function rejects other isolation levels. A contender blocks on the unique reservation until the actual winner transaction ends. After commit it reads the now-visible immutable row; after rollback its own insert can win. VOLATILE statement snapshots are essential here, and concurrent real sessions must prove the behavior. A failed statement or deferred completion check aborts all command effects together. Sequence gaps after rollback remain allowed.

Create a dedicated nonprivileged, NOINHERIT source_command role, with database CONNECT/schema USAGE and EXECUTE only on execute_command. It needs no direct table reads: callers supply the expected epoch, and the entry point returns the retained result. Revoke PUBLIC execution and all runtime table/sequence/function privileges explicitly in the same migration transaction. The old source_writer keeps only its original M1 table reads and two mutation functions; it receives no command-receipt access. No owner membership, raw writes, capture modification or role escalation is allowed. Owner bypass is outside scope.

## Application ownership and verification

Extend the existing expiring capability with command(request), plus Source.command(request) for a single owned invocation. Both use the same private session, active guards, operation-failure tracking, COMMIT tags, cleanup and poisoned-owner rules. No private pg.Client or transaction control is exposed. The legacy create/mutate methods remain outside idempotency. A caller may explicitly retry the identical key with a new healthy owner after ambiguity; the owner never retries or generates a replacement key. A new key denotes a different command.

The C01–C12 inventory covers results, equality/conflicts, historical/no-op replay, real lock contention and rollback takeover, both real SIGKILL windows with healthy controls, actual SQL failure and restricted grants/immutability/completion enforcement. Retain M1/M1.1, including their original real kills and ownership tests. Private child barriers pause the production operation; independent admin observations identify its session/transaction/locks and receipt/source/outbox state. No command credential needs broad diagnostic reads.

No pipeline ledger/acknowledgement, end-to-end exactly-once, retry scheduler, failure-response retention, batching or new transaction manager is added. The existing full-history inspection is only for tiny diagnostic fixtures, not an incremental reader.

References: [READ COMMITTED and ON CONFLICT visibility](https://www.postgresql.org/docs/18/transaction-iso.html), [VOLATILE statement snapshots](https://www.postgresql.org/docs/18/xfunc-volatility.html), [deferred constraint triggers](https://www.postgresql.org/docs/18/sql-createtrigger.html).
