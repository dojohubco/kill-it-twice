# ADR 016: bounded operator polling settings

Date: 2026-09-25. Status: accepted for the user-authorized completion of UI parameter editing and public GitHub publication; implemented with developmental real-service evidence; fresh clean full verification pending.

## Problem and decision

The Configuration page only inspected immutable identities and fixed API limits. Backfill already accepts a per-run scan-range count, but operators also need editable runtime parameters. Expose incremental capture polling and idle-backfill discovery intervals, both integer milliseconds in 50–30000 with existing 1000-ms defaults. They control cadence only: memory/page/byte limits, concurrency, leases, remote deadlines, identity, versioning, checkpoint and ACK semantics remain unchanged.

An additive pipeline migration stores one revisioned settings row and immutable command receipts. The operator API offers GET/PUT `/api/v1/config/polling`. PUT requires the existing operator token, UUID Idempotency-Key, expected revision and exact bounded fields. A stale revision or changed input under an existing key is a conflict. Same-key retries return the original committed receipt without applying it again, including after later changes. No network I/O occurs inside the settings transaction. Restricted worker roles can only read these two values; they cannot write settings or read operator credentials.

Root runtime workers read settings between completed units of work, caching for at most five seconds between reads. A delay already underway is not interrupted; updated settings affect a later iteration. Failed reads do not silently invent defaults or claim application. Structured worker logs identify the settings revision and chosen delay without credentials. Defaults preserve prior scheduling. Historical standalone profiles retain their explicitly configured cadence; retained runtime enables this capability only after its forward migration.

The UI uses existing Angular components, numeric inputs, visible labels/bounds, a current revision and confirmation with the actual new values. Read-only users can inspect; operator authorization is enforced by the server. Drafts survive refresh, stale edits receive a conflict with reload instructions, and unknown outcomes retry the same immutable request. Saved means durably stored, not synchronously applied by every worker. Identity/credential controls remain unavailable by design.

## Alternatives and tradeoffs

Environment-only configuration was simpler but did not meet UI editing. Browser-only preferences do not change pipeline behavior. Arbitrary batch/concurrency/lease editing would broaden correctness and capacity guarantees without a requirement. Two cadence settings are useful, narrow and bounded; lower delays can increase database load, while higher delays increase idle latency. Durable compare-and-set avoids silent concurrent overwrite; it requires an explicit reload after a conflict.

## Verification and publication

Before publication: validate auth, input bounds and unknown fields, same-key replay/conflict, revision conflict, immutable receipts and read-only role permissions against real PostgreSQL. Exercise actual browser save/reload/validation/keyboard/narrow layout, retained restart and actual worker revision observation. Preserve default-million historical PASS at ecdfdb5; new source must have its own clean runtime/functional and full acceptance evidence. No shared-production server workload, provider changes, subagents or application submission. The user explicitly authorized publishing the completed code to the existing public repository; no force push or history rewrite.

UI review uses the existing nine vendored skills. Reuse layout, typography, color, focus and dialog tokens; introduce no decorative animation or dependency. Rendered keyboard, contrast and 320px/200%-text checks belong to the actual verification report. Source inspection alone does not prove them.
