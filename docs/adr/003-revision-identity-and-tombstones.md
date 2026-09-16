# ADR 003: Source revision identity and persistent tombstones

Status: accepted design; implementation verification pending.
Origin: pre-implementation architectural review.

## Context
Backfill, incremental changes, and retries can arrive in different orders. Entity identity alone does not distinguish legitimate revisions. Deletion must survive an arbitrarily late older event within the retained epoch.

## Decision
Identify a revision by (source_epoch, entity_id, entity_version). The source owns increasing per-entity versions and immutable revision metadata. Capture path cannot change event identity/content. Retain tombstone identity/version and forbid entity-ID reuse within an epoch; restoration creates a higher revision.

Use deterministic canonical content with JCS/SHA-256 when the pipeline envelope is introduced. Same identity with different content is an integrity failure. Elasticsearch uses strict external versions; the consumer accepts only newer projection state.

## Alternatives
Timestamps, global allocation order, random identity on retry, and physical deletion without retained version protection are rejected for this contract.

## Consequences and validation
Retained metadata consumes storage. M1 proves source revisions; M2 specifies and tests canonical bytes and safe number handling. Sink milestones must prove equal-version consistency, stale-write rejection, and no resurrection. Those tests have not run.

## Reference basis

https://www.rfc-editor.org/rfc/rfc8785
https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index
