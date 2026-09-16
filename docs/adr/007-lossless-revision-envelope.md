# ADR 007: lossless v1 revision envelope

Status: accepted for M2B implementation; verification pending. Origin: authorized pre-implementation refinement using the existing M2A precision fixtures, not discovery of an unknown requirement.

## Identity and bytes

The body has exactly these fields: schema_version (number 1); source_epoch (lowercase UUID string); entity_id and entity_version (canonical positive signed-BIGINT decimal strings); event_id (epoch:entity:version); source_change_id (canonical UUID for mutation, null for baseline); source_recorded_at (UTC YYYY-MM-DDTHH:mm:ss.ffffffZ); kind (mutation or baseline); is_deleted (boolean); payload_encoding (pg18-jsonb-text/v1); payload_json (PostgreSQL-produced JSONB object text, or null for tombstone). No extra keys. All current real revisions are mutations. Baseline shape has synthetic unit fixtures only; a current-row/backfill read never changes kind.

content_sha256 is lowercase SHA-256 of UTF-8 RFC 8785 JCS(body). Wire is UTF-8 JCS({body,content_sha256}). Store body bytes and hash as authoritative evidence. Allocation/command/run/delivery IDs, observation/staging times and capture paths are excluded. Every body field is hashed; the hash does not hash itself. Hash equality is not source authenticity.

JCS covers the envelope. payload_json is an opaque string, never arbitrary-precision JSON parsed into JavaScript Number. Preserve the complete PostgreSQL 18 payload::text representation, including high integers and long decimals. No-op requests cannot replace stored revision text with their differently formatted input. Current and immutable outbox exports share an explicit UTC microsecond SQL timestamp expression and UTF-8 client/server encoding. Pipeline binding fixes epoch and codec; changing codec rules requires explicit compatibility work and never rehashes retained events. Searchable-field extraction and Elasticsearch numeric mappings remain later contracts.

## Scoped canonicalization and validation

Implement only this fixed, flat scalar body and its two-key wire wrapper, using ECMAScript JSON.stringify for primitive string escaping, ordinal UTF-16 sorting of the fixed ASCII field names, native SHA-256 and fatal UTF-8 decoding. The only numeric value is literal 1. Reject unknown fields, lone surrogates, invalid UTF-8, noncanonical UUID/BIGINT/timestamps, inconsistent kind/change/deletion fields, wrong hashes and supplied bytes that differ from canonical reserialization. This is not a general JCS or arbitrary-precision JSON library; no new dependency is needed. Independent checked-in literal golden bytes and Python hashlib-derived fixed digests test live/tombstone/synthetic baseline vectors, Unicode and escaping without calling the production serializer to derive expected values.

Real PostgreSQL validates payload text as a JSONB object and requires payload::jsonb::text to equal the supplied codec text. Equivalent-looking arbitrary caller text is not automatically a source export. SQL also validates body field types/keys, indexed metadata correspondence, canonical flat-body representation and sha256(bytea). NOT NULL and IS TRUE guards prevent NULL CHECK escapes. Application and SQL validation are tested independently. Trusted application/credential control connects this envelope to actual source reads; there is no cross-database FK or authenticity proof.

## Bounded reader

Source migration 003 adds source_reader: CONNECT, schema USAGE and SELECT only on persisted identity and required entity/outbox revision columns. No receipts, mutation EXECUTE, writes, sequences or owner membership. The adapter selects explicit (entity ID, version) pairs, plus a bounded single-statement current-revision read. It does not call SourceWork.inspect, poll, advance a watermark or ACK anything.

Starter limits: 16 selected records, 64 KiB per canonical wire record, 256 KiB per batch. SQL materializes a coherent selected snapshot, measures each JSON-string-escaped payload plus a conservative 1024-byte envelope allowance and the sum, and conditionally suppresses all payload projections on overflow. Thus over-limit payloads are rejected before transfer into Node. Bounds also apply to staging inputs before parsing supplied bytes, and to actual serialized wire length. Oversized records stay durable and explicitly unstaged. Limits are configuration, not benchmark findings. Missing/uncommitted explicitly selected revisions are reported not_visible, never silently staged.

## Known future integration obligation

Migration 002's command-result FK references source.outbox(epoch, entity_id, version). Current M2A fixtures are all captured mutations; they do not demonstrate baseline compatibility. The seeding/activation milestone needs an explicit forward migration and regression: seed a protected baseline without mutation outbox, activate, issue a same-payload update, retain a successful no-op command receipt without inventing a mutation. M2B neither rewrites 002 nor weakens receipt integrity or emits every seed into the incremental stream.

References: [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785), [PostgreSQL binary hashes](https://www.postgresql.org/docs/18/functions-binarystring.html).
