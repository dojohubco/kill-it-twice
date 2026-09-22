-- Numeric export navigation; the original epoch/version identity remains authoritative.
-- Controlled initializer/maintenance transaction, not a concurrent online index build.
SET LOCAL ROLE source_owner;
CREATE INDEX baseline_entity_navigation ON source.baseline_revisions(entity_id);
RESET ROLE;
