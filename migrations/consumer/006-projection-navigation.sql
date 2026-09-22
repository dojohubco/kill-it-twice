-- Exact keyset export order without changing data, constraints or permissions.
-- Ordinary indexes are installed during controlled initialization/maintenance.
SET LOCAL ROLE consumer_owner;
CREATE INDEX entity_totals_navigation ON consumer.entity_totals(entity_id);
CREATE INDEX entity_projection_navigation ON consumer.entity_projection(entity_id);
RESET ROLE;
