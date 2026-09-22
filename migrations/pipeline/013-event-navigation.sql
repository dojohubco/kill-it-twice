-- Match the fixed byte-order export cursor without changing event identity or payload.
SET LOCAL ROLE pipeline_owner;
CREATE INDEX events_ordinal_navigation ON pipeline.events(event_id COLLATE "C");
RESET ROLE;
