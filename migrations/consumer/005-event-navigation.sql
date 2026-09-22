-- Preserve byte-order export pagination independently of the database's locale.
SET LOCAL ROLE consumer_owner;
CREATE INDEX processed_events_ordinal_navigation ON consumer.processed_events(event_id COLLATE "C");
RESET ROLE;
