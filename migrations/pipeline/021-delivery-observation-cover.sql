-- Per-run observation joins need event identity and state for each sink.
-- Keep the exact query, missing-obligation semantics and terminal validator.
-- A covering index avoids reading wide delivery evidence rows for these counts.
SET LOCAL ROLE pipeline_owner;
CREATE INDEX delivery_member_observation ON pipeline.delivery_intents(kind,event_id) INCLUDE(state);
RESET ROLE;
