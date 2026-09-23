-- Exact operational counts should not scan canonical payload/receipt heap pages.
-- No event, delivery state, historical receipt, checkpoint or terminal proof changes.
SET LOCAL ROLE pipeline_owner;
CREATE INDEX event_staging_observation ON pipeline.events(staged_at,event_id);
CREATE INDEX delivery_state_observation ON pipeline.delivery_intents(kind,state,disposition,created_at);
CREATE INDEX consumer_state_observation ON pipeline.consumer_observations(state);
RESET ROLE;
