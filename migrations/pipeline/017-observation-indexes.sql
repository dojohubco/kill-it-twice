-- Exact observational reads must not repeatedly scan retained receipt bodies.
-- These indexes change no event, delivery state, receipt or terminal proof.
SET LOCAL ROLE pipeline_owner;
CREATE INDEX consumer_observation_cover ON pipeline.consumer_observations(event_id) INCLUDE(state);
CREATE INDEX es_unresolved_observation ON pipeline.delivery_intents(event_id) WHERE kind='elasticsearch' AND state<>'satisfied';
RESET ROLE;
