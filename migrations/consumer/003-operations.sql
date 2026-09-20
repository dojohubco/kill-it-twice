CREATE ROLE consumer_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE consumer_m4 TO consumer_operator;
SET LOCAL ROLE consumer_owner;
GRANT USAGE ON SCHEMA consumer TO consumer_operator;
GRANT SELECT(event_id,processed_at) ON consumer.processed_events TO consumer_operator;
GRANT SELECT ON consumer.mutation_effects TO consumer_operator;
GRANT SELECT ON consumer.identity TO consumer_operator;
-- Raw wire bytes and transport metadata are deliberately not readable by this role.
GRANT SELECT(quarantine_id,claimed_id,classification,context,recorded_at) ON consumer.quarantine TO consumer_operator;
RESET ROLE;
