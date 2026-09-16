-- Additive to populated M2A; no mutation of source data, receipts or original migrations.
CREATE ROLE source_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
ALTER ROLE source_reader SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE source_m1 TO source_reader;
GRANT USAGE ON SCHEMA source TO source_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA source FROM source_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source FROM source_reader;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA source FROM source_reader;
GRANT SELECT (singleton, source_epoch) ON source.source_identity TO source_reader;
GRANT SELECT (source_epoch, entity_id, entity_version, change_id, recorded_at, is_deleted, payload)
  ON source.entities, source.outbox TO source_reader;
