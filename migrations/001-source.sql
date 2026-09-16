-- Applied once by scripts/migrate.ts inside its explicit transaction.
CREATE ROLE source_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE source_writer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
REVOKE ALL ON DATABASE source_m1 FROM PUBLIC;
GRANT CONNECT ON DATABASE source_m1 TO source_writer;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA source AUTHORIZATION source_owner;
SET LOCAL ROLE source_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA source REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

CREATE TABLE source.source_identity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  source_epoch uuid NOT NULL UNIQUE DEFAULT pg_catalog.gen_random_uuid()
);
INSERT INTO source.source_identity DEFAULT VALUES;

CREATE TABLE source.entities (
  entity_id bigint GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 9223372036854775807 NO CYCLE) PRIMARY KEY CHECK (entity_id > 0),
  source_epoch uuid NOT NULL REFERENCES source.source_identity(source_epoch),
  entity_version bigint NOT NULL CHECK (entity_version BETWEEN 1 AND 9223372036854775807),
  change_id uuid NOT NULL UNIQUE,
  recorded_at timestamptz NOT NULL CHECK (pg_catalog.isfinite(recorded_at)),
  is_deleted boolean NOT NULL DEFAULT false,
  payload jsonb,
  CONSTRAINT entities_payload_state CHECK (
    (is_deleted AND payload IS NULL) OR
    (NOT is_deleted AND payload IS NOT NULL AND pg_catalog.jsonb_typeof(payload) = 'object')
  )
);

CREATE TABLE source.outbox (
  allocation_id bigint GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 9223372036854775807 NO CYCLE) PRIMARY KEY CHECK (allocation_id > 0),
  source_epoch uuid NOT NULL REFERENCES source.source_identity(source_epoch),
  entity_id bigint NOT NULL REFERENCES source.entities(entity_id) CHECK (entity_id > 0),
  entity_version bigint NOT NULL CHECK (entity_version BETWEEN 1 AND 9223372036854775807),
  change_id uuid NOT NULL UNIQUE,
  recorded_at timestamptz NOT NULL CHECK (pg_catalog.isfinite(recorded_at)),
  is_deleted boolean NOT NULL,
  payload jsonb,
  CONSTRAINT outbox_revision_identity UNIQUE (source_epoch, entity_id, entity_version),
  CONSTRAINT outbox_payload_state CHECK (
    (is_deleted AND payload IS NULL) OR
    (NOT is_deleted AND payload IS NOT NULL AND pg_catalog.jsonb_typeof(payload) = 'object')
  )
);

CREATE FUNCTION source.next_version(previous bigint) RETURNS bigint
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF previous IS NULL OR previous < 1 OR previous >= 9223372036854775807 THEN
    RAISE EXCEPTION 'source version exhausted or invalid' USING ERRCODE = '22003';
  END IF;
  RETURN previous + 1;
END;
$$;

CREATE FUNCTION source.prepare_revision() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.source_epoch IS NOT NULL OR NEW.entity_version IS NOT NULL OR
       NEW.change_id IS NOT NULL OR NEW.recorded_at IS NOT NULL OR NEW.is_deleted THEN
      RAISE EXCEPTION 'source metadata is database-owned; insert must be live' USING ERRCODE = '42501';
    END IF;
    SELECT source_epoch INTO STRICT NEW.source_epoch FROM source.source_identity WHERE singleton;
    NEW.entity_version := 1;
  ELSIF TG_OP = 'UPDATE' THEN
    IF ROW(NEW.entity_id, NEW.source_epoch, NEW.entity_version, NEW.change_id, NEW.recorded_at)
       IS DISTINCT FROM ROW(OLD.entity_id, OLD.source_epoch, OLD.entity_version, OLD.change_id, OLD.recorded_at) THEN
      RAISE EXCEPTION 'source metadata is database-owned' USING ERRCODE = '42501';
    END IF;
    IF NEW.is_deleted = OLD.is_deleted AND NEW.payload IS NOT DISTINCT FROM OLD.payload THEN
      RETURN NULL;
    END IF;
    NEW.entity_version := source.next_version(OLD.entity_version);
  ELSE
    RAISE EXCEPTION 'unsupported source operation' USING ERRCODE = '0A000';
  END IF;
  NEW.change_id := pg_catalog.gen_random_uuid();
  NEW.recorded_at := pg_catalog.clock_timestamp();
  RETURN NEW;
END;
$$;

CREATE FUNCTION source.capture_revision() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  INSERT INTO source.outbox (source_epoch, entity_id, entity_version, change_id, recorded_at, is_deleted, payload)
  VALUES (NEW.source_epoch, NEW.entity_id, NEW.entity_version, NEW.change_id, NEW.recorded_at, NEW.is_deleted, NEW.payload);
  RETURN NEW;
END;
$$;

CREATE FUNCTION source.reject_removal_or_rewrite() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  RAISE EXCEPTION 'retained identity and revision evidence cannot be removed or rewritten' USING ERRCODE = '0A000';
END;
$$;

CREATE TRIGGER entities_prepare BEFORE INSERT OR UPDATE ON source.entities
FOR EACH ROW EXECUTE FUNCTION source.prepare_revision();
CREATE TRIGGER entities_capture AFTER INSERT OR UPDATE ON source.entities
FOR EACH ROW EXECUTE FUNCTION source.capture_revision();
CREATE TRIGGER entities_no_delete BEFORE DELETE ON source.entities
FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER entities_no_truncate BEFORE TRUNCATE ON source.entities
FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER outbox_no_rewrite BEFORE UPDATE OR DELETE ON source.outbox
FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER outbox_no_truncate BEFORE TRUNCATE ON source.outbox
FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();

CREATE FUNCTION source.create_entity(live_payload jsonb) RETURNS source.entities
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE result source.entities;
BEGIN
  INSERT INTO source.entities (payload) VALUES (live_payload) RETURNING * INTO result;
  RETURN result;
END;
$$;

CREATE FUNCTION source.mutate_entity(target_id bigint, operation text, live_payload jsonb) RETURNS source.entities
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE result source.entities;
BEGIN
  IF operation IS NULL OR operation NOT IN ('update', 'delete', 'restore') THEN
    RAISE EXCEPTION 'unsupported mutation operation' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO result FROM source.entities WHERE entity_id = target_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'entity does not exist' USING ERRCODE = 'P0002'; END IF;
  IF operation = 'update' THEN
    IF result.is_deleted THEN RAISE EXCEPTION 'deleted identity requires explicit restore' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET payload = live_payload WHERE entity_id = target_id;
  ELSIF operation = 'delete' THEN
    IF live_payload IS NOT NULL THEN RAISE EXCEPTION 'delete takes no live payload' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET is_deleted = true, payload = NULL WHERE entity_id = target_id;
  ELSE
    IF NOT result.is_deleted THEN RAISE EXCEPTION 'restore requires a deleted identity' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET is_deleted = false, payload = live_payload WHERE entity_id = target_id;
  END IF;
  -- A no-op BEFORE trigger suppresses UPDATE; return the unchanged locked row.
  SELECT * INTO STRICT result FROM source.entities WHERE entity_id = target_id;
  RETURN result;
END;
$$;

REVOKE ALL ON ALL TABLES IN SCHEMA source FROM PUBLIC, source_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source FROM PUBLIC, source_writer;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA source FROM PUBLIC, source_writer;
GRANT USAGE ON SCHEMA source TO source_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA source TO source_writer;
GRANT EXECUTE ON FUNCTION source.create_entity(jsonb), source.mutate_entity(bigint, text, jsonb) TO source_writer;
RESET ROLE;
