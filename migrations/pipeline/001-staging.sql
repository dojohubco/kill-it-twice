-- Run as database administrator in one transaction, including binding/credential setup.
CREATE ROLE pipeline_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE pipeline_stager NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
REVOKE ALL ON DATABASE pipeline_m2b FROM PUBLIC;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_stager;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA pipeline AUTHORIZATION pipeline_owner;
SET LOCAL ROLE pipeline_owner;

CREATE TABLE pipeline.source_binding (
  singleton boolean PRIMARY KEY CHECK (singleton),
  source_epoch uuid NOT NULL UNIQUE,
  payload_encoding text NOT NULL CHECK (payload_encoding = 'pg18-jsonb-text/v1')
);
CREATE TABLE pipeline.destinations (
  destination_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kind text NOT NULL UNIQUE CHECK (kind IN ('elasticsearch','rabbitmq')),
  generation bigint NOT NULL DEFAULT 1 CHECK (generation = 1),
  state text NOT NULL DEFAULT 'unbound' CHECK (state = 'unbound'),
  receiver_identity text CHECK (receiver_identity IS NULL),
  UNIQUE (destination_id,kind)
);
INSERT INTO pipeline.destinations(kind) VALUES ('elasticsearch'),('rabbitmq');
CREATE TABLE pipeline.events (
  event_id text PRIMARY KEY,
  source_epoch uuid NOT NULL REFERENCES pipeline.source_binding(source_epoch),
  entity_id bigint NOT NULL CHECK (entity_id > 0),
  entity_version bigint NOT NULL CHECK (entity_version > 0),
  source_change_id uuid,
  source_recorded_at timestamptz NOT NULL CHECK (isfinite(source_recorded_at)),
  kind text NOT NULL CHECK (kind IN ('mutation','baseline')),
  is_deleted boolean NOT NULL,
  body_bytes bytea NOT NULL CHECK (octet_length(body_bytes) <= 65536),
  content_sha256 text NOT NULL CHECK (content_sha256 = encode(sha256(body_bytes),'hex')),
  staged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (source_epoch,entity_id,entity_version),
  CHECK (event_id = source_epoch::text || ':' || entity_id::text || ':' || entity_version::text),
  CHECK ((kind = 'mutation' AND source_change_id IS NOT NULL) OR (kind = 'baseline' AND source_change_id IS NULL))
);
CREATE TABLE pipeline.delivery_intents (
  event_id text NOT NULL REFERENCES pipeline.events(event_id),
  kind text NOT NULL CHECK (kind IN ('elasticsearch','rabbitmq')),
  destination_id uuid NOT NULL,
  state text NOT NULL DEFAULT 'pending' CHECK (state = 'pending'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (event_id,kind),
  UNIQUE (event_id,destination_id),
  FOREIGN KEY (destination_id,kind) REFERENCES pipeline.destinations(destination_id,kind)
);
CREATE TABLE pipeline.consumer_observations (
  event_id text PRIMARY KEY REFERENCES pipeline.events(event_id),
  state text NOT NULL DEFAULT 'pending' CHECK (state = 'pending'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline.integrity_incidents (
  incident_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id text CHECK (event_id IS NULL OR octet_length(event_id) <= 100),
  code text NOT NULL CHECK (code IN ('P3001','P3002')),
  observed_sha256 text CHECK (observed_sha256 IS NULL OR observed_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- This validates only the fixed flat envelope. It is not a general JCS implementation.
-- PostgreSQL parses the opaque payload losslessly to check the selected PG18 text codec.
CREATE FUNCTION pipeline.valid_body(e pipeline.events) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog,pg_temp AS $$
DECLARE b jsonb; canonical text; k text;
BEGIN
  b := convert_from(e.body_bytes,'UTF8')::jsonb;
  IF jsonb_typeof(b) IS DISTINCT FROM 'object' THEN RETURN false; END IF;
  IF (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(b) key)
      IS DISTINCT FROM ARRAY['entity_id','entity_version','event_id','is_deleted','kind','payload_encoding','payload_json','schema_version','source_change_id','source_epoch','source_recorded_at']::text[] THEN RETURN false; END IF;
  FOREACH k IN ARRAY ARRAY['entity_id','entity_version','event_id','kind','payload_encoding','source_epoch','source_recorded_at'] LOOP
    IF jsonb_typeof(b->k) IS DISTINCT FROM 'string' THEN RETURN false; END IF;
  END LOOP;
  IF (b->'schema_version')::text IS DISTINCT FROM '1'
    OR jsonb_typeof(b->'is_deleted') IS DISTINCT FROM 'boolean'
    OR b->>'source_epoch' IS DISTINCT FROM e.source_epoch::text
    OR b->>'entity_id' IS DISTINCT FROM e.entity_id::text
    OR b->>'entity_version' IS DISTINCT FROM e.entity_version::text
    OR b->>'event_id' IS DISTINCT FROM e.event_id
    OR b->>'kind' IS DISTINCT FROM e.kind
    OR (b->>'is_deleted')::boolean IS DISTINCT FROM e.is_deleted
    OR b->>'payload_encoding' IS DISTINCT FROM 'pg18-jsonb-text/v1'
    OR b->>'source_recorded_at' IS DISTINCT FROM to_char(e.source_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    OR (b->>'source_recorded_at' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$') IS NOT TRUE
    THEN RETURN false; END IF;
  IF e.kind = 'mutation' THEN
    IF jsonb_typeof(b->'source_change_id') IS DISTINCT FROM 'string'
      OR b->>'source_change_id' IS DISTINCT FROM e.source_change_id::text THEN RETURN false; END IF;
  ELSIF b->'source_change_id' IS DISTINCT FROM 'null'::jsonb THEN RETURN false;
  END IF;
  IF e.is_deleted THEN
    IF b->'payload_json' IS DISTINCT FROM 'null'::jsonb THEN RETURN false; END IF;
  ELSE
    IF jsonb_typeof(b->'payload_json') IS DISTINCT FROM 'string'
      OR jsonb_typeof((b->>'payload_json')::jsonb) IS DISTINCT FROM 'object'
      OR ((b->>'payload_json')::jsonb)::text IS DISTINCT FROM b->>'payload_json' THEN RETURN false; END IF;
  END IF;
  SELECT '{' || string_agg(to_json(key)::text || ':' || value::text, ',' ORDER BY key COLLATE "C") || '}'
    INTO canonical FROM jsonb_each(b);
  RETURN convert_to(canonical,'UTF8') = e.body_bytes;
EXCEPTION WHEN OTHERS THEN RETURN false;
END $$;
ALTER TABLE pipeline.events ADD CONSTRAINT event_body_consistency CHECK (pipeline.valid_body(events) IS TRUE);

CREATE FUNCTION pipeline.immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog,pg_temp AS $$
BEGIN RAISE EXCEPTION 'M2B retained evidence is immutable' USING ERRCODE = 'P3002'; END $$;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['source_binding','destinations','events','delivery_intents','consumer_observations','integrity_incidents'] LOOP
    EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.%I FOR EACH ROW EXECUTE FUNCTION pipeline.immutable()', t);
    EXECUTE format('CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.%I FOR EACH STATEMENT EXECUTE FUNCTION pipeline.immutable()', t);
  END LOOP;
END $$;
CREATE FUNCTION pipeline.assert_obligations(id text) RETURNS void
LANGUAGE plpgsql STABLE SET search_path = pg_catalog,pg_temp AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pipeline.events WHERE event_id=id)
    OR (SELECT count(*) FROM pipeline.delivery_intents d JOIN pipeline.destinations t USING (destination_id,kind)
      WHERE d.event_id=id AND d.state='pending' AND t.state='unbound' AND t.receiver_identity IS NULL) <> 2
    OR NOT EXISTS (SELECT 1 FROM pipeline.delivery_intents WHERE event_id=id AND kind='elasticsearch')
    OR NOT EXISTS (SELECT 1 FROM pipeline.delivery_intents WHERE event_id=id AND kind='rabbitmq')
    OR (SELECT count(*) FROM pipeline.consumer_observations WHERE event_id=id AND state='pending') <> 1
    THEN RAISE EXCEPTION 'Incomplete event obligations' USING ERRCODE='P3002'; END IF;
END $$;
CREATE FUNCTION pipeline.check_obligations() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog,pg_temp AS $$
BEGIN PERFORM pipeline.assert_obligations(NEW.event_id); RETURN NULL; END $$;
CREATE CONSTRAINT TRIGGER complete_event AFTER INSERT ON pipeline.events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_obligations();
CREATE CONSTRAINT TRIGGER complete_deliveries AFTER INSERT ON pipeline.delivery_intents DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_obligations();
CREATE CONSTRAINT TRIGGER complete_observation AFTER INSERT ON pipeline.consumer_observations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_obligations();

CREATE FUNCTION pipeline.stage_event(bytes bytea, digest text) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog,pg_temp AS $$
DECLARE b jsonb; id text; inserted integer; prior pipeline.events;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'Staging requires READ COMMITTED' USING ERRCODE='P3002'; END IF;
  IF bytes IS NULL OR digest IS NULL OR octet_length(bytes)>65536 THEN
    RAISE EXCEPTION 'Invalid event input' USING ERRCODE='P3002'; END IF;
  b := convert_from(bytes,'UTF8')::jsonb;
  id := b->>'event_id';
  IF NOT EXISTS (SELECT 1 FROM pipeline.source_binding WHERE source_epoch::text=b->>'source_epoch' AND payload_encoding=b->>'payload_encoding') THEN
    RAISE EXCEPTION 'Unexpected source binding' USING ERRCODE='P3002'; END IF;
  INSERT INTO pipeline.events(event_id,source_epoch,entity_id,entity_version,source_change_id,source_recorded_at,kind,is_deleted,body_bytes,content_sha256)
    VALUES (id,(b->>'source_epoch')::uuid,(b->>'entity_id')::bigint,(b->>'entity_version')::bigint,(b->>'source_change_id')::uuid,(b->>'source_recorded_at')::timestamptz,b->>'kind',(b->>'is_deleted')::boolean,bytes,digest)
    ON CONFLICT (event_id) DO NOTHING;
  GET DIAGNOSTICS inserted = ROW_COUNT;
  IF inserted=0 THEN
    -- Fresh statement/snapshot AFTER unique-key arbitration. Never an immutable dummy UPDATE.
    SELECT * INTO STRICT prior FROM pipeline.events WHERE event_id=id;
    IF prior.body_bytes IS DISTINCT FROM bytes OR prior.content_sha256 IS DISTINCT FROM digest THEN
      RAISE EXCEPTION 'Conflicting canonical event' USING ERRCODE='P3001'; END IF;
    PERFORM pipeline.assert_obligations(id);
    RETURN 'already_staged';
  END IF;
  INSERT INTO pipeline.delivery_intents(event_id,kind,destination_id) SELECT id,kind,destination_id FROM pipeline.destinations;
  INSERT INTO pipeline.consumer_observations(event_id) VALUES (id);
  PERFORM pipeline.assert_obligations(id);
  RETURN 'inserted';
END $$;
CREATE FUNCTION pipeline.record_incident(id text, incident_code text, digest text) RETURNS void
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog,pg_temp AS $$
  INSERT INTO pipeline.integrity_incidents(event_id,code,observed_sha256) VALUES (id,incident_code,digest);
$$;
REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM PUBLIC,pipeline_stager;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pipeline FROM PUBLIC,pipeline_stager;
GRANT USAGE ON SCHEMA pipeline TO pipeline_stager;
GRANT EXECUTE ON FUNCTION pipeline.stage_event(bytea,text),pipeline.record_incident(text,text,text) TO pipeline_stager;
RESET ROLE;
