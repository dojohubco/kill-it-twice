-- Dedicated consumer_m4 database on the state PostgreSQL service; one transaction.
CREATE ROLE consumer_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE consumer_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
CREATE ROLE consumer_receipt_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
REVOKE ALL ON DATABASE consumer_m4 FROM PUBLIC;
GRANT CONNECT ON DATABASE consumer_m4 TO consumer_runtime,consumer_receipt_reader;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA consumer AUTHORIZATION consumer_owner;
SET LOCAL ROLE consumer_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA consumer REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
CREATE TABLE consumer.identity (
 singleton boolean PRIMARY KEY CHECK(singleton), consumer_id uuid NOT NULL UNIQUE, source_epoch uuid NOT NULL,
 pipeline_id uuid NOT NULL, registration_id uuid NOT NULL, payload_encoding text NOT NULL CHECK(payload_encoding='pg18-jsonb-text/v1')
);
CREATE TABLE consumer.processed_events (
 event_id text PRIMARY KEY, source_epoch uuid NOT NULL, entity_id bigint NOT NULL CHECK(entity_id>0), entity_version bigint NOT NULL CHECK(entity_version>0),
 source_change_id uuid, source_recorded_at timestamptz NOT NULL CHECK(isfinite(source_recorded_at)), kind text NOT NULL CHECK(kind IN ('mutation','baseline')), is_deleted boolean NOT NULL,
 body_bytes bytea NOT NULL CHECK(octet_length(body_bytes)<=65536), content_sha256 text NOT NULL CHECK(content_sha256=encode(sha256(body_bytes),'hex')),
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(source_epoch,entity_id,entity_version), UNIQUE(source_epoch,source_change_id), UNIQUE(source_epoch,entity_id,entity_version,event_id),
 CHECK(event_id=source_epoch::text||':'||entity_id::text||':'||entity_version::text), CHECK((kind='mutation' AND source_change_id IS NOT NULL) OR (kind='baseline' AND source_change_id IS NULL))
);
CREATE TABLE consumer.entity_projection (
 source_epoch uuid NOT NULL, entity_id bigint NOT NULL, entity_version bigint NOT NULL, event_id text NOT NULL UNIQUE,
 PRIMARY KEY(source_epoch,entity_id), FOREIGN KEY(source_epoch,entity_id,entity_version,event_id) REFERENCES consumer.processed_events(source_epoch,entity_id,entity_version,event_id)
);
CREATE TABLE consumer.mutation_effects (
 source_epoch uuid NOT NULL, source_change_id uuid NOT NULL, event_id text NOT NULL UNIQUE REFERENCES consumer.processed_events(event_id),
 PRIMARY KEY(source_epoch,source_change_id), FOREIGN KEY(source_epoch,source_change_id) REFERENCES consumer.processed_events(source_epoch,source_change_id)
);
CREATE TABLE consumer.entity_totals (
 source_epoch uuid NOT NULL, entity_id bigint NOT NULL CHECK(entity_id>0), units bigint NOT NULL CHECK(units>=0), PRIMARY KEY(source_epoch,entity_id),
 FOREIGN KEY(source_epoch,entity_id) REFERENCES consumer.entity_projection(source_epoch,entity_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE consumer.quarantine (
 quarantine_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), raw_bytes bytea NOT NULL CHECK(octet_length(raw_bytes)<=131072),
 metadata bytea NOT NULL CHECK(octet_length(metadata)<=4096), raw_hash text NOT NULL CHECK(raw_hash=encode(sha256(raw_bytes),'hex')),
 metadata_hash text NOT NULL CHECK(metadata_hash=encode(sha256(metadata),'hex')), claimed_id text CHECK(claimed_id IS NULL OR octet_length(claimed_id)<=100),
 classification text NOT NULL CHECK(classification IN ('validation','conflicting_content')), context text NOT NULL CHECK(octet_length(context)<=1024),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(raw_hash,metadata_hash)
);
CREATE INDEX quarantine_claim ON consumer.quarantine(claimed_id,raw_hash);
CREATE FUNCTION consumer.immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN RAISE EXCEPTION 'Immutable consumer evidence' USING ERRCODE='P6002'; END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['identity','processed_events','mutation_effects','quarantine'] LOOP
 EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON consumer.%I FOR EACH ROW EXECUTE FUNCTION consumer.immutable()',t);
 END LOOP;
 FOREACH t IN ARRAY ARRAY['identity','processed_events','mutation_effects','quarantine','entity_projection','entity_totals'] LOOP
 EXECUTE format('CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON consumer.%I EXECUTE FUNCTION consumer.immutable()',t);
 END LOOP;
END $$;
CREATE FUNCTION consumer.guard_projection() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR NEW.source_epoch<>OLD.source_epoch OR NEW.entity_id<>OLD.entity_id OR NEW.entity_version<=OLD.entity_version THEN RAISE EXCEPTION 'Consumer projection cannot regress' USING ERRCODE='P6002'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER monotonic_projection BEFORE UPDATE OR DELETE ON consumer.entity_projection FOR EACH ROW EXECUTE FUNCTION consumer.guard_projection();
-- Fixed envelope protocol validated independently in this database; no cross-database FK.
CREATE FUNCTION consumer.valid_body(e consumer.processed_events) RETURNS boolean
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
ALTER TABLE consumer.processed_events ADD CONSTRAINT event_body_consistency CHECK (consumer.valid_body(processed_events) IS TRUE);

CREATE FUNCTION consumer.assert_effects(id text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE e consumer.processed_events; total bigint;
BEGIN
 SELECT * INTO STRICT e FROM consumer.processed_events WHERE event_id=id;
 IF e.source_epoch IS DISTINCT FROM (SELECT source_epoch FROM consumer.identity) OR
 (SELECT count(*) FROM consumer.mutation_effects m WHERE m.event_id=id AND m.source_epoch=e.source_epoch AND m.source_change_id=e.source_change_id)<>(CASE WHEN e.kind='mutation' THEN 1 ELSE 0 END) THEN RAISE EXCEPTION 'Incomplete consumer effect' USING ERRCODE='P6002'; END IF;
 SELECT units INTO total FROM consumer.entity_totals WHERE source_epoch=e.source_epoch AND entity_id=e.entity_id;
 IF total IS NULL OR total<>(SELECT count(*) FROM consumer.mutation_effects m JOIN consumer.processed_events p USING(event_id) WHERE p.source_epoch=e.source_epoch AND p.entity_id=e.entity_id) OR
 NOT EXISTS(SELECT 1 FROM consumer.entity_projection p WHERE p.source_epoch=e.source_epoch AND p.entity_id=e.entity_id AND p.entity_version>=e.entity_version) THEN RAISE EXCEPTION 'Incomplete consumer total/projection' USING ERRCODE='P6002'; END IF;
END $$;
CREATE FUNCTION consumer.check_effects() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN PERFORM consumer.assert_effects(NEW.event_id); RETURN NULL; END $$;
CREATE CONSTRAINT TRIGGER complete_consumer AFTER INSERT ON consumer.processed_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION consumer.check_effects();
CREATE CONSTRAINT TRIGGER complete_effect AFTER INSERT ON consumer.mutation_effects DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION consumer.check_effects();
CREATE FUNCTION consumer.read_identity() RETURNS SETOF consumer.identity LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$ SELECT * FROM consumer.identity $$;
CREATE FUNCTION consumer.process_batch(consumer_id uuid,epoch uuid,instance uuid,registration uuid,items jsonb) RETURNS TABLE(event_id text,status text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE item jsonb; b jsonb; e consumer.processed_events; prior consumer.processed_events; inserted integer; lock_key bigint; bytes_total bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR NOT EXISTS(SELECT 1 FROM consumer.identity i WHERE i.consumer_id=process_batch.consumer_id AND i.source_epoch=epoch AND i.pipeline_id=instance AND i.registration_id=registration) THEN RAISE EXCEPTION 'Consumer identity/isolation mismatch' USING ERRCODE='P6002'; END IF;
 IF items IS NULL OR jsonb_typeof(items)<>'array' OR jsonb_array_length(items) NOT BETWEEN 1 AND 32 THEN RAISE EXCEPTION 'Invalid consumer batch' USING ERRCODE='P6002'; END IF;
 SELECT sum(octet_length(decode(i->>'body','hex'))+108) INTO bytes_total FROM jsonb_array_elements(items) i;
 IF bytes_total IS NULL OR bytes_total>1048576 THEN RAISE EXCEPTION 'Consumer batch exceeds 1 MiB' USING ERRCODE='P6002'; END IF;
 -- Sort actual advisory keys, including hash collisions, rather than lexical entity names.
 FOR lock_key IN SELECT DISTINCT hashtextextended((convert_from(decode(i->>'body','hex'),'UTF8')::jsonb->>'source_epoch')||':'||(convert_from(decode(i->>'body','hex'),'UTF8')::jsonb->>'entity_id'),0) k FROM jsonb_array_elements(items) i ORDER BY k LOOP
 PERFORM pg_advisory_xact_lock(lock_key); END LOOP;
 FOR item IN SELECT i FROM jsonb_array_elements(items) i ORDER BY (convert_from(decode(i->>'body','hex'),'UTF8')::jsonb->>'entity_id')::bigint,(convert_from(decode(i->>'body','hex'),'UTF8')::jsonb->>'entity_version')::bigint LOOP
 e:=NULL; e.body_bytes:=decode(item->>'body','hex'); e.content_sha256:=item->>'hash'; b:=convert_from(e.body_bytes,'UTF8')::jsonb;
 e.event_id:=b->>'event_id'; e.source_epoch:=(b->>'source_epoch')::uuid; e.entity_id:=(b->>'entity_id')::bigint; e.entity_version:=(b->>'entity_version')::bigint;
 e.source_change_id:=(b->>'source_change_id')::uuid; e.source_recorded_at:=(b->>'source_recorded_at')::timestamptz; e.kind:=b->>'kind'; e.is_deleted:=(b->>'is_deleted')::boolean; e.processed_at:=clock_timestamp();
 IF e.source_epoch IS DISTINCT FROM epoch OR e.content_sha256 IS DISTINCT FROM encode(sha256(e.body_bytes),'hex') OR consumer.valid_body(e) IS NOT TRUE THEN RAISE EXCEPTION 'Invalid consumer envelope/payload' USING ERRCODE='P6003'; END IF;
 INSERT INTO consumer.processed_events SELECT e.* ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS inserted=ROW_COUNT;
 IF inserted=0 THEN
 SELECT * INTO prior FROM consumer.processed_events p WHERE p.event_id=e.event_id;
 IF NOT FOUND OR prior.body_bytes IS DISTINCT FROM e.body_bytes OR prior.content_sha256 IS DISTINCT FROM e.content_sha256 THEN RAISE EXCEPTION 'Conflicting consumer identity/content' USING ERRCODE='P6001'; END IF;
 PERFORM consumer.assert_effects(e.event_id); event_id:=e.event_id;status:='already_processed';RETURN NEXT; CONTINUE;
 END IF;
 IF e.kind='mutation' THEN INSERT INTO consumer.mutation_effects VALUES(e.source_epoch,e.source_change_id,e.event_id); END IF;
 INSERT INTO consumer.entity_totals VALUES(e.source_epoch,e.entity_id,CASE WHEN e.kind='mutation' THEN 1 ELSE 0 END)
 ON CONFLICT(source_epoch,entity_id) DO UPDATE SET units=consumer.entity_totals.units+EXCLUDED.units;
 INSERT INTO consumer.entity_projection VALUES(e.source_epoch,e.entity_id,e.entity_version,e.event_id)
 ON CONFLICT(source_epoch,entity_id) DO UPDATE SET entity_version=EXCLUDED.entity_version,event_id=EXCLUDED.event_id WHERE consumer.entity_projection.entity_version<EXCLUDED.entity_version;
 event_id:=e.event_id;status:='processed';RETURN NEXT;
 END LOOP;
END $$;
CREATE FUNCTION consumer.retain_quarantine(raw bytea,meta bytea,claimed text,reason text,context text) RETURNS uuid
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE q consumer.quarantine; r text; m text; result uuid;
BEGIN
 IF raw IS NULL OR meta IS NULL OR octet_length(raw)>131072 OR octet_length(meta)>4096 OR reason IS NULL OR reason NOT IN ('validation','conflicting_content') OR context IS NULL OR octet_length(context)>1024 THEN RAISE EXCEPTION 'Unsupported quarantine input' USING ERRCODE='P6002'; END IF;
 r:=encode(sha256(raw),'hex');m:=encode(sha256(meta),'hex');
 INSERT INTO consumer.quarantine(raw_bytes,metadata,raw_hash,metadata_hash,claimed_id,classification,context) VALUES(raw,meta,r,m,claimed,reason,context) ON CONFLICT(raw_hash,metadata_hash) DO NOTHING RETURNING quarantine_id INTO result;
 IF result IS NOT NULL THEN RETURN result; END IF;
 SELECT * INTO STRICT q FROM consumer.quarantine WHERE raw_hash=r AND metadata_hash=m;
 IF q.raw_bytes IS DISTINCT FROM raw OR q.metadata IS DISTINCT FROM meta OR q.claimed_id IS DISTINCT FROM claimed THEN RAISE EXCEPTION 'Quarantine digest collision' USING ERRCODE='P6002'; END IF; RETURN q.quarantine_id;
END $$;
-- Request IDs plus expected wire hashes avoid an unbounded scan of forged claimed IDs.
CREATE FUNCTION consumer.receipts(ids text[],wire_hashes text[]) RETURNS TABLE(event_id text,state text,receipt_id text,body bytea,hash text,raw bytea,metadata bytea)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE id text; i integer;
BEGIN
 IF ids IS NULL OR wire_hashes IS NULL OR cardinality(ids) NOT BETWEEN 1 AND 8 OR cardinality(ids)<>cardinality(wire_hashes) OR EXISTS(SELECT 1 FROM unnest(ids) x WHERE x IS NULL OR octet_length(x)>100) THEN RAISE EXCEPTION 'Invalid receipt read bound' USING ERRCODE='P6002'; END IF;
 FOR i IN 1..cardinality(ids) LOOP
 id:=ids[i];
 RETURN QUERY SELECT p.event_id,'processed'::text,p.event_id,p.body_bytes,p.content_sha256,NULL::bytea,NULL::bytea FROM consumer.processed_events p WHERE p.event_id=id;
 IF FOUND THEN CONTINUE; END IF;
 RETURN QUERY SELECT id,'quarantined'::text,q.quarantine_id::text,NULL::bytea,NULL::text,q.raw_bytes,q.metadata FROM consumer.quarantine q WHERE q.claimed_id=id AND q.raw_hash=wire_hashes[i] ORDER BY q.quarantine_id LIMIT 1;
 END LOOP;
END $$;
CREATE FUNCTION consumer.status() RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('observed_at',clock_timestamp()::text,'identity',(SELECT to_jsonb(i) FROM consumer.identity i),'processed',(SELECT count(*)::text FROM consumer.processed_events),'effects',(SELECT count(*)::text FROM consumer.mutation_effects),'units',(SELECT COALESCE(sum(units),0)::text FROM consumer.entity_totals),'quarantined',(SELECT count(*)::text FROM consumer.quarantine),'quarantine_tail',COALESCE((SELECT jsonb_agg(q) FROM (SELECT quarantine_id::text,claimed_id,classification,context,raw_hash,metadata_hash,octet_length(raw_bytes) raw_bytes,octet_length(metadata) metadata_bytes,recorded_at::text FROM consumer.quarantine ORDER BY recorded_at DESC,quarantine_id LIMIT 20) q),'[]'::jsonb))
$$;
REVOKE ALL ON ALL TABLES IN SCHEMA consumer FROM PUBLIC,consumer_runtime,consumer_receipt_reader;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA consumer FROM PUBLIC;
GRANT USAGE ON SCHEMA consumer TO consumer_runtime,consumer_receipt_reader;
GRANT EXECUTE ON FUNCTION consumer.read_identity(),consumer.process_batch(uuid,uuid,uuid,uuid,jsonb),consumer.retain_quarantine(bytea,bytea,text,text,text),consumer.status() TO consumer_runtime;
GRANT EXECUTE ON FUNCTION consumer.read_identity(),consumer.receipts(text[],text[]),consumer.status() TO consumer_receipt_reader;
RESET ROLE;
