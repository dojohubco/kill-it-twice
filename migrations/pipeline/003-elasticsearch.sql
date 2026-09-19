-- Forward migration, executed by administrator in one transaction.
CREATE ROLE pipeline_es NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_es;
SET LOCAL ROLE pipeline_owner;
ALTER TABLE pipeline.destinations DROP CONSTRAINT destinations_state_check, DROP CONSTRAINT destinations_receiver_identity_check;
ALTER TABLE pipeline.destinations ADD CHECK ((state='unbound' AND receiver_identity IS NULL) OR (kind='elasticsearch' AND state='bound' AND receiver_identity IS NOT NULL));
CREATE FUNCTION pipeline.guard_destination() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.kind<>'elasticsearch' OR OLD.state<>'unbound' OR NEW.state<>'bound'
 OR NEW.destination_id<>OLD.destination_id OR NEW.kind<>OLD.kind OR NEW.generation<>OLD.generation OR NEW.receiver_identity IS NULL THEN
 RAISE EXCEPTION 'Immutable destination identity' USING ERRCODE='P5001'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER immutable_rows ON pipeline.destinations;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.destinations FOR EACH ROW EXECUTE FUNCTION pipeline.guard_destination();

CREATE TABLE pipeline.es_target (
 destination_id uuid PRIMARY KEY REFERENCES pipeline.destinations(destination_id),
 generation bigint NOT NULL CHECK(generation=1),
 registration_id uuid NOT NULL UNIQUE,
 pipeline_id uuid NOT NULL REFERENCES pipeline.source_binding(pipeline_id),
 source_epoch uuid NOT NULL REFERENCES pipeline.source_binding(source_epoch),
 index_name text NOT NULL UNIQUE CHECK(index_name ~ '^kit-[a-z0-9-]{10,100}$'),
 configuration jsonb NOT NULL CHECK(jsonb_typeof(configuration)='object' AND octet_length(configuration::text)<=16384),
 configuration_sha256 text NOT NULL CHECK(configuration_sha256=encode(sha256(convert_to(configuration::text,'UTF8')),'hex')),
 cluster_uuid text NOT NULL CHECK(length(cluster_uuid) BETWEEN 1 AND 100),
 index_uuid text CHECK(index_uuid IS NULL OR length(index_uuid) BETWEEN 1 AND 100),
 registered_at timestamptz,
 mode text NOT NULL DEFAULT 'preparing' CHECK(mode IN ('preparing','ready','cooldown','blocked')),
 reason text CHECK(reason IS NULL OR octet_length(reason)<=512),
 failures bigint NOT NULL DEFAULT 0 CHECK(failures>=0),
 next_probe_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 probe_owner uuid,
 probe_generation bigint NOT NULL DEFAULT 0 CHECK(probe_generation>=0),
 probe_until timestamptz,
 CHECK((probe_owner IS NULL)=(probe_until IS NULL)),
 CHECK((mode='preparing' AND index_uuid IS NULL AND registered_at IS NULL) OR (mode<>'preparing' AND index_uuid IS NOT NULL AND registered_at IS NOT NULL))
);
CREATE FUNCTION pipeline.guard_es_target() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR (to_jsonb(NEW)-ARRAY['mode','reason','failures','next_probe_at','probe_owner','probe_generation','probe_until','index_uuid','registered_at']) IS DISTINCT FROM
 (to_jsonb(OLD)-ARRAY['mode','reason','failures','next_probe_at','probe_owner','probe_generation','probe_until','index_uuid','registered_at']) OR
 (OLD.registered_at IS NOT NULL AND (NEW.index_uuid IS DISTINCT FROM OLD.index_uuid OR NEW.registered_at IS DISTINCT FROM OLD.registered_at)) THEN
 RAISE EXCEPTION 'Immutable receiver registration' USING ERRCODE='P5001'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.es_target FOR EACH ROW EXECUTE FUNCTION pipeline.guard_es_target();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.es_target EXECUTE FUNCTION pipeline.immutable();

ALTER TABLE pipeline.delivery_intents DROP CONSTRAINT delivery_intents_state_check;
ALTER TABLE pipeline.delivery_intents
 ADD COLUMN claim_generation bigint NOT NULL DEFAULT 0 CHECK(claim_generation>=0),
 ADD COLUMN owner_id uuid,
 ADD COLUMN lease_until timestamptz,
 ADD COLUMN next_retry_at timestamptz NOT NULL DEFAULT '-infinity',
 ADD COLUMN error_class text CHECK(error_class IS NULL OR error_class IN ('transient','mapping','oversized','auth','configuration','integrity')),
 ADD COLUMN disposition text CHECK(disposition IS NULL OR disposition IN ('applied','already_applied','superseded')),
 ADD COLUMN remote_version bigint CHECK(remote_version IS NULL OR remote_version>0),
 ADD COLUMN witness_event_id text REFERENCES pipeline.events(event_id),
 ADD COLUMN attempt_id uuid,
 ADD COLUMN settled_at timestamptz,
 ADD CHECK(state IN ('pending','leased','retry_wait','satisfied','dead_letter')),
 ADD CHECK((state='leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL AND claim_generation>0 AND attempt_id IS NOT NULL) OR (state<>'leased' AND owner_id IS NULL AND lease_until IS NULL)),
 ADD CHECK((state='satisfied' AND disposition IS NOT NULL AND remote_version IS NOT NULL AND witness_event_id IS NOT NULL AND settled_at IS NOT NULL AND attempt_id IS NOT NULL AND error_class IS NULL)
 OR (state='dead_letter' AND disposition IS NULL AND remote_version IS NULL AND witness_event_id IS NULL AND settled_at IS NOT NULL AND attempt_id IS NOT NULL AND error_class IS NOT NULL AND error_class IN ('mapping','oversized'))
 OR (state IN ('pending','leased','retry_wait') AND settled_at IS NULL AND disposition IS NULL AND remote_version IS NULL AND witness_event_id IS NULL)),
 ADD CHECK(kind='elasticsearch' OR (state='pending' AND claim_generation=0 AND owner_id IS NULL AND lease_until IS NULL AND next_retry_at='-infinity' AND error_class IS NULL AND disposition IS NULL AND remote_version IS NULL AND witness_event_id IS NULL AND attempt_id IS NULL AND settled_at IS NULL));
CREATE INDEX es_due ON pipeline.delivery_intents(next_retry_at,event_id) WHERE kind='elasticsearch' AND state IN ('pending','leased','retry_wait');
CREATE FUNCTION pipeline.guard_delivery() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.kind<>'elasticsearch' OR OLD.state IN ('satisfied','dead_letter') OR
 NEW.event_id<>OLD.event_id OR NEW.kind<>OLD.kind OR NEW.destination_id<>OLD.destination_id OR NEW.created_at<>OLD.created_at OR NEW.claim_generation<OLD.claim_generation THEN
 RAISE EXCEPTION 'Immutable delivery evidence' USING ERRCODE='P5001'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER immutable_rows ON pipeline.delivery_intents;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.delivery_intents FOR EACH ROW EXECUTE FUNCTION pipeline.guard_delivery();
CREATE TABLE pipeline.es_attempts (
 attempt_id uuid PRIMARY KEY,
 event_id text NOT NULL,
 destination_id uuid NOT NULL,
 claim_generation bigint NOT NULL CHECK(claim_generation>0),
 owner_id uuid NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 outcome text CHECK(outcome IS NULL OR outcome IN ('applied','already_applied','superseded','mapping','oversized','transient','auth','configuration','integrity','expired')),
 context text CHECK(context IS NULL OR octet_length(context)<=2048),
 remote_version bigint CHECK(remote_version IS NULL OR remote_version>0),
 FOREIGN KEY(event_id,destination_id) REFERENCES pipeline.delivery_intents(event_id,destination_id),
 UNIQUE(event_id,destination_id,claim_generation),
 CHECK((finished_at IS NULL)=(outcome IS NULL))
);
ALTER TABLE pipeline.delivery_intents ADD FOREIGN KEY(attempt_id) REFERENCES pipeline.es_attempts(attempt_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE pipeline.es_dead_letters (
 event_id text NOT NULL,
 destination_id uuid NOT NULL,
 attempt_id uuid NOT NULL REFERENCES pipeline.es_attempts(attempt_id),
 error_class text NOT NULL CHECK(error_class IN ('mapping','oversized')),
 context text NOT NULL CHECK(octet_length(context)<=2048),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(event_id,destination_id),
 FOREIGN KEY(event_id,destination_id) REFERENCES pipeline.delivery_intents(event_id,destination_id)
);
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.es_dead_letters FOR EACH ROW EXECUTE FUNCTION pipeline.immutable();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.es_dead_letters EXECUTE FUNCTION pipeline.immutable();
CREATE FUNCTION pipeline.guard_es_attempt() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP='UPDATE' AND (OLD.finished_at IS NOT NULL OR (to_jsonb(NEW)-ARRAY['finished_at','outcome','context','remote_version']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['finished_at','outcome','context','remote_version'])) THEN
 RAISE EXCEPTION 'Immutable completed attempt' USING ERRCODE='P5001'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.es_attempts FOR EACH ROW EXECUTE FUNCTION pipeline.guard_es_attempt();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.es_attempts EXECUTE FUNCTION pipeline.immutable();

CREATE OR REPLACE FUNCTION pipeline.assert_obligations(id text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF (SELECT count(*) FROM pipeline.delivery_intents d JOIN pipeline.destinations t USING(destination_id,kind) WHERE d.event_id=id AND
 ((d.kind='rabbitmq' AND d.state='pending' AND t.state='unbound' AND t.receiver_identity IS NULL) OR
 (d.kind='elasticsearch' AND ((t.state='unbound' AND d.state='pending') OR (t.state='bound' AND EXISTS(SELECT 1 FROM pipeline.es_target x WHERE x.destination_id=t.destination_id AND x.index_uuid=t.receiver_identity AND x.registered_at IS NOT NULL))))))<>2
 OR (SELECT count(*) FROM pipeline.consumer_observations WHERE event_id=id AND state='pending')<>1 THEN
 RAISE EXCEPTION 'Missing or inconsistent event obligations' USING ERRCODE='P3002'; END IF;
END $$;
-- Deferred terminal/attempt evidence consistency, in addition to structural sink obligations.
ALTER TABLE pipeline.es_attempts ADD UNIQUE(attempt_id,event_id,destination_id,claim_generation), ADD UNIQUE(attempt_id,event_id,destination_id);
ALTER TABLE pipeline.delivery_intents ADD FOREIGN KEY(attempt_id,event_id,destination_id,claim_generation) REFERENCES pipeline.es_attempts(attempt_id,event_id,destination_id,claim_generation) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pipeline.es_dead_letters ADD FOREIGN KEY(attempt_id,event_id,destination_id) REFERENCES pipeline.es_attempts(attempt_id,event_id,destination_id);
CREATE FUNCTION pipeline.es_check_evidence() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; a pipeline.es_attempts;
BEGIN
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=NEW.event_id AND kind='elasticsearch';
 IF NOT FOUND THEN RAISE EXCEPTION 'Missing ES intent' USING ERRCODE='P5001'; END IF;
 IF d.attempt_id IS NULL THEN RETURN NULL; END IF;
 SELECT * INTO STRICT a FROM pipeline.es_attempts WHERE attempt_id=d.attempt_id;
 IF (d.state='leased' AND (a.finished_at IS NOT NULL OR a.owner_id IS DISTINCT FROM d.owner_id))
 OR (d.state='satisfied' AND (a.outcome IS DISTINCT FROM d.disposition OR a.remote_version IS DISTINCT FROM d.remote_version OR a.finished_at IS DISTINCT FROM d.settled_at))
 OR (d.state='retry_wait' AND (a.finished_at IS NULL OR a.outcome IS DISTINCT FROM d.error_class))
 OR (d.state='dead_letter' AND (a.outcome IS DISTINCT FROM d.error_class OR a.finished_at IS DISTINCT FROM d.settled_at OR NOT EXISTS(SELECT 1 FROM pipeline.es_dead_letters l WHERE l.event_id=d.event_id AND l.destination_id=d.destination_id AND l.attempt_id=d.attempt_id AND l.error_class=d.error_class))) THEN
 RAISE EXCEPTION 'Incomplete ES result evidence' USING ERRCODE='P5001'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER es_evidence_intent AFTER INSERT OR UPDATE ON pipeline.delivery_intents DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='elasticsearch') EXECUTE FUNCTION pipeline.es_check_evidence();
CREATE CONSTRAINT TRIGGER es_evidence_attempt AFTER INSERT OR UPDATE ON pipeline.es_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.es_check_evidence();
CREATE CONSTRAINT TRIGGER es_evidence_dead_letter AFTER INSERT ON pipeline.es_dead_letters DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.es_check_evidence();
REVOKE ALL ON FUNCTION pipeline.es_check_evidence() FROM PUBLIC;

CREATE FUNCTION pipeline.es_projection(id text) RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('projection_schema','search-v1','source_epoch',e.source_epoch::text,'entity_id',e.entity_id::text,'entity_version',e.entity_version::text,
 'is_deleted',e.is_deleted,'canonical_body_json',convert_from(e.body_bytes,'UTF8'),'content_sha256',e.content_sha256,'search_fields',
 CASE WHEN e.is_deleted THEN '{}'::jsonb ELSE COALESCE((SELECT jsonb_object_agg(k,v) FROM jsonb_each((convert_from(e.body_bytes,'UTF8')::jsonb->>'payload_json')::jsonb) AS f(k,v) WHERE k IN ('name','country','loyalty_points')),'{}'::jsonb) END)::text
 FROM pipeline.events e WHERE event_id=id;
$$;
CREATE FUNCTION pipeline.es_read(id text) RETURNS TABLE(event_id text,document_id text,version text,projection text,bytes text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT e.event_id,e.source_epoch::text||':'||e.entity_id::text,e.entity_version::text,
 CASE WHEN octet_length(p.doc)<=262144 THEN p.doc ELSE NULL END,octet_length(p.doc)::text
 FROM pipeline.events e CROSS JOIN LATERAL (SELECT pipeline.es_projection(e.event_id) doc) p WHERE e.event_id=id;
$$;
CREATE FUNCTION pipeline.es_identity() RETURNS SETOF pipeline.es_target LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$ SELECT * FROM pipeline.es_target $$;

-- Only administrator uses these setup statements directly: INSERT es_target preparing,
-- verify actual receiver, UPDATE index_uuid/registered_at/mode, UPDATE destination bound,
-- all binding finalization changes in one transaction. No runtime setup grant exists.
CREATE FUNCTION pipeline.es_claim(target uuid, target_generation bigint, incarnation uuid, n integer, lease_ms integer)
RETURNS TABLE(event_id text,generation text,attempt_id uuid,projection_bytes text,probe_generation text,backend_pid text,transaction_id text,claimed_at text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE t pipeline.es_target; d pipeline.delivery_intents; now_at timestamptz; a uuid;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR n IS NULL OR n NOT BETWEEN 1 AND 500 OR lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 OR incarnation IS NULL THEN RAISE EXCEPTION 'Invalid ES claim' USING ERRCODE='P5001'; END IF;
 SELECT * INTO STRICT t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE;
 now_at:=clock_timestamp();
 IF t.generation IS DISTINCT FROM target_generation THEN RAISE EXCEPTION 'Target generation mismatch' USING ERRCODE='P5001'; END IF;
 IF t.mode IN ('blocked','preparing') THEN RETURN; END IF;
 IF t.mode='cooldown' THEN
  IF t.next_probe_at>now_at OR (t.probe_until IS NOT NULL AND t.probe_until>now_at) THEN RETURN; END IF;
  UPDATE pipeline.es_target SET probe_owner=incarnation,probe_generation=pipeline.es_target.probe_generation+1,probe_until=now_at+lease_ms*interval '1 millisecond' WHERE destination_id=target RETURNING * INTO t;
  n:=1;
 END IF;
 FOR d IN SELECT * FROM pipeline.delivery_intents i WHERE i.destination_id=target AND i.kind='elasticsearch' AND
 ((i.state IN ('pending','retry_wait') AND i.next_retry_at<=now_at) OR (i.state='leased' AND i.lease_until<=now_at)) ORDER BY i.next_retry_at,i.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
  IF d.state='leased' THEN UPDATE pipeline.es_attempts SET finished_at=now_at,outcome='expired',context='Lease expired before local outcome' WHERE pipeline.es_attempts.attempt_id=d.attempt_id AND finished_at IS NULL; END IF;
  a:=gen_random_uuid();
  INSERT INTO pipeline.es_attempts(attempt_id,event_id,destination_id,claim_generation,owner_id) VALUES(a,d.event_id,target,d.claim_generation+1,incarnation);
  UPDATE pipeline.delivery_intents SET state='leased',owner_id=incarnation,lease_until=clock_timestamp()+lease_ms*interval '1 millisecond',claim_generation=d.claim_generation+1,attempt_id=a,error_class=NULL WHERE pipeline.delivery_intents.event_id=d.event_id AND kind='elasticsearch';
  DELETE FROM pipeline.es_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31;
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; projection_bytes:=octet_length(pipeline.es_projection(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;
CREATE FUNCTION pipeline.es_renew(target uuid, target_generation bigint, id text, incarnation uuid, gen bigint, lease_ms integer) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; t pipeline.es_target; now_at timestamptz;
BEGIN
 IF lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 THEN RAISE EXCEPTION 'Invalid lease' USING ERRCODE='P5001'; END IF;
 -- Same target-before-intent lock order as claim, including cooldown probe renewal.
 SELECT * INTO t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE;
 IF NOT FOUND OR t.generation IS DISTINCT FROM target_generation OR t.mode='blocked' THEN RETURN false; END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND destination_id=target AND kind='elasticsearch' FOR UPDATE;
 now_at:=clock_timestamp();
 IF NOT FOUND OR d.state<>'leased' OR d.owner_id IS DISTINCT FROM incarnation OR d.claim_generation IS DISTINCT FROM gen OR d.lease_until<=now_at THEN RETURN false; END IF;
 UPDATE pipeline.delivery_intents SET lease_until=now_at+lease_ms*interval '1 millisecond' WHERE event_id=id AND kind='elasticsearch';
 UPDATE pipeline.es_target SET probe_until=now_at+lease_ms*interval '1 millisecond' WHERE destination_id=target AND mode='cooldown' AND probe_owner=incarnation AND probe_until>now_at;
 RETURN true;
END $$;
CREATE FUNCTION pipeline.es_settle(target uuid,target_generation bigint,id text,incarnation uuid,gen bigint,outcome text,remote bigint,witness text,context text,delay_ms integer) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; now_at timestamptz; e pipeline.events; w pipeline.events;
BEGIN
 IF outcome IS NULL OR outcome NOT IN ('applied','already_applied','superseded','mapping','oversized','transient','auth','configuration','integrity') OR context IS NULL OR octet_length(context)>2048 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 1 AND 30000 THEN RAISE EXCEPTION 'Invalid settlement' USING ERRCODE='P5001'; END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND destination_id=target AND kind='elasticsearch' FOR UPDATE;
 now_at:=clock_timestamp();
 IF NOT FOUND OR d.state<>'leased' OR d.owner_id IS DISTINCT FROM incarnation OR d.claim_generation IS DISTINCT FROM gen OR d.lease_until<=now_at OR NOT EXISTS(SELECT 1 FROM pipeline.es_target WHERE destination_id=target AND generation=target_generation AND registered_at IS NOT NULL AND mode<>'blocked') THEN RETURN 'stale'; END IF;
 IF outcome IN ('applied','already_applied','superseded') THEN
 SELECT * INTO STRICT e FROM pipeline.events WHERE event_id=id;
 SELECT * INTO w FROM pipeline.events WHERE event_id=witness;
 IF NOT FOUND OR remote IS NULL OR w.entity_version<>remote OR w.entity_id<>e.entity_id OR w.source_epoch<>e.source_epoch OR
 (outcome='superseded' AND remote<=e.entity_version) OR (outcome<>'superseded' AND witness<>id) THEN RAISE EXCEPTION 'Invalid local witness' USING ERRCODE='P5001'; END IF;
 END IF;
 UPDATE pipeline.es_attempts SET finished_at=now_at,outcome=es_settle.outcome,context=es_settle.context,remote_version=remote WHERE attempt_id=d.attempt_id;
 IF outcome IN ('mapping','oversized') THEN
 INSERT INTO pipeline.es_dead_letters(event_id,destination_id,attempt_id,error_class,context) VALUES(id,target,d.attempt_id,outcome,context);
 END IF;
 UPDATE pipeline.delivery_intents SET
 state=CASE WHEN outcome IN ('applied','already_applied','superseded') THEN 'satisfied' WHEN outcome IN ('mapping','oversized') THEN 'dead_letter' ELSE 'retry_wait' END,
 owner_id=NULL,lease_until=NULL,next_retry_at=now_at+delay_ms*interval '1 millisecond',
 error_class=CASE WHEN outcome IN ('applied','already_applied','superseded') THEN NULL ELSE outcome END,
 disposition=CASE WHEN outcome IN ('applied','already_applied','superseded') THEN outcome ELSE NULL END,
 remote_version=CASE WHEN outcome IN ('applied','already_applied','superseded') THEN remote ELSE NULL END,
 witness_event_id=CASE WHEN outcome IN ('applied','already_applied','superseded') THEN witness ELSE NULL END,
 settled_at=CASE WHEN outcome IN ('applied','already_applied','superseded','mapping','oversized') THEN now_at ELSE NULL END
 WHERE event_id=id AND kind='elasticsearch';
 RETURN 'settled';
END $$;
CREATE FUNCTION pipeline.es_admission(target uuid,target_generation bigint,incarnation uuid,probe_gen bigint,anchor text,claim_gen bigint,claim_attempt uuid,outcome text,reason text,delay_ms integer) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE t pipeline.es_target; now_at timestamptz;
BEGIN
 IF outcome IS NULL OR outcome NOT IN ('healthy','transient','auth','configuration','integrity') OR reason IS NULL OR octet_length(reason)>512 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 1 AND 30000 THEN RAISE EXCEPTION 'Invalid admission outcome' USING ERRCODE='P5001'; END IF;
 SELECT * INTO STRICT t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE; now_at:=clock_timestamp();
 IF t.generation IS DISTINCT FROM target_generation OR t.mode IN ('preparing','blocked') THEN RETURN false; END IF;
 IF NOT EXISTS (SELECT 1 FROM pipeline.delivery_intents d JOIN pipeline.es_attempts a ON a.attempt_id=d.attempt_id WHERE d.event_id=anchor AND d.destination_id=target AND d.kind='elasticsearch' AND d.claim_generation=claim_gen AND a.attempt_id=claim_attempt AND a.owner_id=incarnation AND ((d.state='leased' AND d.owner_id=incarnation AND d.lease_until>now_at) OR (a.finished_at IS NOT NULL AND a.outcome<>'expired'))) THEN RETURN false; END IF;
 IF t.mode='cooldown' AND (t.probe_owner IS DISTINCT FROM incarnation OR t.probe_generation IS DISTINCT FROM probe_gen OR t.probe_until IS NULL OR t.probe_until<=now_at) THEN RETURN false; END IF;
 UPDATE pipeline.es_target SET mode=CASE WHEN outcome='healthy' THEN 'ready' WHEN outcome='transient' THEN 'cooldown' ELSE 'blocked' END,
 reason=CASE WHEN outcome='healthy' THEN NULL ELSE es_admission.reason END,failures=CASE WHEN outcome='healthy' THEN 0 ELSE failures+1 END,
 next_probe_at=now_at+delay_ms*interval '1 millisecond',probe_owner=NULL,probe_until=NULL WHERE destination_id=target;
 RETURN true;
END $$;
CREATE FUNCTION pipeline.es_status(n integer) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF n IS NULL OR n NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'Invalid inspection bound' USING ERRCODE='P5001'; END IF;
 RETURN jsonb_build_object('observed_at',clock_timestamp()::text,'target',(SELECT to_jsonb(t) FROM pipeline.es_target t),
 'counts',(SELECT jsonb_object_agg(state,c) FROM (SELECT state,count(*)::text c FROM pipeline.delivery_intents WHERE kind='elasticsearch' GROUP BY state) q),
 'dead_letters',COALESCE((SELECT jsonb_agg(to_jsonb(d)) FROM (SELECT * FROM pipeline.es_dead_letters ORDER BY recorded_at,event_id LIMIT n) d),'[]'::jsonb));
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM PUBLIC,pipeline_es;
REVOKE ALL ON FUNCTION pipeline.guard_destination(),pipeline.guard_es_target(),pipeline.guard_delivery(),pipeline.guard_es_attempt(),pipeline.es_projection(text),pipeline.es_read(text),pipeline.es_identity(),pipeline.es_claim(uuid,bigint,uuid,integer,integer),pipeline.es_renew(uuid,bigint,text,uuid,bigint,integer),pipeline.es_settle(uuid,bigint,text,uuid,bigint,text,bigint,text,text,integer),pipeline.es_admission(uuid,bigint,uuid,bigint,text,bigint,uuid,text,text,integer),pipeline.es_status(integer) FROM PUBLIC;
GRANT USAGE ON SCHEMA pipeline TO pipeline_es;
GRANT EXECUTE ON FUNCTION pipeline.es_read(text),pipeline.es_identity(),pipeline.es_claim(uuid,bigint,uuid,integer,integer),pipeline.es_renew(uuid,bigint,text,uuid,bigint,integer),pipeline.es_settle(uuid,bigint,text,uuid,bigint,text,bigint,text,text,integer),pipeline.es_admission(uuid,bigint,uuid,bigint,text,bigint,uuid,text,text,integer),pipeline.es_status(integer) TO pipeline_es;
RESET ROLE;
