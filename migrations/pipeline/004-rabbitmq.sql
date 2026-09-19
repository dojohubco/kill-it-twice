-- Additive M4 migration, administrator-owned transaction. Prior history is unchanged.
CREATE ROLE pipeline_rabbit NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
CREATE ROLE pipeline_receipts NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_rabbit,pipeline_receipts;
SET LOCAL ROLE pipeline_owner;
ALTER TABLE pipeline.destinations DROP CONSTRAINT destinations_check;
ALTER TABLE pipeline.destinations ADD CONSTRAINT destination_binding_shape CHECK((state='unbound' AND receiver_identity IS NULL) OR (state='bound' AND receiver_identity IS NOT NULL));
CREATE OR REPLACE FUNCTION pipeline.guard_destination() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state<>'unbound' OR NEW.state<>'bound' OR NEW.destination_id<>OLD.destination_id OR NEW.kind<>OLD.kind OR NEW.generation<>OLD.generation OR NEW.receiver_identity IS NULL THEN
 RAISE EXCEPTION 'Immutable destination identity' USING ERRCODE='P5001'; END IF;
 RETURN NEW;
END $$;
CREATE TABLE pipeline.rabbit_target (
 destination_id uuid PRIMARY KEY REFERENCES pipeline.destinations(destination_id), generation bigint NOT NULL CHECK(generation=1),
 registration_id uuid NOT NULL UNIQUE, consumer_id uuid NOT NULL UNIQUE,
 pipeline_id uuid NOT NULL REFERENCES pipeline.source_binding(pipeline_id), source_epoch uuid NOT NULL REFERENCES pipeline.source_binding(source_epoch),
 vhost text NOT NULL UNIQUE, exchange text NOT NULL, queue text NOT NULL, routing_key text NOT NULL,
 configuration jsonb NOT NULL CHECK(jsonb_typeof(configuration)='object' AND octet_length(configuration::text)<=16384),
 configuration_sha256 text NOT NULL CHECK(configuration_sha256=encode(sha256(convert_to(configuration::text,'UTF8')),'hex')),
 registered_at timestamptz CHECK(registered_at IS NULL OR isfinite(registered_at)),
 mode text NOT NULL DEFAULT 'preparing' CHECK(mode IN ('preparing','ready','cooldown','blocked')),
 reason text CHECK(reason IS NULL OR octet_length(reason)<=512), failures bigint NOT NULL DEFAULT 0 CHECK(failures>=0),
 next_probe_at timestamptz NOT NULL DEFAULT clock_timestamp(), probe_owner uuid, probe_generation bigint NOT NULL DEFAULT 0 CHECK(probe_generation>=0), probe_until timestamptz,
 CHECK(vhost='kit-'||registration_id::text AND exchange=vhost||'-events' AND queue=vhost||'-consumer' AND routing_key='revision-v1'),
 CHECK((probe_owner IS NULL)=(probe_until IS NULL)), CHECK((mode='preparing')=(registered_at IS NULL))
);
CREATE FUNCTION pipeline.guard_rabbit_target() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR (to_jsonb(NEW)-ARRAY['mode','reason','failures','next_probe_at','probe_owner','probe_generation','probe_until','registered_at']) IS DISTINCT FROM
 (to_jsonb(OLD)-ARRAY['mode','reason','failures','next_probe_at','probe_owner','probe_generation','probe_until','registered_at']) OR
 (OLD.registered_at IS NOT NULL AND NEW.registered_at IS DISTINCT FROM OLD.registered_at) THEN RAISE EXCEPTION 'Immutable broker registration' USING ERRCODE='P7001'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.rabbit_target FOR EACH ROW EXECUTE FUNCTION pipeline.guard_rabbit_target();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.rabbit_target EXECUTE FUNCTION pipeline.immutable();
-- Replace the old pending-only Rabbit CHECKs deliberately, retaining ES semantics.
DO $$ DECLARE c record; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='pipeline.delivery_intents'::regclass AND contype='c' LOOP EXECUTE format('ALTER TABLE pipeline.delivery_intents DROP CONSTRAINT %I',c.conname); END LOOP; END $$;
ALTER TABLE pipeline.delivery_intents ADD COLUMN rabbit_attempt_id uuid,
 ADD CONSTRAINT delivery_kind CHECK(kind IN ('elasticsearch','rabbitmq')),
 ADD CONSTRAINT delivery_generation CHECK(claim_generation>=0),
 ADD CONSTRAINT delivery_error CHECK(error_class IS NULL OR error_class IN ('transient','mapping','oversized','auth','configuration','integrity')),
 ADD CONSTRAINT delivery_state CHECK(state IN ('pending','leased','retry_wait','satisfied','dead_letter')),
 ADD CONSTRAINT delivery_lease CHECK((state='leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL AND isfinite(lease_until) AND claim_generation>0 AND COALESCE(attempt_id,rabbit_attempt_id) IS NOT NULL) OR (state<>'leased' AND owner_id IS NULL AND lease_until IS NULL)),
 ADD CONSTRAINT delivery_protocol CHECK(
 (kind='elasticsearch' AND rabbit_attempt_id IS NULL AND (remote_version IS NULL OR remote_version>0) AND (disposition IS NULL OR disposition IN ('applied','already_applied','superseded')) AND
 ((state='satisfied' AND disposition IS NOT NULL AND remote_version IS NOT NULL AND witness_event_id IS NOT NULL AND settled_at IS NOT NULL AND attempt_id IS NOT NULL AND error_class IS NULL)
 OR (state='dead_letter' AND disposition IS NULL AND remote_version IS NULL AND witness_event_id IS NULL AND settled_at IS NOT NULL AND attempt_id IS NOT NULL AND error_class IS NOT NULL AND error_class IN ('mapping','oversized'))
 OR (state IN ('pending','leased','retry_wait') AND settled_at IS NULL AND disposition IS NULL AND remote_version IS NULL AND witness_event_id IS NULL)))
 OR (kind='rabbitmq' AND attempt_id IS NULL AND remote_version IS NULL AND witness_event_id IS NULL AND (error_class IS NULL OR error_class IN ('transient','auth','configuration','integrity')) AND
 ((state='satisfied' AND disposition IS NOT NULL AND disposition='broker_confirmed' AND settled_at IS NOT NULL AND rabbit_attempt_id IS NOT NULL AND error_class IS NULL)
 OR (state IN ('pending','leased','retry_wait') AND disposition IS NULL AND settled_at IS NULL)))) ;
CREATE OR REPLACE FUNCTION pipeline.guard_delivery() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state IN ('satisfied','dead_letter') OR NEW.event_id<>OLD.event_id OR NEW.kind<>OLD.kind OR NEW.destination_id<>OLD.destination_id OR NEW.created_at<>OLD.created_at OR NEW.claim_generation<OLD.claim_generation THEN RAISE EXCEPTION 'Immutable delivery evidence' USING ERRCODE='P5001'; END IF; RETURN NEW;
END $$;
CREATE INDEX rabbit_due ON pipeline.delivery_intents(next_retry_at,event_id) WHERE kind='rabbitmq' AND state IN ('pending','leased','retry_wait');
CREATE TABLE pipeline.rabbit_attempts (
 attempt_id uuid PRIMARY KEY, event_id text NOT NULL, destination_id uuid NOT NULL,
 claim_generation bigint NOT NULL CHECK(claim_generation>0), owner_id uuid NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz, channel_id uuid,
 outcome text CHECK(outcome IS NULL OR outcome IN ('confirmed','transient','auth','configuration','integrity','expired')),
 context text CHECK(context IS NULL OR octet_length(context)<=2048),
 FOREIGN KEY(event_id,destination_id) REFERENCES pipeline.delivery_intents(event_id,destination_id),
 UNIQUE(event_id,destination_id,claim_generation), UNIQUE(attempt_id,event_id,destination_id,claim_generation),
 CHECK((finished_at IS NULL)=(outcome IS NULL)), CHECK(outcome IS DISTINCT FROM 'confirmed' OR channel_id IS NOT NULL)
);
ALTER TABLE pipeline.delivery_intents ADD FOREIGN KEY(rabbit_attempt_id,event_id,destination_id,claim_generation) REFERENCES pipeline.rabbit_attempts(attempt_id,event_id,destination_id,claim_generation) DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION pipeline.guard_rabbit_attempt() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP='UPDATE' AND (OLD.finished_at IS NOT NULL OR (to_jsonb(NEW)-ARRAY['finished_at','outcome','context','channel_id']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['finished_at','outcome','context','channel_id'])) THEN RAISE EXCEPTION 'Immutable completed broker attempt' USING ERRCODE='P7001'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.rabbit_attempts FOR EACH ROW EXECUTE FUNCTION pipeline.guard_rabbit_attempt();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.rabbit_attempts EXECUTE FUNCTION pipeline.immutable();
CREATE FUNCTION pipeline.rabbit_check_evidence() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; a pipeline.rabbit_attempts;
BEGIN
 SELECT * INTO STRICT d FROM pipeline.delivery_intents WHERE event_id=NEW.event_id AND kind='rabbitmq';
 IF d.rabbit_attempt_id IS NULL THEN RETURN NULL; END IF;
 SELECT * INTO STRICT a FROM pipeline.rabbit_attempts WHERE attempt_id=d.rabbit_attempt_id;
 IF (d.state='leased' AND (a.finished_at IS NOT NULL OR a.owner_id IS DISTINCT FROM d.owner_id)) OR
 (d.state='satisfied' AND (a.outcome IS DISTINCT FROM 'confirmed' OR a.channel_id IS NULL OR a.finished_at IS DISTINCT FROM d.settled_at)) OR
 (d.state='retry_wait' AND (a.finished_at IS NULL OR a.outcome IS DISTINCT FROM d.error_class)) THEN RAISE EXCEPTION 'Incomplete broker evidence' USING ERRCODE='P7001'; END IF; RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER rabbit_evidence_intent AFTER INSERT OR UPDATE ON pipeline.delivery_intents DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='rabbitmq') EXECUTE FUNCTION pipeline.rabbit_check_evidence();
CREATE CONSTRAINT TRIGGER rabbit_evidence_attempt AFTER INSERT OR UPDATE ON pipeline.rabbit_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.rabbit_check_evidence();
ALTER TABLE pipeline.consumer_observations DROP CONSTRAINT consumer_observations_state_check;
ALTER TABLE pipeline.consumer_observations ADD COLUMN next_check_at timestamptz NOT NULL DEFAULT '-infinity',
 ADD COLUMN consumer_id uuid REFERENCES pipeline.rabbit_target(consumer_id), ADD COLUMN registration_id uuid REFERENCES pipeline.rabbit_target(registration_id),
 ADD COLUMN receipt_hash text, ADD COLUMN receipt_bytes bytea, ADD COLUMN receipt_id text, ADD COLUMN observed_at timestamptz,
 ADD CONSTRAINT observation_shape CHECK((state='pending' AND consumer_id IS NULL AND registration_id IS NULL AND receipt_hash IS NULL AND receipt_bytes IS NULL AND receipt_id IS NULL AND observed_at IS NULL) OR
 (state IN ('processed','quarantined') AND consumer_id IS NOT NULL AND registration_id IS NOT NULL AND receipt_hash IS NOT NULL AND receipt_bytes IS NOT NULL AND receipt_id IS NOT NULL AND octet_length(receipt_id)<=128 AND observed_at IS NOT NULL AND isfinite(observed_at) AND octet_length(receipt_bytes)<=65536 AND receipt_hash=encode(sha256(receipt_bytes),'hex')));
CREATE FUNCTION pipeline.guard_observation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state<>'pending' OR NEW.event_id<>OLD.event_id OR NEW.created_at<>OLD.created_at THEN RAISE EXCEPTION 'Immutable consumer observation' USING ERRCODE='P7001'; END IF; RETURN NEW;
END $$;
DROP TRIGGER immutable_rows ON pipeline.consumer_observations;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.consumer_observations FOR EACH ROW EXECUTE FUNCTION pipeline.guard_observation();
CREATE INDEX consumer_due ON pipeline.consumer_observations(next_check_at,event_id) WHERE state='pending';
CREATE OR REPLACE FUNCTION pipeline.assert_obligations(id text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pipeline.events WHERE event_id=id) OR
 (SELECT count(*) FROM pipeline.delivery_intents d JOIN pipeline.destinations t USING(destination_id,kind) WHERE d.event_id=id AND
 ((t.state='unbound' AND d.state='pending' AND t.receiver_identity IS NULL) OR (t.state='bound' AND
 ((d.kind='elasticsearch' AND EXISTS(SELECT 1 FROM pipeline.es_target x WHERE x.destination_id=t.destination_id AND x.index_uuid=t.receiver_identity AND x.registered_at IS NOT NULL)) OR
 (d.kind='rabbitmq' AND EXISTS(SELECT 1 FROM pipeline.rabbit_target x WHERE x.destination_id=t.destination_id AND x.registration_id::text=t.receiver_identity AND x.registered_at IS NOT NULL))))))<>2 OR
 (SELECT count(*) FROM pipeline.consumer_observations o JOIN pipeline.events e USING(event_id) WHERE o.event_id=id AND (o.state='pending' OR
 (o.state IN ('processed','quarantined') AND o.receipt_hash=e.content_sha256 AND o.receipt_bytes=e.body_bytes AND EXISTS(SELECT 1 FROM pipeline.rabbit_target t WHERE t.registration_id=o.registration_id AND t.consumer_id=o.consumer_id AND t.registered_at IS NOT NULL))))<>1
 THEN RAISE EXCEPTION 'Missing or inconsistent event obligations' USING ERRCODE='P3002'; END IF;
END $$;
CREATE CONSTRAINT TRIGGER complete_progressed_observation AFTER UPDATE ON pipeline.consumer_observations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_obligations();
CREATE FUNCTION pipeline.rabbit_identity() RETURNS SETOF pipeline.rabbit_target LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$ SELECT * FROM pipeline.rabbit_target $$;
CREATE FUNCTION pipeline.rabbit_wire(id text) RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT convert_to('{"body":','UTF8')||body_bytes||convert_to(',"content_sha256":"'||content_sha256||'"}','UTF8') FROM pipeline.events WHERE event_id=id;
$$;
CREATE FUNCTION pipeline.rabbit_read(ids text[]) RETURNS TABLE(event_id text,wire bytea,bytes text) LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF ids IS NULL OR cardinality(ids) NOT BETWEEN 1 AND 128 OR EXISTS(SELECT 1 FROM unnest(ids) i WHERE i IS NULL OR octet_length(i)>100) THEN RAISE EXCEPTION 'Invalid broker read bound' USING ERRCODE='P7001'; END IF;
 IF (SELECT COALESCE(sum(octet_length(pipeline.rabbit_wire(e.event_id))),0) FROM pipeline.events e WHERE e.event_id=ANY(ids))>4194304 THEN RAISE EXCEPTION 'Broker transfer exceeds 4 MiB' USING ERRCODE='P7001'; END IF;
 RETURN QUERY SELECT e.event_id,CASE WHEN octet_length(pipeline.rabbit_wire(e.event_id))<=65536 THEN pipeline.rabbit_wire(e.event_id) ELSE NULL END,octet_length(pipeline.rabbit_wire(e.event_id))::text FROM pipeline.events e WHERE e.event_id=ANY(ids) ORDER BY e.event_id;
END $$;
CREATE FUNCTION pipeline.rabbit_claim(target uuid, target_generation bigint, incarnation uuid, n integer, lease_ms integer)
RETURNS TABLE(event_id text,generation text,attempt_id uuid,wire_bytes text,probe_generation text,backend_pid text,transaction_id text,claimed_at text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE t pipeline.rabbit_target; d pipeline.delivery_intents; now_at timestamptz; a uuid;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR n IS NULL OR n NOT BETWEEN 1 AND 128 OR lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 OR incarnation IS NULL THEN RAISE EXCEPTION 'Invalid broker claim' USING ERRCODE='P7001'; END IF;
 SELECT * INTO STRICT t FROM pipeline.rabbit_target WHERE destination_id=target FOR UPDATE;
 now_at:=clock_timestamp();
 IF t.generation IS DISTINCT FROM target_generation THEN RAISE EXCEPTION 'Target generation mismatch' USING ERRCODE='P7001'; END IF;
 IF t.mode IN ('blocked','preparing') THEN RETURN; END IF;
 IF t.mode='cooldown' THEN
  IF t.next_probe_at>now_at OR (t.probe_until IS NOT NULL AND t.probe_until>now_at) THEN RETURN; END IF;
  UPDATE pipeline.rabbit_target SET probe_owner=incarnation,probe_generation=pipeline.rabbit_target.probe_generation+1,probe_until=now_at+lease_ms*interval '1 millisecond' WHERE destination_id=target RETURNING * INTO t;
  n:=1;
 END IF;
 FOR d IN SELECT * FROM pipeline.delivery_intents i WHERE i.destination_id=target AND i.kind='rabbitmq' AND
 ((i.state IN ('pending','retry_wait') AND i.next_retry_at<=now_at) OR (i.state='leased' AND i.lease_until<=now_at)) ORDER BY i.next_retry_at,i.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
  IF d.state='leased' THEN UPDATE pipeline.rabbit_attempts SET finished_at=now_at,outcome='expired',context='Lease expired before local outcome' WHERE pipeline.rabbit_attempts.attempt_id=d.rabbit_attempt_id AND finished_at IS NULL; END IF;
  a:=gen_random_uuid();
  INSERT INTO pipeline.rabbit_attempts(attempt_id,event_id,destination_id,claim_generation,owner_id) VALUES(a,d.event_id,target,d.claim_generation+1,incarnation);
  UPDATE pipeline.delivery_intents SET state='leased',owner_id=incarnation,lease_until=clock_timestamp()+lease_ms*interval '1 millisecond',claim_generation=d.claim_generation+1,rabbit_attempt_id=a,error_class=NULL WHERE pipeline.delivery_intents.event_id=d.event_id AND kind='rabbitmq';
  DELETE FROM pipeline.rabbit_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31;
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; wire_bytes:=octet_length(pipeline.rabbit_wire(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;
CREATE FUNCTION pipeline.rabbit_renew(target uuid, target_generation bigint, id text, incarnation uuid, gen bigint, lease_ms integer) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; t pipeline.rabbit_target; now_at timestamptz;
BEGIN
 IF lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 THEN RAISE EXCEPTION 'Invalid lease' USING ERRCODE='P7001'; END IF;
 -- Same target-before-intent lock order as claim, including cooldown probe renewal.
 SELECT * INTO t FROM pipeline.rabbit_target WHERE destination_id=target FOR UPDATE;
 IF NOT FOUND OR t.generation IS DISTINCT FROM target_generation OR t.mode='blocked' THEN RETURN false; END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND destination_id=target AND kind='rabbitmq' FOR UPDATE;
 now_at:=clock_timestamp();
 IF NOT FOUND OR d.state<>'leased' OR d.owner_id IS DISTINCT FROM incarnation OR d.claim_generation IS DISTINCT FROM gen OR d.lease_until<=now_at THEN RETURN false; END IF;
 UPDATE pipeline.delivery_intents SET lease_until=now_at+lease_ms*interval '1 millisecond' WHERE event_id=id AND kind='rabbitmq';
 UPDATE pipeline.rabbit_target SET probe_until=now_at+lease_ms*interval '1 millisecond' WHERE destination_id=target AND mode='cooldown' AND probe_owner=incarnation AND probe_until>now_at;
 RETURN true;
END $$;
CREATE FUNCTION pipeline.rabbit_admission(target uuid,target_generation bigint,incarnation uuid,probe_gen bigint,anchor text,claim_gen bigint,claim_attempt uuid,outcome text,reason text,delay_ms integer) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE t pipeline.rabbit_target; now_at timestamptz;
BEGIN
 IF outcome IS NULL OR outcome NOT IN ('healthy','transient','auth','configuration','integrity') OR reason IS NULL OR octet_length(reason)>512 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 1 AND 30000 THEN RAISE EXCEPTION 'Invalid admission outcome' USING ERRCODE='P7001'; END IF;
 SELECT * INTO STRICT t FROM pipeline.rabbit_target WHERE destination_id=target FOR UPDATE; now_at:=clock_timestamp();
 IF t.generation IS DISTINCT FROM target_generation OR t.mode IN ('preparing','blocked') THEN RETURN false; END IF;
 IF NOT EXISTS (SELECT 1 FROM pipeline.delivery_intents d JOIN pipeline.rabbit_attempts a ON a.attempt_id=d.rabbit_attempt_id WHERE d.event_id=anchor AND d.destination_id=target AND d.kind='rabbitmq' AND d.claim_generation=claim_gen AND a.attempt_id=claim_attempt AND a.owner_id=incarnation AND ((d.state='leased' AND d.owner_id=incarnation AND d.lease_until>now_at) OR (a.finished_at IS NOT NULL AND a.outcome<>'expired'))) THEN RETURN false; END IF;
 IF t.mode='cooldown' AND (t.probe_owner IS DISTINCT FROM incarnation OR t.probe_generation IS DISTINCT FROM probe_gen OR t.probe_until IS NULL OR t.probe_until<=now_at) THEN RETURN false; END IF;
 UPDATE pipeline.rabbit_target SET mode=CASE WHEN outcome='healthy' THEN 'ready' WHEN outcome='transient' THEN 'cooldown' ELSE 'blocked' END,
 reason=CASE WHEN outcome='healthy' THEN NULL ELSE rabbit_admission.reason END,failures=CASE WHEN outcome='healthy' THEN 0 ELSE failures+1 END,
 next_probe_at=now_at+delay_ms*interval '1 millisecond',probe_owner=NULL,probe_until=NULL WHERE destination_id=target;
 RETURN true;
END $$;
CREATE FUNCTION pipeline.rabbit_settle(target uuid,target_generation bigint,id text,incarnation uuid,gen bigint,outcome text,channel uuid,context text,delay_ms integer) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents; now_at timestamptz;
BEGIN
 IF outcome IS NULL OR outcome NOT IN ('confirmed','transient','auth','configuration','integrity') OR (outcome='confirmed' AND channel IS NULL) OR context IS NULL OR octet_length(context)>2048 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 1 AND 30000 THEN RAISE EXCEPTION 'Invalid broker settlement' USING ERRCODE='P7001'; END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND destination_id=target AND kind='rabbitmq' FOR UPDATE;
 now_at:=clock_timestamp();
 IF NOT FOUND OR d.state<>'leased' OR d.owner_id IS DISTINCT FROM incarnation OR d.claim_generation IS DISTINCT FROM gen OR d.lease_until<=now_at OR NOT EXISTS(SELECT 1 FROM pipeline.rabbit_target WHERE destination_id=target AND generation=target_generation AND registered_at IS NOT NULL AND mode<>'blocked') THEN RETURN 'stale'; END IF;
 UPDATE pipeline.rabbit_attempts SET finished_at=now_at,outcome=rabbit_settle.outcome,context=rabbit_settle.context,channel_id=channel WHERE attempt_id=d.rabbit_attempt_id;
 UPDATE pipeline.delivery_intents SET state=CASE WHEN outcome='confirmed' THEN 'satisfied' ELSE 'retry_wait' END,
 owner_id=NULL,lease_until=NULL,next_retry_at=now_at+delay_ms*interval '1 millisecond', error_class=CASE WHEN outcome='confirmed' THEN NULL ELSE outcome END,
 disposition=CASE WHEN outcome='confirmed' THEN 'broker_confirmed' ELSE NULL END, settled_at=CASE WHEN outcome='confirmed' THEN now_at ELSE NULL END
 WHERE event_id=id AND kind='rabbitmq'; RETURN 'settled';
END $$;
CREATE FUNCTION pipeline.rabbit_status(n integer) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF n IS NULL OR n NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'Invalid broker inspection bound' USING ERRCODE='P7001'; END IF;
 RETURN jsonb_build_object('observed_at',clock_timestamp()::text,'target',(SELECT to_jsonb(t)||jsonb_build_object('generation',t.generation::text,'probe_generation',t.probe_generation::text,'failures',t.failures::text) FROM pipeline.rabbit_target t),
 'counts',(SELECT jsonb_object_agg(state,c) FROM (SELECT state,count(*)::text c FROM pipeline.delivery_intents WHERE kind='rabbitmq' GROUP BY state) q),
 'attempts',COALESCE((SELECT jsonb_agg(to_jsonb(a)||jsonb_build_object('claim_generation',a.claim_generation::text)) FROM (SELECT * FROM pipeline.rabbit_attempts ORDER BY started_at DESC,attempt_id LIMIT n) a),'[]'::jsonb));
END $$;
CREATE FUNCTION pipeline.receipt_due(n integer,delay_ms integer) RETURNS TABLE(event_id text,body bytea,hash text) LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE o pipeline.consumer_observations;
BEGIN
 IF n IS NULL OR n NOT BETWEEN 1 AND 8 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 100 AND 30000 THEN RAISE EXCEPTION 'Invalid receipt lookup bound' USING ERRCODE='P7001'; END IF;
 IF NOT EXISTS(SELECT 1 FROM pipeline.rabbit_target WHERE registered_at IS NOT NULL) THEN RAISE EXCEPTION 'Consumer registration incomplete' USING ERRCODE='P7001'; END IF;
 FOR o IN SELECT * FROM pipeline.consumer_observations c WHERE c.state='pending' AND c.next_check_at<=clock_timestamp() ORDER BY c.next_check_at,c.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
 UPDATE pipeline.consumer_observations SET next_check_at=clock_timestamp()+delay_ms*interval '1 millisecond' WHERE pipeline.consumer_observations.event_id=o.event_id;
 RETURN QUERY SELECT e.event_id,e.body_bytes,e.content_sha256 FROM pipeline.events e WHERE e.event_id=o.event_id;
 END LOOP;
END $$;
CREATE FUNCTION pipeline.observe_receipt(id text,consumer uuid,registration uuid,epoch uuid,instance uuid,outcome text,bytes bytea,hash text,receipt text) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE o pipeline.consumer_observations; e pipeline.events;
BEGIN
 IF outcome IS NULL OR outcome NOT IN ('processed','quarantined') OR receipt IS NULL OR octet_length(receipt) NOT BETWEEN 1 AND 128 OR bytes IS NULL OR hash IS NULL THEN RAISE EXCEPTION 'Invalid receipt' USING ERRCODE='P7001'; END IF;
 SELECT * INTO STRICT o FROM pipeline.consumer_observations WHERE event_id=id FOR UPDATE;
 SELECT * INTO STRICT e FROM pipeline.events WHERE event_id=id;
 IF e.body_bytes IS DISTINCT FROM bytes OR e.content_sha256 IS DISTINCT FROM hash OR NOT EXISTS(SELECT 1 FROM pipeline.rabbit_target t WHERE t.consumer_id=consumer AND t.registration_id=registration AND t.source_epoch=epoch AND t.pipeline_id=instance AND t.registered_at IS NOT NULL) THEN RAISE EXCEPTION 'Consumer receipt identity/content mismatch' USING ERRCODE='P7001'; END IF;
 IF o.state<>'pending' THEN
 IF o.state IS DISTINCT FROM outcome OR o.consumer_id IS DISTINCT FROM consumer OR o.registration_id IS DISTINCT FROM registration OR o.receipt_id IS DISTINCT FROM receipt THEN RAISE EXCEPTION 'Conflicting terminal receipt' USING ERRCODE='P7001'; END IF; RETURN 'already_observed'; END IF;
 UPDATE pipeline.consumer_observations SET state=outcome,consumer_id=consumer,registration_id=registration,receipt_hash=hash,receipt_bytes=bytes,receipt_id=receipt,observed_at=clock_timestamp() WHERE event_id=id;
 RETURN 'observed';
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM PUBLIC,pipeline_rabbit,pipeline_receipts;
REVOKE ALL ON FUNCTION pipeline.guard_rabbit_target(),pipeline.guard_rabbit_attempt(),pipeline.rabbit_check_evidence(),pipeline.guard_observation(),pipeline.rabbit_identity(),pipeline.rabbit_wire(text),pipeline.rabbit_read(text[]),pipeline.rabbit_claim(uuid,bigint,uuid,integer,integer),pipeline.rabbit_renew(uuid,bigint,text,uuid,bigint,integer),pipeline.rabbit_settle(uuid,bigint,text,uuid,bigint,text,uuid,text,integer),pipeline.rabbit_admission(uuid,bigint,uuid,bigint,text,bigint,uuid,text,text,integer),pipeline.rabbit_status(integer),pipeline.receipt_due(integer,integer),pipeline.observe_receipt(text,uuid,uuid,uuid,uuid,text,bytea,text,text) FROM PUBLIC;
GRANT USAGE ON SCHEMA pipeline TO pipeline_rabbit,pipeline_receipts;
GRANT EXECUTE ON FUNCTION pipeline.rabbit_identity(),pipeline.rabbit_read(text[]),pipeline.rabbit_claim(uuid,bigint,uuid,integer,integer),pipeline.rabbit_renew(uuid,bigint,text,uuid,bigint,integer),pipeline.rabbit_settle(uuid,bigint,text,uuid,bigint,text,uuid,text,integer),pipeline.rabbit_admission(uuid,bigint,uuid,bigint,text,bigint,uuid,text,text,integer),pipeline.rabbit_status(integer) TO pipeline_rabbit;
GRANT EXECUTE ON FUNCTION pipeline.rabbit_identity(),pipeline.receipt_due(integer,integer),pipeline.observe_receipt(text,uuid,uuid,uuid,uuid,text,bytea,text,text) TO pipeline_receipts;
RESET ROLE;
