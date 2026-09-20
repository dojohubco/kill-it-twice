-- Additive run/checkpoint evidence; canonical events and receiver histories are unchanged.
CREATE ROLE pipeline_backfill NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_backfill;
GRANT USAGE ON SCHEMA pipeline TO pipeline_backfill;
SET LOCAL ROLE pipeline_owner;
CREATE TABLE pipeline.backfill_runs (
 run_id uuid PRIMARY KEY, pipeline_id uuid NOT NULL REFERENCES pipeline.source_binding(pipeline_id),
 source_epoch uuid NOT NULL REFERENCES pipeline.source_binding(source_epoch),
 range_count integer NOT NULL CHECK(range_count BETWEEN 1 AND 16), upper_key bigint NOT NULL CHECK(upper_key>=0),
 binding jsonb NOT NULL CHECK(jsonb_typeof(binding)='object' AND octet_length(binding::text)<=4096),
 source_observation jsonb NOT NULL CHECK(jsonb_typeof(source_observation)='object' AND octet_length(source_observation::text)<=8192),
 phase text NOT NULL DEFAULT 'scanning' CHECK(phase IN ('scanning','sealing','importing','draining','complete','complete_with_errors')),
 desired_paused boolean NOT NULL DEFAULT false, blocked_reason text CHECK(blocked_reason IS NULL OR octet_length(blocked_reason)<=256),
 fence jsonb CHECK(fence IS NULL OR (jsonb_typeof(fence)='object' AND octet_length(fence::text)<=8192)),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), sealed_at timestamptz, completed_at timestamptz,
 CHECK((phase IN ('scanning','sealing') AND fence IS NULL AND sealed_at IS NULL AND completed_at IS NULL) OR
 (phase='importing' AND fence IS NOT NULL AND sealed_at IS NULL AND completed_at IS NULL) OR
 (phase='draining' AND fence IS NOT NULL AND sealed_at IS NOT NULL AND completed_at IS NULL) OR
 (phase IN ('complete','complete_with_errors') AND fence IS NOT NULL AND sealed_at IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE UNIQUE INDEX one_unfinished_backfill ON pipeline.backfill_runs(pipeline_id) WHERE phase NOT IN ('complete','complete_with_errors');
CREATE TABLE pipeline.backfill_ranges (
 run_id uuid NOT NULL REFERENCES pipeline.backfill_runs(run_id), range_no integer NOT NULL CHECK(range_no BETWEEN 0 AND 16),
 lower_key bigint NOT NULL CHECK(lower_key>=0), upper_key bigint NOT NULL CHECK(upper_key>=lower_key), checkpoint bigint NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','closed','blocked')),
 generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0), owner_id uuid, lease_until timestamptz,
 next_eligible_at timestamptz NOT NULL DEFAULT clock_timestamp(), reason text CHECK(reason IS NULL OR octet_length(reason)<=256), last_batch uuid,
 PRIMARY KEY(run_id,range_no), CHECK(checkpoint BETWEEN lower_key AND upper_key),
 CHECK((state='leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL AND isfinite(lease_until) AND generation>0) OR (state<>'leased' AND owner_id IS NULL AND lease_until IS NULL)),
 CHECK((state='blocked' AND reason IS NOT NULL) OR (state<>'blocked' AND reason IS NULL))
);
CREATE INDEX backfill_range_due ON pipeline.backfill_ranges(run_id,next_eligible_at,range_no) WHERE state IN ('pending','leased');
CREATE TABLE pipeline.backfill_batches (
 batch_id uuid PRIMARY KEY, run_id uuid NOT NULL, range_no integer NOT NULL,
 owner_id uuid NOT NULL, generation bigint NOT NULL CHECK(generation>0), lease_until timestamptz NOT NULL,
 previous_key bigint NOT NULL CHECK(previous_key>=0), next_key bigint NOT NULL CHECK(next_key>=previous_key), eof boolean NOT NULL,
 items jsonb NOT NULL CHECK(jsonb_typeof(items)='array' AND jsonb_array_length(items)<=16 AND octet_length(items::text)<=8192),
 observation jsonb NOT NULL CHECK(jsonb_typeof(observation)='object' AND octet_length(observation::text)<=8192),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(run_id,range_no) REFERENCES pipeline.backfill_ranges(run_id,range_no),
 UNIQUE(run_id,range_no,batch_id), UNIQUE(run_id,batch_id)
);
ALTER TABLE pipeline.backfill_ranges ADD FOREIGN KEY(run_id,range_no,last_batch) REFERENCES pipeline.backfill_batches(run_id,range_no,batch_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE pipeline.backfill_members (
 run_id uuid NOT NULL REFERENCES pipeline.backfill_runs(run_id), event_id text NOT NULL REFERENCES pipeline.events(event_id), first_batch uuid NOT NULL,
 PRIMARY KEY(run_id,event_id), FOREIGN KEY(run_id,first_batch) REFERENCES pipeline.backfill_batches(run_id,batch_id)
);
CREATE INDEX backfill_members_batch ON pipeline.backfill_members(first_batch,event_id);
CREATE FUNCTION pipeline.backfill_binding(epoch uuid,instance uuid) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE value jsonb;
BEGIN
 SELECT jsonb_build_object('source_epoch',s.source_epoch,'pipeline_id',s.pipeline_id,'codec',s.payload_encoding,
 'es_destination',e.destination_id,'es_generation',e.generation::text,'es_receiver',e.receiver_identity,
 'rabbit_destination',r.destination_id,'rabbit_generation',r.generation::text,'rabbit_registration',t.registration_id,'consumer_id',t.consumer_id)
 INTO value FROM pipeline.source_binding s CROSS JOIN pipeline.destinations e CROSS JOIN pipeline.destinations r JOIN pipeline.rabbit_target t ON t.destination_id=r.destination_id
 JOIN pipeline.es_target x ON x.destination_id=e.destination_id
 WHERE s.pipeline_id=instance AND s.source_epoch=epoch AND e.kind='elasticsearch' AND r.kind='rabbitmq' AND e.state='bound' AND r.state='bound' AND x.registered_at IS NOT NULL AND t.registered_at IS NOT NULL
 AND e.receiver_identity=x.index_uuid AND r.receiver_identity=t.registration_id::text AND t.pipeline_id=instance AND t.source_epoch=epoch;
 IF value IS NULL THEN RAISE EXCEPTION 'Backfill target binding incomplete or inconsistent' USING ERRCODE='P8001'; END IF; RETURN value;
END $$;
CREATE FUNCTION pipeline.guard_backfill_run() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.phase IN ('complete','complete_with_errors') OR
 ROW(NEW.run_id,NEW.pipeline_id,NEW.source_epoch,NEW.range_count,NEW.upper_key,NEW.binding,NEW.source_observation,NEW.created_at) IS DISTINCT FROM ROW(OLD.run_id,OLD.pipeline_id,OLD.source_epoch,OLD.range_count,OLD.upper_key,OLD.binding,OLD.source_observation,OLD.created_at) OR
 (OLD.fence IS NOT NULL AND NEW.fence IS DISTINCT FROM OLD.fence) OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS DISTINCT FROM OLD.sealed_at) OR
 NOT (NEW.phase=OLD.phase OR (OLD.phase='scanning' AND NEW.phase='sealing') OR (OLD.phase='sealing' AND NEW.phase='importing') OR (OLD.phase='importing' AND NEW.phase='draining') OR (OLD.phase='draining' AND NEW.phase IN ('complete','complete_with_errors'))) THEN RAISE EXCEPTION 'Immutable run identity or invalid transition' USING ERRCODE='P8003'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER backfill_run_guard BEFORE UPDATE OR DELETE ON pipeline.backfill_runs FOR EACH ROW EXECUTE FUNCTION pipeline.guard_backfill_run();
CREATE FUNCTION pipeline.guard_backfill_range() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE b pipeline.backfill_batches;
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state='closed' OR ROW(NEW.run_id,NEW.range_no,NEW.lower_key,NEW.upper_key) IS DISTINCT FROM ROW(OLD.run_id,OLD.range_no,OLD.lower_key,OLD.upper_key) OR NEW.generation<OLD.generation OR NEW.checkpoint<OLD.checkpoint THEN RAISE EXCEPTION 'Immutable range or regressing progress' USING ERRCODE='P8003'; END IF;
 IF NEW.checkpoint IS DISTINCT FROM OLD.checkpoint OR NEW.last_batch IS DISTINCT FROM OLD.last_batch OR NEW.state='closed' THEN
  SELECT * INTO b FROM pipeline.backfill_batches WHERE batch_id=NEW.last_batch AND run_id=NEW.run_id AND range_no=NEW.range_no;
  IF NOT FOUND OR NEW.last_batch IS NOT DISTINCT FROM OLD.last_batch OR b.previous_key<>OLD.checkpoint OR b.next_key<>NEW.checkpoint OR b.owner_id IS DISTINCT FROM OLD.owner_id OR b.generation<>OLD.generation OR b.eof IS DISTINCT FROM (NEW.state='closed') OR OLD.state<>'leased' THEN RAISE EXCEPTION 'Checkpoint lacks matching committed batch' USING ERRCODE='P8003'; END IF;
 END IF; RETURN NEW;
END $$;
CREATE TRIGGER backfill_range_guard BEFORE UPDATE OR DELETE ON pipeline.backfill_ranges FOR EACH ROW EXECUTE FUNCTION pipeline.guard_backfill_range();
CREATE FUNCTION pipeline.guard_backfill_member() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT FROM pipeline.backfill_runs WHERE run_id=NEW.run_id AND phase IN ('scanning','importing')) OR NOT EXISTS(SELECT FROM pipeline.backfill_batches b,jsonb_array_elements(b.items) x WHERE b.batch_id=NEW.first_batch AND b.run_id=NEW.run_id AND x->>'event_id'=NEW.event_id) THEN RAISE EXCEPTION 'Unsealed matching batch required for membership' USING ERRCODE='P8003'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER backfill_member_guard BEFORE INSERT ON pipeline.backfill_members FOR EACH ROW EXECUTE FUNCTION pipeline.guard_backfill_member();
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['backfill_runs','backfill_ranges','backfill_batches','backfill_members'] LOOP
 EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON pipeline.%I EXECUTE FUNCTION pipeline.immutable()',t);
 END LOOP;
 FOREACH t IN ARRAY ARRAY['backfill_batches','backfill_members'] LOOP
 EXECUTE format('CREATE TRIGGER no_rewrite BEFORE UPDATE OR DELETE ON pipeline.%I FOR EACH ROW EXECUTE FUNCTION pipeline.immutable()',t);
 END LOOP;
END $$;
CREATE FUNCTION pipeline.check_backfill_batch() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_ranges; item jsonb;
BEGIN
 SELECT * INTO STRICT r FROM pipeline.backfill_ranges WHERE run_id=NEW.run_id AND range_no=NEW.range_no;
 IF r.last_batch IS DISTINCT FROM NEW.batch_id OR r.checkpoint<>NEW.next_key OR clock_timestamp()>=NEW.lease_until THEN RAISE EXCEPTION 'Incomplete or expired page progress at COMMIT' USING ERRCODE='P8002'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(NEW.items) LOOP
  IF NOT EXISTS(SELECT FROM pipeline.events e JOIN pipeline.backfill_members m USING(event_id) WHERE m.run_id=NEW.run_id AND e.event_id=item->>'event_id' AND e.content_sha256=item->>'hash') THEN RAISE EXCEPTION 'Batch member evidence missing' USING ERRCODE='P8003'; END IF;
  PERFORM pipeline.assert_obligations(item->>'event_id');
 END LOOP; RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER backfill_batch_complete AFTER INSERT ON pipeline.backfill_batches DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_backfill_batch();
CREATE FUNCTION pipeline.backfill_run(id uuid) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT to_jsonb(r)||jsonb_build_object('upper_key',upper_key::text) FROM pipeline.backfill_runs r WHERE run_id=id
$$;
CREATE FUNCTION pipeline.start_backfill(id uuid,epoch uuid,instance uuid,n integer,source_observation jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; binding jsonb; u bigint; inserted integer; parts integer; i integer; lo bigint; hi bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR n IS NULL OR n NOT BETWEEN 1 AND 16 OR id IS NULL OR source_observation IS NULL OR octet_length(source_observation::text)>8192 OR source_observation->>'source_epoch' IS DISTINCT FROM epoch::text OR source_observation->>'pipeline_id' IS DISTINCT FROM instance::text OR source_observation->>'codec' IS DISTINCT FROM 'pg18-jsonb-text/v1' OR source_observation->>'upper_key' IS NULL THEN RAISE EXCEPTION 'Invalid run request' USING ERRCODE='P8001'; END IF;
 binding:=pipeline.backfill_binding(epoch,instance); u:=(source_observation->>'upper_key')::bigint;
 INSERT INTO pipeline.backfill_runs(run_id,pipeline_id,source_epoch,range_count,upper_key,binding,source_observation) VALUES(id,instance,epoch,n,u,binding,source_observation) ON CONFLICT(run_id) DO NOTHING;
 GET DIAGNOSTICS inserted=ROW_COUNT;
 SELECT * INTO STRICT r FROM pipeline.backfill_runs WHERE run_id=id;
 IF r.pipeline_id IS DISTINCT FROM instance OR r.source_epoch IS DISTINCT FROM epoch OR r.range_count IS DISTINCT FROM n OR r.binding IS DISTINCT FROM binding THEN RAISE EXCEPTION 'Run request key conflict' USING ERRCODE='P8001'; END IF;
 IF inserted=1 THEN
  parts:=greatest(1,least(n,u)::integer);
  FOR i IN 1..parts LOOP
   lo:=floor(u::numeric*(i-1)/parts)::bigint; hi:=floor(u::numeric*i/parts)::bigint;
   INSERT INTO pipeline.backfill_ranges(run_id,range_no,lower_key,upper_key,checkpoint,state) VALUES(id,i,lo,hi,lo,CASE WHEN u=0 THEN 'closed' ELSE 'pending' END);
  END LOOP;
 END IF;
 RETURN pipeline.backfill_run(id);
END $$;
CREATE FUNCTION pipeline.backfill_validate(id uuid) RETURNS pipeline.backfill_runs LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 SELECT * INTO STRICT r FROM pipeline.backfill_runs WHERE run_id=id;
 IF r.binding IS DISTINCT FROM pipeline.backfill_binding(r.source_epoch,r.pipeline_id) THEN RAISE EXCEPTION 'Run binding changed' USING ERRCODE='P8001'; END IF;
 RETURN r;
END $$;
CREATE FUNCTION pipeline.backfill_claim(id uuid,incarnation uuid,lease_ms integer) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; q pipeline.backfill_ranges; now_at timestamptz;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR incarnation IS NULL OR lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 THEN RAISE EXCEPTION 'Invalid backfill claim' USING ERRCODE='P8003'; END IF;
 PERFORM 1 FROM pipeline.backfill_runs WHERE run_id=id FOR SHARE; r:=pipeline.backfill_validate(id);
 IF r.desired_paused OR r.phase NOT IN ('scanning','importing') THEN RETURN NULL; END IF;
 SELECT * INTO q FROM pipeline.backfill_ranges WHERE run_id=id AND ((r.phase='scanning' AND range_no>0) OR (r.phase='importing' AND range_no=0)) AND
 ((state='pending' AND next_eligible_at<=clock_timestamp()) OR (state='leased' AND lease_until<=clock_timestamp())) ORDER BY range_no FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF (q.last_batch IS NULL AND q.checkpoint<>q.lower_key) OR (q.last_batch IS NOT NULL AND NOT EXISTS(SELECT FROM pipeline.backfill_batches b WHERE b.batch_id=q.last_batch AND b.next_key=q.checkpoint AND b.run_id=id AND b.range_no=q.range_no)) THEN
 UPDATE pipeline.backfill_ranges SET state='blocked',owner_id=NULL,lease_until=NULL,reason='checkpoint_evidence' WHERE run_id=id AND range_no=q.range_no; RETURN NULL; END IF;
 now_at:=clock_timestamp();
 UPDATE pipeline.backfill_ranges SET state='leased',generation=generation+1,owner_id=incarnation,lease_until=now_at+lease_ms*interval '1 millisecond' WHERE run_id=id AND range_no=q.range_no RETURNING * INTO q;
 RETURN to_jsonb(q)||jsonb_build_object('lower_key',q.lower_key::text,'upper_key',q.upper_key::text,'checkpoint',q.checkpoint::text,'generation',q.generation::text,'claimed_at',now_at::text);
END $$;
CREATE FUNCTION pipeline.backfill_change(id uuid,num integer,incarnation uuid,gen bigint,action text,duration_ms integer,reason text DEFAULT NULL) RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE q pipeline.backfill_ranges;
BEGIN
 IF action IS NULL OR action NOT IN ('renew','defer','block') OR duration_ms IS NULL OR duration_ms NOT BETWEEN 1 AND 30000 OR (action='block' AND (reason IS NULL OR reason NOT IN ('oversized_record','missing_revision','integrity'))) THEN RAISE EXCEPTION 'Invalid range transition' USING ERRCODE='P8003'; END IF;
 PERFORM 1 FROM pipeline.backfill_runs WHERE run_id=id FOR SHARE; PERFORM pipeline.backfill_validate(id);
 SELECT * INTO q FROM pipeline.backfill_ranges WHERE run_id=id AND range_no=num FOR UPDATE;
 IF NOT FOUND OR q.state<>'leased' OR q.owner_id IS DISTINCT FROM incarnation OR q.generation IS DISTINCT FROM gen OR q.lease_until<=clock_timestamp() THEN RETURN false; END IF;
 IF action='renew' THEN UPDATE pipeline.backfill_ranges SET lease_until=clock_timestamp()+duration_ms*interval '1 millisecond' WHERE run_id=id AND range_no=num;
 ELSE UPDATE pipeline.backfill_ranges SET state=CASE WHEN action='block' THEN 'blocked' ELSE 'pending' END,owner_id=NULL,lease_until=NULL,next_eligible_at=clock_timestamp()+duration_ms*interval '1 millisecond',reason=CASE WHEN action='block' THEN backfill_change.reason END WHERE run_id=id AND range_no=num; END IF;
 RETURN true;
END $$;
CREATE FUNCTION pipeline.backfill_page(id uuid,num integer,incarnation uuid,gen bigint,batch uuid,previous_key bigint,next_key bigint,eof boolean,inputs jsonb,observation jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; q pipeline.backfill_ranges; old pipeline.backfill_batches; x jsonb; b jsonb; bytes bytea; items jsonb:='[]'; total bigint:=0; last_key bigint:=previous_key; key bigint; status text; result jsonb:='[]';
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR inputs IS NULL OR jsonb_typeof(inputs)<>'array' OR jsonb_array_length(inputs)>16 OR octet_length(inputs::text)>600000 OR observation IS NULL OR jsonb_typeof(observation)<>'object' OR octet_length(observation::text)>8192 OR batch IS NULL OR eof IS NULL OR previous_key IS NULL OR next_key IS NULL THEN RAISE EXCEPTION 'Invalid page bound' USING ERRCODE='P8003'; END IF;
 PERFORM 1 FROM pipeline.backfill_runs WHERE run_id=id FOR SHARE; r:=pipeline.backfill_validate(id);
 SELECT * INTO STRICT q FROM pipeline.backfill_ranges WHERE run_id=id AND range_no=num FOR UPDATE;
 FOR x IN SELECT value FROM jsonb_array_elements(inputs) LOOP
  bytes:=decode(x->>'body','hex'); b:=convert_from(bytes,'UTF8')::jsonb; key:=(x->>'key')::bigint;
  IF bytes IS NULL OR key IS NULL OR x->>'hash' IS NULL OR key<=last_key OR key>q.upper_key OR (num>0 AND key IS DISTINCT FROM (b->>'entity_id')::bigint) OR (num=0 AND b->>'kind' IS DISTINCT FROM 'mutation') THEN RAISE EXCEPTION 'Page key/content mismatch' USING ERRCODE='P8003'; END IF;
  total:=total+octet_length(bytes)+93; IF octet_length(bytes)+93>65536 OR total>262144 THEN RAISE EXCEPTION 'Page exceeds transfer bound' USING ERRCODE='P8003'; END IF;
  items:=items||jsonb_build_array(jsonb_build_object('key',key::text,'event_id',b->>'event_id','hash',x->>'hash')); last_key:=key;
 END LOOP;
 IF (SELECT count(DISTINCT value->>'event_id') FROM jsonb_array_elements(items))<>jsonb_array_length(items) THEN RAISE EXCEPTION 'Duplicate page revision identity' USING ERRCODE='P8003'; END IF;
 IF next_key IS DISTINCT FROM last_key OR next_key>q.upper_key OR (next_key=previous_key AND NOT eof) THEN RAISE EXCEPTION 'Invalid page checkpoint' USING ERRCODE='P8003'; END IF;
 SELECT * INTO old FROM pipeline.backfill_batches WHERE batch_id=batch;
 IF FOUND THEN
  IF ROW(old.run_id,old.range_no,old.owner_id,old.generation,old.previous_key,old.next_key,old.eof,old.items,old.observation) IS DISTINCT FROM ROW(id,num,incarnation,gen,previous_key,next_key,eof,items,observation) OR EXISTS(SELECT FROM jsonb_array_elements(inputs) supplied WHERE NOT EXISTS(SELECT FROM pipeline.events e WHERE e.event_id=convert_from(decode(supplied->>'body','hex'),'UTF8')::jsonb->>'event_id' AND e.body_bytes=decode(supplied->>'body','hex') AND e.content_sha256=supplied->>'hash')) THEN RAISE EXCEPTION 'Batch request key conflict' USING ERRCODE='P8003'; END IF;
  FOR x IN SELECT value FROM jsonb_array_elements(items) LOOP PERFORM pipeline.assert_obligations(x->>'event_id'); END LOOP;
  RETURN jsonb_build_object('batch_id',batch,'replayed',true,'next_key',old.next_key::text,'eof',old.eof,'items',old.items,'created_at',old.created_at);
 END IF;
 IF r.phase IS DISTINCT FROM (CASE WHEN num=0 THEN 'importing' ELSE 'scanning' END) OR q.state<>'leased' OR q.owner_id IS DISTINCT FROM incarnation OR q.generation IS DISTINCT FROM gen OR q.checkpoint IS DISTINCT FROM previous_key OR q.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'Stale page ownership or cursor' USING ERRCODE='P8002'; END IF;
 FOR x IN SELECT value FROM jsonb_array_elements(inputs) ORDER BY (convert_from(decode(value->>'body','hex'),'UTF8')::jsonb->>'event_id') COLLATE "C" LOOP
  status:=pipeline.stage_bound_event(r.pipeline_id,decode(x->>'body','hex'),x->>'hash');
  result:=result||jsonb_build_array(jsonb_build_object('event_id',convert_from(decode(x->>'body','hex'),'UTF8')::jsonb->>'event_id','status',status));
 END LOOP;
 INSERT INTO pipeline.backfill_batches(batch_id,run_id,range_no,owner_id,generation,lease_until,previous_key,next_key,eof,items,observation) VALUES(batch,id,num,incarnation,gen,q.lease_until,previous_key,next_key,eof,items,observation) RETURNING * INTO old;
 INSERT INTO pipeline.backfill_members(run_id,event_id,first_batch) SELECT id,member->>'event_id',batch FROM jsonb_array_elements(items) member ON CONFLICT(run_id,event_id) DO NOTHING;
 UPDATE pipeline.backfill_ranges SET checkpoint=next_key,last_batch=batch,state=CASE WHEN eof THEN 'closed' ELSE 'pending' END,owner_id=NULL,lease_until=NULL WHERE run_id=id AND range_no=num;
 IF q.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'Page ownership expired during staging' USING ERRCODE='P8002'; END IF;
 RETURN jsonb_build_object('batch_id',batch,'replayed',false,'next_key',next_key::text,'eof',eof,'items',items,'staged',result,'created_at',old.created_at);
END $$;
CREATE FUNCTION pipeline.backfill_pause(id uuid,paused boolean) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 IF paused IS NULL THEN RAISE EXCEPTION 'Invalid pause request' USING ERRCODE='P8003'; END IF;
 SELECT * INTO STRICT r FROM pipeline.backfill_runs WHERE run_id=id FOR UPDATE; PERFORM pipeline.backfill_validate(id);
 IF r.phase NOT IN ('complete','complete_with_errors') THEN UPDATE pipeline.backfill_runs SET desired_paused=paused WHERE run_id=id; END IF;
END $$;
CREATE FUNCTION pipeline.attach_backfill_fence(id uuid,fence jsonb) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; count bigint; bound bigint;
BEGIN
 SELECT * INTO STRICT r FROM pipeline.backfill_runs WHERE run_id=id FOR UPDATE; PERFORM pipeline.backfill_validate(id);
 IF fence IS NULL OR fence->>'run_id' IS DISTINCT FROM id::text OR fence->>'pipeline_id' IS DISTINCT FROM r.pipeline_id::text OR fence->>'source_epoch' IS DISTINCT FROM r.source_epoch::text OR fence->>'codec' IS DISTINCT FROM 'pg18-jsonb-text/v1' OR fence->>'snapshot' IS NULL OR fence->>'observed_at' IS NULL OR fence->>'member_count' IS NULL OR fence->>'max_key' IS NULL THEN RAISE EXCEPTION 'Fence attachment mismatch' USING ERRCODE='P8001'; END IF;
 count:=(fence->>'member_count')::bigint; bound:=(fence->>'max_key')::bigint;
 IF count<0 OR bound<0 OR (count=0) IS DISTINCT FROM (bound=0) THEN RAISE EXCEPTION 'Invalid fence cardinality' USING ERRCODE='P8003'; END IF;
 IF r.fence IS NOT NULL THEN IF r.fence IS DISTINCT FROM fence THEN RAISE EXCEPTION 'Fence is immutable' USING ERRCODE='P8003'; END IF; RETURN; END IF;
 IF r.phase<>'sealing' THEN RAISE EXCEPTION 'Scan ranges not sealed' USING ERRCODE='P8003'; END IF;
 UPDATE pipeline.backfill_runs SET phase='importing',fence=attach_backfill_fence.fence WHERE run_id=id;
 INSERT INTO pipeline.backfill_ranges(run_id,range_no,lower_key,upper_key,checkpoint) VALUES(id,0,0,bound,0);
END $$;
CREATE FUNCTION pipeline.backfill_counts(id uuid) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 WITH evidence AS (
 SELECT m.event_id,es.state es_state,mq.state mq_state,o.state consumer_state,
 (e.event_id IS NOT NULL AND pipeline.valid_body(e) IS TRUE AND e.content_sha256=encode(sha256(e.body_bytes),'hex') AND
 es.event_id IS NOT NULL AND es.destination_id=(r.binding->>'es_destination')::uuid AND mq.event_id IS NOT NULL AND mq.destination_id=(r.binding->>'rabbit_destination')::uuid AND o.event_id IS NOT NULL AND
 (es.state<>'satisfied' OR (a.attempt_id IS NOT NULL AND a.event_id=e.event_id AND a.destination_id=es.destination_id AND a.claim_generation=es.claim_generation AND a.outcome=es.disposition AND a.finished_at=es.settled_at AND a.remote_version=es.remote_version AND w.event_id IS NOT NULL AND w.source_epoch=e.source_epoch AND w.entity_id=e.entity_id AND w.entity_version=es.remote_version AND ((es.disposition IN ('applied','already_applied') AND w.event_id=e.event_id) OR (es.disposition='superseded' AND w.entity_version>e.entity_version)))) AND
 (es.state<>'dead_letter' OR (a.attempt_id IS NOT NULL AND a.event_id=e.event_id AND a.outcome=es.error_class AND a.finished_at=es.settled_at AND dl.attempt_id=es.attempt_id AND dl.error_class=es.error_class)) AND
 (mq.state<>'satisfied' OR (mq.disposition='broker_confirmed' AND ra.attempt_id IS NOT NULL AND ra.event_id=e.event_id AND ra.destination_id=mq.destination_id AND ra.claim_generation=mq.claim_generation AND ra.channel_id IS NOT NULL AND ra.outcome='confirmed' AND ra.finished_at=mq.settled_at)) AND
 (o.state='pending' OR (o.state IN ('processed','quarantined') AND o.consumer_id=(r.binding->>'consumer_id')::uuid AND o.registration_id=(r.binding->>'rabbit_registration')::uuid AND o.receipt_bytes=e.body_bytes AND o.receipt_hash=e.content_sha256 AND o.receipt_id IS NOT NULL AND o.observed_at IS NOT NULL))) IS TRUE AS valid
 FROM pipeline.backfill_members m JOIN pipeline.backfill_runs r USING(run_id)
 LEFT JOIN pipeline.events e USING(event_id)
 LEFT JOIN pipeline.delivery_intents es ON es.event_id=m.event_id AND es.kind='elasticsearch'
 LEFT JOIN pipeline.delivery_intents mq ON mq.event_id=m.event_id AND mq.kind='rabbitmq'
 LEFT JOIN pipeline.consumer_observations o ON o.event_id=m.event_id
 LEFT JOIN pipeline.es_attempts a ON a.attempt_id=es.attempt_id
 LEFT JOIN pipeline.events w ON w.event_id=es.witness_event_id
 LEFT JOIN pipeline.es_dead_letters dl ON dl.event_id=es.event_id AND dl.destination_id=es.destination_id
 LEFT JOIN pipeline.rabbit_attempts ra ON ra.attempt_id=mq.rabbit_attempt_id
 WHERE m.run_id=id
 ) SELECT jsonb_build_object('required',count(*)::text,'invalid',count(*) FILTER(WHERE NOT valid)::text,
 'es_satisfied',count(*) FILTER(WHERE es_state='satisfied')::text,'es_errors',count(*) FILTER(WHERE es_state='dead_letter')::text,'es_pending',count(*) FILTER(WHERE es_state NOT IN ('satisfied','dead_letter') OR es_state IS NULL)::text,
 'rabbit_satisfied',count(*) FILTER(WHERE mq_state='satisfied')::text,'rabbit_pending',count(*) FILTER(WHERE mq_state IS DISTINCT FROM 'satisfied')::text,
 'consumer_processed',count(*) FILTER(WHERE consumer_state='processed')::text,'consumer_errors',count(*) FILTER(WHERE consumer_state='quarantined')::text,'consumer_pending',count(*) FILTER(WHERE consumer_state='pending' OR consumer_state IS NULL)::text) FROM evidence
$$;
CREATE FUNCTION pipeline.backfill_progress_valid(id uuid) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT NOT EXISTS(SELECT FROM pipeline.backfill_ranges q LEFT JOIN pipeline.backfill_batches b ON b.batch_id=q.last_batch WHERE q.run_id=id AND
 ((q.last_batch IS NULL AND (q.checkpoint<>q.lower_key OR (q.state='closed' AND q.upper_key<>q.lower_key))) OR
 (q.last_batch IS NOT NULL AND (b.batch_id IS NULL OR b.run_id<>id OR b.range_no<>q.range_no OR b.next_key<>q.checkpoint OR b.eof IS DISTINCT FROM (q.state='closed')))))
 AND NOT EXISTS(SELECT FROM pipeline.backfill_batches b CROSS JOIN LATERAL jsonb_array_elements(b.items) x LEFT JOIN pipeline.backfill_members m ON m.run_id=b.run_id AND m.event_id=x->>'event_id' LEFT JOIN pipeline.events e ON e.event_id=m.event_id WHERE b.run_id=id AND (m.event_id IS NULL OR e.content_sha256 IS DISTINCT FROM x->>'hash'))
 AND NOT EXISTS(SELECT FROM pipeline.backfill_members m LEFT JOIN pipeline.backfill_batches b ON b.batch_id=m.first_batch AND b.run_id=m.run_id WHERE m.run_id=id AND (b.batch_id IS NULL OR NOT EXISTS(SELECT FROM jsonb_array_elements(b.items) x WHERE x->>'event_id'=m.event_id)))
$$;
CREATE FUNCTION pipeline.backfill_advance(id uuid) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; counts jsonb; imported bigint;
BEGIN
 SELECT * INTO STRICT r FROM pipeline.backfill_runs WHERE run_id=id FOR UPDATE; PERFORM pipeline.backfill_validate(id);
 IF r.phase IN ('complete','complete_with_errors') THEN RETURN pipeline.backfill_run(id); END IF;
 IF (r.phase='draining' OR NOT EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state<>'closed')) AND NOT pipeline.backfill_progress_valid(id) THEN UPDATE pipeline.backfill_runs SET blocked_reason='progress_evidence' WHERE run_id=id; RETURN pipeline.backfill_run(id); END IF;
 IF r.blocked_reason IS NOT NULL THEN RETURN pipeline.backfill_run(id); END IF;
 IF r.phase='scanning' AND NOT EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND range_no>0 AND state<>'closed') THEN
 UPDATE pipeline.backfill_runs SET phase='sealing' WHERE run_id=id;
 ELSIF r.phase='importing' AND EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND range_no=0 AND state='closed') THEN
 SELECT coalesce(sum(jsonb_array_length(items)),0) INTO imported FROM pipeline.backfill_batches WHERE run_id=id AND range_no=0;
 IF imported<>(r.fence->>'member_count')::bigint THEN UPDATE pipeline.backfill_runs SET blocked_reason='fence_cardinality' WHERE run_id=id;
 ELSE UPDATE pipeline.backfill_runs SET phase='draining',sealed_at=clock_timestamp() WHERE run_id=id; END IF;
 ELSIF r.phase='draining' AND r.blocked_reason IS NULL THEN
 counts:=pipeline.backfill_counts(id);
 IF (counts->>'invalid')::bigint>0 THEN UPDATE pipeline.backfill_runs SET blocked_reason='required_evidence' WHERE run_id=id;
 ELSIF (counts->>'es_pending')::bigint=0 AND (counts->>'rabbit_pending')::bigint=0 AND (counts->>'consumer_pending')::bigint=0 THEN
 UPDATE pipeline.backfill_runs SET phase=CASE WHEN (counts->>'es_errors')::bigint+(counts->>'consumer_errors')::bigint>0 THEN 'complete_with_errors' ELSE 'complete' END,completed_at=clock_timestamp() WHERE run_id=id;
 END IF;
 END IF;
 RETURN pipeline.backfill_run(id);
END $$;
CREATE FUNCTION pipeline.backfill_status(id uuid) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 r:=pipeline.backfill_validate(id);
 RETURN pipeline.backfill_run(id)||jsonb_build_object('observed_at',clock_timestamp()::text,'counts',pipeline.backfill_counts(id),
 'effective_paused',r.desired_paused AND NOT EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='leased' AND lease_until>clock_timestamp()),
 'blocked',r.blocked_reason IS NOT NULL OR EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='blocked'),
 'scan_observations',(SELECT coalesce(sum(jsonb_array_length(items)),0)::text FROM pipeline.backfill_batches WHERE run_id=id AND range_no>0),
 'imported',(SELECT coalesce(sum(jsonb_array_length(items)),0)::text FROM pipeline.backfill_batches WHERE run_id=id AND range_no=0),
 'ranges',(SELECT jsonb_agg(to_jsonb(q)||jsonb_build_object('lower_key',q.lower_key::text,'upper_key',q.upper_key::text,'checkpoint',q.checkpoint::text,'generation',q.generation::text) ORDER BY range_no) FROM pipeline.backfill_ranges q WHERE q.run_id=id));
END $$;
CREATE FUNCTION pipeline.check_backfill_partition() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE parts integer; i integer; q pipeline.backfill_ranges;
BEGIN
 parts:=greatest(1,least(NEW.range_count,NEW.upper_key)::integer);
 IF (SELECT count(*) FROM pipeline.backfill_ranges WHERE run_id=NEW.run_id AND range_no>0)<>parts THEN RAISE EXCEPTION 'Incomplete fixed range partition' USING ERRCODE='P8003'; END IF;
 FOR i IN 1..parts LOOP
 SELECT * INTO q FROM pipeline.backfill_ranges WHERE run_id=NEW.run_id AND range_no=i;
 IF NOT FOUND OR q.lower_key<>floor(NEW.upper_key::numeric*(i-1)/parts)::bigint OR q.upper_key<>floor(NEW.upper_key::numeric*i/parts)::bigint THEN RAISE EXCEPTION 'Invalid fixed range coverage' USING ERRCODE='P8003'; END IF;
 END LOOP; RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER backfill_partition_complete AFTER INSERT ON pipeline.backfill_runs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_backfill_partition();
REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM pipeline_backfill;
REVOKE ALL ON FUNCTION pipeline.backfill_binding(uuid,uuid),pipeline.guard_backfill_run(),pipeline.guard_backfill_range(),pipeline.guard_backfill_member(),pipeline.check_backfill_batch(),pipeline.backfill_run(uuid),pipeline.start_backfill(uuid,uuid,uuid,integer,jsonb),pipeline.backfill_validate(uuid),pipeline.backfill_claim(uuid,uuid,integer),pipeline.backfill_change(uuid,integer,uuid,bigint,text,integer,text),pipeline.backfill_page(uuid,integer,uuid,bigint,uuid,bigint,bigint,boolean,jsonb,jsonb),pipeline.backfill_pause(uuid,boolean),pipeline.attach_backfill_fence(uuid,jsonb),pipeline.backfill_counts(uuid),pipeline.backfill_progress_valid(uuid),pipeline.backfill_advance(uuid),pipeline.backfill_status(uuid),pipeline.check_backfill_partition() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.start_backfill(uuid,uuid,uuid,integer,jsonb),pipeline.backfill_claim(uuid,uuid,integer),pipeline.backfill_change(uuid,integer,uuid,bigint,text,integer,text),pipeline.backfill_page(uuid,integer,uuid,bigint,uuid,bigint,bigint,boolean,jsonb,jsonb),pipeline.backfill_pause(uuid,boolean),pipeline.attach_backfill_fence(uuid,jsonb),pipeline.backfill_advance(uuid),pipeline.backfill_status(uuid) TO pipeline_backfill;
RESET ROLE;
