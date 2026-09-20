-- Additive M6 control migration; administrator executes inside one owned transaction.
CREATE ROLE pipeline_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_operator;
SET LOCAL ROLE pipeline_owner;
ALTER TABLE pipeline.es_dead_letters DROP CONSTRAINT es_dead_letters_pkey;
ALTER TABLE pipeline.es_dead_letters ADD PRIMARY KEY(event_id,destination_id,attempt_id);
CREATE INDEX es_failure_page ON pipeline.es_dead_letters(event_id,destination_id,attempt_id);
CREATE TABLE pipeline.replay_requests (
 request_id uuid PRIMARY KEY, request_correlation uuid NOT NULL,
 kind text NOT NULL CHECK(kind='elasticsearch'), actor text NOT NULL CHECK(actor='local-operator'),
 reason text NOT NULL CHECK(octet_length(reason) BETWEEN 1 AND 512),
 event_id text NOT NULL, destination_id uuid NOT NULL, generation bigint NOT NULL CHECK(generation>0),
 terminal_attempt uuid NOT NULL, requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(event_id,destination_id,terminal_attempt) REFERENCES pipeline.es_dead_letters(event_id,destination_id,attempt_id)
);
CREATE TABLE pipeline.replay_items (
 request_id uuid PRIMARY KEY REFERENCES pipeline.replay_requests(request_id),
 event_id text NOT NULL, destination_id uuid NOT NULL, terminal_attempt uuid NOT NULL,
 scheduled_at timestamptz NOT NULL DEFAULT clock_timestamp(), schedule_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 FOREIGN KEY(event_id,destination_id,terminal_attempt) REFERENCES pipeline.es_dead_letters(event_id,destination_id,attempt_id)
);
CREATE TABLE pipeline.operator_receipts (
 request_id uuid PRIMARY KEY, request_correlation uuid NOT NULL,
 operation text NOT NULL CHECK(operation IN ('backfill_start','backfill_pause','backfill_resume','network_elasticsearch','network_rabbitmq')),
 target text NOT NULL CHECK(octet_length(target)<=128), actor text NOT NULL CHECK(actor='local-operator'),
 input jsonb NOT NULL CHECK(octet_length(input::text)<=4096), result jsonb NOT NULL CHECK(octet_length(result::text)<=16384),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY['replay_requests','replay_items','operator_receipts'] LOOP
 EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.%I FOR EACH ROW EXECUTE FUNCTION pipeline.immutable()',t);
 EXECUTE format('CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.%I EXECUTE FUNCTION pipeline.immutable()',t);
END LOOP; END $$;
CREATE OR REPLACE FUNCTION pipeline.guard_delivery() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.kind='elasticsearch' AND OLD.state='dead_letter' AND NEW.state='pending'
 AND NEW.attempt_id IS NULL AND NEW.settled_at IS NULL AND NEW.error_class IS NULL AND NEW.next_retry_at<>'infinity'
 AND (to_jsonb(NEW)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at'])=(to_jsonb(OLD)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at'])
 AND EXISTS(SELECT FROM pipeline.replay_items i JOIN pipeline.replay_requests r USING(request_id) WHERE i.event_id=OLD.event_id AND i.destination_id=OLD.destination_id AND i.terminal_attempt=OLD.attempt_id AND i.schedule_xid=pg_current_xact_id() AND r.event_id=i.event_id AND r.destination_id=i.destination_id AND r.terminal_attempt=i.terminal_attempt)
 THEN RETURN NEW; END IF;
 IF TG_OP<>'UPDATE' OR OLD.state IN ('satisfied','dead_letter') OR NEW.event_id<>OLD.event_id OR NEW.kind<>OLD.kind OR NEW.destination_id<>OLD.destination_id OR NEW.created_at<>OLD.created_at OR NEW.claim_generation<OLD.claim_generation THEN RAISE EXCEPTION 'Immutable delivery evidence' USING ERRCODE='P5001'; END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION pipeline.request_es_replay(key uuid,correlation uuid,id text,target uuid,gen bigint,attempt uuid,why text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE old pipeline.replay_requests; d pipeline.delivery_intents; result jsonb; t pipeline.es_target;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'READ COMMITTED required' USING ERRCODE='25001'; END IF;
 IF key IS NULL OR correlation IS NULL OR id IS NULL OR target IS NULL OR gen IS NULL OR attempt IS NULL OR why IS NULL OR octet_length(why) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'Invalid replay input' USING ERRCODE='22023'; END IF;
 -- Serializes the idempotency key even before its receipt exists. Hash collision only adds waiting.
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,914));
 SELECT * INTO old FROM pipeline.replay_requests WHERE request_id=key;
 IF FOUND THEN
  IF (old.event_id,old.destination_id,old.generation,old.terminal_attempt,old.reason) IS DISTINCT FROM (id,target,gen,attempt,why) THEN RAISE EXCEPTION 'Replay key conflict' USING ERRCODE='P9001'; END IF;
  RETURN (SELECT jsonb_build_object('request_id',key,'event_id',id,'attempt_id',attempt,'scheduled_at',scheduled_at,'replayed',true) FROM pipeline.replay_items WHERE request_id=key);
 END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND kind='elasticsearch' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Unknown event' USING ERRCODE='P9004'; END IF;
 SELECT * INTO t FROM pipeline.es_target WHERE destination_id=target;
 IF NOT FOUND OR t.generation<>gen OR t.registered_at IS NULL OR t.mode='preparing' OR NOT EXISTS(SELECT FROM pipeline.destinations WHERE destination_id=target AND generation=gen AND state='bound' AND receiver_identity=t.index_uuid) THEN RAISE EXCEPTION 'Receiver binding blocked' USING ERRCODE='P9002'; END IF;
 IF d.state<>'dead_letter' OR d.destination_id<>target OR d.attempt_id<>attempt THEN RAISE EXCEPTION 'Stale terminal attempt' USING ERRCODE='P9001'; END IF;
 INSERT INTO pipeline.replay_requests VALUES(key,correlation,'elasticsearch','local-operator',why,id,target,gen,attempt,clock_timestamp());
 INSERT INTO pipeline.replay_items(request_id,event_id,destination_id,terminal_attempt) VALUES(key,id,target,attempt);
 UPDATE pipeline.delivery_intents SET state='pending',attempt_id=NULL,settled_at=NULL,error_class=NULL,next_retry_at=clock_timestamp() WHERE event_id=id AND kind='elasticsearch';
 SELECT jsonb_build_object('request_id',key,'event_id',id,'attempt_id',attempt,'scheduled_at',scheduled_at,'replayed',false) INTO result FROM pipeline.replay_items WHERE request_id=key;
 RETURN result;
END $$;
CREATE OR REPLACE FUNCTION pipeline.es_claim(target uuid, target_generation bigint, incarnation uuid, n integer, lease_ms integer)
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
  DELETE FROM pipeline.es_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31 AND NOT EXISTS(SELECT FROM pipeline.es_dead_letters f WHERE f.attempt_id=h.attempt_id);
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; projection_bytes:=octet_length(pipeline.es_projection(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION pipeline.backfill_counts(id uuid) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
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
 LEFT JOIN pipeline.es_dead_letters dl ON dl.event_id=es.event_id AND dl.destination_id=es.destination_id AND dl.attempt_id=es.attempt_id
 LEFT JOIN pipeline.rabbit_attempts ra ON ra.attempt_id=mq.rabbit_attempt_id
 WHERE m.run_id=id
 ) SELECT jsonb_build_object('required',count(*)::text,'invalid',count(*) FILTER(WHERE NOT valid)::text,
 'es_satisfied',count(*) FILTER(WHERE es_state='satisfied')::text,'es_errors',count(*) FILTER(WHERE es_state='dead_letter')::text,'es_pending',count(*) FILTER(WHERE es_state NOT IN ('satisfied','dead_letter') OR es_state IS NULL)::text,
 'rabbit_satisfied',count(*) FILTER(WHERE mq_state='satisfied')::text,'rabbit_pending',count(*) FILTER(WHERE mq_state IS DISTINCT FROM 'satisfied')::text,
 'consumer_processed',count(*) FILTER(WHERE consumer_state='processed')::text,'consumer_errors',count(*) FILTER(WHERE consumer_state='quarantined')::text,'consumer_pending',count(*) FILTER(WHERE consumer_state='pending' OR consumer_state IS NULL)::text) FROM evidence
$$;
-- A bounded outcome aggregate survives pruning of diagnostic attempt rows.
CREATE TABLE pipeline.attempt_totals (
 sink text NOT NULL CHECK(sink IN ('elasticsearch','rabbitmq')),
 outcome_class text NOT NULL CHECK(outcome_class IN ('applied','already_applied','superseded','mapping','oversized','transient','auth','configuration','integrity','expired','confirmed','historical_unknown')),
 total bigint NOT NULL CHECK(total>=0), PRIMARY KEY(sink,outcome_class)
);
INSERT INTO pipeline.attempt_totals SELECT 'elasticsearch',outcome,count(*) FROM pipeline.es_attempts WHERE outcome IS NOT NULL GROUP BY outcome;
INSERT INTO pipeline.attempt_totals SELECT 'rabbitmq',outcome,count(*) FROM pipeline.rabbit_attempts WHERE outcome IS NOT NULL GROUP BY outcome;
INSERT INTO pipeline.attempt_totals VALUES
 ('elasticsearch','historical_unknown',(SELECT coalesce(sum(claim_generation),0) FROM pipeline.delivery_intents WHERE kind='elasticsearch')-(SELECT count(*) FROM pipeline.es_attempts)),
 ('rabbitmq','historical_unknown',(SELECT coalesce(sum(claim_generation),0) FROM pipeline.delivery_intents WHERE kind='rabbitmq')-(SELECT count(*) FROM pipeline.rabbit_attempts));
CREATE FUNCTION pipeline.count_attempt() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NEW.finished_at IS NOT NULL AND (TG_OP='INSERT' OR OLD.finished_at IS NULL) THEN
 INSERT INTO pipeline.attempt_totals VALUES(CASE TG_TABLE_NAME WHEN 'es_attempts' THEN 'elasticsearch' ELSE 'rabbitmq' END,NEW.outcome,1)
 ON CONFLICT(sink,outcome_class) DO UPDATE SET total=pipeline.attempt_totals.total+1;
 END IF; RETURN NULL;
END $$;
CREATE TRIGGER count_completed AFTER INSERT OR UPDATE ON pipeline.es_attempts FOR EACH ROW EXECUTE FUNCTION pipeline.count_attempt();
CREATE TRIGGER count_completed AFTER INSERT OR UPDATE ON pipeline.rabbit_attempts FOR EACH ROW EXECUTE FUNCTION pipeline.count_attempt();
CREATE FUNCTION pipeline.operator_backfill(key uuid,correlation uuid,op text,id uuid,epoch uuid,instance uuid,n integer,observation jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE old pipeline.operator_receipts; input jsonb; result jsonb;
BEGIN
 IF op NOT IN ('backfill_start','backfill_pause','backfill_resume') OR key IS NULL OR correlation IS NULL OR id IS NULL THEN RAISE EXCEPTION 'Invalid control input' USING ERRCODE='22023'; END IF;
 input:=jsonb_build_object('run_id',id,'epoch',epoch,'instance',instance,'ranges',n);
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,915));
 SELECT * INTO old FROM pipeline.operator_receipts WHERE request_id=key;
 IF FOUND THEN
  IF old.operation<>op OR old.input<>input THEN RAISE EXCEPTION 'Control key conflict' USING ERRCODE='P9001'; END IF;
  RETURN old.result||jsonb_build_object('replayed',true);
 END IF;
 IF op='backfill_start' THEN
  IF key<>id THEN RAISE EXCEPTION 'Run identity must equal key' USING ERRCODE='22023'; END IF;
  PERFORM pipeline.start_backfill(id,epoch,instance,n,observation);
 ELSE
  IF NOT EXISTS(SELECT FROM pipeline.backfill_runs WHERE run_id=id) THEN RAISE EXCEPTION 'Unknown run' USING ERRCODE='P9004'; END IF;
  PERFORM pipeline.backfill_pause(id,op='backfill_pause');
 END IF;
 result:=jsonb_build_object('run_id',id,'operation',op,'scheduled_at',clock_timestamp(),'replayed',false);
 INSERT INTO pipeline.operator_receipts VALUES(key,correlation,op,id::text,'local-operator',input,result,clock_timestamp(),clock_timestamp());
 RETURN result;
END $$;
GRANT USAGE ON SCHEMA pipeline TO pipeline_operator;
GRANT SELECT ON pipeline.attempt_totals,pipeline.replay_requests,pipeline.replay_items,pipeline.operator_receipts,pipeline.destinations,pipeline.es_target,pipeline.rabbit_target,pipeline.backfill_runs,pipeline.backfill_ranges,pipeline.backfill_batches,pipeline.es_dead_letters,pipeline.es_attempts,pipeline.rabbit_attempts TO pipeline_operator;
GRANT SELECT(event_id,source_epoch,entity_id,entity_version,source_change_id,source_recorded_at,kind,is_deleted,content_sha256,staged_at) ON pipeline.events TO pipeline_operator;
GRANT SELECT ON pipeline.delivery_intents TO pipeline_operator;
GRANT SELECT(event_id,state,created_at,consumer_id,registration_id,receipt_hash,receipt_id,observed_at) ON pipeline.consumer_observations TO pipeline_operator;
REVOKE ALL ON FUNCTION pipeline.request_es_replay(uuid,uuid,text,uuid,bigint,uuid,text),pipeline.operator_backfill(uuid,uuid,text,uuid,uuid,uuid,integer,jsonb),pipeline.count_attempt() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.request_es_replay(uuid,uuid,text,uuid,bigint,uuid,text),pipeline.operator_backfill(uuid,uuid,text,uuid,uuid,uuid,integer,jsonb),pipeline.backfill_status(uuid),pipeline.es_identity() TO pipeline_operator;
RESET ROLE;
SET LOCAL ROLE pipeline_owner;
CREATE TABLE pipeline.network_requests (
 request_id uuid PRIMARY KEY, request_correlation uuid NOT NULL,
 kind text NOT NULL CHECK(kind IN ('elasticsearch','rabbitmq')),
 desired text NOT NULL CHECK(desired IN ('connected','disconnected')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz
);
CREATE UNIQUE INDEX network_one_pending ON pipeline.network_requests(kind) WHERE finished_at IS NULL;
CREATE FUNCTION pipeline.network_request(key uuid,correlation uuid,sink text,desired_state text,finish boolean) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.network_requests;
BEGIN
 IF key IS NULL OR correlation IS NULL OR sink NOT IN ('elasticsearch','rabbitmq') OR desired_state NOT IN ('connected','disconnected') OR finish IS NULL THEN RAISE EXCEPTION 'Invalid network request' USING ERRCODE='22023'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(sink,918));
 SELECT * INTO r FROM pipeline.network_requests WHERE request_id=key FOR UPDATE;
 IF FOUND THEN
  IF r.kind<>sink OR r.desired<>desired_state THEN RAISE EXCEPTION 'Network key conflict' USING ERRCODE='P9001'; END IF;
  IF finish AND r.finished_at IS NULL THEN UPDATE pipeline.network_requests SET finished_at=clock_timestamp() WHERE request_id=key RETURNING * INTO r; END IF;
  RETURN jsonb_build_object('request_id',key,'state',desired_state,'completed',r.finished_at IS NOT NULL,'replayed',r.finished_at IS NOT NULL);
 END IF;
 IF finish OR EXISTS(SELECT FROM pipeline.network_requests WHERE kind=sink AND finished_at IS NULL) THEN RAISE EXCEPTION 'Resolve pending network request first' USING ERRCODE='P9001'; END IF;
 INSERT INTO pipeline.network_requests(request_id,request_correlation,kind,desired) VALUES(key,correlation,sink,desired_state);
 RETURN jsonb_build_object('request_id',key,'state',desired_state,'completed',false,'replayed',false);
END $$;
CREATE FUNCTION pipeline.guard_network_request() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.finished_at IS NOT NULL OR NEW.finished_at IS NULL OR (to_jsonb(OLD)-'finished_at') IS DISTINCT FROM (to_jsonb(NEW)-'finished_at') THEN RAISE EXCEPTION 'Immutable network evidence' USING ERRCODE='P9002'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.network_requests FOR EACH ROW EXECUTE FUNCTION pipeline.guard_network_request();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.network_requests EXECUTE FUNCTION pipeline.immutable();
REVOKE ALL ON FUNCTION pipeline.network_request(uuid,uuid,text,text,boolean),pipeline.guard_network_request() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.network_request(uuid,uuid,text,text,boolean) TO pipeline_operator;
RESET ROLE;
