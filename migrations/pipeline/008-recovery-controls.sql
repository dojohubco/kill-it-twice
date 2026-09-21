-- Forward extension of the retained operational model. Apply in one owned transaction.
SET LOCAL ROLE pipeline_owner;

ALTER TABLE pipeline.replay_items ADD UNIQUE(request_id,event_id,destination_id);
ALTER TABLE pipeline.delivery_intents ADD COLUMN replay_request_id uuid;
ALTER TABLE pipeline.delivery_intents ADD FOREIGN KEY(replay_request_id,event_id,destination_id)
 REFERENCES pipeline.replay_items(request_id,event_id,destination_id);
ALTER TABLE pipeline.delivery_intents ADD CHECK(kind='elasticsearch' OR replay_request_id IS NULL);

CREATE TABLE pipeline.replay_attempt_links (
 attempt_id uuid PRIMARY KEY,
 request_id uuid NOT NULL,
 event_id text NOT NULL,
 destination_id uuid NOT NULL,
 claim_generation bigint NOT NULL CHECK(claim_generation>0),
 linked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(attempt_id,event_id,destination_id,claim_generation)
  REFERENCES pipeline.es_attempts(attempt_id,event_id,destination_id,claim_generation),
 FOREIGN KEY(request_id,event_id,destination_id)
  REFERENCES pipeline.replay_items(request_id,event_id,destination_id)
);
CREATE INDEX replay_attempt_history ON pipeline.replay_attempt_links(request_id,claim_generation);

CREATE TABLE pipeline.recovery_requests (
 request_id uuid PRIMARY KEY,
 correlation_id uuid NOT NULL,
 operation text NOT NULL CHECK(operation IN ('replay','supersession','verify_target')),
 pipeline_id uuid NOT NULL REFERENCES pipeline.source_binding(pipeline_id),
 destination_id uuid NOT NULL REFERENCES pipeline.es_target(destination_id),
 target_generation bigint NOT NULL CHECK(target_generation>0),
 actor text NOT NULL CHECK(octet_length(actor) BETWEEN 1 AND 64 AND actor !~ '[[:cntrl:]]'),
 reason text NOT NULL CHECK(octet_length(reason) BETWEEN 1 AND 512 AND reason !~ '[[:cntrl:]]'),
 selection jsonb NOT NULL CHECK(jsonb_typeof(selection)='array' AND octet_length(selection::text)<=16384),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((operation='replay' AND jsonb_array_length(selection) BETWEEN 1 AND 50)
    OR (operation='supersession' AND jsonb_array_length(selection)=1)
    OR (operation='verify_target' AND selection='[]'::jsonb))
);
CREATE TABLE pipeline.recovery_items (
 request_id uuid NOT NULL REFERENCES pipeline.recovery_requests(request_id),
 event_id text NOT NULL,
 destination_id uuid NOT NULL,
 expected_attempt uuid NOT NULL,
 replay_request_id uuid NOT NULL UNIQUE,
 scheduled_generation bigint NOT NULL CHECK(scheduled_generation>0),
 PRIMARY KEY(request_id,event_id),
 FOREIGN KEY(event_id,destination_id,expected_attempt)
  REFERENCES pipeline.es_dead_letters(event_id,destination_id,attempt_id),
 FOREIGN KEY(replay_request_id,event_id,destination_id)
  REFERENCES pipeline.replay_items(request_id,event_id,destination_id)
);
CREATE TABLE pipeline.recovery_checks (
 request_id uuid PRIMARY KEY REFERENCES pipeline.recovery_requests(request_id),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','passed','failed','stale')),
 generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0),
 owner_id uuid,
 lease_until timestamptz,
 started_at timestamptz,
 target_state jsonb CHECK(target_state IS NULL OR octet_length(target_state::text)<=2048),
 finished_at timestamptz,
 finished_xid xid8,
 cleanup_failures integer NOT NULL DEFAULT 0 CHECK(cleanup_failures BETWEEN 0 AND 16),
 error_class text CHECK(error_class IS NULL OR error_class IN ('transient','auth','configuration','integrity','not_superseded','stale')),
 witness_event_id text REFERENCES pipeline.events(event_id),
 remote_version bigint CHECK(remote_version IS NULL OR remote_version>0),
 attempt_id uuid REFERENCES pipeline.es_attempts(attempt_id),
 CHECK((state='leased')=(owner_id IS NOT NULL AND lease_until IS NOT NULL)),
 CHECK(state='leased' OR (owner_id IS NULL AND lease_until IS NULL)),
 CHECK((state IN ('passed','failed','stale') AND finished_at IS NOT NULL AND finished_xid IS NOT NULL) OR (state IN ('pending','leased') AND finished_at IS NULL AND finished_xid IS NULL)),
 CHECK((state IN ('pending','leased') AND error_class IS NULL AND witness_event_id IS NULL AND remote_version IS NULL AND attempt_id IS NULL)
   OR (state='passed' AND error_class IS NULL)
   OR (state IN ('failed','stale') AND error_class IS NOT NULL AND witness_event_id IS NULL AND remote_version IS NULL AND attempt_id IS NULL))
);
CREATE INDEX recovery_check_attempt ON pipeline.recovery_checks(attempt_id) WHERE attempt_id IS NOT NULL;
DO $$ DECLARE name text; BEGIN
 FOREACH name IN ARRAY ARRAY['recovery_requests','recovery_items','replay_attempt_links'] LOOP
  EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.%I FOR EACH ROW EXECUTE FUNCTION pipeline.immutable()',name);
  EXECUTE format('CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.%I EXECUTE FUNCTION pipeline.immutable()',name);
 END LOOP;
END $$;
CREATE FUNCTION pipeline.guard_recovery_check() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.state IN ('passed','failed','stale') OR NEW.request_id<>OLD.request_id OR NEW.generation<OLD.generation THEN
  RAISE EXCEPTION 'Immutable recovery result' USING ERRCODE='P9102';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.recovery_checks FOR EACH ROW EXECUTE FUNCTION pipeline.guard_recovery_check();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.recovery_checks EXECUTE FUNCTION pipeline.immutable();

CREATE FUNCTION pipeline.check_recovery_complete() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.recovery_requests;
BEGIN
 SELECT * INTO STRICT r FROM pipeline.recovery_requests WHERE request_id=NEW.request_id;
 IF r.operation='replay' THEN
  IF (SELECT count(*) FROM pipeline.recovery_items WHERE request_id=r.request_id)<>jsonb_array_length(r.selection)
   OR EXISTS(SELECT FROM pipeline.recovery_items i WHERE i.request_id=r.request_id AND
      (i.destination_id<>r.destination_id OR NOT EXISTS(SELECT FROM jsonb_array_elements(r.selection) j WHERE j->>'event_id'=i.event_id AND j->>'attempt_id'=i.expected_attempt::text))) THEN
   RAISE EXCEPTION 'Incomplete recovery membership' USING ERRCODE='P9102';
  END IF;
 ELSE
  IF NOT EXISTS(SELECT FROM pipeline.recovery_checks WHERE request_id=r.request_id) OR EXISTS(SELECT FROM pipeline.recovery_items WHERE request_id=r.request_id) THEN
   RAISE EXCEPTION 'Missing recovery check' USING ERRCODE='P9102';
  END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER complete_recovery AFTER INSERT ON pipeline.recovery_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_recovery_complete();
CREATE CONSTRAINT TRIGGER complete_members AFTER INSERT ON pipeline.recovery_items DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipeline.check_recovery_complete();

CREATE FUNCTION pipeline.link_replay_attempt() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE d pipeline.delivery_intents;
BEGIN
 SELECT * INTO STRICT d FROM pipeline.delivery_intents WHERE event_id=NEW.event_id AND destination_id=NEW.destination_id;
 IF d.replay_request_id IS NOT NULL AND d.state IN ('pending','retry_wait','leased') AND NEW.claim_generation=d.claim_generation+1 THEN
  INSERT INTO pipeline.replay_attempt_links(attempt_id,request_id,event_id,destination_id,claim_generation)
  VALUES(NEW.attempt_id,d.replay_request_id,NEW.event_id,NEW.destination_id,NEW.claim_generation);
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER replay_attempt AFTER INSERT ON pipeline.es_attempts FOR EACH ROW EXECUTE FUNCTION pipeline.link_replay_attempt();

CREATE OR REPLACE FUNCTION pipeline.guard_delivery() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.kind='elasticsearch' AND OLD.state='dead_letter' AND NEW.state='pending'
 AND NEW.claim_generation=OLD.claim_generation+1 AND NEW.replay_request_id IS NOT NULL
 AND NEW.attempt_id IS NULL AND NEW.settled_at IS NULL AND NEW.error_class IS NULL AND NEW.next_retry_at<>'infinity'
 AND (to_jsonb(NEW)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at','claim_generation','replay_request_id'])=(to_jsonb(OLD)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at','claim_generation','replay_request_id'])
 AND EXISTS(SELECT FROM pipeline.replay_items i JOIN pipeline.replay_requests r USING(request_id)
  WHERE i.request_id=NEW.replay_request_id AND i.event_id=OLD.event_id AND i.destination_id=OLD.destination_id
  AND i.terminal_attempt=OLD.attempt_id AND i.schedule_xid=pg_current_xact_id()
  AND r.event_id=i.event_id AND r.destination_id=i.destination_id AND r.terminal_attempt=i.terminal_attempt)
 THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND OLD.kind='elasticsearch' AND OLD.state='dead_letter' AND NEW.state='satisfied'
 AND NEW.disposition='superseded' AND NEW.claim_generation=OLD.claim_generation+1 AND NEW.replay_request_id IS NULL
 AND (to_jsonb(NEW)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at','claim_generation','replay_request_id','disposition','remote_version','witness_event_id'])=(to_jsonb(OLD)-ARRAY['state','attempt_id','settled_at','error_class','next_retry_at','claim_generation','replay_request_id','disposition','remote_version','witness_event_id'])
 AND EXISTS(SELECT FROM pipeline.recovery_checks c JOIN pipeline.recovery_requests r USING(request_id)
  WHERE r.operation='supersession' AND c.state='passed' AND c.finished_xid=pg_current_xact_id() AND c.attempt_id=NEW.attempt_id
  AND r.destination_id=OLD.destination_id AND r.selection->0->>'event_id'=OLD.event_id AND r.selection->0->>'attempt_id'=OLD.attempt_id::text)
 THEN RETURN NEW; END IF;
 IF TG_OP<>'UPDATE' OR OLD.state IN ('satisfied','dead_letter') OR NEW.event_id<>OLD.event_id OR NEW.kind<>OLD.kind
 OR NEW.destination_id<>OLD.destination_id OR NEW.created_at<>OLD.created_at OR NEW.claim_generation<OLD.claim_generation
 OR NEW.replay_request_id IS DISTINCT FROM OLD.replay_request_id THEN
  RAISE EXCEPTION 'Immutable delivery evidence' USING ERRCODE='P5001';
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION pipeline.request_es_replay(key uuid,correlation uuid,id text,target uuid,gen bigint,attempt uuid,why text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE old pipeline.replay_requests; d pipeline.delivery_intents; t pipeline.es_target;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'READ COMMITTED required' USING ERRCODE='25001'; END IF;
 IF key IS NULL OR correlation IS NULL OR id IS NULL OR target IS NULL OR gen IS NULL OR attempt IS NULL OR why IS NULL OR octet_length(why) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION 'Invalid replay input' USING ERRCODE='22023';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,914));
 IF EXISTS(SELECT FROM pipeline.recovery_requests WHERE request_id=key) THEN RAISE EXCEPTION 'Operation key conflict' USING ERRCODE='P9001'; END IF;
 SELECT * INTO old FROM pipeline.replay_requests WHERE request_id=key;
 IF FOUND THEN
  IF (old.event_id,old.destination_id,old.generation,old.terminal_attempt,old.reason) IS DISTINCT FROM (id,target,gen,attempt,why) THEN RAISE EXCEPTION 'Replay key conflict' USING ERRCODE='P9001'; END IF;
  RETURN (SELECT jsonb_build_object('request_id',key,'event_id',id,'attempt_id',attempt,'scheduled_at',scheduled_at,'replayed',true) FROM pipeline.replay_items WHERE request_id=key);
 END IF;
 SELECT * INTO t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE;
 IF NOT FOUND OR t.generation<>gen OR t.registered_at IS NULL OR t.mode='preparing' OR NOT EXISTS(SELECT FROM pipeline.destinations WHERE destination_id=target AND generation=gen AND state='bound' AND receiver_identity=t.index_uuid) THEN RAISE EXCEPTION 'Receiver binding blocked' USING ERRCODE='P9002'; END IF;
 SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=id AND kind='elasticsearch' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Unknown event' USING ERRCODE='P9004'; END IF;
 IF d.state<>'dead_letter' OR d.destination_id<>target OR d.attempt_id<>attempt THEN RAISE EXCEPTION 'Stale terminal attempt' USING ERRCODE='P9001'; END IF;
 INSERT INTO pipeline.replay_requests VALUES(key,correlation,'elasticsearch','local-operator',why,id,target,gen,attempt,clock_timestamp());
 INSERT INTO pipeline.replay_items(request_id,event_id,destination_id,terminal_attempt) VALUES(key,id,target,attempt);
 UPDATE pipeline.delivery_intents SET state='pending',attempt_id=NULL,settled_at=NULL,error_class=NULL,next_retry_at=clock_timestamp(),claim_generation=claim_generation+1,replay_request_id=key WHERE event_id=id AND kind='elasticsearch';
 RETURN (SELECT jsonb_build_object('request_id',key,'event_id',id,'attempt_id',attempt,'scheduled_at',scheduled_at,'replayed',false) FROM pipeline.replay_items WHERE request_id=key);
END $$;

CREATE FUNCTION pipeline.normalize_recovery_selection(kind text, supplied jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE item jsonb; normalized jsonb;
BEGIN
 IF supplied IS NULL OR jsonb_typeof(supplied)<>'array' OR octet_length(supplied::text)>16384 THEN RAISE EXCEPTION 'Invalid selection' USING ERRCODE='22023'; END IF;
 IF kind='verify_target' THEN
  IF supplied<>'[]'::jsonb THEN RAISE EXCEPTION 'Target check has no event selection' USING ERRCODE='22023'; END IF;
  RETURN supplied;
 END IF;
 IF (kind='replay' AND jsonb_array_length(supplied) NOT BETWEEN 1 AND 50)
 OR (kind='supersession' AND jsonb_array_length(supplied)<>1) THEN RAISE EXCEPTION 'Selection count bound' USING ERRCODE='22023'; END IF;
 FOR item IN SELECT * FROM jsonb_array_elements(supplied) LOOP
  IF jsonb_typeof(item)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(item))<>2
   OR jsonb_typeof(item->'event_id') IS DISTINCT FROM 'string' OR jsonb_typeof(item->'attempt_id') IS DISTINCT FROM 'string'
   OR (item->>'event_id') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[1-9][0-9]{0,18}:[1-9][0-9]{0,18}$'
   OR (item->>'attempt_id') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
   RAISE EXCEPTION 'Malformed selection identity' USING ERRCODE='22023';
  END IF;
  PERFORM split_part(item->>'event_id',':',2)::bigint,split_part(item->>'event_id',':',3)::bigint;
 END LOOP;
 IF (SELECT count(DISTINCT e->>'event_id') FROM jsonb_array_elements(supplied) e)<>jsonb_array_length(supplied) THEN RAISE EXCEPTION 'Duplicate selected event' USING ERRCODE='22023'; END IF;
 SELECT jsonb_agg(e ORDER BY (e->>'event_id') COLLATE "C") INTO normalized FROM jsonb_array_elements(supplied) e;
 RETURN normalized;
END $$;

CREATE FUNCTION pipeline.prepare_recovery(key uuid,correlation uuid,kind text,instance uuid,target uuid,gen bigint,who text,why text,supplied jsonb) RETURNS boolean
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE prior pipeline.recovery_requests; normalized jsonb; t pipeline.es_target;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'READ COMMITTED required' USING ERRCODE='25001'; END IF;
 IF key IS NULL OR correlation IS NULL OR kind IS NULL OR kind NOT IN ('replay','supersession','verify_target') OR instance IS NULL OR target IS NULL OR gen IS NULL OR gen<1
 OR who IS NULL OR octet_length(who) NOT BETWEEN 1 AND 64 OR who ~ '[[:cntrl:]]' OR why IS NULL OR octet_length(why) NOT BETWEEN 1 AND 512 OR why ~ '[[:cntrl:]]' THEN
  RAISE EXCEPTION 'Invalid recovery request' USING ERRCODE='22023';
 END IF;
 normalized:=pipeline.normalize_recovery_selection(kind,supplied);
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,914));
 SELECT * INTO prior FROM pipeline.recovery_requests WHERE request_id=key;
 IF FOUND THEN
  IF (prior.operation,prior.pipeline_id,prior.destination_id,prior.target_generation,prior.actor,prior.reason,prior.selection)
     IS DISTINCT FROM (kind,instance,target,gen,who,why,normalized) THEN RAISE EXCEPTION 'Recovery key conflict' USING ERRCODE='P9001'; END IF;
  RETURN false;
 END IF;
 IF EXISTS(SELECT FROM pipeline.replay_requests WHERE request_id=key) OR EXISTS(SELECT FROM pipeline.operator_receipts WHERE request_id=key) THEN RAISE EXCEPTION 'Operation key conflict' USING ERRCODE='P9001'; END IF;
 SELECT * INTO t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE;
 IF NOT FOUND OR t.pipeline_id<>instance OR t.generation<>gen OR t.registered_at IS NULL OR t.mode='preparing'
 OR NOT EXISTS(SELECT FROM pipeline.destinations WHERE destination_id=target AND generation=gen AND state='bound' AND receiver_identity=t.index_uuid) THEN RAISE EXCEPTION 'Receiver binding blocked' USING ERRCODE='P9002'; END IF;
 INSERT INTO pipeline.recovery_requests(request_id,correlation_id,operation,pipeline_id,destination_id,target_generation,actor,reason,selection)
 VALUES(key,correlation,kind,instance,target,gen,who,why,normalized);
 RETURN true;
END $$;

CREATE FUNCTION pipeline.recovery_status(key uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.recovery_requests; items jsonb; result_state text; c pipeline.recovery_checks;
BEGIN
 SELECT * INTO r FROM pipeline.recovery_requests WHERE request_id=key;
 IF NOT FOUND THEN RAISE EXCEPTION 'Unknown recovery request' USING ERRCODE='P9004'; END IF;
 IF r.operation='replay' THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('event_id',i.event_id,'expected_attempt',i.expected_attempt,'replay_request_id',i.replay_request_id,
    'scheduled_generation',i.scheduled_generation::text,'current_obligation_state',d.state,'current_terminal_attempt',d.attempt_id,
    'latest_attempt',a.attempt_id,'attempt_generation',a.claim_generation::text,'outcome',a.outcome,
    'state',CASE WHEN a.attempt_id IS NULL THEN 'scheduled' WHEN a.finished_at IS NULL THEN 'running'
      WHEN a.outcome IN ('applied','already_applied','superseded') THEN 'satisfied' WHEN a.outcome IN ('mapping','oversized') THEN 'failed' ELSE 'retry_wait' END,
    'attempts',(SELECT count(*)::text FROM pipeline.replay_attempt_links l WHERE l.request_id=i.replay_request_id)) ORDER BY i.event_id COLLATE "C"),'[]'::jsonb)
   INTO items FROM pipeline.recovery_items i
   JOIN pipeline.delivery_intents d ON d.event_id=i.event_id AND d.destination_id=i.destination_id
   LEFT JOIN LATERAL(SELECT a.* FROM pipeline.replay_attempt_links l JOIN pipeline.es_attempts a USING(attempt_id)
      WHERE l.request_id=i.replay_request_id ORDER BY l.claim_generation DESC LIMIT 1) a ON true
   WHERE i.request_id=key;
  IF jsonb_array_length(items)<>jsonb_array_length(r.selection) THEN RAISE EXCEPTION 'Missing recovery relation' USING ERRCODE='P9102'; END IF;
  result_state:=CASE WHEN NOT EXISTS(SELECT FROM jsonb_array_elements(items) i WHERE i->>'state'<>'satisfied') THEN 'satisfied'
    WHEN EXISTS(SELECT FROM jsonb_array_elements(items) i WHERE i->>'state' IN ('scheduled','running','retry_wait')) THEN 'pending' ELSE 'complete_with_errors' END;
 ELSE
  SELECT * INTO STRICT c FROM pipeline.recovery_checks WHERE request_id=key;
  result_state:=c.state;
  items:=jsonb_build_array(jsonb_build_object('state',c.state,'owner_id',c.owner_id,'generation',c.generation::text,
   'lease_until',c.lease_until,'started_at',c.started_at,'finished_at',c.finished_at,'error_class',c.error_class,'cleanup_failures',c.cleanup_failures,
   'witness_event_id',c.witness_event_id,'remote_version',c.remote_version::text,'attempt_id',c.attempt_id));
 END IF;
 RETURN jsonb_build_object('request_id',r.request_id,'operation',r.operation,'pipeline_id',r.pipeline_id,'destination_id',r.destination_id,
  'target_generation',r.target_generation::text,'actor',r.actor,'reason',r.reason,'created_at',r.created_at,'state',result_state,'items',items,'observed_at',clock_timestamp()::text);
END $$;

CREATE FUNCTION pipeline.request_batch_replay(key uuid,correlation uuid,instance uuid,target uuid,gen bigint,who text,why text,selection jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE created boolean; r pipeline.recovery_requests; item jsonb; d pipeline.delivery_intents; child uuid;
BEGIN
 created:=pipeline.prepare_recovery(key,correlation,'replay',instance,target,gen,who,why,selection);
 IF NOT created THEN RETURN pipeline.recovery_status(key)||jsonb_build_object('replayed',true); END IF;
 SELECT * INTO STRICT r FROM pipeline.recovery_requests WHERE request_id=key;
 -- All preconditions under stable target-then-event lock order, before any scheduling.
 FOR item IN SELECT * FROM jsonb_array_elements(r.selection) LOOP
  SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=item->>'event_id' AND kind='elasticsearch' FOR UPDATE;
  IF NOT FOUND OR d.destination_id<>target OR d.state<>'dead_letter' OR d.attempt_id IS DISTINCT FROM (item->>'attempt_id')::uuid THEN RAISE EXCEPTION 'Stale failure selection' USING ERRCODE='P9001'; END IF;
 END LOOP;
 FOR item IN SELECT * FROM jsonb_array_elements(r.selection) LOOP
  child:=gen_random_uuid();
  PERFORM pipeline.request_es_replay(child,correlation,item->>'event_id',target,gen,(item->>'attempt_id')::uuid,why);
  INSERT INTO pipeline.recovery_items(request_id,event_id,destination_id,expected_attempt,replay_request_id,scheduled_generation)
  SELECT key,di.event_id,target,(item->>'attempt_id')::uuid,child,di.claim_generation FROM pipeline.delivery_intents di WHERE di.event_id=item->>'event_id' AND di.kind='elasticsearch';
 END LOOP;
 RETURN pipeline.recovery_status(key)||jsonb_build_object('replayed',false);
END $$;

CREATE FUNCTION pipeline.recovery_target_state(t pipeline.es_target) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('mode',t.mode,'reason',t.reason,'failures',t.failures::text,'probe_generation',t.probe_generation::text,
   'probe_owner',t.probe_owner,'probe_until',t.probe_until,'next_probe_at',t.next_probe_at,
   'index_uuid',t.index_uuid,'cluster_uuid',t.cluster_uuid,'configuration_sha256',t.configuration_sha256);
$$;

CREATE FUNCTION pipeline.begin_recovery_check(key uuid,correlation uuid,kind text,instance uuid,target uuid,gen bigint,who text,why text,selection jsonb,incarnation uuid,lease_ms integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE created boolean; r pipeline.recovery_requests; c pipeline.recovery_checks; t pipeline.es_target; d pipeline.delivery_intents; now_at timestamptz;
BEGIN
 IF kind IS NULL OR kind NOT IN ('supersession','verify_target') OR incarnation IS NULL OR lease_ms IS NULL OR lease_ms NOT BETWEEN 300 AND 30000 THEN RAISE EXCEPTION 'Invalid verification lease' USING ERRCODE='22023'; END IF;
 created:=pipeline.prepare_recovery(key,correlation,kind,instance,target,gen,who,why,selection);
 SELECT * INTO STRICT r FROM pipeline.recovery_requests WHERE request_id=key;
 SELECT * INTO STRICT t FROM pipeline.es_target WHERE destination_id=target FOR UPDATE;
 IF created THEN INSERT INTO pipeline.recovery_checks(request_id) VALUES(key); END IF;
 IF kind='supersession' THEN SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=r.selection->0->>'event_id' AND destination_id=target FOR UPDATE; END IF;
 SELECT * INTO STRICT c FROM pipeline.recovery_checks WHERE request_id=key FOR UPDATE;
 now_at:=clock_timestamp();
 IF c.state IN ('passed','failed','stale') THEN RETURN pipeline.recovery_status(key)||jsonb_build_object('replayed',true); END IF;
 IF c.state='leased' AND c.lease_until>now_at THEN RAISE EXCEPTION 'Verification already owned' USING ERRCODE='P9001'; END IF;
 IF t.pipeline_id<>instance OR t.generation<>gen OR t.registered_at IS NULL OR t.mode='preparing' THEN RAISE EXCEPTION 'Verification binding changed' USING ERRCODE='P9002'; END IF;
 IF kind='supersession' AND (d.event_id IS NULL OR d.state<>'dead_letter' OR d.attempt_id IS DISTINCT FROM (r.selection->0->>'attempt_id')::uuid) THEN
  IF created THEN RAISE EXCEPTION 'Stale terminal attempt' USING ERRCODE='P9001'; END IF;
  UPDATE pipeline.recovery_checks SET state='stale',owner_id=NULL,lease_until=NULL,error_class='stale',finished_at=now_at,finished_xid=pg_current_xact_id() WHERE request_id=key;
  RETURN pipeline.recovery_status(key)||jsonb_build_object('replayed',true);
 END IF;
 UPDATE pipeline.recovery_checks SET state='leased',generation=generation+1,owner_id=incarnation,lease_until=now_at+lease_ms*interval '1 millisecond',started_at=now_at,target_state=pipeline.recovery_target_state(t) WHERE request_id=key;
 RETURN pipeline.recovery_status(key)||jsonb_build_object('replayed',NOT created);
END $$;

CREATE FUNCTION pipeline.finish_recovery_check(key uuid,incarnation uuid,claim bigint,passed boolean,remote bigint,witness text,failure text,cleanup_count integer DEFAULT 0) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.recovery_requests; c pipeline.recovery_checks; t pipeline.es_target; d pipeline.delivery_intents; e pipeline.events; w pipeline.events; now_at timestamptz; a uuid;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'READ COMMITTED required' USING ERRCODE='25001'; END IF;
 IF cleanup_count IS NULL OR cleanup_count NOT BETWEEN 0 AND 16 OR key IS NULL OR incarnation IS NULL OR claim IS NULL OR passed IS NULL OR (NOT passed AND (failure IS NULL OR failure NOT IN ('transient','auth','configuration','integrity','not_superseded'))) THEN RAISE EXCEPTION 'Invalid verification result' USING ERRCODE='22023'; END IF;
 SELECT * INTO STRICT r FROM pipeline.recovery_requests WHERE request_id=key;
 SELECT * INTO STRICT t FROM pipeline.es_target WHERE destination_id=r.destination_id FOR UPDATE;
 IF r.operation='supersession' THEN SELECT * INTO d FROM pipeline.delivery_intents WHERE event_id=r.selection->0->>'event_id' AND destination_id=r.destination_id FOR UPDATE; END IF;
 SELECT * INTO STRICT c FROM pipeline.recovery_checks WHERE request_id=key FOR UPDATE;
 now_at:=clock_timestamp();
 IF c.state<>'leased' OR c.owner_id IS DISTINCT FROM incarnation OR c.generation IS DISTINCT FROM claim OR c.lease_until<=now_at THEN RAISE EXCEPTION 'Expired verification ownership' USING ERRCODE='P9001'; END IF;
 IF t.pipeline_id<>r.pipeline_id OR t.generation<>r.target_generation OR c.target_state IS DISTINCT FROM pipeline.recovery_target_state(t)
 OR (r.operation='supersession' AND (d.event_id IS NULL OR d.state<>'dead_letter' OR d.attempt_id IS DISTINCT FROM (r.selection->0->>'attempt_id')::uuid)) THEN
  UPDATE pipeline.recovery_checks SET state='stale',owner_id=NULL,lease_until=NULL,error_class='stale',finished_at=now_at,finished_xid=pg_current_xact_id() WHERE request_id=key;
  RETURN pipeline.recovery_status(key);
 END IF;
 IF NOT passed THEN
  UPDATE pipeline.recovery_checks SET state='failed',owner_id=NULL,lease_until=NULL,error_class=failure,cleanup_failures=cleanup_count,finished_at=now_at,finished_xid=pg_current_xact_id() WHERE request_id=key;
  RETURN pipeline.recovery_status(key);
 END IF;
 IF r.operation='supersession' THEN
  SELECT * INTO STRICT e FROM pipeline.events WHERE event_id=d.event_id;
  SELECT * INTO w FROM pipeline.events WHERE event_id=witness;
  IF NOT FOUND OR remote IS NULL OR w.entity_version<>remote OR w.entity_version<=e.entity_version OR w.entity_id<>e.entity_id OR w.source_epoch<>e.source_epoch OR t.mode='blocked' THEN RAISE EXCEPTION 'Invalid supersession witness' USING ERRCODE='P9102'; END IF;
  a:=gen_random_uuid();
  INSERT INTO pipeline.es_attempts(attempt_id,event_id,destination_id,claim_generation,owner_id,started_at,finished_at,outcome,context,remote_version)
   VALUES(a,d.event_id,d.destination_id,d.claim_generation+1,incarnation,c.started_at,now_at,'superseded','Operator realtime GET verified a strictly higher exact ledger projection',remote);
  UPDATE pipeline.recovery_checks SET state='passed',owner_id=NULL,lease_until=NULL,finished_at=now_at,finished_xid=pg_current_xact_id(),witness_event_id=witness,remote_version=remote,attempt_id=a WHERE request_id=key;
  UPDATE pipeline.delivery_intents SET state='satisfied',claim_generation=claim_generation+1,attempt_id=a,disposition='superseded',remote_version=remote,witness_event_id=witness,settled_at=now_at,error_class=NULL,next_retry_at=now_at,replay_request_id=NULL WHERE event_id=d.event_id AND kind='elasticsearch';
 ELSE
  IF remote IS NOT NULL OR witness IS NOT NULL THEN RAISE EXCEPTION 'Target check has no event witness' USING ERRCODE='22023'; END IF;
  UPDATE pipeline.recovery_checks SET state='passed',owner_id=NULL,lease_until=NULL,finished_at=now_at,finished_xid=pg_current_xact_id() WHERE request_id=key;
  UPDATE pipeline.es_target SET mode='ready',reason=NULL,failures=0,next_probe_at=now_at,probe_owner=NULL,probe_until=NULL,probe_generation=probe_generation+1 WHERE destination_id=t.destination_id;
 END IF;
 RETURN pipeline.recovery_status(key);
END $$;

-- Preserve correlated recovery attempts when the ordinary diagnostic retention window advances.
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
  DELETE FROM pipeline.es_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31 AND NOT EXISTS(SELECT FROM pipeline.es_dead_letters f WHERE f.attempt_id=h.attempt_id) AND NOT EXISTS(SELECT FROM pipeline.replay_attempt_links l WHERE l.attempt_id=h.attempt_id) AND NOT EXISTS(SELECT FROM pipeline.recovery_checks c WHERE c.attempt_id=h.attempt_id);
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; projection_bytes:=octet_length(pipeline.es_projection(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;

CREATE FUNCTION pipeline.recovery_list(category text,after_key text,n integer,filter_event text DEFAULT NULL) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE result jsonb;
BEGIN
 IF category IS NULL OR category NOT IN ('operations','runs','failures','attempts') OR after_key IS NULL OR octet_length(after_key)>256 OR n IS NULL OR n NOT BETWEEN 1 AND 100
 OR (category='attempts' AND (filter_event IS NULL OR octet_length(filter_event)>80))
 OR (category<>'attempts' AND filter_event IS NOT NULL) THEN RAISE EXCEPTION 'Invalid bounded read' USING ERRCODE='22023'; END IF;
 WITH candidates AS (
  SELECT 'operation:'||r.request_id AS key,jsonb_build_object('key','operation:'||r.request_id,'request_id',r.request_id,'operation',r.operation,
    'actor',r.actor,'reason',r.reason,'destination_id',r.destination_id,'generation',r.target_generation::text,'selected',jsonb_array_length(r.selection),'created_at',r.created_at) AS value
   FROM pipeline.recovery_requests r WHERE category='operations'
  UNION ALL
  SELECT 'run:'||r.run_id,jsonb_build_object('key','run:'||r.run_id,'run_id',r.run_id,'phase',r.phase,'created_at',r.created_at,'completed_at',r.completed_at,'sealed_at',r.sealed_at,'blocked_reason',r.blocked_reason)
   FROM pipeline.backfill_runs r WHERE category='runs'
  UNION ALL
  SELECT 'failure:'||f.event_id||':'||f.attempt_id,jsonb_build_object('key','failure:'||f.event_id||':'||f.attempt_id,'event_id',f.event_id,'attempt_id',f.attempt_id,
    'destination_id',f.destination_id,'generation',t.generation::text,'error_class',f.error_class,'context',f.context,'recorded_at',f.recorded_at,
    'current_failure',d.state='dead_letter' AND d.attempt_id=f.attempt_id,'current_obligation_state',d.state)
   FROM pipeline.es_dead_letters f JOIN pipeline.delivery_intents d USING(event_id,destination_id) JOIN pipeline.es_target t USING(destination_id) WHERE category='failures'
  UNION ALL
  SELECT 'attempt:'||lpad(a.claim_generation::text,19,'0')||':'||a.attempt_id,jsonb_build_object('key','attempt:'||lpad(a.claim_generation::text,19,'0')||':'||a.attempt_id,
    'event_id',a.event_id,'attempt_id',a.attempt_id,'generation',a.claim_generation::text,'outcome',a.outcome,'context',a.context,'started_at',a.started_at,'finished_at',a.finished_at,
    'remote_version',a.remote_version::text,'replay_request_id',l.request_id)
   FROM pipeline.es_attempts a LEFT JOIN pipeline.replay_attempt_links l USING(attempt_id) WHERE category='attempts' AND a.event_id=filter_event
 ), limited AS MATERIALIZED(SELECT key,value FROM candidates WHERE key COLLATE "C">after_key COLLATE "C" ORDER BY key COLLATE "C" LIMIT n+1),
 measured AS(SELECT key,value,row_number() OVER(ORDER BY key COLLATE "C") rn,sum(octet_length(value::text)+2) OVER(ORDER BY key COLLATE "C") bytes FROM limited),
 included AS(SELECT key,value FROM measured WHERE rn<=n AND bytes<=261120),
 flags AS(SELECT (SELECT count(*) FROM included) included_count,(SELECT count(*) FROM limited) candidate_count)
 SELECT jsonb_build_object('observed_at',clock_timestamp()::text,'category',category,'items',coalesce((SELECT jsonb_agg(value ORDER BY key COLLATE "C") FROM included),'[]'),
    'next_key',CASE WHEN candidate_count>included_count THEN (SELECT key FROM included ORDER BY key COLLATE "C" DESC LIMIT 1) ELSE NULL END,
    'byte_limit',262144,'count_limit',n) INTO result FROM flags;
 IF octet_length(result::text)>262144 THEN RAISE EXCEPTION 'Operational response limit' USING ERRCODE='54000'; END IF;
 RETURN result;
END $$;

CREATE FUNCTION pipeline.recovery_overview() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('observed_at',clock_timestamp()::text,
  'identity',(SELECT jsonb_build_object('pipeline_id',pipeline_id,'source_epoch',source_epoch) FROM pipeline.source_binding),
  'failure_history',(SELECT count(*)::text FROM pipeline.es_dead_letters),
  'open_failures',(SELECT count(*)::text FROM pipeline.delivery_intents WHERE kind='elasticsearch' AND state='dead_letter'),
  'historical_recovered_failures',(SELECT count(*)::text FROM pipeline.es_dead_letters f JOIN pipeline.delivery_intents d USING(event_id,destination_id) WHERE d.state='satisfied'),
  'recovery_requests',(SELECT count(*)::text FROM pipeline.recovery_requests),
  'verification_checks',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM (SELECT state,count(*)::text count FROM pipeline.recovery_checks GROUP BY state) x),
  'delivery_eligibility',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM (
    SELECT kind sink,count(*) FILTER(WHERE state IN ('pending','retry_wait') AND next_retry_at<=statement_timestamp())::text due,
      count(*) FILTER(WHERE state IN ('pending','retry_wait') AND next_retry_at>statement_timestamp())::text delayed,
      count(*) FILTER(WHERE state='leased' AND lease_until<=statement_timestamp())::text expired_leases
    FROM pipeline.delivery_intents GROUP BY kind) x));
$$;

CREATE FUNCTION pipeline.recovery_run(run uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; c jsonb; recovered text; valid_progress boolean;
BEGIN
 SELECT * INTO r FROM pipeline.backfill_runs WHERE run_id=run;
 IF NOT FOUND THEN RAISE EXCEPTION 'Unknown run' USING ERRCODE='P9004'; END IF;
 PERFORM pipeline.backfill_validate(run);
 c:=pipeline.backfill_counts(run);valid_progress:=pipeline.backfill_progress_valid(run);
 recovered:=CASE WHEN NOT valid_progress THEN 'integrity_blocked' WHEN r.sealed_at IS NULL THEN 'membership_unsealed' WHEN (c->>'invalid')::bigint<>0 THEN 'integrity_blocked'
   WHEN (c->>'es_pending')::bigint+(c->>'rabbit_pending')::bigint+(c->>'consumer_pending')::bigint>0 THEN 'pending'
   WHEN (c->>'es_errors')::bigint+(c->>'consumer_errors')::bigint>0 THEN 'unresolved_errors' ELSE 'satisfied' END;
 RETURN jsonb_build_object('run_id',run,'observed_at',clock_timestamp()::text,
   'historical',jsonb_build_object('phase',r.phase,'completed_at',r.completed_at,'sealed_at',r.sealed_at,'counts_at_completion','not_retained'),
   'current_recovery',jsonb_build_object('state',recovered,'counts',c,'progress_valid',valid_progress),
   'membership_immutable',r.sealed_at IS NOT NULL);
END $$;

-- Trigger and helper functions are not runtime entry points.
REVOKE ALL ON FUNCTION pipeline.guard_recovery_check(),pipeline.check_recovery_complete(),pipeline.link_replay_attempt(),
 pipeline.normalize_recovery_selection(text,jsonb),pipeline.prepare_recovery(uuid,uuid,text,uuid,uuid,bigint,text,text,jsonb),
 pipeline.recovery_target_state(pipeline.es_target),pipeline.recovery_status(uuid),
 pipeline.request_batch_replay(uuid,uuid,uuid,uuid,bigint,text,text,jsonb),
 pipeline.begin_recovery_check(uuid,uuid,text,uuid,uuid,bigint,text,text,jsonb,uuid,integer),
 pipeline.finish_recovery_check(uuid,uuid,bigint,boolean,bigint,text,text,integer),
 pipeline.recovery_list(text,text,integer,text),pipeline.recovery_overview(),pipeline.recovery_run(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.recovery_status(uuid),pipeline.request_batch_replay(uuid,uuid,uuid,uuid,bigint,text,text,jsonb),
 pipeline.begin_recovery_check(uuid,uuid,text,uuid,uuid,bigint,text,text,jsonb,uuid,integer),
 pipeline.finish_recovery_check(uuid,uuid,bigint,boolean,bigint,text,text,integer),pipeline.recovery_list(text,text,integer,text),
 pipeline.recovery_overview(),pipeline.recovery_run(uuid),pipeline.es_read(text) TO pipeline_operator;
RESET ROLE;
