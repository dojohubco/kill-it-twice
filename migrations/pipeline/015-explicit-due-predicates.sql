-- Make the existing partial-index predicate syntactically explicit.
-- Eligibility, ordering, target/probe locking, generations and attempt retention are unchanged.
SET LOCAL ROLE pipeline_owner;
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
 FOR d IN SELECT * FROM pipeline.delivery_intents i WHERE i.destination_id=target AND i.kind='elasticsearch' AND i.state IN ('pending','leased','retry_wait') AND
 ((i.state IN ('pending','retry_wait') AND i.next_retry_at<=now_at) OR (i.state='leased' AND i.lease_until<=now_at)) ORDER BY i.next_retry_at,i.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
  IF d.state='leased' THEN UPDATE pipeline.es_attempts SET finished_at=now_at,outcome='expired',context='Lease expired before local outcome' WHERE pipeline.es_attempts.attempt_id=d.attempt_id AND finished_at IS NULL; END IF;
  a:=gen_random_uuid();
  INSERT INTO pipeline.es_attempts(attempt_id,event_id,destination_id,claim_generation,owner_id) VALUES(a,d.event_id,target,d.claim_generation+1,incarnation);
  UPDATE pipeline.delivery_intents SET state='leased',owner_id=incarnation,lease_until=clock_timestamp()+lease_ms*interval '1 millisecond',claim_generation=d.claim_generation+1,attempt_id=a,error_class=NULL WHERE pipeline.delivery_intents.event_id=d.event_id AND kind='elasticsearch';
  DELETE FROM pipeline.es_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31 AND NOT EXISTS(SELECT FROM pipeline.es_dead_letters f WHERE f.attempt_id=h.attempt_id) AND NOT EXISTS(SELECT FROM pipeline.replay_attempt_links l WHERE l.attempt_id=h.attempt_id) AND NOT EXISTS(SELECT FROM pipeline.recovery_checks c WHERE c.attempt_id=h.attempt_id);
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; projection_bytes:=octet_length(pipeline.es_projection(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;

CREATE OR REPLACE FUNCTION pipeline.rabbit_claim(target uuid, target_generation bigint, incarnation uuid, n integer, lease_ms integer)
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
  n:=1;
 END IF;
 FOR d IN SELECT * FROM pipeline.delivery_intents i WHERE i.destination_id=target AND i.kind='rabbitmq' AND i.state IN ('pending','leased','retry_wait') AND
 ((i.state IN ('pending','retry_wait') AND i.next_retry_at<=now_at) OR (i.state='leased' AND i.lease_until<=now_at)) ORDER BY i.next_retry_at,i.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
  -- An empty eligible set must not occupy the recovery probe. The target lock
  -- still serializes admission, and cooldown permits exactly one selected row.
  IF t.mode='cooldown' THEN
   UPDATE pipeline.rabbit_target SET probe_owner=incarnation,probe_generation=pipeline.rabbit_target.probe_generation+1,probe_until=clock_timestamp()+lease_ms*interval '1 millisecond' WHERE destination_id=target RETURNING * INTO t;
  END IF;
  IF d.state='leased' THEN UPDATE pipeline.rabbit_attempts SET finished_at=now_at,outcome='expired',context='Lease expired before local outcome' WHERE pipeline.rabbit_attempts.attempt_id=d.rabbit_attempt_id AND finished_at IS NULL; END IF;
  a:=gen_random_uuid();
  INSERT INTO pipeline.rabbit_attempts(attempt_id,event_id,destination_id,claim_generation,owner_id) VALUES(a,d.event_id,target,d.claim_generation+1,incarnation);
  UPDATE pipeline.delivery_intents SET state='leased',owner_id=incarnation,lease_until=clock_timestamp()+lease_ms*interval '1 millisecond',claim_generation=d.claim_generation+1,rabbit_attempt_id=a,error_class=NULL WHERE pipeline.delivery_intents.event_id=d.event_id AND kind='rabbitmq';
  DELETE FROM pipeline.rabbit_attempts h WHERE h.event_id=d.event_id AND h.destination_id=target AND h.claim_generation<=d.claim_generation-31;
  event_id:=d.event_id; generation:=(d.claim_generation+1)::text; attempt_id:=a; wire_bytes:=octet_length(pipeline.rabbit_wire(d.event_id))::text; probe_generation:=t.probe_generation::text; backend_pid:=pg_backend_pid()::text; transaction_id:=pg_current_xact_id()::text; claimed_at:=clock_timestamp()::text; RETURN NEXT;
 END LOOP;
END $$;
REVOKE ALL ON FUNCTION pipeline.es_claim(uuid,bigint,uuid,integer,integer),pipeline.rabbit_claim(uuid,bigint,uuid,integer,integer) FROM PUBLIC;
RESET ROLE;
