-- More identity entries, not a larger canonical transfer or SQL parameter budget.
SET LOCAL ROLE pipeline_owner;
ALTER TABLE pipeline.backfill_batches DROP CONSTRAINT backfill_batches_items_check;
ALTER TABLE pipeline.backfill_batches ADD CONSTRAINT backfill_batches_items_check CHECK(jsonb_typeof(items)='array' AND jsonb_array_length(items)<=64 AND octet_length(items::text)<=32768);
CREATE OR REPLACE FUNCTION pipeline.backfill_page(id uuid,num integer,incarnation uuid,gen bigint,batch uuid,previous_key bigint,next_key bigint,eof boolean,inputs jsonb,observation jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs; q pipeline.backfill_ranges; old pipeline.backfill_batches; x jsonb; b jsonb; bytes bytea; items jsonb:='[]'; total bigint:=0; last_key bigint:=previous_key; key bigint; status text; result jsonb:='[]';
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR inputs IS NULL OR jsonb_typeof(inputs)<>'array' OR jsonb_array_length(inputs)>64 OR octet_length(inputs::text)>600000 OR observation IS NULL OR jsonb_typeof(observation)<>'object' OR octet_length(observation::text)>8192 OR batch IS NULL OR eof IS NULL OR previous_key IS NULL OR next_key IS NULL THEN RAISE EXCEPTION 'Invalid page bound' USING ERRCODE='P8003'; END IF;
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
  FOR x IN SELECT value FROM jsonb_array_elements(items) LOOP
   IF NOT EXISTS(SELECT FROM pipeline.backfill_members m WHERE m.run_id=id AND m.event_id=x->>'event_id') THEN RAISE EXCEPTION 'Committed batch membership missing' USING ERRCODE='P8003'; END IF;
   PERFORM pipeline.assert_obligations(x->>'event_id');
  END LOOP;
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
REVOKE ALL ON FUNCTION pipeline.backfill_page(uuid,integer,uuid,bigint,uuid,bigint,bigint,boolean,jsonb,jsonb) FROM PUBLIC;
RESET ROLE;
