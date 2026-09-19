-- Apply in one controlled transaction after 001; no data or protocol change.
-- Count exact UTF-8 canonical wire bytes, not hex parameter/heap overhead.
-- For valid hashes the frozen wrapper is 93 bytes; reconstruct it instead of estimating.
SET LOCAL ROLE consumer_owner;
CREATE OR REPLACE FUNCTION consumer.process_batch(consumer_id uuid,epoch uuid,instance uuid,registration uuid,items jsonb) RETURNS TABLE(event_id text,status text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE item jsonb; b jsonb; e consumer.processed_events; prior consumer.processed_events; inserted integer; lock_key bigint; bytes_total bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR NOT EXISTS(SELECT 1 FROM consumer.identity i WHERE i.consumer_id=process_batch.consumer_id AND i.source_epoch=epoch AND i.pipeline_id=instance AND i.registration_id=registration) THEN RAISE EXCEPTION 'Consumer identity/isolation mismatch' USING ERRCODE='P6002'; END IF;
 IF items IS NULL OR jsonb_typeof(items)<>'array' OR jsonb_array_length(items) NOT BETWEEN 1 AND 32 THEN RAISE EXCEPTION 'Invalid consumer batch' USING ERRCODE='P6002'; END IF;
 SELECT sum(octet_length(convert_to('{"body":','UTF8') || decode(i->>'body','hex') || convert_to(',"content_sha256":"' || (i->>'hash') || '"}','UTF8'))) INTO bytes_total FROM jsonb_array_elements(items) i;
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
REVOKE ALL ON FUNCTION consumer.process_batch(uuid,uuid,uuid,uuid,jsonb) FROM PUBLIC;
RESET ROLE;
