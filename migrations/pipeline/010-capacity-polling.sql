-- Admission snapshots omit historical metrics; public status and completion proofs are unchanged.
SET LOCAL ROLE pipeline_owner;
CREATE FUNCTION pipeline.backfill_poll(id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 r:=pipeline.backfill_validate(id);
 RETURN pipeline.backfill_run(id)||jsonb_build_object('observed_at',clock_timestamp()::text,
  'polling_scope','admission_only',
  'effective_paused',r.desired_paused AND NOT EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='leased' AND lease_until>clock_timestamp()),
  'blocked',r.blocked_reason IS NOT NULL OR EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='blocked'));
END $$;
REVOKE ALL ON FUNCTION pipeline.backfill_poll(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.backfill_poll(uuid) TO pipeline_backfill;
CREATE OR REPLACE FUNCTION pipeline.receipt_due(n integer,delay_ms integer)
RETURNS TABLE(event_id text,body bytea,hash text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE o pipeline.consumer_observations;
BEGIN
 IF n IS NULL OR n NOT BETWEEN 1 AND 32 OR delay_ms IS NULL OR delay_ms NOT BETWEEN 100 AND 30000 THEN RAISE EXCEPTION 'Invalid receipt lookup bound' USING ERRCODE='P7001'; END IF;
 IF NOT EXISTS(SELECT 1 FROM pipeline.rabbit_target WHERE registered_at IS NOT NULL) THEN RAISE EXCEPTION 'Consumer registration incomplete' USING ERRCODE='P7001'; END IF;
 FOR o IN SELECT * FROM pipeline.consumer_observations c WHERE c.state='pending' AND c.next_check_at<=clock_timestamp() ORDER BY c.next_check_at,c.event_id FOR UPDATE SKIP LOCKED LIMIT n LOOP
  UPDATE pipeline.consumer_observations SET next_check_at=clock_timestamp()+delay_ms*interval '1 millisecond' WHERE pipeline.consumer_observations.event_id=o.event_id;
  RETURN QUERY SELECT e.event_id,e.body_bytes,e.content_sha256 FROM pipeline.events e WHERE e.event_id=o.event_id;
 END LOOP;
END $$;
REVOKE ALL ON FUNCTION pipeline.receipt_due(integer,integer) FROM PUBLIC;
RESET ROLE;
