-- A readiness precheck may postpone full validation; it can never record completion.
SET LOCAL ROLE pipeline_owner;
CREATE FUNCTION pipeline.backfill_advance_if_ready(id uuid) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 r:=pipeline.backfill_validate(id);
 IF r.phase='scanning' AND EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND range_no>0 AND state<>'closed') THEN RETURN; END IF;
 IF r.phase='importing' AND EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND range_no=0 AND state<>'closed') THEN RETURN; END IF;
 IF r.phase='draining' AND (
  EXISTS(SELECT FROM pipeline.delivery_intents d WHERE d.state IN ('pending','leased','retry_wait') AND EXISTS(SELECT FROM pipeline.backfill_members m WHERE m.run_id=id AND m.event_id=d.event_id)) OR
  EXISTS(SELECT FROM pipeline.consumer_observations o WHERE o.state='pending' AND EXISTS(SELECT FROM pipeline.backfill_members m WHERE m.run_id=id AND m.event_id=o.event_id))
 ) THEN RETURN; END IF;
 -- Missing, corrupt or terminal evidence still goes through the original exhaustive proof.
 PERFORM pipeline.backfill_advance(id);
END $$;
REVOKE ALL ON FUNCTION pipeline.backfill_advance_if_ready(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.backfill_advance_if_ready(uuid) TO pipeline_backfill;
RESET ROLE;
