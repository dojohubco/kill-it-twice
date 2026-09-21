-- Additive bounded run navigation for the local dispatcher; no table-read grant.
SET LOCAL ROLE pipeline_owner;
CREATE FUNCTION pipeline.discover_backfills(after_at timestamptz, after_id uuid, requested integer)
RETURNS TABLE(run_id uuid, created_at text)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF requested IS NULL OR requested NOT BETWEEN 1 AND 16 OR after_at IS NULL OR after_id IS NULL THEN
  RAISE EXCEPTION 'Invalid bounded runtime navigation' USING ERRCODE='22023';
 END IF;
 RETURN QUERY SELECT r.run_id,r.created_at::text FROM pipeline.backfill_runs r
 WHERE r.phase NOT IN ('complete','complete_with_errors')
 AND ROW(r.created_at,r.run_id)>ROW(after_at,after_id)
 ORDER BY r.created_at,r.run_id LIMIT requested;
END $$;
REVOKE ALL ON FUNCTION pipeline.discover_backfills(timestamptz,uuid,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.discover_backfills(timestamptz,uuid,integer) TO pipeline_backfill;
RESET ROLE;
