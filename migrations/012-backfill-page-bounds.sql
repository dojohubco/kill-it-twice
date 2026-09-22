-- Opt-in record bound; the original wire-byte and per-record ceilings are unchanged.
SET LOCAL ROLE source_owner;
CREATE OR REPLACE FUNCTION source.backfill_page(epoch uuid,instance uuid,after_key bigint,end_key bigint,n integer,fence uuid DEFAULT NULL) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE answer jsonb;
BEGIN
 PERFORM source.backfill_identity(epoch,instance);
 IF after_key IS NULL OR end_key IS NULL OR after_key<0 OR end_key<after_key OR n IS NULL OR n NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'Invalid bounded backfill selection' USING ERRCODE='P8003'; END IF;
 IF fence IS NOT NULL AND NOT EXISTS(SELECT FROM source.backfill_fences WHERE run_id=fence AND pipeline_id=instance AND source_epoch=epoch AND member_count IS NOT NULL AND max_key=end_key) THEN RAISE EXCEPTION 'Unsealed or mismatched source fence' USING ERRCODE='P8003'; END IF;
 WITH selected AS MATERIALIZED (
  SELECT e.entity_id AS key,e.source_epoch,e.entity_id,e.entity_version,e.change_id,e.recorded_at,e.is_deleted,e.payload::text AS exported
  FROM source.entities e WHERE fence IS NULL AND e.entity_id>after_key AND e.entity_id<=end_key ORDER BY e.entity_id LIMIT n
 ), fenced AS MATERIALIZED (
  SELECT f.allocation_id AS key,o.source_epoch,o.entity_id,o.entity_version,o.change_id,o.recorded_at,o.is_deleted,o.payload::text AS exported
  FROM source.backfill_fence_members f LEFT JOIN source.outbox o ON ROW(o.source_epoch,o.entity_id,o.entity_version)=ROW(f.source_epoch,f.entity_id,f.entity_version)
  WHERE fence IS NOT NULL AND f.run_id=fence AND f.allocation_id>after_key AND f.allocation_id<=end_key ORDER BY f.allocation_id LIMIT n
 ), measured AS MATERIALIZED (
  SELECT *,coalesce(octet_length(to_json(exported)::text),4)+1024 AS bytes FROM (SELECT * FROM selected UNION ALL SELECT * FROM fenced) s
 ), bounded AS MATERIALIZED (
  SELECT *,sum(bytes) OVER(ORDER BY key)<=262144 AND bool_and(bytes<=65536 AND entity_id IS NOT NULL) OVER(ORDER BY key) AS fits FROM measured
 ), rows AS MATERIALIZED (
  SELECT coalesce(jsonb_agg(jsonb_build_object('key',key::text,'source_epoch',source_epoch,'entity_id',entity_id::text,'entity_version',entity_version::text,'change_id',change_id,'recorded_at',to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'is_deleted',is_deleted,'payload_json',exported,'bytes',bytes::text) ORDER BY key) FILTER(WHERE fits),'[]'::jsonb) items,coalesce(max(key) FILTER(WHERE fits),after_key) next_key FROM bounded
 ) SELECT jsonb_build_object('items',items,'next_key',next_key::text,'observed_at',clock_timestamp()::text,'snapshot',pg_current_snapshot()::text,
 'blocked',CASE WHEN jsonb_array_length(items)=0 THEN (SELECT jsonb_build_object('key',key::text,'bytes',bytes::text,'reason',CASE WHEN entity_id IS NULL THEN 'missing_revision' ELSE 'oversized_record' END) FROM bounded WHERE NOT fits ORDER BY key LIMIT 1) END,
 'eof',CASE WHEN fence IS NULL THEN NOT EXISTS(SELECT FROM source.entities e WHERE e.entity_id>next_key AND e.entity_id<=end_key) ELSE NOT EXISTS(SELECT FROM source.backfill_fence_members f WHERE f.run_id=fence AND f.allocation_id>next_key AND f.allocation_id<=end_key) END) INTO answer FROM rows;
 RETURN answer;
END $$;
REVOKE ALL ON FUNCTION source.backfill_page(uuid,uuid,bigint,bigint,integer,uuid) FROM PUBLIC;
RESET ROLE;
