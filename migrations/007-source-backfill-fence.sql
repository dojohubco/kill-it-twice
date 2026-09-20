-- Additive source backfill evidence. Apply once in a controlled transaction.
CREATE ROLE source_backfill NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE source_m1 TO source_backfill;
GRANT USAGE ON SCHEMA source TO source_backfill;
SET LOCAL ROLE source_owner;
CREATE TABLE source.backfill_fences (
 run_id uuid PRIMARY KEY, pipeline_id uuid NOT NULL, source_epoch uuid NOT NULL,
 codec text NOT NULL CHECK(codec='pg18-jsonb-text/v1'),
 snapshot text, observed_at timestamptz, member_count bigint, max_key bigint,
 FOREIGN KEY(pipeline_id,source_epoch) REFERENCES source.capture_binding(pipeline_id,source_epoch),
 CHECK ((snapshot IS NULL AND observed_at IS NULL AND member_count IS NULL AND max_key IS NULL) OR
 (snapshot IS NOT NULL AND octet_length(snapshot)<=4096 AND observed_at IS NOT NULL AND isfinite(observed_at) AND member_count IS NOT NULL AND member_count>=0 AND max_key IS NOT NULL AND max_key>=0))
);
CREATE TABLE source.backfill_fence_members (
 run_id uuid NOT NULL REFERENCES source.backfill_fences(run_id),
 allocation_id bigint NOT NULL REFERENCES source.outbox(allocation_id),
 source_epoch uuid NOT NULL, entity_id bigint NOT NULL, entity_version bigint NOT NULL,
 PRIMARY KEY(run_id,allocation_id), UNIQUE(run_id,source_epoch,entity_id,entity_version),
 FOREIGN KEY(source_epoch,entity_id,entity_version) REFERENCES source.outbox(source_epoch,entity_id,entity_version)
);
CREATE FUNCTION source.guard_backfill_fence() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP<>'UPDATE' OR OLD.member_count IS NOT NULL OR ROW(NEW.run_id,NEW.pipeline_id,NEW.source_epoch,NEW.codec) IS DISTINCT FROM ROW(OLD.run_id,OLD.pipeline_id,OLD.source_epoch,OLD.codec) OR NEW.member_count IS NULL THEN
 RAISE EXCEPTION 'Immutable source fence' USING ERRCODE='P8003'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER fence_guard BEFORE UPDATE OR DELETE ON source.backfill_fences FOR EACH ROW EXECUTE FUNCTION source.guard_backfill_fence();
CREATE TRIGGER fence_no_truncate BEFORE TRUNCATE ON source.backfill_fences EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER fence_members_no_rewrite BEFORE UPDATE OR DELETE ON source.backfill_fence_members FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER fence_members_no_truncate BEFORE TRUNCATE ON source.backfill_fence_members EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE FUNCTION source.guard_fence_member() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT FROM source.backfill_fences f JOIN source.outbox o ON o.allocation_id=NEW.allocation_id WHERE f.run_id=NEW.run_id AND f.member_count IS NULL AND f.source_epoch=NEW.source_epoch AND ROW(o.source_epoch,o.entity_id,o.entity_version)=ROW(NEW.source_epoch,NEW.entity_id,NEW.entity_version)) THEN RAISE EXCEPTION 'Invalid or already sealed fence membership' USING ERRCODE='P8003'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER fence_member_guard BEFORE INSERT ON source.backfill_fence_members FOR EACH ROW EXECUTE FUNCTION source.guard_fence_member();
CREATE FUNCTION source.require_complete_fence() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE f source.backfill_fences; n bigint; last_key bigint;
BEGIN
 SELECT * INTO STRICT f FROM source.backfill_fences WHERE run_id=NEW.run_id;
 SELECT count(*),coalesce(max(allocation_id),0) INTO n,last_key FROM source.backfill_fence_members WHERE run_id=f.run_id;
 IF f.member_count IS NULL OR f.member_count<>n OR f.max_key<>last_key THEN RAISE EXCEPTION 'Incomplete source fence' USING ERRCODE='P8003'; END IF; RETURN NULL;
END $$;
-- One header check per fence, never one full-history count per member.
CREATE CONSTRAINT TRIGGER complete_fence AFTER INSERT ON source.backfill_fences DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_complete_fence();
CREATE FUNCTION source.backfill_identity(epoch uuid,instance uuid) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF current_setting('server_encoding')<>'UTF8' OR current_setting('client_encoding')<>'UTF8' OR current_setting('server_version_num')::integer/10000<>18 OR
 NOT EXISTS(SELECT FROM source.capture_binding b JOIN source.bootstrap_manifest m USING(source_epoch) WHERE b.source_epoch=epoch AND b.pipeline_id=instance AND b.payload_encoding='pg18-jsonb-text/v1' AND m.phase='active') THEN RAISE EXCEPTION 'Inactive or mismatched backfill source binding' USING ERRCODE='P8001'; END IF;
 RETURN jsonb_build_object('source_epoch',epoch,'pipeline_id',instance,'codec','pg18-jsonb-text/v1','upper_key',(SELECT coalesce(max(entity_id),0)::text FROM source.entities),'observed_at',clock_timestamp()::text,'snapshot',pg_current_snapshot()::text);
END $$;
CREATE FUNCTION source.fence_result(id uuid) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT to_jsonb(f)||jsonb_build_object('member_count',member_count::text,'max_key',max_key::text) FROM source.backfill_fences f WHERE run_id=id AND member_count IS NOT NULL
$$;
CREATE FUNCTION source.seal_backfill_fence(id uuid,epoch uuid,instance uuid) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE inserted integer; f source.backfill_fences;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Fence requires READ COMMITTED' USING ERRCODE='25001'; END IF;
 PERFORM source.backfill_identity(epoch,instance);
 INSERT INTO source.backfill_fences(run_id,pipeline_id,source_epoch,codec) VALUES(id,instance,epoch,'pg18-jsonb-text/v1') ON CONFLICT(run_id) DO NOTHING;
 GET DIAGNOSTICS inserted=ROW_COUNT;
 -- Fresh statement after arbitration: the competing committed row may not be in INSERT's snapshot.
 SELECT * INTO STRICT f FROM source.backfill_fences WHERE run_id=id;
 IF f.source_epoch IS DISTINCT FROM epoch OR f.pipeline_id IS DISTINCT FROM instance THEN RAISE EXCEPTION 'Fence key binding conflict' USING ERRCODE='P8001'; END IF;
 IF inserted=1 THEN
  WITH cut AS MATERIALIZED (SELECT pg_current_snapshot()::text snapshot,clock_timestamp() observed_at), copied AS (
   INSERT INTO source.backfill_fence_members(run_id,allocation_id,source_epoch,entity_id,entity_version)
   SELECT id,o.allocation_id,o.source_epoch,o.entity_id,o.entity_version FROM source.outbox o CROSS JOIN cut WHERE o.source_epoch=epoch RETURNING allocation_id
  ) UPDATE source.backfill_fences SET snapshot=cut.snapshot,observed_at=cut.observed_at,member_count=(SELECT count(*) FROM copied),max_key=(SELECT coalesce(max(allocation_id),0) FROM copied) FROM cut WHERE run_id=id;
 END IF;
 RETURN source.fence_result(id);
END $$;
CREATE FUNCTION source.backfill_page(epoch uuid,instance uuid,after_key bigint,end_key bigint,n integer,fence uuid DEFAULT NULL) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE answer jsonb;
BEGIN
 PERFORM source.backfill_identity(epoch,instance);
 IF after_key IS NULL OR end_key IS NULL OR after_key<0 OR end_key<after_key OR n IS NULL OR n NOT BETWEEN 1 AND 16 THEN RAISE EXCEPTION 'Invalid bounded backfill selection' USING ERRCODE='P8003'; END IF;
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
REVOKE ALL ON ALL TABLES IN SCHEMA source FROM source_backfill;
REVOKE ALL ON FUNCTION source.guard_backfill_fence(),source.guard_fence_member(),source.require_complete_fence(),source.backfill_identity(uuid,uuid),source.fence_result(uuid),source.seal_backfill_fence(uuid,uuid,uuid),source.backfill_page(uuid,uuid,bigint,bigint,integer,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION source.backfill_identity(uuid,uuid),source.seal_backfill_fence(uuid,uuid,uuid),source.backfill_page(uuid,uuid,bigint,bigint,integer,uuid) TO source_backfill;
RESET ROLE;
