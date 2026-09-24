-- Additive runtime cadence controls. No event, lease or checkpoint changes.
SET LOCAL ROLE pipeline_owner;
CREATE TABLE pipeline.polling_settings (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 revision bigint NOT NULL CHECK(revision>0),
 capture_poll_ms integer NOT NULL CHECK(capture_poll_ms BETWEEN 50 AND 30000),
 backfill_idle_ms integer NOT NULL CHECK(backfill_idle_ms BETWEEN 50 AND 30000),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO pipeline.polling_settings(singleton,revision,capture_poll_ms,backfill_idle_ms) VALUES(true,1,1000,1000);
CREATE TABLE pipeline.polling_requests (
 request_id uuid PRIMARY KEY, request_correlation uuid NOT NULL,
 actor text NOT NULL CHECK(actor='local-operator'),
 input jsonb NOT NULL, result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON pipeline.polling_requests FOR EACH ROW EXECUTE FUNCTION pipeline.immutable();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pipeline.polling_requests FOR EACH STATEMENT EXECUTE FUNCTION pipeline.immutable();
CREATE FUNCTION pipeline.read_polling() RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('revision',revision::text,'capture_poll_ms',capture_poll_ms,'backfill_idle_ms',backfill_idle_ms,'updated_at',updated_at) FROM pipeline.polling_settings WHERE singleton
$$;
CREATE FUNCTION pipeline.polling_status() RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT pipeline.read_polling()||jsonb_build_object('observed_at',clock_timestamp(),'minimum_ms',50,'maximum_ms',30000,'recent_changes',
 coalesce((SELECT jsonb_agg(to_jsonb(r)) FROM (SELECT request_id,request_correlation,actor,result,created_at FROM pipeline.polling_requests ORDER BY created_at DESC,request_id DESC LIMIT 10) r),'[]'::jsonb))
$$;
CREATE FUNCTION pipeline.set_polling(key uuid,correlation uuid,expected bigint,capture_ms integer,backfill_ms integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE old pipeline.polling_requests; cfg pipeline.polling_settings; input jsonb; result jsonb;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'READ COMMITTED required' USING ERRCODE='25001'; END IF;
 IF key IS NULL OR correlation IS NULL OR expected IS NULL OR expected<1 OR capture_ms IS NULL OR capture_ms NOT BETWEEN 50 AND 30000 OR backfill_ms IS NULL OR backfill_ms NOT BETWEEN 50 AND 30000 THEN RAISE EXCEPTION 'Invalid polling settings' USING ERRCODE='22023'; END IF;
 input:=jsonb_build_object('expected_revision',expected::text,'capture_poll_ms',capture_ms,'backfill_idle_ms',backfill_ms);
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,920));
 SELECT * INTO old FROM pipeline.polling_requests WHERE request_id=key;
 IF FOUND THEN
  IF old.input IS DISTINCT FROM input THEN RAISE EXCEPTION 'Polling key conflict' USING ERRCODE='P9001'; END IF;
  RETURN old.result||jsonb_build_object('replayed',true);
 END IF;
 SELECT * INTO STRICT cfg FROM pipeline.polling_settings WHERE singleton FOR UPDATE;
 IF cfg.revision<>expected THEN RAISE EXCEPTION 'Polling revision conflict' USING ERRCODE='P9001'; END IF;
 UPDATE pipeline.polling_settings SET revision=revision+1,capture_poll_ms=capture_ms,backfill_idle_ms=backfill_ms,updated_at=clock_timestamp() WHERE singleton;
 result:=pipeline.read_polling()||jsonb_build_object('request_id',key,'replayed',false);
 INSERT INTO pipeline.polling_requests(request_id,request_correlation,actor,input,result) VALUES(key,correlation,'local-operator',input,result);
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION pipeline.read_polling(),pipeline.polling_status(),pipeline.set_polling(uuid,uuid,bigint,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.read_polling() TO pipeline_capture,pipeline_backfill,pipeline_operator;
GRANT EXECUTE ON FUNCTION pipeline.polling_status(),pipeline.set_polling(uuid,uuid,bigint,integer,integer) TO pipeline_operator;
RESET ROLE;
