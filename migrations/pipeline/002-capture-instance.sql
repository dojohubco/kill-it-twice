-- Forward-only M2C identity. The existing immutable guard also protects this column.
CREATE ROLE pipeline_capture NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE pipeline_m2b TO pipeline_capture;
GRANT USAGE ON SCHEMA pipeline TO pipeline_capture;
SET LOCAL ROLE pipeline_owner;
ALTER TABLE pipeline.source_binding ADD COLUMN pipeline_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid();
CREATE FUNCTION pipeline.capture_identity()
RETURNS TABLE(pipeline_id uuid, source_epoch uuid, payload_encoding text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog,pg_temp AS $$
  SELECT pipeline_id,source_epoch,payload_encoding FROM pipeline.source_binding WHERE singleton;
$$;
CREATE FUNCTION pipeline.stage_bound_event(expected_pipeline uuid, bytes bytea, digest text) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog,pg_temp AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pipeline.source_binding WHERE singleton AND pipeline_id=expected_pipeline) THEN
    RAISE EXCEPTION 'Unexpected pipeline instance' USING ERRCODE='P4001';
  END IF;
  RETURN pipeline.stage_event(bytes,digest);
END $$;
REVOKE ALL ON FUNCTION pipeline.capture_identity(),pipeline.stage_bound_event(uuid,bytea,text) FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM pipeline_capture;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pipeline FROM pipeline_capture;
GRANT EXECUTE ON FUNCTION pipeline.capture_identity(),pipeline.stage_bound_event(uuid,bytea,text),pipeline.record_incident(text,text,text) TO pipeline_capture;
GRANT EXECUTE ON FUNCTION pipeline.capture_identity() TO pipeline_stager;
RESET ROLE;
