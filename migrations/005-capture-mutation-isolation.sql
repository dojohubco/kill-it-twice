-- Forward correction: a registration table lock cannot refresh a writer's transaction snapshot.
SET LOCAL ROLE source_owner;
CREATE OR REPLACE FUNCTION source.enqueue_capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  -- Check BEFORE looking for the binding: an old snapshot can hide a committed binding.
  IF pg_catalog.current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
    RAISE EXCEPTION 'captured source mutations require READ COMMITTED' USING ERRCODE='25001';
  END IF;
  INSERT INTO source.capture_work(pipeline_id,source_epoch,entity_id,entity_version)
  SELECT pipeline_id,NEW.source_epoch,NEW.entity_id,NEW.entity_version FROM source.capture_binding WHERE singleton;
  RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION source.enqueue_capture() FROM PUBLIC;
RESET ROLE;
