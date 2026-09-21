-- Immutable history permits induction over contiguous chunks; validate old data first.
SET LOCAL ROLE source_owner;
DO $$ DECLARE m source.bootstrap_manifest; c source.bootstrap_chunks;
BEGIN
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton;
 IF m.bootstrap_key IS NOT NULL THEN
  IF m.completed_count<>(SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=m.bootstrap_key) OR
     m.completed_count<>(SELECT coalesce(sum(item_count),0) FROM source.bootstrap_chunks WHERE bootstrap_key=m.bootstrap_key) THEN
   RAISE EXCEPTION 'Existing seed progress lacks evidence' USING ERRCODE='P7003';
  END IF;
  FOR c IN SELECT * FROM source.bootstrap_chunks WHERE bootstrap_key=m.bootstrap_key LOOP
   PERFORM source.assert_bootstrap_chunk(c.bootstrap_key,c.first_ordinal);
  END LOOP;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION source.require_bootstrap_chunk() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest; c source.bootstrap_chunks;
BEGIN
 IF TG_TABLE_NAME='bootstrap_chunks' THEN
  PERFORM source.assert_bootstrap_chunk(NEW.bootstrap_key,NEW.first_ordinal);
 ELSE
  SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE bootstrap_key=NEW.bootstrap_key;
  SELECT * INTO c FROM source.bootstrap_chunks WHERE bootstrap_key=NEW.bootstrap_key AND first_ordinal=NEW.chunk_first;
  IF NOT FOUND OR NEW.ordinal NOT BETWEEN c.first_ordinal AND c.first_ordinal+c.item_count-1 OR
     NEW.ordinal>m.requested_count OR (c.first_ordinal-1)%m.chunk_size<>0 OR
     c.item_count<>least(m.chunk_size,m.requested_count-c.first_ordinal+1) OR
     NEW.source_epoch<>m.source_epoch OR NEW.recorded_at<>m.baseline_at OR
     NEW.payload IS DISTINCT FROM source.seed_payload(m.seed,NEW.ordinal) THEN
   RAISE EXCEPTION 'Incomplete or mismatched seed row' USING ERRCODE='P7003';
  END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION source.require_bootstrap_progress() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton;
 IF NEW.bootstrap_key IS NOT NULL THEN
  IF TG_OP='UPDATE' AND NEW.completed_count>OLD.completed_count THEN
   IF OLD.phase<>'bootstrapping' OR NEW.phase<>'bootstrapping' OR
      OLD.bootstrap_key IS DISTINCT FROM NEW.bootstrap_key OR NOT EXISTS(
      SELECT FROM source.bootstrap_chunks c WHERE c.bootstrap_key=NEW.bootstrap_key AND
      c.first_ordinal=OLD.completed_count+1 AND c.item_count=NEW.completed_count-OLD.completed_count) THEN
    RAISE EXCEPTION 'Seed progress lacks matching next chunk' USING ERRCODE='P7003';
   END IF;
   PERFORM source.assert_bootstrap_chunk(NEW.bootstrap_key,OLD.completed_count+1);
  ELSE
   -- Beginning, sealing and activation still inspect the complete accumulated evidence.
   IF m.completed_count<>(SELECT coalesce(sum(item_count),0) FROM source.bootstrap_chunks WHERE bootstrap_key=m.bootstrap_key) OR
      m.completed_count<>(SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=m.bootstrap_key) THEN
    RAISE EXCEPTION 'Seed progress lacks committed evidence' USING ERRCODE='P7003';
   END IF;
  END IF;
 END IF;
 IF m.phase='active' AND m.origin='fresh' AND NOT EXISTS(SELECT FROM source.capture_binding WHERE source_epoch=m.source_epoch) THEN RAISE EXCEPTION 'Active bootstrap lacks capture binding' USING ERRCODE='P7003'; END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION source.require_bootstrap_chunk(),source.require_bootstrap_progress() FROM PUBLIC;
RESET ROLE;
