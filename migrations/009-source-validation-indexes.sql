-- Bounded identity lookups; complete retained-tuple validation and old triggers stay in force.
SET LOCAL ROLE source_owner;
CREATE INDEX baseline_chunk_members ON source.baseline_revisions(bootstrap_key,chunk_first);
CREATE OR REPLACE FUNCTION source.require_current_baseline() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NEW.change_id IS NULL AND NOT EXISTS(SELECT FROM source.baseline_revisions b WHERE b.source_epoch=NEW.source_epoch AND b.entity_id=NEW.entity_id AND b.entity_version=NEW.entity_version AND ROW(b.source_epoch,b.entity_id,b.entity_version,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(NEW.source_epoch,NEW.entity_id,NEW.entity_version,NEW.recorded_at,NEW.is_deleted,NEW.payload::text)) THEN RAISE EXCEPTION 'Current baseline lacks retained evidence' USING ERRCODE='P7003'; END IF; RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION source.require_command_completion() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r source.command_receipts; valid boolean;
BEGIN
 SELECT * INTO STRICT r FROM source.command_receipts WHERE source_epoch=NEW.source_epoch AND command_id=NEW.command_id;
 IF NOT r.completed THEN RAISE EXCEPTION 'incomplete command receipt cannot commit' USING ERRCODE='P2003'; END IF;
 IF r.result_change_id IS NULL THEN
 SELECT EXISTS(SELECT FROM source.baseline_revisions b WHERE b.source_epoch=r.source_epoch AND b.entity_id=r.result_entity_id AND b.entity_version=r.result_version AND ROW(b.source_epoch,b.entity_id,b.entity_version,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(r.source_epoch,r.result_entity_id,r.result_version,r.result_recorded_at,r.result_deleted,r.result_payload::text)) INTO valid;
 ELSE
 SELECT EXISTS(SELECT FROM source.outbox b WHERE b.source_epoch=r.source_epoch AND b.entity_id=r.result_entity_id AND b.entity_version=r.result_version AND ROW(b.source_epoch,b.entity_id,b.entity_version,b.change_id,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(r.source_epoch,r.result_entity_id,r.result_version,r.result_change_id,r.result_recorded_at,r.result_deleted,r.result_payload::text)) INTO valid;
 END IF;
 IF valid IS NOT TRUE THEN RAISE EXCEPTION 'Command result does not match retained revision' USING ERRCODE='P2003'; END IF; RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION source.require_current_baseline(),source.require_command_completion() FROM PUBLIC;
RESET ROLE;
