-- Forward M5A source extension. Apply under controlled initialization in one transaction.
CREATE ROLE source_bootstrap NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE source_seed_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE source_m1 TO source_bootstrap;
GRANT USAGE ON SCHEMA source TO source_bootstrap,source_seed_owner;
SET LOCAL ROLE source_owner;
-- Controlled install excludes command/entity writes before deciding fresh versus legacy.
LOCK TABLE source.command_receipts,source.entities IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE source.bootstrap_manifest (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 source_epoch uuid NOT NULL UNIQUE REFERENCES source.source_identity(source_epoch),
 phase text NOT NULL CHECK(phase IN ('unselected','bootstrapping','sealed','active')),
 origin text NOT NULL CHECK(origin IN ('fresh','legacy')),
 bootstrap_key uuid UNIQUE,
 recipe_version integer,
 seed text,
 requested_count bigint,
 chunk_size integer,
 baseline_at timestamptz,
 completed_count bigint NOT NULL DEFAULT 0 CHECK(completed_count>=0),
 sealed_at timestamptz,
 activated_at timestamptz,
 CONSTRAINT bootstrap_shape CHECK ((
  (bootstrap_key IS NULL AND recipe_version IS NULL AND seed IS NULL AND requested_count IS NULL AND chunk_size IS NULL AND baseline_at IS NULL AND completed_count=0 AND sealed_at IS NULL AND
   ((origin='fresh' AND phase='unselected' AND activated_at IS NULL) OR (origin='legacy' AND phase='active' AND activated_at IS NOT NULL AND isfinite(activated_at)))) OR
  (origin='fresh' AND bootstrap_key IS NOT NULL AND recipe_version=1 AND seed IS NOT NULL AND octet_length(seed) BETWEEN 1 AND 128 AND requested_count BETWEEN 1 AND 2000000 AND chunk_size BETWEEN 1 AND 128 AND baseline_at IS NOT NULL AND isfinite(baseline_at) AND completed_count<=requested_count AND
   ((phase='bootstrapping' AND sealed_at IS NULL AND activated_at IS NULL) OR
    (phase='sealed' AND completed_count=requested_count AND sealed_at IS NOT NULL AND isfinite(sealed_at) AND activated_at IS NULL) OR
    (phase='active' AND completed_count=requested_count AND sealed_at IS NOT NULL AND isfinite(sealed_at) AND activated_at IS NOT NULL AND isfinite(activated_at))))
 ) IS TRUE)
);
INSERT INTO source.bootstrap_manifest(source_epoch,phase,origin,activated_at)
 SELECT source_epoch,CASE WHEN used THEN 'active' ELSE 'unselected' END,CASE WHEN used THEN 'legacy' ELSE 'fresh' END,CASE WHEN used THEN clock_timestamp() END
 FROM source.source_identity CROSS JOIN LATERAL (SELECT EXISTS(SELECT FROM source.entities) OR EXISTS(SELECT FROM source.outbox) OR EXISTS(SELECT FROM source.command_receipts) OR EXISTS(SELECT FROM source.capture_binding) AS used) u;
CREATE TABLE source.bootstrap_chunks (
 bootstrap_key uuid NOT NULL REFERENCES source.bootstrap_manifest(bootstrap_key),
 first_ordinal bigint NOT NULL CHECK(first_ordinal>0),
 item_count integer NOT NULL CHECK(item_count BETWEEN 1 AND 128),
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK(isfinite(committed_at)),
 PRIMARY KEY(bootstrap_key,first_ordinal)
);
CREATE TABLE source.baseline_revisions (
 source_epoch uuid NOT NULL REFERENCES source.source_identity(source_epoch),
 entity_id bigint NOT NULL REFERENCES source.entities(entity_id),
 entity_version bigint NOT NULL CHECK(entity_version=1),
 change_id uuid CHECK(change_id IS NULL),
 recorded_at timestamptz NOT NULL CHECK(isfinite(recorded_at)),
 is_deleted boolean NOT NULL CHECK(NOT is_deleted),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 bootstrap_key uuid NOT NULL REFERENCES source.bootstrap_manifest(bootstrap_key),
 ordinal bigint NOT NULL CHECK(ordinal>0),
 chunk_first bigint NOT NULL CHECK(chunk_first>0),
 PRIMARY KEY(source_epoch,entity_id,entity_version),
 UNIQUE(bootstrap_key,ordinal),
 FOREIGN KEY(bootstrap_key,chunk_first) REFERENCES source.bootstrap_chunks(bootstrap_key,first_ordinal) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER baseline_no_rewrite BEFORE UPDATE OR DELETE ON source.baseline_revisions FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER baseline_no_truncate BEFORE TRUNCATE ON source.baseline_revisions FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER chunks_no_rewrite BEFORE UPDATE OR DELETE ON source.bootstrap_chunks FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER chunks_no_truncate BEFORE TRUNCATE ON source.bootstrap_chunks FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER manifest_no_delete BEFORE DELETE ON source.bootstrap_manifest FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER manifest_no_truncate BEFORE TRUNCATE ON source.bootstrap_manifest FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE FUNCTION source.guard_bootstrap_manifest() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF OLD.phase='active' OR ROW(NEW.singleton,NEW.source_epoch,NEW.origin) IS DISTINCT FROM ROW(OLD.singleton,OLD.source_epoch,OLD.origin) OR
 (OLD.phase<>'unselected' AND ROW(NEW.bootstrap_key,NEW.recipe_version,NEW.seed,NEW.requested_count,NEW.chunk_size,NEW.baseline_at) IS DISTINCT FROM ROW(OLD.bootstrap_key,OLD.recipe_version,OLD.seed,OLD.requested_count,OLD.chunk_size,OLD.baseline_at)) OR
 NEW.completed_count<OLD.completed_count OR
 NOT ((OLD.phase='unselected' AND NEW.phase='bootstrapping') OR (OLD.phase='bootstrapping' AND NEW.phase IN ('bootstrapping','sealed')) OR (OLD.phase='sealed' AND NEW.phase='active')) THEN
 RAISE EXCEPTION 'Bootstrap identity or transition is immutable' USING ERRCODE='P7001'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER manifest_guard BEFORE UPDATE ON source.bootstrap_manifest FOR EACH ROW EXECUTE FUNCTION source.guard_bootstrap_manifest();
CREATE FUNCTION source.seed_payload(seed text,ordinal bigint) RETURNS jsonb LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('name','Seed '||seed||' #'||ordinal,'country',CASE WHEN ordinal%2=0 THEN 'GE' ELSE 'FR' END,'loyalty_points',ordinal%1000,'seed_ordinal',ordinal::text,'tags',jsonb_build_array('ქართული','café',ordinal%7,NULL),'optional',CASE WHEN ordinal%3=0 THEN NULL ELSE 'value' END,'exact',9007199254740993::numeric+ordinal,'decimal',0.123456789012345678901234567890::numeric,'padding',repeat(chr(97+(ordinal%26)::integer),768))
$$;
CREATE FUNCTION source.assert_bootstrap_chunk(key uuid,start_ordinal bigint) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest; c source.bootstrap_chunks;
BEGIN
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE bootstrap_key=key;
 SELECT * INTO c FROM source.bootstrap_chunks WHERE bootstrap_key=key AND first_ordinal=start_ordinal;
 IF NOT FOUND OR start_ordinal>m.requested_count OR (start_ordinal-1)%m.chunk_size<>0 OR c.item_count<>least(m.chunk_size,m.requested_count-start_ordinal+1) OR
 (SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=key AND chunk_first=start_ordinal)<>c.item_count OR EXISTS(
 SELECT FROM generate_series(start_ordinal,start_ordinal+c.item_count-1) g(ordinal) LEFT JOIN source.baseline_revisions b ON b.bootstrap_key=key AND b.ordinal=g.ordinal
 WHERE b.ordinal IS NULL OR b.chunk_first<>start_ordinal OR b.source_epoch<>m.source_epoch OR b.recorded_at<>m.baseline_at OR b.payload IS DISTINCT FROM source.seed_payload(m.seed,g.ordinal)) THEN
 RAISE EXCEPTION 'Incomplete or mismatched seed chunk' USING ERRCODE='P7003'; END IF;
END $$;
CREATE FUNCTION source.require_bootstrap_chunk() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_TABLE_NAME='bootstrap_chunks' THEN PERFORM source.assert_bootstrap_chunk(NEW.bootstrap_key,NEW.first_ordinal); ELSE PERFORM source.assert_bootstrap_chunk(NEW.bootstrap_key,NEW.chunk_first); END IF; RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER chunk_complete AFTER INSERT ON source.bootstrap_chunks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_bootstrap_chunk();
CREATE CONSTRAINT TRIGGER baseline_complete AFTER INSERT ON source.baseline_revisions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_bootstrap_chunk();
CREATE FUNCTION source.require_active() RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE phase text;
BEGIN
 -- RC writers share this row lock; transitions take FOR UPDATE before any entity/outbox lock.
 IF current_setting('transaction_isolation')='read committed' THEN
 SELECT m.phase INTO phase FROM source.bootstrap_manifest m WHERE singleton FOR SHARE;
 ELSE
 -- Active is terminal. Existing read-only/no-revision snapshot use remains supported;
 -- actual changed revisions still hit the unconditional 25001 enqueue guard.
 SELECT m.phase INTO phase FROM source.bootstrap_manifest m WHERE singleton;
 END IF;
 IF phase IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'Source is closed for bootstrap' USING ERRCODE='P7001'; END IF;
END $$;
ALTER TABLE source.entities ALTER COLUMN change_id DROP NOT NULL;
ALTER TABLE source.entities ADD CONSTRAINT entities_revision_kind CHECK(change_id IS NOT NULL OR (entity_version=1 AND NOT is_deleted));
CREATE OR REPLACE FUNCTION source.prepare_revision() RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 IF current_user='source_seed_owner' THEN
  IF TG_OP<>'INSERT' OR current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Invalid seed operation/isolation' USING ERRCODE='25001'; END IF;
  SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton FOR UPDATE;
  IF m.phase<>'bootstrapping' OR NEW.source_epoch IS NOT NULL OR NEW.entity_version IS NOT NULL OR NEW.change_id IS NOT NULL OR NEW.recorded_at IS NOT NULL OR NEW.is_deleted THEN RAISE EXCEPTION 'Invalid protected seed insertion' USING ERRCODE='P7001'; END IF;
  NEW.source_epoch:=m.source_epoch; NEW.entity_version:=1; NEW.change_id:=NULL; NEW.recorded_at:=m.baseline_at; RETURN NEW;
 END IF;
 PERFORM source.require_active();
 IF TG_OP='INSERT' THEN
  IF NEW.source_epoch IS NOT NULL OR NEW.entity_version IS NOT NULL OR NEW.change_id IS NOT NULL OR NEW.recorded_at IS NOT NULL OR NEW.is_deleted THEN RAISE EXCEPTION 'source metadata is database-owned; insert must be live' USING ERRCODE='42501'; END IF;
  SELECT source_epoch INTO STRICT NEW.source_epoch FROM source.source_identity WHERE singleton; NEW.entity_version:=1;
 ELSIF TG_OP='UPDATE' THEN
  IF ROW(NEW.entity_id,NEW.source_epoch,NEW.entity_version,NEW.change_id,NEW.recorded_at) IS DISTINCT FROM ROW(OLD.entity_id,OLD.source_epoch,OLD.entity_version,OLD.change_id,OLD.recorded_at) THEN RAISE EXCEPTION 'source metadata is database-owned' USING ERRCODE='42501'; END IF;
  IF NEW.is_deleted=OLD.is_deleted AND NEW.payload IS NOT DISTINCT FROM OLD.payload THEN RETURN NULL; END IF;
  NEW.entity_version:=source.next_version(OLD.entity_version);
 ELSE RAISE EXCEPTION 'unsupported source operation' USING ERRCODE='0A000'; END IF;
 NEW.change_id:=gen_random_uuid(); NEW.recorded_at:=clock_timestamp(); RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION source.capture_revision() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest; ordinal bigint;
BEGIN
 IF NEW.change_id IS NULL THEN
  SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton;
  ordinal:=(NEW.payload->>'seed_ordinal')::bigint;
  IF TG_OP<>'INSERT' OR m.phase<>'bootstrapping' OR ordinal IS NULL OR ordinal NOT BETWEEN 1 AND m.requested_count OR NEW.payload IS DISTINCT FROM source.seed_payload(m.seed,ordinal) THEN RAISE EXCEPTION 'Invalid baseline origin' USING ERRCODE='P7003'; END IF;
  INSERT INTO source.baseline_revisions VALUES(NEW.source_epoch,NEW.entity_id,NEW.entity_version,NULL,NEW.recorded_at,NEW.is_deleted,NEW.payload,m.bootstrap_key,ordinal,1+((ordinal-1)/m.chunk_size)*m.chunk_size);
 ELSE
  INSERT INTO source.outbox(source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload) VALUES(NEW.source_epoch,NEW.entity_id,NEW.entity_version,NEW.change_id,NEW.recorded_at,NEW.is_deleted,NEW.payload);
 END IF; RETURN NEW;
END $$;
CREATE FUNCTION source.require_current_baseline() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NEW.change_id IS NULL AND NOT EXISTS(SELECT FROM source.baseline_revisions b WHERE ROW(b.source_epoch,b.entity_id,b.entity_version,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(NEW.source_epoch,NEW.entity_id,NEW.entity_version,NEW.recorded_at,NEW.is_deleted,NEW.payload::text)) THEN RAISE EXCEPTION 'Current baseline lacks retained evidence' USING ERRCODE='P7003'; END IF; RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER current_baseline_history AFTER INSERT OR UPDATE ON source.entities DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_current_baseline();
-- The result discriminator is its protected nullable change identity, not an unchecked nullable FK.
DO $$ DECLARE constraint_name text; BEGIN
 SELECT conname INTO STRICT constraint_name FROM pg_constraint WHERE conrelid='source.command_receipts'::regclass AND confrelid='source.outbox'::regclass;
 EXECUTE format('ALTER TABLE source.command_receipts DROP CONSTRAINT %I',constraint_name);
END $$;
ALTER TABLE source.command_receipts DROP CONSTRAINT command_result_shape;
ALTER TABLE source.command_receipts ADD CONSTRAINT command_result_shape CHECK ((
 (NOT completed AND result_entity_id IS NULL AND result_version IS NULL AND result_change_id IS NULL AND result_recorded_at IS NULL AND result_deleted IS NULL AND result_payload IS NULL) OR
 (completed AND result_entity_id>0 AND result_version>0 AND (result_change_id IS NOT NULL OR (result_version=1 AND result_deleted=false)) AND result_recorded_at IS NOT NULL AND isfinite(result_recorded_at) AND result_deleted IS NOT NULL AND ((result_deleted AND result_payload IS NULL) OR (NOT result_deleted AND result_payload IS NOT NULL AND jsonb_typeof(result_payload)='object')))
) IS TRUE);
CREATE OR REPLACE FUNCTION source.require_command_completion() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r source.command_receipts; valid boolean;
BEGIN
 SELECT * INTO STRICT r FROM source.command_receipts WHERE source_epoch=NEW.source_epoch AND command_id=NEW.command_id;
 IF NOT r.completed THEN RAISE EXCEPTION 'incomplete command receipt cannot commit' USING ERRCODE='P2003'; END IF;
 IF r.result_change_id IS NULL THEN
 SELECT EXISTS(SELECT FROM source.baseline_revisions b WHERE ROW(b.source_epoch,b.entity_id,b.entity_version,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(r.source_epoch,r.result_entity_id,r.result_version,r.result_recorded_at,r.result_deleted,r.result_payload::text)) INTO valid;
 ELSE
 SELECT EXISTS(SELECT FROM source.outbox b WHERE ROW(b.source_epoch,b.entity_id,b.entity_version,b.change_id,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(r.source_epoch,r.result_entity_id,r.result_version,r.result_change_id,r.result_recorded_at,r.result_deleted,r.result_payload::text)) INTO valid;
 END IF;
 IF valid IS NOT TRUE THEN RAISE EXCEPTION 'Command result does not match retained revision' USING ERRCODE='P2003'; END IF; RETURN NULL;
END $$;
-- Validate all old receipts without firing an UPDATE or rewriting any retained value.
DO $$ BEGIN
 IF EXISTS(SELECT FROM source.command_receipts r WHERE NOT r.completed OR NOT EXISTS(SELECT FROM source.outbox b WHERE ROW(b.source_epoch,b.entity_id,b.entity_version,b.change_id,b.recorded_at,b.is_deleted,b.payload::text) IS NOT DISTINCT FROM ROW(r.source_epoch,r.result_entity_id,r.result_version,r.result_change_id,r.result_recorded_at,r.result_deleted,r.result_payload::text))) THEN RAISE EXCEPTION 'Existing command history is inconsistent' USING ERRCODE='P2003'; END IF;
END $$;
-- Preserve existing mutation/command entry points and OIDs, adding admission before any early return.
CREATE OR REPLACE FUNCTION source.create_entity(live_payload jsonb) RETURNS source.entities
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE result source.entities;
BEGIN
  PERFORM source.require_active();
  INSERT INTO source.entities (payload) VALUES (live_payload) RETURNING * INTO result;
  RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION source.mutate_entity(target_id bigint, operation text, live_payload jsonb) RETURNS source.entities
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE result source.entities;
BEGIN
  PERFORM source.require_active();
  IF operation IS NULL OR operation NOT IN ('update', 'delete', 'restore') THEN
    RAISE EXCEPTION 'unsupported mutation operation' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO result FROM source.entities WHERE entity_id = target_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'entity does not exist' USING ERRCODE = 'P0002'; END IF;
  IF operation = 'update' THEN
    IF result.is_deleted THEN RAISE EXCEPTION 'deleted identity requires explicit restore' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET payload = live_payload WHERE entity_id = target_id;
  ELSIF operation = 'delete' THEN
    IF live_payload IS NOT NULL THEN RAISE EXCEPTION 'delete takes no live payload' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET is_deleted = true, payload = NULL WHERE entity_id = target_id;
  ELSE
    IF NOT result.is_deleted THEN RAISE EXCEPTION 'restore requires a deleted identity' USING ERRCODE = '22023'; END IF;
    UPDATE source.entities SET is_deleted = false, payload = live_payload WHERE entity_id = target_id;
  END IF;
  -- A no-op BEFORE trigger suppresses UPDATE; return the unchanged locked row.
  SELECT * INTO STRICT result FROM source.entities WHERE entity_id = target_id;
  RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION source.execute_command(
  expected_epoch uuid, requested_command uuid, requested_version integer,
  requested_operation text, requested_target bigint, requested_payload jsonb
) RETURNS TABLE (
  entity_id bigint, source_epoch uuid, entity_version bigint, change_id uuid,
  recorded_at text, is_deleted boolean, payload_json text, replayed boolean
)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  receipt source.command_receipts;
  result source.entities;
  allocated integer;
  was_replayed boolean := false;
BEGIN
  PERFORM source.require_active();
  IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'source commands require READ COMMITTED' USING ERRCODE = '25001';
  END IF;
  IF expected_epoch IS NULL OR NOT EXISTS (
    SELECT FROM source.source_identity i WHERE i.singleton AND i.source_epoch = expected_epoch
  ) THEN
    RAISE EXCEPTION 'expected source epoch does not match persisted identity' USING ERRCODE = 'P2002';
  END IF;
  IF requested_command IS NULL OR requested_version IS NULL OR requested_version < 1 OR
     requested_operation IS NULL OR requested_operation NOT IN ('create', 'update', 'delete', 'restore') OR
     (requested_operation = 'create' AND requested_target IS NOT NULL) OR
     (requested_operation <> 'create' AND (requested_target IS NULL OR requested_target < 1)) OR
     (requested_operation = 'delete' AND requested_payload IS NOT NULL) OR
     (requested_operation <> 'delete' AND (requested_payload IS NULL OR pg_catalog.jsonb_typeof(requested_payload) <> 'object')) THEN
    RAISE EXCEPTION 'invalid source command request' USING ERRCODE = '22023';
  END IF;

  -- Unique-index arbitration waits for the actual competing transaction outcome.
  INSERT INTO source.command_receipts (source_epoch, command_id, contract_version, operation, target_id, request_payload)
  VALUES (expected_epoch, requested_command, requested_version, requested_operation, requested_target, requested_payload)
  ON CONFLICT ON CONSTRAINT command_receipts_pkey DO NOTHING;
  GET DIAGNOSTICS allocated = ROW_COUNT;
  IF allocated = 0 THEN
    -- Separate statement in a VOLATILE function: fresh READ COMMITTED snapshot.
    SELECT * INTO STRICT receipt FROM source.command_receipts r
    WHERE r.source_epoch = expected_epoch AND r.command_id = requested_command;
    IF ROW(receipt.contract_version, receipt.operation, receipt.target_id, receipt.request_payload)
       IS DISTINCT FROM ROW(requested_version, requested_operation, requested_target, requested_payload) THEN
      RAISE EXCEPTION 'command identity is bound to a different request' USING ERRCODE = 'P2001';
    END IF;
    IF NOT receipt.completed THEN
      RAISE EXCEPTION 'incomplete command receipt' USING ERRCODE = 'P2003';
    END IF;
    was_replayed := true;
  ELSE
    IF requested_version <> 1 THEN
      RAISE EXCEPTION 'unsupported source command contract version' USING ERRCODE = '22023';
    END IF;
    IF requested_operation = 'create' THEN
      result := source.create_entity(requested_payload);
    ELSE
      result := source.mutate_entity(requested_target, requested_operation, requested_payload);
    END IF;
    UPDATE source.command_receipts r SET
      completed = true, result_entity_id = result.entity_id, result_version = result.entity_version,
      result_change_id = result.change_id, result_recorded_at = result.recorded_at,
      result_deleted = result.is_deleted, result_payload = result.payload
    WHERE r.source_epoch = expected_epoch AND r.command_id = requested_command
    RETURNING r.* INTO STRICT receipt;
  END IF;
  RETURN QUERY SELECT receipt.result_entity_id, receipt.source_epoch, receipt.result_version,
    receipt.result_change_id,
    pg_catalog.to_char(receipt.result_recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    receipt.result_deleted, receipt.result_payload::text, was_replayed;
END;
$$;

CREATE FUNCTION source.bootstrap_status(expected_epoch uuid) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton;
 IF expected_epoch IS DISTINCT FROM m.source_epoch THEN RAISE EXCEPTION 'Bootstrap epoch mismatch' USING ERRCODE='P2002'; END IF;
 RETURN jsonb_build_object('source_epoch',m.source_epoch,'phase',m.phase,'origin',m.origin,'bootstrap_key',m.bootstrap_key,'recipe_version',m.recipe_version,'seed',m.seed,'requested_count',m.requested_count::text,'chunk_size',m.chunk_size,'baseline_at',to_char(m.baseline_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'completed_count',m.completed_count::text,'sealed_at',m.sealed_at::text,'activated_at',m.activated_at::text);
END $$;
CREATE FUNCTION source.begin_bootstrap(expected_epoch uuid,key uuid,recipe integer,requested_seed text,total bigint,bound integer) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Bootstrap requires READ COMMITTED' USING ERRCODE='25001'; END IF;
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton FOR UPDATE;
 IF expected_epoch IS DISTINCT FROM m.source_epoch THEN RAISE EXCEPTION 'Bootstrap epoch mismatch' USING ERRCODE='P2002'; END IF;
 IF key IS NULL OR recipe IS DISTINCT FROM 1 OR requested_seed IS NULL OR octet_length(requested_seed) NOT BETWEEN 1 AND 128 OR total IS NULL OR total NOT BETWEEN 1 AND 2000000 OR bound IS NULL OR bound NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'Invalid bootstrap recipe or bounds' USING ERRCODE='22023'; END IF;
 IF m.phase<>'unselected' THEN
  IF ROW(m.bootstrap_key,m.recipe_version,m.seed,m.requested_count,m.chunk_size) IS DISTINCT FROM ROW(key,recipe,requested_seed,total,bound) THEN RAISE EXCEPTION 'Bootstrap manifest conflict or legacy active epoch' USING ERRCODE='P7002'; END IF;
  RETURN source.bootstrap_status(expected_epoch);
 END IF;
 IF EXISTS(SELECT FROM source.entities) OR EXISTS(SELECT FROM source.outbox) OR EXISTS(SELECT FROM source.command_receipts) OR EXISTS(SELECT FROM source.capture_binding) THEN RAISE EXCEPTION 'Cannot bootstrap used source' USING ERRCODE='P7001'; END IF;
 UPDATE source.bootstrap_manifest SET phase='bootstrapping',bootstrap_key=key,recipe_version=recipe,seed=requested_seed,requested_count=total,chunk_size=bound,baseline_at=clock_timestamp() WHERE singleton;
 RETURN source.bootstrap_status(expected_epoch);
END $$;
CREATE FUNCTION source.seed_chunk(expected_epoch uuid,key uuid,start_ordinal bigint) RETURNS TABLE(ordinal text,entity_id text,recorded_at text,replayed boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest; n integer; replay boolean;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Bootstrap requires READ COMMITTED' USING ERRCODE='25001'; END IF;
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton FOR UPDATE;
 IF expected_epoch IS DISTINCT FROM m.source_epoch THEN RAISE EXCEPTION 'Bootstrap epoch mismatch' USING ERRCODE='P2002'; END IF;
 IF key IS NULL OR key IS DISTINCT FROM m.bootstrap_key THEN RAISE EXCEPTION 'Bootstrap key mismatch' USING ERRCODE='P7002'; END IF;
 IF start_ordinal IS NULL OR start_ordinal<1 OR start_ordinal>m.requested_count OR (start_ordinal-1)%m.chunk_size<>0 THEN RAISE EXCEPTION 'Invalid seed chunk boundary' USING ERRCODE='22023'; END IF;
 SELECT EXISTS(SELECT FROM source.bootstrap_chunks c WHERE c.bootstrap_key=key AND c.first_ordinal=start_ordinal) INTO replay;
 IF replay THEN PERFORM source.assert_bootstrap_chunk(key,start_ordinal);
 ELSE
  IF m.phase<>'bootstrapping' OR start_ordinal<>m.completed_count+1 THEN RAISE EXCEPTION 'Source is closed to this seed chunk' USING ERRCODE='P7001'; END IF;
  n:=least(m.chunk_size,m.requested_count-start_ordinal+1);
  INSERT INTO source.entities(payload) SELECT source.seed_payload(m.seed,g.ordinal) FROM generate_series(start_ordinal,start_ordinal+n-1) g(ordinal) ORDER BY g.ordinal;
  INSERT INTO source.bootstrap_chunks(bootstrap_key,first_ordinal,item_count) VALUES(key,start_ordinal,n);
  UPDATE source.bootstrap_manifest SET completed_count=completed_count+n WHERE singleton;
 END IF;
 RETURN QUERY SELECT b.ordinal::text,b.entity_id::text,to_char(b.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),replay FROM source.baseline_revisions b WHERE b.bootstrap_key=key AND b.chunk_first=start_ordinal ORDER BY b.ordinal;
END $$;
CREATE FUNCTION source.seal_bootstrap(expected_epoch uuid,key uuid) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest; start_ordinal bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Bootstrap requires READ COMMITTED' USING ERRCODE='25001'; END IF;
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton FOR UPDATE;
 IF expected_epoch IS DISTINCT FROM m.source_epoch THEN RAISE EXCEPTION 'Bootstrap epoch mismatch' USING ERRCODE='P2002'; END IF;
 IF key IS NULL OR key IS DISTINCT FROM m.bootstrap_key THEN RAISE EXCEPTION 'Bootstrap key mismatch' USING ERRCODE='P7002'; END IF;
 IF m.phase IN ('sealed','active') THEN RETURN source.bootstrap_status(expected_epoch); END IF;
 IF m.phase<>'bootstrapping' OR m.completed_count<>m.requested_count OR (SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=key)<>m.requested_count THEN RAISE EXCEPTION 'Seed is incomplete' USING ERRCODE='P7003'; END IF;
 FOR start_ordinal IN SELECT generate_series(1,m.requested_count,m.chunk_size) LOOP PERFORM source.assert_bootstrap_chunk(key,start_ordinal); END LOOP;
 UPDATE source.bootstrap_manifest SET phase='sealed',sealed_at=clock_timestamp() WHERE singleton;
 RETURN source.bootstrap_status(expected_epoch);
END $$;
CREATE FUNCTION source.activate_bootstrap(expected_epoch uuid,key uuid,expected_pipeline uuid,encoding text) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Activation requires READ COMMITTED' USING ERRCODE='25001'; END IF;
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton FOR UPDATE;
 IF expected_epoch IS DISTINCT FROM m.source_epoch THEN RAISE EXCEPTION 'Bootstrap epoch mismatch' USING ERRCODE='P2002'; END IF;
 IF key IS NULL OR key IS DISTINCT FROM m.bootstrap_key THEN RAISE EXCEPTION 'Bootstrap key mismatch' USING ERRCODE='P7002'; END IF;
 IF m.phase NOT IN ('sealed','active') THEN RAISE EXCEPTION 'Activation requires sealed seed' USING ERRCODE='P7001'; END IF;
 PERFORM source.register_capture(expected_pipeline,expected_epoch,encoding);
 IF m.phase='sealed' THEN UPDATE source.bootstrap_manifest SET phase='active',activated_at=clock_timestamp() WHERE singleton; END IF;
 RETURN source.bootstrap_status(expected_epoch);
END $$;
CREATE FUNCTION source.require_bootstrap_progress() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE m source.bootstrap_manifest;
BEGIN
 SELECT * INTO STRICT m FROM source.bootstrap_manifest WHERE singleton;
 IF m.bootstrap_key IS NOT NULL AND (m.completed_count<>(SELECT coalesce(sum(item_count),0) FROM source.bootstrap_chunks WHERE bootstrap_key=m.bootstrap_key) OR m.completed_count<>(SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=m.bootstrap_key)) THEN RAISE EXCEPTION 'Seed progress lacks committed evidence' USING ERRCODE='P7003'; END IF;
 IF m.phase='active' AND m.origin='fresh' AND NOT EXISTS(SELECT FROM source.capture_binding WHERE source_epoch=m.source_epoch) THEN RAISE EXCEPTION 'Active bootstrap lacks capture binding' USING ERRCODE='P7003'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER bootstrap_progress AFTER INSERT OR UPDATE ON source.bootstrap_manifest DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_bootstrap_progress();
GRANT SELECT ON source.bootstrap_manifest,source.bootstrap_chunks,source.baseline_revisions,source.entities TO source_seed_owner;
GRANT UPDATE(completed_count) ON source.bootstrap_manifest TO source_seed_owner;
GRANT INSERT(payload) ON source.entities TO source_seed_owner;
GRANT INSERT ON source.bootstrap_chunks TO source_seed_owner;
GRANT USAGE ON SEQUENCE source.entities_entity_id_seq TO source_seed_owner;
GRANT EXECUTE ON FUNCTION source.seed_payload(text,bigint),source.assert_bootstrap_chunk(uuid,bigint) TO source_seed_owner;
GRANT SELECT(source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload) ON source.baseline_revisions TO source_reader;
REVOKE ALL ON FUNCTION source.begin_bootstrap(uuid,uuid,integer,text,bigint,integer),source.bootstrap_status(uuid),source.seed_chunk(uuid,uuid,bigint),source.seal_bootstrap(uuid,uuid),source.activate_bootstrap(uuid,uuid,uuid,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION source.begin_bootstrap(uuid,uuid,integer,text,bigint,integer),source.bootstrap_status(uuid),source.seed_chunk(uuid,uuid,bigint),source.seal_bootstrap(uuid,uuid),source.activate_bootstrap(uuid,uuid,uuid,text) TO source_bootstrap;
RESET ROLE;
ALTER FUNCTION source.seed_chunk(uuid,uuid,bigint) OWNER TO source_seed_owner;
