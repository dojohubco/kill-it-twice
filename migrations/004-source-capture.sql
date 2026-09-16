-- Install before controlled registration. Prior migrations and immutable history remain unchanged.
CREATE ROLE source_capture NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE source_m1 TO source_capture;
GRANT USAGE ON SCHEMA source TO source_capture;
SET LOCAL ROLE source_owner;
CREATE TABLE source.capture_binding (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  pipeline_id uuid NOT NULL UNIQUE,
  source_epoch uuid NOT NULL UNIQUE REFERENCES source.source_identity(source_epoch),
  payload_encoding text NOT NULL CHECK (payload_encoding='pg18-jsonb-text/v1'),
  registered_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(registered_at)),
  UNIQUE(pipeline_id,source_epoch)
);
CREATE TABLE source.capture_work (
  work_id bigint GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 9223372036854775807 NO CYCLE) PRIMARY KEY CHECK (work_id>0),
  pipeline_id uuid NOT NULL,
  source_epoch uuid NOT NULL,
  entity_id bigint NOT NULL CHECK (entity_id>0),
  entity_version bigint NOT NULL CHECK (entity_version>0),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','leased','acknowledged','blocked')),
  generation bigint NOT NULL DEFAULT 0 CHECK (generation>=0),
  owner_id uuid,
  lease_until timestamptz CHECK (lease_until IS NULL OR isfinite(lease_until)),
  next_eligible_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(next_eligible_at)),
  reason text CHECK (reason IS NULL OR reason IN ('transient_failure','oversized_record')),
  acknowledged_hash text CHECK (acknowledged_hash IS NULL OR acknowledged_hash ~ '^[0-9a-f]{64}$'),
  acknowledged_at timestamptz CHECK (acknowledged_at IS NULL OR isfinite(acknowledged_at)),
  acknowledged_generation bigint,
  UNIQUE(source_epoch,entity_id,entity_version),
  FOREIGN KEY(pipeline_id,source_epoch) REFERENCES source.capture_binding(pipeline_id,source_epoch),
  FOREIGN KEY(source_epoch,entity_id,entity_version) REFERENCES source.outbox(source_epoch,entity_id,entity_version),
  CONSTRAINT work_shape CHECK ((
    (state='pending' AND owner_id IS NULL AND lease_until IS NULL AND (reason IS NULL OR reason='transient_failure')) OR
    (state='blocked' AND owner_id IS NULL AND lease_until IS NULL AND reason='oversized_record' AND generation>0) OR
    (state='leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL AND generation>0 AND reason IS NULL) OR
    (state='acknowledged' AND owner_id IS NOT NULL AND lease_until IS NULL AND generation>0 AND reason IS NULL)
  ) IS TRUE),
  CONSTRAINT acknowledgement_shape CHECK ((
    (state='acknowledged' AND acknowledged_hash IS NOT NULL AND acknowledged_at IS NOT NULL AND acknowledged_generation IS NOT NULL AND acknowledged_generation=generation) OR
    (state<>'acknowledged' AND acknowledged_hash IS NULL AND acknowledged_at IS NULL AND acknowledged_generation IS NULL)
  ) IS TRUE)
);
CREATE INDEX capture_pending_due ON source.capture_work(next_eligible_at,work_id) WHERE state='pending';
CREATE INDEX capture_lease_expiry ON source.capture_work(lease_until,work_id) WHERE state='leased';
CREATE TRIGGER capture_binding_no_rewrite BEFORE UPDATE OR DELETE ON source.capture_binding
FOR EACH ROW EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE TRIGGER capture_binding_no_truncate BEFORE TRUNCATE ON source.capture_binding
FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE FUNCTION source.guard_capture_work() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF TG_OP<>'UPDATE' THEN RAISE EXCEPTION 'Capture evidence cannot be removed' USING ERRCODE='P4003'; END IF;
  IF OLD.state='acknowledged' OR
    ROW(NEW.work_id,NEW.pipeline_id,NEW.source_epoch,NEW.entity_id,NEW.entity_version) IS DISTINCT FROM
    ROW(OLD.work_id,OLD.pipeline_id,OLD.source_epoch,OLD.entity_id,OLD.entity_version) OR NEW.generation<OLD.generation THEN
    RAISE EXCEPTION 'Capture identity or terminal evidence is immutable' USING ERRCODE='P4003';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER capture_work_guard BEFORE UPDATE OR DELETE ON source.capture_work FOR EACH ROW EXECUTE FUNCTION source.guard_capture_work();
CREATE TRIGGER capture_work_no_truncate BEFORE TRUNCATE ON source.capture_work FOR EACH STATEMENT EXECUTE FUNCTION source.guard_capture_work();
CREATE FUNCTION source.enqueue_capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  INSERT INTO source.capture_work(pipeline_id,source_epoch,entity_id,entity_version)
  SELECT pipeline_id,NEW.source_epoch,NEW.entity_id,NEW.entity_version FROM source.capture_binding WHERE singleton;
  RETURN NULL;
END $$;
CREATE TRIGGER outbox_enqueue_capture AFTER INSERT ON source.outbox FOR EACH ROW EXECUTE FUNCTION source.enqueue_capture();
CREATE FUNCTION source.register_capture(expected_pipeline uuid, expected_epoch uuid, encoding text) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF current_setting('transaction_isolation')<>'read committed' THEN
    RAISE EXCEPTION 'Capture requires READ COMMITTED' USING ERRCODE='25001'; END IF;
  IF expected_pipeline IS NULL OR encoding IS DISTINCT FROM 'pg18-jsonb-text/v1' OR
    NOT EXISTS(SELECT 1 FROM source.source_identity WHERE source_epoch=expected_epoch) THEN
    RAISE EXCEPTION 'Unexpected capture binding' USING ERRCODE='P4001'; END IF;
  -- Blocks existing/new outbox writers; fresh subsequent statements see committed predecessors.
  LOCK TABLE source.outbox IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS(SELECT 1 FROM source.capture_binding) THEN
    IF NOT EXISTS(SELECT 1 FROM source.capture_binding WHERE pipeline_id=expected_pipeline AND source_epoch=expected_epoch AND payload_encoding=encoding) THEN
      RAISE EXCEPTION 'Capture already registered elsewhere' USING ERRCODE='P4001'; END IF;
    RETURN;
  END IF;
  INSERT INTO source.capture_binding(pipeline_id,source_epoch,payload_encoding) VALUES(expected_pipeline,expected_epoch,encoding);
  INSERT INTO source.capture_work(pipeline_id,source_epoch,entity_id,entity_version)
  SELECT expected_pipeline,source_epoch,entity_id,entity_version FROM source.outbox ORDER BY allocation_id;
END $$;
CREATE FUNCTION source.assert_capture_binding(expected_pipeline uuid, expected_epoch uuid) RETURNS void
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM source.capture_binding WHERE pipeline_id=expected_pipeline AND source_epoch=expected_epoch AND payload_encoding='pg18-jsonb-text/v1') THEN
    RAISE EXCEPTION 'Capture binding absent or mismatched' USING ERRCODE='P4001'; END IF;
END $$;
CREATE FUNCTION source.capture_claim(expected_pipeline uuid, expected_epoch uuid, worker uuid, claim_count integer, lease_ms integer)
RETURNS TABLE(entity_id text, entity_version text, generation text, owner_id text, lease_until text, observed_at text, transfer_bytes text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE w source.capture_work; observed timestamptz;
BEGIN
  PERFORM source.assert_capture_binding(expected_pipeline,expected_epoch);
  IF current_setting('transaction_isolation')<>'read committed' THEN
    RAISE EXCEPTION 'Capture requires READ COMMITTED' USING ERRCODE='25001'; END IF;
  IF worker IS NULL OR claim_count IS NULL OR claim_count NOT BETWEEN 1 AND 16 OR lease_ms IS NULL OR lease_ms NOT BETWEEN 100 AND 30000 THEN
    RAISE EXCEPTION 'Invalid claim bounds' USING ERRCODE='22023'; END IF;
  FOR w IN SELECT q.* FROM source.capture_work q WHERE q.pipeline_id=expected_pipeline AND
    ((q.state='pending' AND q.next_eligible_at<=clock_timestamp()) OR (q.state='leased' AND q.lease_until<=clock_timestamp()))
    ORDER BY q.work_id LIMIT claim_count FOR UPDATE SKIP LOCKED LOOP
    observed:=clock_timestamp();
    IF w.generation=9223372036854775807 THEN RAISE EXCEPTION 'Capture generation exhausted' USING ERRCODE='22003'; END IF;
    UPDATE source.capture_work q SET state='leased',generation=q.generation+1,owner_id=worker,
      lease_until=observed+lease_ms*interval '1 millisecond',reason=NULL WHERE q.work_id=w.work_id RETURNING q.* INTO w;
    RETURN QUERY SELECT w.entity_id::text,w.entity_version::text,w.generation::text,w.owner_id::text,
      to_char(w.lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      to_char(observed AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      (coalesce(octet_length(to_json(o.payload::text)::text),4)::bigint+1024)::text
      FROM source.outbox o WHERE o.source_epoch=w.source_epoch AND o.entity_id=w.entity_id AND o.entity_version=w.entity_version;
  END LOOP;
END $$;
CREATE FUNCTION source.capture_change(expected_pipeline uuid, expected_epoch uuid, id bigint, version bigint, worker uuid, token bigint, action text, duration_ms integer, digest text)
RETURNS TABLE(status text, observed_at text, lease_until text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE w source.capture_work; observed timestamptz;
BEGIN
  PERFORM source.assert_capture_binding(expected_pipeline,expected_epoch);
  IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Capture requires READ COMMITTED' USING ERRCODE='25001'; END IF;
  IF action IS NULL OR action NOT IN ('renew','defer','block','ack') OR worker IS NULL OR token IS NULL OR token<1 THEN
    RAISE EXCEPTION 'Invalid capture transition' USING ERRCODE='22023'; END IF;
  IF action IN ('renew','defer') AND (duration_ms IS NULL OR duration_ms NOT BETWEEN 1 AND 30000 OR (action='renew' AND duration_ms<100)) THEN
    RAISE EXCEPTION 'Invalid capture duration' USING ERRCODE='22023'; END IF;
  IF action='ack' AND (digest IS NULL OR digest !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'Invalid acknowledgement hash' USING ERRCODE='P4003'; END IF;
  SELECT * INTO w FROM source.capture_work q WHERE q.source_epoch=expected_epoch AND q.entity_id=id AND q.entity_version=version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Required capture work missing' USING ERRCODE='P4003'; END IF;
  observed:=clock_timestamp(); -- Crucially AFTER the possibly blocking row lock, never transaction_timestamp().
  IF w.state='acknowledged' AND action='ack' THEN
    IF w.acknowledged_hash IS DISTINCT FROM digest THEN RAISE EXCEPTION 'Conflicting terminal hash' USING ERRCODE='P4003'; END IF;
    IF w.owner_id IS DISTINCT FROM worker OR w.acknowledged_generation IS DISTINCT FROM token THEN
      RAISE EXCEPTION 'Stale terminal claim identity' USING ERRCODE='P4002'; END IF;
    RETURN QUERY SELECT 'already_acknowledged'::text,to_char(observed AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),NULL::text;
    RETURN;
  END IF;
  IF w.state<>'leased' OR w.owner_id IS DISTINCT FROM worker OR w.generation IS DISTINCT FROM token OR w.lease_until<=observed THEN
    RAISE EXCEPTION 'Stale or expired capture claim' USING ERRCODE='P4002'; END IF;
  IF action='renew' THEN
    UPDATE source.capture_work SET lease_until=observed+duration_ms*interval '1 millisecond' WHERE work_id=w.work_id;
  ELSIF action='defer' THEN
    UPDATE source.capture_work SET state='pending',owner_id=NULL,lease_until=NULL,reason='transient_failure',
      next_eligible_at=observed+duration_ms*interval '1 millisecond' WHERE work_id=w.work_id;
  ELSIF action='block' THEN
    UPDATE source.capture_work SET state='blocked',owner_id=NULL,lease_until=NULL,reason='oversized_record' WHERE work_id=w.work_id;
  ELSE
    UPDATE source.capture_work SET state='acknowledged',lease_until=NULL,acknowledged_hash=digest,acknowledged_at=observed,
      acknowledged_generation=token,reason=NULL WHERE work_id=w.work_id;
  END IF;
  RETURN QUERY SELECT CASE WHEN action='ack' THEN 'acknowledged' ELSE action END,
    to_char(observed AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    to_char(q.lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') FROM source.capture_work q WHERE q.work_id=w.work_id;
END $$;
CREATE FUNCTION source.capture_summary(expected_pipeline uuid, expected_epoch uuid)
RETURNS TABLE(observed_at text, pending_due text, pending_delayed text, leased_current text, leased_expired text, blocked text, acknowledged text, missing text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  PERFORM source.assert_capture_binding(expected_pipeline,expected_epoch);
  RETURN QUERY WITH moment AS MATERIALIZED (SELECT clock_timestamp() AS t)
    SELECT to_char(t AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      count(*) FILTER(WHERE q.state='pending' AND q.next_eligible_at<=t)::text,
      count(*) FILTER(WHERE q.state='pending' AND q.next_eligible_at>t)::text,
      count(*) FILTER(WHERE q.state='leased' AND q.lease_until>t)::text,
      count(*) FILTER(WHERE q.state='leased' AND q.lease_until<=t)::text,
      count(*) FILTER(WHERE q.state='blocked')::text,count(*) FILTER(WHERE q.state='acknowledged')::text,
      (SELECT count(*)::text FROM source.outbox o LEFT JOIN source.capture_work w USING(source_epoch,entity_id,entity_version) WHERE w.work_id IS NULL)
      FROM moment LEFT JOIN source.capture_work q ON true GROUP BY t;
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA source FROM source_capture;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source FROM source_capture;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA source FROM source_capture;
-- Includes all new internal functions; old runtime grants remain untouched.
REVOKE ALL ON FUNCTION source.guard_capture_work(),source.enqueue_capture(),source.register_capture(uuid,uuid,text),source.assert_capture_binding(uuid,uuid),
  source.capture_claim(uuid,uuid,uuid,integer,integer),source.capture_change(uuid,uuid,bigint,bigint,uuid,bigint,text,integer,text),source.capture_summary(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION source.capture_claim(uuid,uuid,uuid,integer,integer),source.capture_change(uuid,uuid,bigint,bigint,uuid,bigint,text,integer,text),source.capture_summary(uuid,uuid) TO source_capture;
GRANT SELECT(singleton,source_epoch) ON source.source_identity TO source_capture;
GRANT SELECT(source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,payload) ON source.outbox TO source_capture;
RESET ROLE;
