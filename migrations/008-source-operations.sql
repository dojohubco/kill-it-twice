-- Dedicated local-demo capability; no source owner or arbitrary payload authority.
CREATE ROLE source_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE source_m1 TO source_operator;
SET LOCAL ROLE source_owner;
CREATE TABLE source.operator_fixtures (
 name text PRIMARY KEY CHECK(name ~ '^fixture-(0[1-9]|1[0-6])$'),
 entity_id bigint NOT NULL UNIQUE REFERENCES source.entities(entity_id)
);
CREATE TABLE source.operator_receipts (
 request_id uuid PRIMARY KEY, request_correlation uuid NOT NULL,
 fixture text NOT NULL, operation text NOT NULL CHECK(operation IN ('create','update','delete','restore','corrupt')),
 value integer NOT NULL CHECK(value BETWEEN 0 AND 1000000),
 result jsonb NOT NULL CHECK(octet_length(result::text)<=8192),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION source.operator_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$ BEGIN RAISE EXCEPTION 'Immutable operator evidence' USING ERRCODE='P9002'; END $$;
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON source.operator_receipts FOR EACH ROW EXECUTE FUNCTION source.operator_immutable();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON source.operator_receipts EXECUTE FUNCTION source.operator_immutable();
CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON source.operator_fixtures FOR EACH ROW EXECUTE FUNCTION source.operator_immutable();
CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON source.operator_fixtures EXECUTE FUNCTION source.operator_immutable();
CREATE FUNCTION source.operator_change(epoch uuid,key uuid,correlation uuid,fixture text,op text,val integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE old source.operator_receipts; id bigint; result jsonb; payload jsonb;
BEGIN
 IF fixture IS NULL OR fixture !~ '^fixture-(0[1-9]|1[0-6])$' OR op IS NULL OR op NOT IN ('create','update','delete','restore','corrupt') OR val IS NULL OR val NOT BETWEEN 0 AND 1000000 OR key IS NULL OR correlation IS NULL THEN RAISE EXCEPTION 'Invalid fixture request' USING ERRCODE='22023'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(key::text,916));
 SELECT * INTO old FROM source.operator_receipts WHERE request_id=key;
 IF FOUND THEN
  IF (old.fixture,old.operation,old.value) IS DISTINCT FROM (fixture,op,val) OR old.result->>'source_epoch'<>epoch::text THEN RAISE EXCEPTION 'Fixture key conflict' USING ERRCODE='P9001'; END IF;
  RETURN old.result||jsonb_build_object('replayed',true);
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(fixture,917));
 SELECT entity_id INTO id FROM source.operator_fixtures WHERE name=fixture;
 IF (op='create' AND id IS NOT NULL) OR (op<>'create' AND id IS NULL) THEN RAISE EXCEPTION 'Fixture state conflict' USING ERRCODE='P9001'; END IF;
 payload:=CASE WHEN op='delete' THEN NULL ELSE jsonb_build_object('name',fixture,'country','GE','loyalty_points',CASE WHEN op='corrupt' THEN to_jsonb('not-a-number'::text) ELSE to_jsonb(val) END) END;
 SELECT jsonb_build_object('source_epoch',c.source_epoch,'entity_id',c.entity_id::text,'entity_version',c.entity_version::text,'change_id',c.change_id,'recorded_at',c.recorded_at,'is_deleted',c.is_deleted,'replayed',c.replayed) INTO result FROM source.execute_command(epoch,key,1,CASE WHEN op='corrupt' THEN 'update' ELSE op END,id,payload) c;
 IF op='create' THEN INSERT INTO source.operator_fixtures VALUES(fixture,(result->>'entity_id')::bigint); END IF;
 INSERT INTO source.operator_receipts VALUES(key,correlation,fixture,op,val,result,clock_timestamp());
 RETURN result;
END $$;
GRANT USAGE ON SCHEMA source TO source_operator;
GRANT SELECT ON source.capture_work,source.capture_binding,source.operator_fixtures,source.operator_receipts TO source_operator;
GRANT SELECT(entity_id,source_epoch,entity_version,change_id,recorded_at,is_deleted) ON source.entities,source.outbox TO source_operator;
REVOKE ALL ON FUNCTION source.operator_change(uuid,uuid,uuid,text,text,integer),source.operator_immutable() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION source.operator_change(uuid,uuid,uuid,text,text,integer),source.capture_summary(uuid,uuid),source.backfill_identity(uuid,uuid) TO source_operator;
RESET ROLE;
