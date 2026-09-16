-- Forward-only M2A migration, applied in an explicit migration transaction.
CREATE ROLE source_command NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
GRANT CONNECT ON DATABASE source_m1 TO source_command;
SET LOCAL ROLE source_owner;

CREATE TABLE source.command_receipts (
  source_epoch uuid NOT NULL REFERENCES source.source_identity(source_epoch),
  command_id uuid NOT NULL,
  contract_version integer NOT NULL CHECK (contract_version > 0),
  operation text NOT NULL CHECK (operation IN ('create', 'update', 'delete', 'restore')),
  target_id bigint,
  request_payload jsonb,
  completed boolean NOT NULL DEFAULT false,
  result_entity_id bigint,
  result_version bigint,
  result_change_id uuid,
  result_recorded_at timestamptz,
  result_deleted boolean,
  result_payload jsonb,
  PRIMARY KEY (source_epoch, command_id),
  CHECK ((operation = 'create' AND target_id IS NULL) OR
         (operation <> 'create' AND target_id IS NOT NULL AND target_id > 0)),
  CHECK ((operation = 'delete' AND request_payload IS NULL) OR
         (operation <> 'delete' AND request_payload IS NOT NULL AND pg_catalog.jsonb_typeof(request_payload) = 'object')),
  CONSTRAINT command_result_shape CHECK (
    (NOT completed AND result_entity_id IS NULL AND result_version IS NULL AND
     result_change_id IS NULL AND result_recorded_at IS NULL AND result_deleted IS NULL AND result_payload IS NULL) OR
    (completed AND result_entity_id IS NOT NULL AND result_entity_id > 0 AND
     result_version IS NOT NULL AND result_version > 0 AND result_change_id IS NOT NULL AND
     result_recorded_at IS NOT NULL AND pg_catalog.isfinite(result_recorded_at) AND result_deleted IS NOT NULL AND
     ((result_deleted AND result_payload IS NULL) OR
      (NOT result_deleted AND result_payload IS NOT NULL AND pg_catalog.jsonb_typeof(result_payload) = 'object')))
  ),
  FOREIGN KEY (source_epoch, result_entity_id, result_version)
    REFERENCES source.outbox (source_epoch, entity_id, entity_version)
);

CREATE FUNCTION source.guard_command_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'command receipts cannot be removed' USING ERRCODE = '0A000';
  END IF;
  IF OLD.completed OR NOT NEW.completed OR
     ROW(NEW.source_epoch, NEW.command_id, NEW.contract_version, NEW.operation, NEW.target_id, NEW.request_payload)
     IS DISTINCT FROM
     ROW(OLD.source_epoch, OLD.command_id, OLD.contract_version, OLD.operation, OLD.target_id, OLD.request_payload) THEN
    RAISE EXCEPTION 'only initial command result finalization is allowed' USING ERRCODE = '0A000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION source.require_command_completion() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  -- The INSERT event's NEW image is a reservation even after finalization.
  -- Inspect the final row, including this transaction's own later UPDATE.
  IF NOT EXISTS (SELECT FROM source.command_receipts r
                WHERE r.source_epoch = NEW.source_epoch AND r.command_id = NEW.command_id AND r.completed) THEN
    RAISE EXCEPTION 'incomplete command receipt cannot commit' USING ERRCODE = 'P2003';
  END IF;
  RETURN NULL;
END;
$$;

CREATE TRIGGER command_receipts_finalize_only BEFORE UPDATE OR DELETE ON source.command_receipts
FOR EACH ROW EXECUTE FUNCTION source.guard_command_receipt();
CREATE TRIGGER command_receipts_no_truncate BEFORE TRUNCATE ON source.command_receipts
FOR EACH STATEMENT EXECUTE FUNCTION source.reject_removal_or_rewrite();
CREATE CONSTRAINT TRIGGER command_receipts_complete AFTER INSERT OR UPDATE ON source.command_receipts
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_command_completion();

CREATE FUNCTION source.execute_command(
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

REVOKE ALL ON source.command_receipts FROM PUBLIC, source_writer, source_command;
REVOKE ALL ON ALL TABLES IN SCHEMA source FROM source_command;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source FROM source_command;
REVOKE ALL ON FUNCTION source.guard_command_receipt(), source.require_command_completion(),
  source.execute_command(uuid, uuid, integer, text, bigint, jsonb) FROM PUBLIC, source_writer;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA source FROM source_command;
GRANT USAGE ON SCHEMA source TO source_command;
GRANT EXECUTE ON FUNCTION source.execute_command(uuid, uuid, integer, text, bigint, jsonb) TO source_command;
RESET ROLE;
