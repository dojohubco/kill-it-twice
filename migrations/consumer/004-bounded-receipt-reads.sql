-- Existing exact identity/content lookups, with an explicitly bounded runtime batch.
SET LOCAL ROLE consumer_owner;
CREATE OR REPLACE FUNCTION consumer.receipts(ids text[],wire_hashes text[]) RETURNS TABLE(event_id text,state text,receipt_id text,body bytea,hash text,raw bytea,metadata bytea)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE id text; i integer;
BEGIN
 IF ids IS NULL OR wire_hashes IS NULL OR cardinality(ids) NOT BETWEEN 1 AND 32 OR cardinality(ids)<>cardinality(wire_hashes) OR EXISTS(SELECT 1 FROM unnest(ids) x WHERE x IS NULL OR octet_length(x)>100) THEN RAISE EXCEPTION 'Invalid receipt read bound' USING ERRCODE='P6002'; END IF;
 FOR i IN 1..cardinality(ids) LOOP
 id:=ids[i];
 RETURN QUERY SELECT p.event_id,'processed'::text,p.event_id,p.body_bytes,p.content_sha256,NULL::bytea,NULL::bytea FROM consumer.processed_events p WHERE p.event_id=id;
 IF FOUND THEN CONTINUE; END IF;
 RETURN QUERY SELECT id,'quarantined'::text,q.quarantine_id::text,NULL::bytea,NULL::text,q.raw_bytes,q.metadata FROM consumer.quarantine q WHERE q.claimed_id=id AND q.raw_hash=wire_hashes[i] ORDER BY q.quarantine_id LIMIT 1;
 END LOOP;
END $$;
REVOKE ALL ON FUNCTION consumer.receipts(text[],text[]) FROM PUBLIC;
RESET ROLE;
