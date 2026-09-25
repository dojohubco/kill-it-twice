-- Narrow current-state reads; original rows and terminal proofs stay authoritative.
-- Copy and trigger installation are atomic with respect to every original writer.
SET LOCAL ROLE pipeline_owner;
DO $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'Observation migration requires READ COMMITTED' USING ERRCODE='25001';
 END IF;
END $$;
LOCK TABLE pipeline.events,pipeline.delivery_intents,pipeline.consumer_observations IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE pipeline.observation_events (
 event_id text PRIMARY KEY REFERENCES pipeline.events(event_id) ON DELETE CASCADE,
 staged_at timestamptz NOT NULL
);
CREATE TABLE pipeline.observation_deliveries (
 event_id text NOT NULL,
 kind text NOT NULL,
 state text NOT NULL,
 disposition text,
 created_at timestamptz NOT NULL,
 staged_at timestamptz NOT NULL,
 PRIMARY KEY(event_id,kind),
 FOREIGN KEY(event_id,kind) REFERENCES pipeline.delivery_intents(event_id,kind) ON DELETE CASCADE
) WITH (fillfactor=70);
CREATE TABLE pipeline.observation_receipts (
 event_id text PRIMARY KEY REFERENCES pipeline.consumer_observations(event_id) ON DELETE CASCADE,
 state text NOT NULL
) WITH (fillfactor=70);

INSERT INTO pipeline.observation_events SELECT event_id,staged_at FROM pipeline.events;
INSERT INTO pipeline.observation_deliveries
 SELECT d.event_id,d.kind,d.state,d.disposition,d.created_at,e.staged_at
 FROM pipeline.delivery_intents d JOIN pipeline.events e USING(event_id);
INSERT INTO pipeline.observation_receipts SELECT event_id,state FROM pipeline.consumer_observations;

CREATE FUNCTION pipeline.observe_event_metadata() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 INSERT INTO pipeline.observation_events VALUES(NEW.event_id,NEW.staged_at);
 RETURN NULL;
END $$;
CREATE FUNCTION pipeline.observe_delivery_metadata() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE staged timestamptz;
BEGIN
 IF TG_OP='INSERT' THEN
  SELECT staged_at INTO STRICT staged FROM pipeline.events WHERE event_id=NEW.event_id;
  INSERT INTO pipeline.observation_deliveries VALUES(NEW.event_id,NEW.kind,NEW.state,NEW.disposition,NEW.created_at,staged);
 ELSIF NEW.state IS DISTINCT FROM OLD.state OR NEW.disposition IS DISTINCT FROM OLD.disposition THEN
  UPDATE pipeline.observation_deliveries SET state=NEW.state,disposition=NEW.disposition
   WHERE event_id=NEW.event_id AND kind=NEW.kind;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing delivery observation metadata' USING ERRCODE='P8001'; END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE FUNCTION pipeline.observe_receipt_metadata() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  INSERT INTO pipeline.observation_receipts VALUES(NEW.event_id,NEW.state);
 ELSIF NEW.state IS DISTINCT FROM OLD.state THEN
  UPDATE pipeline.observation_receipts SET state=NEW.state WHERE event_id=NEW.event_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing receipt observation metadata' USING ERRCODE='P8001'; END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER observation_metadata AFTER INSERT ON pipeline.events
 FOR EACH ROW EXECUTE FUNCTION pipeline.observe_event_metadata();
CREATE TRIGGER observation_metadata AFTER INSERT OR UPDATE OF state,disposition ON pipeline.delivery_intents
 FOR EACH ROW EXECUTE FUNCTION pipeline.observe_delivery_metadata();
CREATE TRIGGER observation_metadata AFTER INSERT OR UPDATE OF state ON pipeline.consumer_observations
 FOR EACH ROW EXECUTE FUNCTION pipeline.observe_receipt_metadata();

REVOKE ALL ON TABLE pipeline.observation_events,pipeline.observation_deliveries,pipeline.observation_receipts FROM PUBLIC;
REVOKE ALL ON FUNCTION pipeline.observe_event_metadata(),pipeline.observe_delivery_metadata(),pipeline.observe_receipt_metadata() FROM PUBLIC;
GRANT SELECT ON TABLE pipeline.observation_events,pipeline.observation_deliveries,pipeline.observation_receipts TO pipeline_operator;

-- Identical run membership, filters and missing/pending partitions to migration 022.
-- The independent terminal validator continues to use the original relations.
CREATE OR REPLACE FUNCTION pipeline.backfill_observed_counts(id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE counts jsonb;
BEGIN
 EXECUTE $count$SELECT jsonb_build_object('required',count(*)::text,'invalid',NULL,
 'es_satisfied',count(*) FILTER(WHERE es.state='satisfied')::text,
 'es_errors',count(*) FILTER(WHERE es.state='dead_letter')::text,
 'es_pending',count(*) FILTER(WHERE es.state NOT IN ('satisfied','dead_letter') OR es.state IS NULL)::text,
 'rabbit_satisfied',count(*) FILTER(WHERE mq.state='satisfied')::text,
 'rabbit_pending',count(*) FILTER(WHERE mq.state IS DISTINCT FROM 'satisfied')::text,
 'consumer_processed',count(*) FILTER(WHERE o.state='processed')::text,
 'consumer_errors',count(*) FILTER(WHERE o.state='quarantined')::text,
 'consumer_pending',count(*) FILTER(WHERE o.state='pending' OR o.state IS NULL)::text)
 FROM pipeline.backfill_members m
 LEFT JOIN pipeline.observation_deliveries es ON es.event_id=m.event_id AND es.kind='elasticsearch'
 LEFT JOIN pipeline.observation_deliveries mq ON mq.event_id=m.event_id AND mq.kind='rabbitmq'
 LEFT JOIN pipeline.observation_receipts o ON o.event_id=m.event_id WHERE m.run_id= $1$count$ INTO counts USING id;
 RETURN counts;
END
$$;
ANALYZE pipeline.observation_events;
ANALYZE pipeline.observation_deliveries;
ANALYZE pipeline.observation_receipts;
RESET ROLE;
