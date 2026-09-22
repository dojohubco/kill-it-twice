-- Canonical validity is enforced by the validated CHECK and immutable-event guards.
-- Keep checksum, identity, attempt, witness and receipt comparisons in the read.
SET LOCAL ROLE pipeline_owner;
CREATE OR REPLACE FUNCTION pipeline.backfill_counts(id uuid) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT FROM pg_catalog.pg_constraint WHERE conrelid='pipeline.events'::regclass AND conname='event_body_consistency' AND contype='c' AND convalidated AND conenforced AND pg_get_constraintdef(oid)='CHECK ((pipeline.valid_body(events.*) IS TRUE))') THEN
  RAISE EXCEPTION 'Canonical constraint proof is unavailable' USING ERRCODE='P8003';
 END IF;
 RETURN (
WITH evidence AS (
 SELECT m.event_id,es.state es_state,mq.state mq_state,o.state consumer_state,
 (e.event_id IS NOT NULL AND e.content_sha256=encode(sha256(e.body_bytes),'hex') AND
 es.event_id IS NOT NULL AND es.destination_id=(r.binding->>'es_destination')::uuid AND mq.event_id IS NOT NULL AND mq.destination_id=(r.binding->>'rabbit_destination')::uuid AND o.event_id IS NOT NULL AND
 (es.state<>'satisfied' OR (a.attempt_id IS NOT NULL AND a.event_id=e.event_id AND a.destination_id=es.destination_id AND a.claim_generation=es.claim_generation AND a.outcome=es.disposition AND a.finished_at=es.settled_at AND a.remote_version=es.remote_version AND w.event_id IS NOT NULL AND w.source_epoch=e.source_epoch AND w.entity_id=e.entity_id AND w.entity_version=es.remote_version AND ((es.disposition IN ('applied','already_applied') AND w.event_id=e.event_id) OR (es.disposition='superseded' AND w.entity_version>e.entity_version)))) AND
 (es.state<>'dead_letter' OR (a.attempt_id IS NOT NULL AND a.event_id=e.event_id AND a.outcome=es.error_class AND a.finished_at=es.settled_at AND dl.attempt_id=es.attempt_id AND dl.error_class=es.error_class)) AND
 (mq.state<>'satisfied' OR (mq.disposition='broker_confirmed' AND ra.attempt_id IS NOT NULL AND ra.event_id=e.event_id AND ra.destination_id=mq.destination_id AND ra.claim_generation=mq.claim_generation AND ra.channel_id IS NOT NULL AND ra.outcome='confirmed' AND ra.finished_at=mq.settled_at)) AND
 (o.state='pending' OR (o.state IN ('processed','quarantined') AND o.consumer_id=(r.binding->>'consumer_id')::uuid AND o.registration_id=(r.binding->>'rabbit_registration')::uuid AND o.receipt_bytes=e.body_bytes AND o.receipt_hash=e.content_sha256 AND o.receipt_id IS NOT NULL AND o.observed_at IS NOT NULL))) IS TRUE AS valid
 FROM pipeline.backfill_members m JOIN pipeline.backfill_runs r USING(run_id)
 LEFT JOIN pipeline.events e USING(event_id)
 LEFT JOIN pipeline.delivery_intents es ON es.event_id=m.event_id AND es.kind='elasticsearch'
 LEFT JOIN pipeline.delivery_intents mq ON mq.event_id=m.event_id AND mq.kind='rabbitmq'
 LEFT JOIN pipeline.consumer_observations o ON o.event_id=m.event_id
 LEFT JOIN pipeline.es_attempts a ON a.attempt_id=es.attempt_id
 LEFT JOIN pipeline.events w ON w.event_id=es.witness_event_id
 LEFT JOIN pipeline.es_dead_letters dl ON dl.event_id=es.event_id AND dl.destination_id=es.destination_id AND dl.attempt_id=es.attempt_id
 LEFT JOIN pipeline.rabbit_attempts ra ON ra.attempt_id=mq.rabbit_attempt_id
 WHERE m.run_id=id
 ) SELECT jsonb_build_object('required',count(*)::text,'invalid',count(*) FILTER(WHERE NOT valid)::text,
 'es_satisfied',count(*) FILTER(WHERE es_state='satisfied')::text,'es_errors',count(*) FILTER(WHERE es_state='dead_letter')::text,'es_pending',count(*) FILTER(WHERE es_state NOT IN ('satisfied','dead_letter') OR es_state IS NULL)::text,
 'rabbit_satisfied',count(*) FILTER(WHERE mq_state='satisfied')::text,'rabbit_pending',count(*) FILTER(WHERE mq_state IS DISTINCT FROM 'satisfied')::text,
 'consumer_processed',count(*) FILTER(WHERE consumer_state='processed')::text,'consumer_errors',count(*) FILTER(WHERE consumer_state='quarantined')::text,'consumer_pending',count(*) FILTER(WHERE consumer_state='pending' OR consumer_state IS NULL)::text) FROM evidence
 );
END $$;
RESET ROLE;
