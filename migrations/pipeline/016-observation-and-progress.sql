-- Fast state observation is not the authority for terminal completion.
-- Full counts/attempt/witness/receipt validation remains in backfill_advance.
SET LOCAL ROLE pipeline_owner;
CREATE OR REPLACE FUNCTION pipeline.backfill_progress_valid(id uuid) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 WITH entries AS MATERIALIZED (
  SELECT b.batch_id,b.run_id,x->>'event_id' event_id,x->>'hash' hash
  FROM pipeline.backfill_batches b CROSS JOIN LATERAL jsonb_array_elements(b.items) x WHERE b.run_id=id
 )
 SELECT NOT EXISTS(SELECT FROM pipeline.backfill_ranges q LEFT JOIN pipeline.backfill_batches b ON b.batch_id=q.last_batch WHERE q.run_id=id AND
 ((q.last_batch IS NULL AND (q.checkpoint<>q.lower_key OR (q.state='closed' AND q.upper_key<>q.lower_key))) OR
 (q.last_batch IS NOT NULL AND (b.batch_id IS NULL OR b.run_id<>id OR b.range_no<>q.range_no OR b.next_key<>q.checkpoint OR b.eof IS DISTINCT FROM (q.state='closed')))))
 AND NOT EXISTS(SELECT FROM entries x LEFT JOIN pipeline.backfill_members m ON m.run_id=x.run_id AND m.event_id=x.event_id LEFT JOIN pipeline.events e ON e.event_id=m.event_id WHERE m.event_id IS NULL OR e.content_sha256 IS DISTINCT FROM x.hash)
 AND NOT EXISTS(SELECT FROM pipeline.backfill_members m LEFT JOIN pipeline.backfill_batches b ON b.batch_id=m.first_batch AND b.run_id=m.run_id LEFT JOIN entries x ON x.batch_id=b.batch_id AND x.event_id=m.event_id WHERE m.run_id=id AND (b.batch_id IS NULL OR x.event_id IS NULL))
$$;
CREATE FUNCTION pipeline.backfill_observed_counts(id uuid) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
 SELECT jsonb_build_object('required',count(*)::text,'invalid',NULL,
 'es_satisfied',count(*) FILTER(WHERE es.state='satisfied')::text,
 'es_errors',count(*) FILTER(WHERE es.state='dead_letter')::text,
 'es_pending',count(*) FILTER(WHERE es.state NOT IN ('satisfied','dead_letter') OR es.state IS NULL)::text,
 'rabbit_satisfied',count(*) FILTER(WHERE mq.state='satisfied')::text,
 'rabbit_pending',count(*) FILTER(WHERE mq.state IS DISTINCT FROM 'satisfied')::text,
 'consumer_processed',count(*) FILTER(WHERE o.state='processed')::text,
 'consumer_errors',count(*) FILTER(WHERE o.state='quarantined')::text,
 'consumer_pending',count(*) FILTER(WHERE o.state='pending' OR o.state IS NULL)::text)
 FROM pipeline.backfill_members m
 LEFT JOIN pipeline.delivery_intents es ON es.event_id=m.event_id AND es.kind='elasticsearch'
 LEFT JOIN pipeline.delivery_intents mq ON mq.event_id=m.event_id AND mq.kind='rabbitmq'
 LEFT JOIN pipeline.consumer_observations o ON o.event_id=m.event_id WHERE m.run_id=id
$$;
CREATE FUNCTION pipeline.backfill_observation(id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE r pipeline.backfill_runs;
BEGIN
 r:=pipeline.backfill_validate(id);
 RETURN pipeline.backfill_run(id)||jsonb_build_object('observed_at',clock_timestamp()::text,
 'counts',pipeline.backfill_observed_counts(id),'evidence_scope','durable_state_observation_not_revalidation',
 'historical_terminal_proof',r.phase IN ('complete','complete_with_errors'),
 'effective_paused',r.desired_paused AND NOT EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='leased' AND lease_until>clock_timestamp()),
 'blocked',r.blocked_reason IS NOT NULL OR EXISTS(SELECT FROM pipeline.backfill_ranges WHERE run_id=id AND state='blocked'),
 'scan_observations',(SELECT coalesce(sum(jsonb_array_length(items)),0)::text FROM pipeline.backfill_batches WHERE run_id=id AND range_no>0),
 'imported',(SELECT coalesce(sum(jsonb_array_length(items)),0)::text FROM pipeline.backfill_batches WHERE run_id=id AND range_no=0),
 'ranges',(SELECT jsonb_agg(to_jsonb(q)||jsonb_build_object('lower_key',q.lower_key::text,'upper_key',q.upper_key::text,'checkpoint',q.checkpoint::text,'generation',q.generation::text) ORDER BY range_no) FROM pipeline.backfill_ranges q WHERE q.run_id=id));
END $$;
REVOKE ALL ON FUNCTION pipeline.backfill_observed_counts(uuid),pipeline.backfill_observation(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline.backfill_observation(uuid) TO pipeline_operator;
RESET ROLE;
