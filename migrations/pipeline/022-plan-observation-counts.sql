-- Preserve the exact aggregate and missing/pending-row rules from 016.
-- Plan this fixed statement for its bound run id on each observation. The
-- EXECUTE form passed repeated churned million-member and small-run probes;
-- full-service acceptance remains separate. No budget or planner GUC changes.
SET LOCAL ROLE pipeline_owner;
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
 LEFT JOIN pipeline.delivery_intents es ON es.event_id=m.event_id AND es.kind='elasticsearch'
 LEFT JOIN pipeline.delivery_intents mq ON mq.event_id=m.event_id AND mq.kind='rabbitmq'
 LEFT JOIN pipeline.consumer_observations o ON o.event_id=m.event_id WHERE m.run_id= $1$count$ INTO counts USING id;
 RETURN counts;
END
$$;
RESET ROLE;
