"""Real PostgreSQL projection semantics; synthetic states are not fault acceptance."""
import datetime
import hashlib
import json
import select
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT/'artifacts/capacity'/('projection-check-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S'))
OUT.mkdir(parents=True)
NAME = 'kit-observation-check-'+uuid.uuid4().hex[:12]
IMAGE = 'postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af'
report = dict(status='RUNNING',scope='Synthetic state-space and concurrent real PostgreSQL projection checks; not full-service acceptance',name=NAME,commands=[])
def save(): (OUT/'result.json').write_text(json.dumps(report,indent=2)+'\n')
def command(args,label,statement=None,expected=(0,),timeout=120):
    start=time.monotonic()
    if statement is not None:(OUT/(label+'.sql')).write_text(statement)
    result=subprocess.run(args,cwd=ROOT,input=statement,text=True,capture_output=True,timeout=timeout)
    (OUT/(label+'.stdout')).write_text(result.stdout);(OUT/(label+'.stderr')).write_text(result.stderr)
    report['commands'].append(dict(label=label,exit=result.returncode,seconds=round(time.monotonic()-start,3),stdout_sha256=hashlib.sha256(result.stdout.encode()).hexdigest()))
    save();assert result.returncode in expected,(label,result.returncode,result.stderr[-1000:])
    return result
PSQL=['docker','exec','-i',NAME,'psql','-h','127.0.0.1','-X','-qAt','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','postgres']
def sql(statement,label,expected=(0,)):return command(PSQL,label,statement,expected)
def value(statement,label):return json.loads(sql(statement,label).stdout)
def original_rows(label):
    return sql("SELECT jsonb_agg(to_jsonb(x) ORDER BY event_id) FROM pipeline.events x;SELECT jsonb_agg(to_jsonb(x) ORDER BY event_id,kind) FROM pipeline.delivery_intents x;SELECT jsonb_agg(to_jsonb(x) ORDER BY event_id) FROM pipeline.consumer_observations x;SELECT jsonb_agg(to_jsonb(x) ORDER BY run_id,event_id) FROM pipeline.backfill_members x;",label).stdout
EQUALITY="""SELECT NOT EXISTS((SELECT event_id,staged_at FROM pipeline.events EXCEPT ALL SELECT * FROM pipeline.observation_events) UNION ALL (SELECT * FROM pipeline.observation_events EXCEPT ALL SELECT event_id,staged_at FROM pipeline.events))
AND NOT EXISTS((SELECT d.event_id,d.kind,d.state,d.disposition,d.created_at,e.staged_at FROM pipeline.delivery_intents d JOIN pipeline.events e USING(event_id) EXCEPT ALL SELECT * FROM pipeline.observation_deliveries) UNION ALL (SELECT * FROM pipeline.observation_deliveries EXCEPT ALL SELECT d.event_id,d.kind,d.state,d.disposition,d.created_at,e.staged_at FROM pipeline.delivery_intents d JOIN pipeline.events e USING(event_id)))
AND NOT EXISTS((SELECT event_id,state FROM pipeline.consumer_observations EXCEPT ALL SELECT * FROM pipeline.observation_receipts) UNION ALL (SELECT * FROM pipeline.observation_receipts EXCEPT ALL SELECT event_id,state FROM pipeline.consumer_observations));"""
def equal(label):assert sql(EQUALITY,label).stdout.strip()=='t'
def counts_equal(label):
    result=sql("SELECT bool_and(pipeline.reference_counts(run_id)=pipeline.backfill_observed_counts(run_id)) FROM (SELECT DISTINCT run_id FROM pipeline.backfill_members UNION SELECT '99999999-9999-4999-8999-999999999999'::uuid)x;",label).stdout.strip()
    assert result=='t'
def interactive(statement,label):
    path=OUT/(label+'.stderr');err=path.open('w')
    p=subprocess.Popen(PSQL,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
    processes.append((p,err));p.stdin.write('BEGIN;SET LOCAL statement_timeout=5000;'+statement+";SELECT 'ready';\n");p.stdin.flush()
    assert select.select([p.stdout],[],[],10)[0],label+' did not reach transaction barrier'
    assert p.stdout.readline().strip()=='ready',label
    return p

def finish(p,operation):
    p.stdin.write(operation+';\n');p.stdin.close();assert p.wait(timeout=10)==0

processes=[]
save();print(OUT,flush=True)
try:
    command(['docker','run','-d','--name',NAME,'--label','kit.observation-proof='+NAME,'--network','none','--cpus','1','--memory','512m','--memory-swap','512m','-e','POSTGRES_HOST_AUTH_METHOD=trust',IMAGE],'start')
    for _ in range(60):
        if subprocess.run(['docker','exec',NAME,'pg_isready','-h','127.0.0.1','-U','postgres'],capture_output=True).returncode==0:break
        time.sleep(1)
    else:raise AssertionError('readiness timeout')
    bootstrap="""
CREATE ROLE pipeline_owner NOLOGIN;CREATE ROLE pipeline_operator NOLOGIN;
CREATE SCHEMA pipeline AUTHORIZATION pipeline_owner;
SET ROLE pipeline_owner;
CREATE TABLE pipeline.events(event_id text PRIMARY KEY,staged_at timestamptz NOT NULL,payload bytea NOT NULL);
CREATE TABLE pipeline.delivery_intents(event_id text NOT NULL REFERENCES pipeline.events,event_kind text,kind text NOT NULL,state text NOT NULL,disposition text,created_at timestamptz NOT NULL,PRIMARY KEY(event_id,kind));
CREATE TABLE pipeline.consumer_observations(event_id text PRIMARY KEY REFERENCES pipeline.events,state text NOT NULL,receipt_bytes bytea);
CREATE TABLE pipeline.backfill_members(run_id uuid NOT NULL,event_id text NOT NULL,first_batch uuid NOT NULL,PRIMARY KEY(run_id,event_id));
CREATE TABLE pipeline.attempt_totals(sink text,total bigint);CREATE TABLE pipeline.es_dead_letters(event_id text);CREATE TABLE pipeline.replay_requests(request_id uuid);
CREATE TABLE pipeline.backfill_runs(run_id uuid,created_at timestamptz,completed_at timestamptz);
CREATE TABLE pipeline.es_target(destination_id uuid,generation bigint,mode text,reason text,index_name text,index_uuid text,cluster_uuid text);
CREATE TABLE pipeline.rabbit_target(destination_id uuid,generation bigint,mode text,reason text,registration_id uuid,consumer_id uuid);
CREATE TABLE pipeline.combinations AS SELECT row_number() OVER() n,es,mq,receipt FROM unnest(ARRAY['satisfied','dead_letter','pending','leased','retry_wait',NULL]) es CROSS JOIN unnest(ARRAY['satisfied','dead_letter','pending','leased','retry_wait',NULL]) mq CROSS JOIN unnest(ARRAY['processed','quarantined','pending',NULL]) receipt;
INSERT INTO pipeline.events SELECT n::text,'2026-01-01Z'::timestamptz+n*interval '1 second',convert_to('immutable:'||n,'UTF8') FROM pipeline.combinations;
INSERT INTO pipeline.delivery_intents(event_id,kind,state,disposition,created_at) SELECT n::text,'elasticsearch',es,CASE WHEN es='satisfied' THEN 'applied' END,'2026-01-02Z'::timestamptz-n*interval '1 second' FROM pipeline.combinations WHERE es IS NOT NULL;
INSERT INTO pipeline.delivery_intents(event_id,kind,state,disposition,created_at) SELECT n::text,'rabbitmq',mq,CASE WHEN mq='satisfied' THEN 'broker_confirmed' END,'2026-01-02Z'::timestamptz-n*interval '1 second' FROM pipeline.combinations WHERE mq IS NOT NULL;
INSERT INTO pipeline.consumer_observations SELECT n::text,receipt,convert_to('receipt:'||n,'UTF8') FROM pipeline.combinations WHERE receipt IS NOT NULL;
INSERT INTO pipeline.backfill_members SELECT ('00000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,n::text,'55555555-5555-4555-8555-555555555555' FROM pipeline.combinations;
INSERT INTO pipeline.backfill_members SELECT '11111111-1111-4111-8111-111111111111',event_id,first_batch FROM pipeline.backfill_members;
GRANT USAGE ON SCHEMA pipeline TO pipeline_operator;GRANT SELECT ON ALL TABLES IN SCHEMA pipeline TO pipeline_operator;
RESET ROLE;
"""
    sql(bootstrap,'bootstrap')
    old=(ROOT/'migrations/pipeline/022-plan-observation-counts.sql').read_text()
    sql('BEGIN;'+old+old.replace('pipeline.backfill_observed_counts','pipeline.reference_counts')+'REVOKE ALL ON FUNCTION pipeline.backfill_observed_counts(uuid) FROM PUBLIC;COMMIT;','old-functions')
    before=original_rows('original-before')
    metadata="SELECT jsonb_build_object('oid',oid,'owner',proowner,'acl',proacl,'config',proconfig,'security',prosecdef,'volatility',provolatile,'parallel',proparallel) FROM pg_proc WHERE oid='pipeline.backfill_observed_counts(uuid)'::regprocedure;"
    catalog=sql(metadata,'catalog-before').stdout
    migration=(ROOT/'migrations/pipeline/023-observation-projections.sql').read_text()
    isolation=sql('BEGIN ISOLATION LEVEL REPEATABLE READ;'+migration+'COMMIT;','unsupported-upgrade-isolation',(3,));assert '25001' in isolation.stderr
    assert sql("SELECT to_regclass('pipeline.observation_events') IS NULL;",'rejected-upgrade-atomicity').stdout.strip()=='t'
    sql('BEGIN;'+migration+'COMMIT;','populated-upgrade')
    assert before==original_rows('original-after')
    assert catalog==sql(metadata,'catalog-after').stdout
    equal('populated-exact-equality');counts_equal('all-state-and-missing-partitions')
    queries=json.loads(command(['node','--input-type=module','-e',"import {queries,projectionSnapshot} from './src/operations/queries.ts';console.log(JSON.stringify({old:queries.pipeline.snapshot,new:projectionSnapshot}))"],'queries').stdout)
    def snapshot(query,label):
        v=value('BEGIN READ ONLY;SET LOCAL ROLE pipeline_operator;SET LOCAL statement_timeout=2500;'+query+';ROLLBACK;',label);v.pop('observed_at')
        for k in ('deliveries','observations','attempts','settled'):v[k].sort(key=lambda x:json.dumps(x,sort_keys=True))
        return v
    assert snapshot(queries['old'],'old-snapshot')==snapshot(queries['new'],'projected-snapshot')
    for table in ('observation_events','observation_deliveries','observation_receipts'):
        for verb,statement in [('update',f'UPDATE pipeline.{table} SET event_id=event_id'),('delete',f'DELETE FROM pipeline.{table}'),('truncate',f'TRUNCATE pipeline.{table}'),('insert',f'INSERT INTO pipeline.{table} SELECT * FROM pipeline.{table}')]:
            denied=sql('BEGIN;SET LOCAL ROLE pipeline_operator;'+statement+';ROLLBACK;',table+'-deny-'+verb,(3,));assert '42501' in denied.stderr
    # Separate sink identities stay independently writable; uncommitted mirrors
    # remain invisible and a rollback cannot publish an observation transition.
    first=interactive("UPDATE pipeline.delivery_intents SET state='leased',disposition=NULL WHERE event_id='1' AND kind='elasticsearch'",'concurrent-es')
    second=interactive("UPDATE pipeline.delivery_intents SET state='retry_wait',disposition=NULL WHERE event_id='1' AND kind='rabbitmq'",'concurrent-mq')
    assert before==original_rows('uncommitted-originals');equal('uncommitted-equality')
    finish(first,'COMMIT');finish(second,'ROLLBACK');equal('commit-rollback-equality');counts_equal('commit-rollback-counts')
    inserted=sql("BEGIN;INSERT INTO pipeline.events VALUES('new',clock_timestamp(),'immutable');INSERT INTO pipeline.delivery_intents(event_id,kind,state,created_at) VALUES('new','elasticsearch','pending',clock_timestamp());INSERT INTO pipeline.consumer_observations VALUES('new','pending',NULL);"+EQUALITY+'ROLLBACK;','insertion-rollback')
    assert inserted.stdout.strip()=='t'
    equal('after-insertion-rollback')
    sql("INSERT INTO pipeline.events VALUES('new',clock_timestamp(),'immutable');INSERT INTO pipeline.delivery_intents(event_id,kind,state,created_at) VALUES('new','elasticsearch','pending',clock_timestamp());INSERT INTO pipeline.consumer_observations VALUES('new','pending',NULL);UPDATE pipeline.consumer_observations SET state='processed' WHERE event_id='new';",'new-rows-and-receipt-transition')
    equal('insert-update-equality')
    sql("DELETE FROM pipeline.delivery_intents WHERE event_id='new';DELETE FROM pipeline.consumer_observations WHERE event_id='new';DELETE FROM pipeline.events WHERE event_id='new';",'deletion-cascade')
    equal('deletion-equality');counts_equal('final-partitions')
    for table,original in [('observation_deliveries','delivery_intents'),('observation_receipts','consumer_observations')]:
        result=sql(f"BEGIN;DELETE FROM pipeline.{table} WHERE event_id='1';UPDATE pipeline.{original} SET state='pending' WHERE event_id='1';ROLLBACK;",table+'-missing-fails-closed',(3,));assert 'P8001' in result.stderr
    equal('failed-transition-rolled-back')
    sql('CREATE DATABASE projection_fresh;','fresh-database')
    fresh_schema=bootstrap.split('CREATE TABLE pipeline.combinations',1)[0].replace('CREATE ROLE pipeline_owner NOLOGIN;CREATE ROLE pipeline_operator NOLOGIN;','')+'RESET ROLE;'
    fresh=command(PSQL+['-d','projection_fresh'],'fresh-install',fresh_schema+'BEGIN;'+old+migration+"COMMIT;INSERT INTO pipeline.events VALUES('first',clock_timestamp(),'canonical');INSERT INTO pipeline.delivery_intents(event_id,kind,state,created_at) VALUES('first','elasticsearch','pending',clock_timestamp());INSERT INTO pipeline.consumer_observations VALUES('first','pending',NULL);"+EQUALITY)
    assert fresh.stdout.strip()=='t'
    report.update(status='PASS',state_combinations=144,original_rows_sha256=hashlib.sha256(before.encode()).hexdigest(),migration_sha256=hashlib.sha256(migration.encode()).hexdigest(),catalog_preserved=True,exact_snapshot_preserved=True,uncommitted_visibility=True,separate_sink_concurrency=True,rollback=True,denied_writes=12,fresh_install=True,unsupported_migration_isolation_refused=True)
except BaseException as error:report.update(status='FAIL',error={'type':type(error).__name__,'message':str(error)})
finally:
    for p,err in processes:
        if p.poll() is None:
            try:finish(p,'ROLLBACK')
            except BaseException:p.kill();p.wait(timeout=10)
        err.close()
    identity=subprocess.run(['docker','inspect','--format','{{index .Config.Labels "kit.observation-proof"}}',NAME],text=True,capture_output=True,timeout=15)
    if identity.returncode==0:
        assert identity.stdout.strip()==NAME
        command(['docker','rm','-f','-v',NAME],'cleanup',timeout=60)
    report['cleanup']='PASS';save()
print(json.dumps({'status':report['status'],'out':str(OUT)}),flush=True)
raise SystemExit(0 if report['status']=='PASS' else 1)
