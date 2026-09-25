"""Isolated query microbenchmark; synthetic tables are NOT runtime acceptance."""
import argparse
import datetime
import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--dirty', action='store_true')
options = parser.parse_args()

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT
OUT = ROOT / 'artifacts/capacity' / ('observation-counts-benchmark-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S'))
OUT.mkdir(parents=True)
NAME = 'kit-observation-repro-' + uuid.uuid4().hex[:12]
IMAGE = 'postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af'
report = {'scope': 'Synthetic million-member exact-query aggregate microbenchmark only; not full-service acceptance', 'name': NAME, 'image': IMAGE, 'status': 'RUNNING', 'commands': []}

def save():
    (OUT / 'result.json').write_text(json.dumps(report, indent=2) + '\n')

def command(args, label, sql=None, timeout=1200, expected=(0,)):
    start = time.monotonic()
    if sql is not None:
        (OUT / (label + '.sql')).write_text(sql)
    p = subprocess.run(args, input=sql, text=True, capture_output=True, timeout=timeout)
    (OUT / (label + '.stdout')).write_text(p.stdout)
    (OUT / (label + '.stderr')).write_text(p.stderr)
    rec = {'label': label, 'exit': p.returncode, 'seconds': round(time.monotonic() - start, 3), 'stdout_sha256': hashlib.sha256(p.stdout.encode()).hexdigest()}
    report['commands'].append(rec)
    save()
    print(json.dumps(rec), flush=True)
    assert p.returncode in expected, (label, p.returncode, p.stderr[-1000:])
    return p

def sql(statement, label, expected=(0,)):
    return command(['docker', 'exec', '-i', NAME, 'psql', '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres'], label, statement, expected=expected)

large = '11111111-1111-4111-8111-111111111111'
small = '22222222-2222-4222-8222-222222222222'
empty = '33333333-3333-4333-8333-333333333333'
original = (SOURCE / 'migrations/pipeline/016-observation-and-progress.sql').read_text()
start = original.index('CREATE FUNCTION pipeline.backfill_observed_counts(')
end = original.index('\n$$;', start) + 4
original = original[start:end]
report['head'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
report['function_sha256'] = hashlib.sha256(original.encode()).hexdigest()
save()
print(str(OUT), flush=True)
try:
    command(['docker', 'image', 'inspect', '--format', '{{.Id}}', IMAGE], 'image')
    command(['docker', 'run', '-d', '--name', NAME, '--label', 'kit.observation-proof=' + NAME, '--network', 'none', '--memory', '1g', '--memory-swap', '1g', '--shm-size', '256m', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', IMAGE], 'start')
    for _ in range(60):
        if subprocess.run(['docker', 'exec', NAME, 'pg_isready', '-U', 'postgres'], capture_output=True).returncode == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('PostgreSQL readiness timeout')
    sql(f"""
CREATE ROLE pipeline_owner NOLOGIN;
CREATE SCHEMA pipeline AUTHORIZATION pipeline_owner;
CREATE TABLE pipeline.backfill_members(run_id uuid NOT NULL,event_id text NOT NULL,first_batch uuid NOT NULL,PRIMARY KEY(run_id,event_id));
CREATE TABLE pipeline.delivery_intents(event_id text NOT NULL,kind text NOT NULL,state text NOT NULL,disposition text,created_at timestamptz NOT NULL DEFAULT now(),padding bytea,PRIMARY KEY(event_id,kind));
CREATE TABLE pipeline.consumer_observations(event_id text PRIMARY KEY,state text NOT NULL,receipt_bytes bytea);
ALTER TABLE pipeline.consumer_observations ALTER COLUMN receipt_bytes SET STORAGE EXTERNAL;
CREATE INDEX consumer_observation_cover ON pipeline.consumer_observations(event_id) INCLUDE(state);
CREATE INDEX es_unresolved_observation ON pipeline.delivery_intents(event_id) WHERE kind='elasticsearch' AND state<>'satisfied';
CREATE INDEX delivery_state_observation ON pipeline.delivery_intents(kind,state,disposition,created_at);
CREATE INDEX consumer_state_observation ON pipeline.consumer_observations(state);
INSERT INTO pipeline.backfill_members SELECT '{large}', '44444444-4444-4444-8444-444444444444:'||g||':1','55555555-5555-4555-8555-555555555555' FROM generate_series(1,1000000) g;
INSERT INTO pipeline.backfill_members SELECT '{small}',event_id,first_batch FROM pipeline.backfill_members WHERE run_id='{large}' ORDER BY event_id LIMIT 32;
INSERT INTO pipeline.delivery_intents(event_id,kind,state,disposition,padding)
 SELECT event_id,k,'satisfied',CASE WHEN k='elasticsearch' THEN 'applied' ELSE 'broker_confirmed' END,decode(repeat(md5(event_id),8),'hex')
 FROM pipeline.backfill_members CROSS JOIN unnest(ARRAY['elasticsearch','rabbitmq']) k WHERE run_id='{large}';
INSERT INTO pipeline.consumer_observations SELECT event_id,'processed',convert_to(repeat(md5(event_id),32),'UTF8') FROM pipeline.backfill_members WHERE run_id='{large}';
{original}
ALTER TABLE pipeline.delivery_intents OWNER TO pipeline_owner;
VACUUM (ANALYZE) pipeline.backfill_members;
VACUUM (ANALYZE) pipeline.delivery_intents;
VACUUM (ANALYZE) pipeline.consumer_observations;
""", 'seed')
    def observe(label, run=large):
        p = sql(f"BEGIN READ ONLY;SET LOCAL jit=off;SET LOCAL statement_timeout=8000;SELECT pipeline.backfill_observed_counts('{run}');ROLLBACK;", label, (0, 3))
        if p.returncode:
            assert 'statement timeout' in p.stderr, p.stderr
            return None
        return json.loads(p.stdout)
    sql('BEGIN;\n' + (SOURCE / 'migrations/pipeline/021-delivery-observation-cover.sql').read_text() + '\nCOMMIT; ALTER FUNCTION pipeline.backfill_observed_counts(uuid) OWNER TO pipeline_owner;', 'prior-index')
    if options.dirty:
        sql('''
ALTER TABLE pipeline.delivery_intents SET (autovacuum_enabled=false);
ALTER TABLE pipeline.consumer_observations SET (autovacuum_enabled=false);
UPDATE pipeline.delivery_intents SET state='leased';
UPDATE pipeline.delivery_intents SET state='satisfied';
UPDATE pipeline.consumer_observations SET state='pending';
UPDATE pipeline.consumer_observations SET state='processed';
ANALYZE pipeline.delivery_intents;
ANALYZE pipeline.consumer_observations;
''', 'dirty-obligations')
        report['fixture_deviation'] = 'Synthetic state churn with autovacuum disabled only on owned fixture tables; not original runtime data or execution plan.'
    report['before'] = [observe('before-' + str(i)) for i in range(3)]
    sql('BEGIN;\n' + (SOURCE / 'migrations/pipeline/022-plan-observation-counts.sql').read_text() + '\nCOMMIT;', 'counts-upgrade')
    report['after'] = [observe('after-' + str(i)) for i in range(3)]
    expected = {'required': '1000000', 'invalid': None, 'es_satisfied': '1000000', 'es_errors': '0', 'es_pending': '0', 'rabbit_satisfied': '1000000', 'rabbit_pending': '0', 'consumer_processed': '1000000', 'consumer_errors': '0', 'consumer_pending': '0'}
    assert all(row == expected for row in report['after'])
    assert all(row is None or row == expected for row in report['before'])
    assert observe('small', small) is not None, 'Small-run observation exceeded unchanged 8s bound'
    assert observe('small-count', small)['required'] == '32'
    assert observe('empty', empty)['required'] == '0'

    # One backend retains function plans across repeated large/small/empty reads.
    sequence=[large]*6+[small,empty,large,small,empty,large]
    repeated=sql('\n'.join(f"BEGIN READ ONLY;SET LOCAL jit=off;SET LOCAL statement_timeout=8000;SELECT pipeline.backfill_observed_counts('{run}');ROLLBACK;" for run in sequence),'persistent-backend')
    rows=[json.loads(line) for line in repeated.stdout.splitlines() if line.strip()]
    assert len(rows)==len(sequence)
    for run,row in zip(sequence,rows,strict=True):
        wanted=dict(expected)
        count='1000000' if run==large else '32' if run==small else '0'
        for key in ('required','es_satisfied','rabbit_satisfied','consumer_processed'): wanted[key]=count
        assert row==wanted,(run,row)
    report['persistent_backend_exact_reads']=len(sequence)

    reference = original.replace('pipeline.backfill_observed_counts(', 'pipeline.reference_observed_counts(')
    combinations = sql(reference + '''
CREATE TEMP TABLE combinations AS
 SELECT row_number() OVER() n,es,mq,observation FROM unnest(ARRAY['satisfied','dead_letter','pending','leased','retry_wait',NULL]) es
 CROSS JOIN unnest(ARRAY['satisfied','dead_letter','pending','leased','retry_wait',NULL]) mq
 CROSS JOIN unnest(ARRAY['processed','quarantined','pending',NULL]) observation;
INSERT INTO pipeline.backfill_members SELECT ('00000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,'combination:'||n,'55555555-5555-4555-8555-555555555555' FROM combinations;
INSERT INTO pipeline.backfill_members SELECT '66666666-6666-4666-8666-666666666666','combination:'||n,'55555555-5555-4555-8555-555555555555' FROM combinations;
INSERT INTO pipeline.delivery_intents(event_id,kind,state) SELECT 'combination:'||n,'elasticsearch',es FROM combinations WHERE es IS NOT NULL;
INSERT INTO pipeline.delivery_intents(event_id,kind,state) SELECT 'combination:'||n,'rabbitmq',mq FROM combinations WHERE mq IS NOT NULL;
INSERT INTO pipeline.consumer_observations(event_id,state) SELECT 'combination:'||n,observation FROM combinations WHERE observation IS NOT NULL;
SET statement_timeout=8000;
SET jit=off;
DO $$ DECLARE r uuid; BEGIN
 FOR r IN SELECT DISTINCT run_id FROM pipeline.backfill_members WHERE run_id<>'11111111-1111-4111-8111-111111111111' LOOP
  IF pipeline.backfill_observed_counts(r) IS DISTINCT FROM pipeline.reference_observed_counts(r) THEN RAISE EXCEPTION 'Exact count mismatch for %',r; END IF;
 END LOOP;
END $$;
SELECT count(*) FROM combinations;
''', 'all-state-equivalence')
    assert int(combinations.stdout.strip()) == 144
    report['state_combinations'] = 144
    report['equivalence_scope'] = 'Every supported state or missing obligation for each sink/receipt, individual and aggregated runs; overlapping and empty runs.'
    upgraded = (SOURCE / 'migrations/pipeline/022-plan-observation-counts.sql').read_text()
    old_body=original.split('AS $$',1)[1].split('$$;',1)[0].strip()
    new_body=upgraded.split('$count$')[1].replace('m.run_id= $1','m.run_id=id').strip()
    assert new_body==old_body,'Aggregate query changed'
    report['aggregate_query_unchanged']=True
    nested=sql(f"LOAD 'auto_explain';SET client_min_messages=log;SET auto_explain.log_min_duration=0;SET auto_explain.log_nested_statements=on;SET auto_explain.log_analyze=on;SET auto_explain.log_timing=off;SET auto_explain.log_buffers=on;SET auto_explain.log_format=json;BEGIN READ ONLY;SET LOCAL jit=off;SET LOCAL statement_timeout=8000;SELECT pipeline.backfill_observed_counts('{large}');ROLLBACK;",'nested-plan')
    assert json.loads(nested.stdout)==expected
    report['migration_sha256'] = hashlib.sha256(upgraded.encode()).hexdigest()
    report['status'] = 'PASS'
except BaseException as e:
    report.update(status='FAIL', error={'type': type(e).__name__, 'message': str(e)})
finally:
    try:
        identity = subprocess.run(['docker', 'inspect', '--format', '{{index .Config.Labels "kit.observation-proof"}}', NAME], text=True, capture_output=True, timeout=20)
        if identity.returncode == 0:
            assert identity.stdout.strip() == NAME
            command(['docker', 'rm', '-f', '-v', NAME], 'cleanup', timeout=60)
        report['cleanup'] = 'PASS'
    except BaseException as e:
        report['cleanup'] = str(e)
        report['status'] = 'FAIL'
    save()
print(json.dumps({'status':report['status'], 'result':str(OUT/'result.json')}), flush=True)
raise SystemExit(0 if report['status']=='PASS' else 1)
