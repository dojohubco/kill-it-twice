"""Isolated query microbenchmark; synthetic tables are NOT runtime acceptance."""
import datetime
import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT
OUT = ROOT / 'artifacts/capacity' / ('delivery-index-benchmark-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S'))
OUT.mkdir(parents=True)
NAME = 'kit-observation-repro-' + uuid.uuid4().hex[:12]
IMAGE = 'postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af'
report = {'scope': 'Synthetic million-member exact-query/index microbenchmark only; not full-service acceptance', 'name': NAME, 'image': IMAGE, 'status': 'RUNNING', 'commands': []}

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
    report['before'] = [observe('before-' + str(i)) for i in range(3)]
    sql('BEGIN;\n' + (SOURCE / 'migrations/pipeline/021-delivery-observation-cover.sql').read_text() + '\nCOMMIT;', 'index')
    report['after'] = [observe('after-' + str(i)) for i in range(3)]
    expected = {'required': '1000000', 'invalid': None, 'es_satisfied': '1000000', 'es_errors': '0', 'es_pending': '0', 'rabbit_satisfied': '1000000', 'rabbit_pending': '0', 'consumer_processed': '1000000', 'consumer_errors': '0', 'consumer_pending': '0'}
    assert all(row == expected for row in report['after'])
    assert all(row is None or row == expected for row in report['before'])
    assert observe('small', small)['required'] == '32'
    assert observe('empty', empty)['required'] == '0'
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
