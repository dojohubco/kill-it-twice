"""Run-owned retained Compose smoke test; not the final scale or G1-G5 verifier."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
from verification.runtime import validate_compose_identity, cleanup_owned_resources

ROOT = Path(__file__).resolve().parents[1]
PROJECT = 'kit-verify-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]
OUT = ROOT / 'artifacts/runtime' / PROJECT
OUT.mkdir(parents=True)
HEAD = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
DIRTY = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
COUNT = 257
ENV = dict(os.environ, COMPOSE_PROJECT_NAME=PROJECT, KIT_UI_PORT='0', KIT_IMAGE='kill-it-twice-runtime:verify-' + HEAD[:12])
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'compose.yaml'), '-p', PROJECT]
report = dict(scope='Bounded retained runtime smoke, not final gates or capacity', project=PROJECT,
    started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), head=HEAD, developmental=bool(DIRTY),
    status='RUNNING', cases=[], commands=[], cleanup=None)
sequence = 0

def save():
    (OUT / 'run.json').write_text(json.dumps(report, indent=2) + '\n')

def run(args, label, timeout=120, expected=(0,), input_file=None, maximum=64 * 1024 * 1024):
    global sequence
    sequence += 1
    prefix = OUT / f'{sequence:03d}-{label}'
    record = dict(args=args, started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), label=label)
    source = input_file.open('rb') if input_file else subprocess.DEVNULL
    timed_out = overflow = False
    process = None
    try:
        with prefix.with_suffix('.stdout.log').open('wb') as output, prefix.with_suffix('.stderr.log').open('wb') as error:
            process = subprocess.Popen(args, cwd=ROOT, env=ENV, stdin=source, stdout=output, stderr=error, start_new_session=True)
            deadline = time.monotonic() + timeout
            while process.poll() is None:
                timed_out = time.monotonic() > deadline
                overflow = os.fstat(output.fileno()).st_size + os.fstat(error.fileno()).st_size > maximum
                if timed_out or overflow:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                    break
                time.sleep(0.05)
            record.update(exit=process.returncode, timed_out=timed_out, output_overflow=overflow)
    except BaseException:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        raise
    finally:
        if input_file: source.close()
    record['stdout_sha256'] = hashlib.sha256(prefix.with_suffix('.stdout.log').read_bytes()).hexdigest()
    record['stderr_sha256'] = hashlib.sha256(prefix.with_suffix('.stderr.log').read_bytes()).hexdigest()
    record['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    report['commands'].append(record)
    save()
    assert record['exit'] in expected and not timed_out and not overflow, record
    return prefix.with_suffix('.stdout.log')

compose_project_validated = False
def compose(args, label, **options):
    global compose_project_validated
    if not compose_project_validated:
        validate_compose_identity(PROJECT, COMPOSE, run)
        compose_project_validated = True
    return run(COMPOSE + args, label, **options)

def inspect_state():
    path = compose(['run', '--rm', '--no-deps', '-T', 'inspect'], 'state')
    return json.loads(path.read_text())

def case(name, evidence):
    assert name not in [c['id'] for c in report['cases']]
    report['cases'].append(dict(id=name, status='PASS', evidence=evidence))
    save()
    print(name + ' PASS', flush=True)

def observe(url):
    with urllib.request.urlopen(url + '/api/v1/status', timeout=20) as response:
        raw = response.read(4194305)
        assert len(raw) <= 4194304
        return json.loads(raw)['data']

def settled(url, total, effects, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = observe(url)
            d = last['dependencies']
            p = d['pipeline']['data']
            c = d['consumer']['data']
            if p['staged'] == str(total) and c['processed'] == str(total) and c['effects'] == str(effects):
                if sum(int(r['count']) for r in p['deliveries'] if r['state'] == 'satisfied') == total * 2:
                    if sum(int(r['count']) for r in p['observations'] if r['state'] == 'processed') == total and d['source']['data']['counts']['acknowledged'] == str(effects):
                        if last['backfill']['phase'] == 'complete':
                            return last
        except (OSError, ValueError, KeyError, TypeError) as error:
            last = dict(error=type(error).__name__)
        time.sleep(1)
    (OUT / 'last-observation.json').write_text(json.dumps(last, indent=2))
    raise AssertionError('Timed out waiting for declared data to settle')

def exports(destination):
    destination.mkdir()
    for name in ('baselines', 'source', 'mutations', 'commands', 'work', 'pipeline', 'consumer', 'totals', 'projection', 'receiver'):
        log = compose(['run', '--rm', '--no-deps', '-T', 'inspect', 'node', 'scripts/runtime/inspect.ts', name], 'export-' + name)
        destination.joinpath(name + '.jsonl').write_bytes(log.read_bytes())

def change(request):
    with (OUT / 'journal.jsonl').open('a') as journal:
        journal.write(json.dumps(request, separators=(',', ':')) + '\n')
    path = OUT / ('command-' + str(len(report['commands'])) + '.json')
    path.write_text(json.dumps(request))
    log = compose(['run', '--rm', '--no-deps', '-T', 'writer'], 'source-command', input_file=path)
    return json.loads(log.read_text())

def request(operation, entity=None, payload=None):
    return dict(command_id=str(uuid.uuid4()), operation=operation, entity_id=entity,
        payload_json=json.dumps(payload, ensure_ascii=False, separators=(',', ':')) if payload is not None else None)

paths = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=ROOT).decode().split('\0')
inputs = [{'path': name, 'sha256': hashlib.sha256((ROOT / name).read_bytes()).hexdigest()} for name in sorted(set(filter(None, paths))) if (ROOT / name).is_file()]
report['input_sha256'] = hashlib.sha256(json.dumps(inputs, separators=(',', ':')).encode()).hexdigest()
(OUT / 'inputs.json').write_text(json.dumps(inputs, indent=2) + '\n')
save()
(OUT / 'journal.jsonl').write_text('')
(OUT / 'initial-git-status.txt').write_text(DIRTY)
print(str(OUT), flush=True)
try:
    run(['node', 'scripts/runtime-preflight.ts'], 'prerequisite')
    compose(['config', '--quiet'], 'compose-config')
    compose(['up', '-d', '--build'], 'cold-up', timeout=600)
    port = compose(['port', 'ui', '4200'], 'gateway-port').read_text().strip()
    assert port.startswith('127.0.0.1:')
    url = 'http://' + port
    report['gateway'] = url
    before = inspect_state()
    assert before['source']['entities'] == before['source']['mutations'] == before['pipeline']['events'] == '0'
    ids = compose(['ps', '-q', '--all'], 'owned-containers').read_text().split()
    assert len(ids) >= 16
    for container in ids:
        details = json.loads(run(['docker', 'inspect', container], 'container').read_text())[0]
        assert details['Config']['Labels']['com.docker.compose.project'] == PROJECT
        service = details['Config']['Labels']['com.docker.compose.service']
        assert not any(m['Destination'] in ('/var/run/docker.sock', '/run/docker.sock') for m in details['Mounts'])
        if service in ('capture', 'backfill', 'es-worker', 'publisher', 'consumer', 'observer', 'control-api', 'ui'):
            assert details['Config']['User'] == '1000:0' and details['HostConfig']['ReadonlyRootfs']
            assert 0 < details['HostConfig']['Memory'] <= 268435456
            assert not any(m['Destination'] in ('/private', '/es') for m in details['Mounts'])
        bindings = details['NetworkSettings']['Ports'] or {}
        for ports in bindings.values():
            for binding in ports or []: assert binding['HostIp'] == '127.0.0.1' and service == 'ui'
    case('R01', dict(gateway=url, initial=before, containers=len(ids)))
    run(['make','seed','SEED_COUNT='+str(COUNT)], 'published-make-seed', timeout=240)
    first = settled(url, COUNT, 0)
    original_baselines = compose(['run', '--rm', '--no-deps', '-T', 'inspect', 'node', 'scripts/runtime/inspect.ts', 'baselines'], 'baseline-before-repeat')
    compose(['run', '--rm', '--no-deps', '-T', 'seed', 'node', 'scripts/runtime/seed.ts', str(COUNT)], 'same-seed', timeout=60)
    compose(['run', '--rm', '--no-deps', '-T', 'seed', 'node', 'scripts/runtime/seed.ts', str(COUNT + 1)], 'conflicting-seed', expected=(1,))
    repeated_baselines = compose(['run', '--rm', '--no-deps', '-T', 'inspect', 'node', 'scripts/runtime/inspect.ts', 'baselines'], 'baseline-after-repeat')
    assert original_baselines.read_bytes() == repeated_baselines.read_bytes()
    unchanged = inspect_state()
    assert unchanged['source']['entities'] == str(COUNT) and unchanged['source']['mutations'] == '0'
    case('R02', dict(count=COUNT, exact_baseline_sha256=hashlib.sha256(original_baselines.read_bytes()).hexdigest(), repeat='identical', conflict='rejected'))
    update = request('update', '1', dict(name='Runtime update', country='GE', loyalty_points=42, exact='9007199254740993'))
    changed = change(update)
    replay = change(update)
    assert changed['result'] == replay['result'] and not changed['replayed'] and replay['replayed']
    change(request('delete', '2'))
    change(request('restore', '2', dict(name='Restored', country='FR', loyalty_points=8)))
    change(request('delete', '3'))
    change(request('create', None, dict(name='New after baseline', country='GE', loyalty_points=9)))
    settled(url, COUNT + 5, 5)
    case('R03', dict(captured_mutations=5, command_retry='same_result', historical_events=COUNT + 5))
    consumer_id = compose(['ps', '-q', 'consumer'], 'consumer-id').read_text().strip()
    old = json.loads(run(['docker', 'inspect', consumer_id], 'consumer-before-kill').read_text())[0]
    assert old['State']['Running']
    run(['docker', 'kill', '--signal=KILL', consumer_id], 'actual-consumer-kill')
    event_file = run(['docker', 'events', '--since', old['State']['StartedAt'], '--until', datetime.datetime.now(datetime.timezone.utc).isoformat(), '--filter', 'container=' + consumer_id, '--filter', 'event=die', '--format', '{{json .}}'], 'consumer-kill-evidence')
    died = [json.loads(line) for line in event_file.read_text().splitlines()]
    assert any(item['Actor']['Attributes'].get('exitCode') == '137' for item in died)
    compose(['start', 'consumer'], 'consumer-restart')
    settled(url, COUNT + 5, 5)
    workers = ['capture', 'backfill', 'es-worker', 'publisher', 'consumer', 'observer']
    compose(['stop', '-t', '20', *workers], 'quiesce-before-export', timeout=180)
    exports(OUT / 'before-restart')
    run([sys.executable, 'tests/runtime/reconcile.py', str(OUT / 'before-restart'), '--count', str(COUNT), '--journal', str(OUT / 'journal.jsonl')], 'oracle-before')
    stable = inspect_state()
    compose(['down', '--timeout', '20'], 'retained-down', timeout=180)
    compose(['up', '-d', '--no-build'], 'retained-up', timeout=300)
    port = compose(['port', 'ui', '4200'], 'retained-gateway-port').read_text().strip()
    assert port.startswith('127.0.0.1:')
    url = 'http://' + port
    settled(url, COUNT + 5, 5)
    compose(['stop', '-t', '20', *workers], 'quiesce-after-restart', timeout=180)
    exports(OUT / 'after-restart')
    run([sys.executable, 'tests/runtime/reconcile.py', str(OUT / 'after-restart'), '--count', str(COUNT), '--journal', str(OUT / 'journal.jsonl')], 'oracle-after')
    for file in (OUT / 'before-restart').glob('*.jsonl'):
        assert file.read_bytes() == (OUT / 'after-restart' / file.name).read_bytes(), ('restart changed', file.name)
    assert inspect_state() == stable
    case('R04', dict(killed_container=consumer_id, observed_die_events=died, restart='exact exports retained', pipeline_identity=stable['pipeline']['identity']))
    browser = run(['node', 'scripts/runtime-browser.ts', url, str(OUT), str(COUNT + 5)], 'real-browser', timeout=90)
    case('R05', json.loads(browser.read_text()))
    # Real exported negative controls are separate from the accepted receiver snapshots.
    negatives = []
    for mode in ('missing', 'extra', 'changed'):
        copy = OUT / ('negative-' + mode)
        copy.mkdir()
        for file in (OUT / 'after-restart').glob('*.jsonl'): (copy / file.name).write_bytes(file.read_bytes())
        file = copy / 'receiver.jsonl'
        rows = file.read_text().splitlines()
        if mode == 'missing': rows.pop()
        elif mode == 'extra':
            extra = json.loads(rows[0]); extra['document_id'] += '-unexpected'; rows.append(json.dumps(extra))
        else:
            row = json.loads(rows[0]); row['source']['content_sha256'] = '0' * 64; rows[0] = json.dumps(row)
        file.write_text('\n'.join(rows) + '\n')
        run([sys.executable, 'tests/runtime/reconcile.py', str(copy), '--count', str(COUNT), '--journal', str(OUT / 'journal.jsonl')], 'oracle-negative-' + mode, expected=(1,))
        negatives.append(mode)
    case('R06', dict(independent_oracles='before and after retained restart', negative_controls=negatives, capacity='NOT RUN'))
    compose(['start','capture','es-worker','publisher','consumer','observer'],'start-operator-fixture-workers')
    controls=run(['node','scripts/runtime-operator-browser.ts',url,str(OUT),PROJECT],'real-operator-controls',timeout=360)
    case('R07',json.loads(controls.read_text()))
    assert [c['id'] for c in report['cases']] == ['R01', 'R02', 'R03', 'R04', 'R05', 'R06','R07']
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == HEAD
    assert subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True) == DIRTY
    for item in inputs: assert hashlib.sha256((ROOT / item['path']).read_bytes()).hexdigest() == item['sha256'], item['path']
    report['status'] = 'PASS'
except BaseException as error:
    report['status'] = 'FAIL'
    report['error'] = dict(type=type(error).__name__, message=str(error))
finally:
    report['cleanup'] = cleanup_owned_resources(PROJECT, run)
    if report['cleanup']['status'] == 'FAIL': report['status'] = 'FAIL'
    report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
print(report['status'] + ': ' + str(OUT / 'run.json'), flush=True)
sys.exit(0 if report['status'] == 'PASS' else 1)
