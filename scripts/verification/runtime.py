"""Run-owned integration tools. These do not implement replication or its oracle."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import resource
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(path):
    with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()

def validate_compose_identity(project, compose_args, run):
    config=json.loads(run(compose_args+['config','--format','json'],'owned-compose-identity',timeout=30).read_text())
    assert config['name']==project, 'Compose resolved a different project'
    assert all(v['name'].startswith(project+'_') and not v.get('external') for v in config.get('volumes',{}).values())
    assert all(v['name'].startswith(project+'_') and not v.get('external') for v in config.get('networks',{}).values())

def cleanup_owned_resources(project, run):
    failures=[]
    try:
        # Use explicit, revalidated resource IDs rather than Compose down:
        # a configuration-resolution failure must never choose a demo.
        assert project.startswith(('kit-final-', 'kit-verify-'))
        deadline=time.monotonic()+180
        def bounded(args,label):
            remaining=deadline-time.monotonic()
            assert remaining>0, 'Owned cleanup deadline'
            return run(args,label,timeout=min(60,remaining),maximum=4*1024*1024)
        for kind,listing in [('container',['ps','-aq']),('volume',['volume','ls','-q']),('network',['network','ls','-q'])]:
            identifiers=bounded(['docker',*listing,'--filter','label=com.docker.compose.project='+project],'cleanup-list-'+kind).read_text().split()
            if identifiers:
                records=json.loads(bounded(['docker',kind,'inspect',*identifiers],'cleanup-identities-'+kind).read_text())
                assert len(records)==len(identifiers)
                for record in records:
                    labels=record['Config']['Labels'] if kind=='container' else record['Labels']
                    assert labels['com.docker.compose.project']==project
                    if kind=='network':assert not record['Containers'], 'Owned network still has attached containers'
                if kind=='container':
                    bounded(['docker','stop','--time','20',*identifiers],'cleanup-stop-containers')
                    bounded(['docker','rm','--force',*identifiers],'cleanup-remove-containers')
                else:bounded(['docker',kind,'rm',*identifiers],'cleanup-remove-'+kind)
            assert not bounded(['docker',*listing,'--filter','label=com.docker.compose.project='+project],'remaining-'+kind).read_text().strip(),kind
    except BaseException as e: failures.append({'type':type(e).__name__,'message':str(e)})
    return {'status':'FAIL' if failures else 'PASS','errors':failures}

class Runtime:
    def __init__(self, count):
        self.project = 'kit-final-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]
        self.out = ROOT / 'artifacts/final' / self.project
        self.out.mkdir(parents=True)
        # Fault workers keep UID 1000 and gain only this owned evidence group.
        # A hosted runner's UID need not equal the container's UID.
        self.out.chmod(0o770)
        self.evidence_group = str(self.out.stat().st_gid)
        self.head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        self.dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
        self.env = dict(os.environ, KIT_UI_PORT='0', KIT_IMAGE='kill-it-twice-runtime:final-' + self.head[:12])
        self.compose_args = ['docker', 'compose', '-f', str(ROOT/'compose.yaml'), '-f', str(self.out/'verification.json'), '-p', self.project]
        self.inspector_id = None
        self.compose_project_validated = False
        self.sequence = 0
        self.health_check = lambda: None
        self.report = dict(scope='Integrated real fault gates at an explicit finite fixture; not unrun scale evidence', project=self.project, count=count, head=self.head, developmental=bool(self.dirty), started_at=now(), status='RUNNING', gates=[], commands=[], cleanup=None)
        self.save()
    def save(self): (self.out/'run.json').write_text(json.dumps(self.report, indent=2)+'\n')
    def run(self, args, label, timeout=120, expected=(0,), input_file=None, maximum=64*1024*1024, graceful=0):
        self.sequence += 1
        prefix = self.out / f'{self.sequence:03d}-{label}'
        stdout, stderr = prefix.with_suffix('.stdout.log'), prefix.with_suffix('.stderr.log')
        record = dict(args=args, label=label, started_at=now(), stdout=stdout.name, stderr=stderr.name)
        source = input_file.open('rb') if input_file else subprocess.DEVNULL
        usage_before=resource.getrusage(resource.RUSAGE_CHILDREN)
        peak_rss=0
        timed_out = overflow = False
        process = None
        failure = None
        try:
            with stdout.open('wb') as out, stderr.open('wb') as err:
                process = subprocess.Popen(args, cwd=ROOT, env=self.env, stdin=source, stdout=out, stderr=err, start_new_session=True)
                deadline = time.monotonic()+timeout
                while process.poll() is None:
                    self.health_check()
                    try:
                        for line in Path(f'/proc/{process.pid}/status').read_text().splitlines():
                            if line.startswith('VmHWM:'):peak_rss=max(peak_rss,int(line.split()[1])*1024)
                    except FileNotFoundError:pass
                    timed_out = time.monotonic() >= deadline
                    overflow = os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > maximum
                    if timed_out or overflow:
                        if graceful:
                            process.terminate()
                            try: process.wait(timeout=graceful)
                            except subprocess.TimeoutExpired: pass
                        if process.poll() is None:
                            os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10)
                        break
                    time.sleep(.05)
                overflow |= os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > maximum
                record.update(exit=process.returncode, timed_out=timed_out, output_overflow=overflow)
        except BaseException as error:
            failure=error
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10)
            record.update(exit=process.returncode if process else None,timed_out=timed_out,output_overflow=overflow,interrupted={'type':type(error).__name__,'message':str(error)})
        finally:
            if input_file: source.close()
        usage_after=resource.getrusage(resource.RUSAGE_CHILDREN)
        record.update(process_peak_rss_bytes=peak_rss,cpu_user_seconds=usage_after.ru_utime-usage_before.ru_utime,cpu_system_seconds=usage_after.ru_stime-usage_before.ru_stime)
        record.update(stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), finished_at=now())
        self.report['commands'].append(record); self.save()
        if failure is not None:raise failure
        assert record['exit'] in expected and not timed_out and not overflow, record
        return stdout
    def compose(self, args, label, **options):
        # Resolve configuration from the repository and verify ownership before
        # any operation. An explicit -p alone is not a sufficient cleanup guard.
        if not self.compose_project_validated:
            validate_compose_identity(self.project, self.compose_args, self.run)
            self.compose_project_validated=True
        return self.run(self.compose_args+args, label, **options)
    def start_inspector(self):
        # Only the verifier override supplies this resident process. Each read
        # still starts a fresh Node process and uses the original SQL/roles.
        self.compose(['up','-d','--no-build','--no-deps','inspect'],'resident-inspector')
        self.inspector_id=self.worker_id('inspect')
        self.report['resident_inspector']={'container_id':self.inspector_id,'started_at':now()}
        self.save()
    def json_command(self, args, label, **options):
        if self.inspector_id and args[:5]==['run','--rm','--no-deps','-T','inspect']:
            output=self.run(['docker','exec',self.inspector_id,*args[5:]],label,**options)
        else: output=self.compose(args,label,**options)
        return json.loads(output.read_text())
    def inspect(self, mode='summary'):
        return self.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/verification/inspect.ts',mode], 'inspect-'+mode)
    def worker_id(self, role):
        identity=self.compose(['ps','--all','-q',role],role+'-id').read_text().strip()
        assert len(identity)==64 and all(c in '0123456789abcdef' for c in identity)
        record=json.loads(self.run(['docker','inspect',identity],role+'-identity').read_text())[0]
        assert record['Config']['Labels']['com.docker.compose.project']==self.project
        assert record['Config']['Labels']['com.docker.compose.service']==role
        return identity
    def http(self, path, limit=4*1024*1024):
        with urllib.request.urlopen(self.url+path,timeout=20) as response:
            data=response.read(limit+1); assert len(data)<=limit
            return data.decode('utf8')
    def status(self): return json.loads(self.http('/api/v1/status'))['data']
    def wait(self, operation, predicate, label, timeout=240, interval=.2):
        deadline=time.monotonic()+timeout; last=None
        while time.monotonic()<deadline:
            self.health_check()
            try:
                last=operation()
                if predicate(last): return last
            except (OSError,ValueError) as e: last={'unavailable':type(e).__name__}
            time.sleep(interval)
        (self.out/(label+'-last.json')).write_text(json.dumps(last,indent=2))
        raise AssertionError('Deadline: '+label)
    def consumer_redelivery(self, event_id):
        # Durable counters may already be settled before the restarted process
        # receives the unacknowledged message. Observe the real replay itself.
        def observe():
            path=self.out/'consumer-trace.jsonl'
            rows=[json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []
            delivered=[row['data'] for row in rows if row['type']=='consumer-delivery' and row['data']['message_id']==event_id]
            committed=[result for row in rows if row['type']=='consumer-commit' for result in row['data']['result'] if result['event_id']==event_id and result['status']=='already_processed']
            return {'deliveries':delivered,'duplicate_commits':committed}
        evidence=self.wait(observe,lambda value:len(value['deliveries'])>=2 and any(row['redelivered'] for row in value['deliveries']) and bool(value['duplicate_commits']),'consumer-redelivery',timeout=15)
        assert len({row['wire_sha256'] for row in evidence['deliveries']})==1, 'Consumer redelivery wire bytes changed'
        return evidence
    def control(self, role, boundary='', record=False):
        token=str(uuid.uuid4())
        (self.out/(role+'-control.json')).write_text(json.dumps({'boundary':boundary,'token':token,'record':record}))
        return token
    def boundary(self, token):
        path=self.out/(token+'.reached.json')
        return self.wait(lambda: json.loads(path.read_text()) if path.exists() else None,lambda v:v is not None,'barrier-'+token,timeout=180)
    def kill(self, role, boundary):
        identity=self.worker_id(role)
        before=json.loads(self.run(['docker','inspect',identity],role+'-before-kill').read_text())[0]
        assert before['State']['Running']
        self.run(['docker','kill','--signal=KILL',identity],role+'-sigkill')
        after=json.loads(self.run(['docker','inspect',identity],role+'-after-kill').read_text())[0]
        assert not after['State']['Running'] and after['State']['ExitCode']==137
        return {'boundary':boundary,'container_id':identity,'exit_code':137,'started_at':before['State']['StartedAt'],'finished_at':after['State']['FinishedAt']}
    def start(self,role):
        # Restart exactly the existing owned receiver/worker. Compose start can
        # also restart completed provisioning dependencies during fault tests.
        identity=self.worker_id(role)
        self.run(['docker','start',identity],role+'-start')
    def stop(self,role): self.compose(['stop','-t','20',role],role+'-stop',timeout=60)
    def gate(self,name,evidence):
        assert name in ('G1','G2','G3','G4','G5') and name not in [x['id'] for x in self.report['gates']]
        self.report['gates'].append({'id':name,'status':'PASS','evidence':evidence});self.save();print(name+' PASS',flush=True)
    def cleanup(self):
        self.report['cleanup']=cleanup_owned_resources(self.project, self.run)
        if self.report['cleanup']['status']=='FAIL':self.report['status']='FAIL'
        self.report['finished_at']=now();self.save()
