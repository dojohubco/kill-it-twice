"""Run-owned integration tools. These do not implement replication or its oracle."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(path):
    with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()

class Runtime:
    def __init__(self, count):
        self.project = 'kit-final-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]
        self.out = ROOT / 'artifacts/final' / self.project
        self.out.mkdir(parents=True)
        self.head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        self.dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
        self.env = dict(os.environ, KIT_UI_PORT='0', KIT_IMAGE='kill-it-twice-runtime:final-' + self.head[:12])
        self.compose_args = ['docker', 'compose', '-f', str(ROOT/'compose.yaml'), '-f', str(self.out/'verification.json'), '-p', self.project]
        self.sequence = 0
        self.report = dict(scope='Integrated real fault gates at an explicit finite fixture; not unrun scale evidence', project=self.project, count=count, head=self.head, developmental=bool(self.dirty), started_at=now(), status='RUNNING', gates=[], commands=[], cleanup=None)
        self.save()
    def save(self): (self.out/'run.json').write_text(json.dumps(self.report, indent=2)+'\n')
    def run(self, args, label, timeout=120, expected=(0,), input_file=None, maximum=64*1024*1024):
        self.sequence += 1
        prefix = self.out / f'{self.sequence:03d}-{label}'
        stdout, stderr = prefix.with_suffix('.stdout.log'), prefix.with_suffix('.stderr.log')
        record = dict(args=args, label=label, started_at=now(), stdout=stdout.name, stderr=stderr.name)
        source = input_file.open('rb') if input_file else subprocess.DEVNULL
        timed_out = overflow = False
        process = None
        try:
            with stdout.open('wb') as out, stderr.open('wb') as err:
                process = subprocess.Popen(args, cwd=ROOT, env=self.env, stdin=source, stdout=out, stderr=err, start_new_session=True)
                deadline = time.monotonic()+timeout
                while process.poll() is None:
                    timed_out = time.monotonic() >= deadline
                    overflow = os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > maximum
                    if timed_out or overflow:
                        os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10); break
                    time.sleep(.05)
                overflow |= os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > maximum
                record.update(exit=process.returncode, timed_out=timed_out, output_overflow=overflow)
        except BaseException:
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10)
            raise
        finally:
            if input_file: source.close()
        record.update(stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), finished_at=now())
        self.report['commands'].append(record); self.save()
        assert record['exit'] in expected and not timed_out and not overflow, record
        return stdout
    def compose(self, args, label, **options): return self.run(self.compose_args+args, label, **options)
    def json_command(self, args, label, **options): return json.loads(self.compose(args,label,**options).read_text())
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
    def wait(self, operation, predicate, label, timeout=240):
        deadline=time.monotonic()+timeout; last=None
        while time.monotonic()<deadline:
            try:
                last=operation()
                if predicate(last): return last
            except (OSError,KeyError,ValueError,TypeError) as e: last={'unavailable':type(e).__name__}
            time.sleep(.2)
        (self.out/(label+'-last.json')).write_text(json.dumps(last,indent=2))
        raise AssertionError('Deadline: '+label)
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
    def start(self,role): self.compose(['start',role],role+'-start')
    def stop(self,role): self.compose(['stop','-t','20',role],role+'-stop',timeout=60)
    def gate(self,name,evidence):
        assert name in ('G1','G2','G3','G4','G5') and name not in [x['id'] for x in self.report['gates']]
        self.report['gates'].append({'id':name,'status':'PASS','evidence':evidence});self.save();print(name+' PASS',flush=True)
    def cleanup(self):
        failures=[]
        try:
            self.compose(['down','--volumes','--remove-orphans','--timeout','20'],'owned-cleanup',timeout=180)
            for name,args in [('containers',['ps','-aq']),('volumes',['volume','ls','-q']),('networks',['network','ls','-q'])]:
                p=self.run(['docker',*args,'--filter','label=com.docker.compose.project='+self.project],'remaining-'+name)
                assert not p.read_text().strip(),name
        except BaseException as e: failures.append({'type':type(e).__name__,'message':str(e)})
        self.report['cleanup']={'status':'FAIL' if failures else 'PASS','errors':failures}
        if failures:self.report['status']='FAIL'
        self.report['finished_at']=now();self.save()
