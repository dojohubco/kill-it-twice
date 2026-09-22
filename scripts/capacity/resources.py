"""Bounded read-only sampling of containers owned by one capacity invocation."""
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import threading


def memory_fields(text):
    values={}
    for line in text.splitlines():
        if ':' not in line:continue
        key,value=line.split(':',1)
        if key in ('VmRSS','VmHWM','MemAvailable','MemTotal','SwapFree','SwapTotal'):
            fields=value.split()
            if len(fields)==2 and fields[1]=='kB' and fields[0].isdigit():values[key]=int(fields[0])*1024
    return values

class ResourceSampler:
    def __init__(self,project,directory,interval=5):
        assert project.startswith('kit-final-') and 1<=interval<=30
        self.project=project;self.directory=Path(directory);self.interval=interval
        self.stop_event=threading.Event();self.thread=None;self.errors=[];self.count=0;self.peak={}
    def command(self,args):
        result=subprocess.run(args,capture_output=True,timeout=15)
        if result.returncode:raise RuntimeError('Owned container inspection failed')
        if len(result.stdout)>2*1024*1024:raise RuntimeError('Container inspection response bound')
        return result.stdout
    def sample(self):
        ids=self.command(['docker','ps','-q','--filter','label=com.docker.compose.project='+self.project]).decode().split()
        assert len(ids)<=32
        containers=json.loads(self.command(['docker','inspect',*ids])) if ids else []
        rows=[]
        for container in containers:
            labels=container['Config']['Labels'];assert labels['com.docker.compose.project']==self.project
            identity=container['Id'];pid=container['State']['Pid'];role=labels['com.docker.compose.service']
            row={'container_id':identity,'role':role,'pid':pid,'limit_bytes':container['HostConfig']['Memory'],'cpu_usage_usec':None,'memory_current_bytes':None,'memory_peak_bytes':None,'process_rss_bytes':None,'process_rss_high_water_bytes':None}
            try:
                status=memory_fields(Path(f'/proc/{pid}/status').read_text());row['process_rss_bytes']=status.get('VmRSS');row['process_rss_high_water_bytes']=status.get('VmHWM')
                lines=Path(f'/proc/{pid}/cgroup').read_text().splitlines();groups=[line[3:] for line in lines if line.startswith('0::')]
                if groups:
                    base=Path('/sys/fs/cgroup').resolve();group=(base/groups[0].lstrip('/')).resolve();assert group.is_relative_to(base)
                    for field,filename in [('memory_current_bytes','memory.current'),('memory_peak_bytes','memory.peak')]:
                        text=(group/filename).read_text().strip();row[field]=int(text) if text.isdigit() else None
                    cpu=dict(line.split() for line in (group/'cpu.stat').read_text().splitlines());row['cpu_usage_usec']=int(cpu['usage_usec'])
            except FileNotFoundError:row['process_exited_during_observation']=True
            rows.append(row)
            prior=self.peak.setdefault(identity,{'role':role,'limit_bytes':row['limit_bytes']})
            for field in ['cpu_usage_usec','memory_peak_bytes','process_rss_high_water_bytes']:
                if row[field] is not None:prior[field]=max(prior.get(field,0),row[field])
        entry={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'host_memory':memory_fields(Path('/proc/meminfo').read_text()),'disk_free_bytes':shutil.disk_usage(self.directory).free,'containers':rows}
        path=self.directory/'resources.jsonl';line=json.dumps(entry,separators=(',',':'))+'\n'
        assert (path.stat().st_size if path.exists() else 0)+len(line.encode())<=64*1024*1024
        with path.open('a') as stream:stream.write(line)
        self.count+=1
    def _loop(self):
        while not self.stop_event.wait(self.interval):
            try:self.sample()
            except BaseException as error:
                self.errors.append({'type':type(error).__name__,'phase':'read_only_sample'});return
    def start(self):
        assert self.thread is None
        self.sample();self.thread=threading.Thread(target=self._loop,name='owned-resource-sampler',daemon=False);self.thread.start()
    def finish(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(35)
            if self.thread.is_alive():raise RuntimeError('Resource sampler did not stop')
        result={'status':'FAIL' if self.errors else 'OBSERVED','samples':self.count,'interval_seconds':self.interval,'peak_by_container':self.peak,'errors':self.errors,'scope':'Read-only observed process and cgroup peaks; process exit between samples can omit a final high-water value, and host memory includes unrelated workloads'}
        (self.directory/'resource-summary.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
