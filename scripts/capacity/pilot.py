"""Reproducible bounded capacity observation; partial progress is never correctness PASS."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime
from observations import sample, complete, unavailable
from resources import ResourceSampler
from replicas import replica_counts, compose_scales

parser=argparse.ArgumentParser()
parser.add_argument('--count',type=int,default=8192)
parser.add_argument('--window',type=int,default=120)
parser.add_argument('--reconcile',action='store_true',help='Require convergence within the window and independently compare frozen source/receiver state')
parser.add_argument('--scanners',type=int,default=1,help='One to four actual isolated backfill worker processes')
parser.add_argument('--page-records',type=int,default=16,help='Explicit 1..64 count ceiling; original 256 KiB wire bound remains')
parser.add_argument('--sink-workers',type=int,default=1,help='One or two independent ES/publisher/consumer/observer processes per role, only in this test project')
args=parser.parse_args()
replicas=replica_counts(args.scanners,args.sink_workers)
assert 257<=args.count<=2000000 and 30<=args.window<=7200 and 1<=args.scanners<=4 and 1<=args.page_records<=64
r=Runtime(args.count)
r.report.update(scope='Bounded capacity observation; counters are not independent reconciliation',mode='capacity-pilot',window_seconds=args.window,scanner_processes=args.scanners,page_records=args.page_records,sink_processes_per_role=args.sink_workers,requested_replica_counts=replicas)
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-'+r.head[:12]
(r.out/'verification.json').write_text(json.dumps({'services':{'backfill':{'environment':{'BACKFILL_PAGE_RECORDS':str(args.page_records)}}}}))
r.save();print(str(r.out),flush=True)
resources=None
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.compose(['up','-d','--build',*compose_scales(args.scanners,args.sink_workers)],'cold-up',timeout=600)
    resources=ResourceSampler(r.project,r.out);resources.start()
    pipeline_id=r.worker_id('pipeline')
    pipeline_config=json.loads(r.run(['docker','inspect',pipeline_id],'pipeline-resource-budget').read_text())[0]['HostConfig']
    assert pipeline_config['ShmSize']==268435456 and pipeline_config['Memory']==1073741824
    r.report['pipeline_resource_budget']={'shared_memory_bytes':pipeline_config['ShmSize'],'total_memory_limit_bytes':pipeline_config['Memory']}
    actual=r.compose(['ps','-q','backfill'],'scanner-identities').read_text().splitlines();assert len(actual)==args.scanners
    r.report['scanner_container_ids']=actual
    worker_ids={}
    for role,expected in replicas.items():
        identities=r.compose(['ps','-q',role],'replica-ids-'+role).read_text().splitlines();assert len(identities)==expected
        configurations=json.loads(r.run(['docker','inspect',*identities],'replica-budgets-'+role).read_text())
        assert all(c['Config']['Labels']['com.docker.compose.project']==r.project and c['Config']['Labels']['com.docker.compose.service']==role and c['HostConfig']['Memory']==268435456 for c in configurations)
        worker_ids[role]=identities
    r.report['worker_container_ids']=worker_ids
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    before=r.json_command(['run','--rm','--no-deps','-T','inspect'],'initial-empty')
    assert before['source']['entities']=='0'
    r.report['storage_initial']=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/storage.ts'],'initial-storage')
    start=time.monotonic()
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts',str(args.count)],'seed',timeout=3600)
    r.report['seed_and_activation_seconds']=time.monotonic()-start;r.save()
    samples=[];start=time.monotonic();deadline=start+args.window
    while time.monotonic()<deadline:
        assert not resources.errors,'Resource measurement failed'
        assert shutil.disk_usage(ROOT).free>=10*1024**3,'Stop before exhausting local storage reserve'
        try:
            value=r.status()
            raw=json.dumps({'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'data':value},separators=(',',':'))+'\n'
            path=r.out/'status-observations.jsonl'
            assert (path.stat().st_size if path.exists() else 0)+len(raw.encode())<=64*1024*1024,'Bounded capacity observation log'
            with path.open('a') as output:output.write(raw)
            current=sample(value,time.monotonic()-(deadline-args.window))
        except OSError as error:current=unavailable(time.monotonic()-(deadline-args.window),error)
        samples.append(current)
        (r.out/'capacity-samples.json').write_text(json.dumps(samples,indent=2)+'\n')
        if complete(current,args.count):break
        time.sleep(5)
    r.report['last_sample']=samples[-1]
    r.compose(['logs','--no-color','--tail','80','source','pipeline','capture','backfill','es-worker','publisher','consumer','observer'],'bounded-worker-diagnostics',maximum=16*1024*1024)
    ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project='+r.project],text=True).split()
    r.run(['docker','stats','--no-stream','--format','{{json .}}',*ids],'container-sample')
    sql=r.out/'source-plan.sql'
    sql.write_text("BEGIN READ ONLY; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=(SELECT bootstrap_key FROM source.bootstrap_manifest) AND chunk_first=1; ROLLBACK;\n")
    r.compose(['exec','-T','source','psql','-X','-A','-t','-v','ON_ERROR_STOP=1','-U','source_admin','-d','source_m1'],'source-chunk-plan',input_file=sql)
    r.report['storage_observed']=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/storage.ts'],'observed-storage')
    r.report['status']='MEASURED_PARTIAL' if samples[-1]['backfill_phase']!='complete' else 'MEASURED_CONVERGED_NOT_RECONCILED'
    if args.reconcile and samples[-1]['backfill_phase']=='complete':
        r.compose(['stop','-t','20','capture','backfill','publisher','consumer','es-worker','observer'],'quiesce-oracle',timeout=180)
        exported=r.out/'state';exported.mkdir()
        for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
            log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name,timeout=3600,maximum=max(64*1024*1024,args.count*8192))
            os.link(log,exported/(name+'.jsonl'))  # Two evidence paths, one immutable local export inode.
        failures=r.inspect('failures')
        (exported/'failures.jsonl').write_text(''.join(json.dumps(v,separators=(',',':'))+'\n' for v in failures))
        (r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
        proof=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count',str(args.count),'--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=7200)
        r.report['reconciliation']=json.loads(proof.read_text());assert r.report['reconciliation']['status']=='PASS'
        r.report['storage_reconciled']=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/storage.ts'],'post-refresh-storage')
        assert r.report['storage_reconciled']['elasticsearch']['lucene_documents']==str(args.count)
        r.report['status']='RECONCILED_COMPLETE'

    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)==r.dirty
    r.report['qualification']='Actual scale/count and reconciliation are explicit. This pilot does not rerun fault gates; a partial measurement is not correctness acceptance.'
    if args.reconcile and r.report['status']!='RECONCILED_COMPLETE':raise AssertionError('Required capacity convergence/reconciliation not completed in the declared window')
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:
    if resources is not None:
        try:
            r.report['resources']=resources.finish()
            if r.report['resources']['status']=='FAIL':r.report['status']='FAIL'
        except BaseException as measurement_error:
            r.report['status']='FAIL';r.report['measurement_error']={'type':type(measurement_error).__name__,'message':str(measurement_error)}
    r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(1 if r.report['status']=='FAIL' else 0)
