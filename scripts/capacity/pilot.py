"""Reproducible bounded capacity observation; partial progress is never correctness PASS."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime

parser=argparse.ArgumentParser()
parser.add_argument('--count',type=int,default=8192)
parser.add_argument('--window',type=int,default=120)
args=parser.parse_args()
assert 257<=args.count<=2000000 and 30<=args.window<=7200
r=Runtime(args.count)
r.report.update(scope='Bounded capacity observation; counters are not independent reconciliation',mode='capacity-pilot',window_seconds=args.window)
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-'+r.head[:12]
(r.out/'verification.json').write_text('{"services":{}}')
r.save();print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.compose(['up','-d','--build'],'cold-up',timeout=600)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    before=r.json_command(['run','--rm','--no-deps','-T','inspect'],'initial-empty')
    assert before['source']['entities']=='0'
    start=time.monotonic()
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts',str(args.count)],'seed',timeout=3600)
    r.report['seed_and_activation_seconds']=time.monotonic()-start;r.save()
    samples=[];start=time.monotonic();deadline=start+args.window
    while time.monotonic()<deadline:
        value=r.status();p=value['dependencies']['pipeline']['data'];c=value['dependencies']['consumer']['data']
        sample={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_after_seed':time.monotonic()-start,'staged':p['staged'],'deliveries':p['deliveries'],'observations':p['observations'],'consumer':c,'backfill_phase':value['backfill']['phase']}
        samples.append(sample)
        (r.out/'capacity-samples.json').write_text(json.dumps(samples,indent=2)+'\n')
        if value['backfill']['phase']=='complete':break
        time.sleep(5)
    r.report['last_sample']=samples[-1]
    ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project='+r.project],text=True).split()
    r.run(['docker','stats','--no-stream','--format','{{json .}}',*ids],'container-sample')
    sql=r.out/'source-plan.sql'
    sql.write_text("BEGIN READ ONLY; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=(SELECT bootstrap_key FROM source.bootstrap_manifest) AND chunk_first=1; ROLLBACK;\n")
    r.compose(['exec','-T','source','psql','-X','-A','-t','-v','ON_ERROR_STOP=1','-U','source_admin','-d','source_m1'],'source-chunk-plan',input_file=sql)
    r.report['status']='MEASURED_PARTIAL' if samples[-1]['backfill_phase']!='complete' else 'MEASURED_CONVERGED_NOT_RECONCILED'
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)==r.dirty
    r.report['qualification']='No independent full-state reconciliation ran in this pilot. Progress counters do not establish absence of loss; functional acceptance is separate.'
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(1 if r.report['status']=='FAIL' else 0)
