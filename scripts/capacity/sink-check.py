"""Real receiver settlement rollback/commit and independent whole-fixture content reconciliation."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime
r=Runtime(257)
r.report.update(scope='Bounded sink transaction regression and independent 257-baseline reconciliation',mode='sink-batch-check')
(r.out/'verification.json').write_text('{"services":{}}')
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.compose(['up','-d','--build'],'new-runtime',timeout=600)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    r.stop('es-worker');r.stop('publisher')
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','257'],'seed',timeout=300)
    before=r.wait(r.status,lambda s:s['dependencies']['pipeline']['data']['staged']=='257','staged-without-sinks',timeout=180)
    assert before['backfill']['phase']=='draining'
    pending_counts=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/count-proof.ts'],'pending-count-equivalence')
    assert pending_counts['status']=='PASS'
    proof=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/sink-proof.ts'],'real-sink-proof')
    assert proof['status']=='PASS' and proof['elasticsearch']['actual_applied']==proof['rabbitmq']['actual_confirmed']==32
    r.start('es-worker');r.start('publisher');r.wait(r.status,lambda s:s['backfill']['phase']=='complete','sink-drain',timeout=180)
    r.compose(['stop','-t','20','capture','backfill','publisher','consumer','es-worker','observer'],'quiesce',timeout=180)
    settled_counts=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/count-proof.ts'],'settled-count-equivalence')
    assert settled_counts['status']=='PASS'
    exported=r.out/'state';exported.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name)
        shutil.copyfile(log,exported/(name+'.jsonl'))
    (exported/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
    oracle=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count','257','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle')
    reconciliation=json.loads(oracle.read_text());assert reconciliation['status']=='PASS'
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report.update(status='PASS',transaction_proof=proof,count_proofs={'pending':pending_counts,'settled':settled_counts},reconciliation=reconciliation)
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
