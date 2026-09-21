"""Real receipt rollback/commit and independent whole-fixture content reconciliation."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime
r=Runtime(257)
r.report.update(scope='Bounded receipt transaction regression and independent 257-baseline reconciliation',mode='receipt-batch-check')
(r.out/'verification.json').write_text('{"services":{}}')
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.compose(['up','-d','--build'],'new-runtime',timeout=600)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    r.stop('observer')
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','257'],'seed',timeout=300)
    before=r.wait(r.status,lambda s:s['dependencies']['consumer']['data']['processed']=='257','consumer-ready-without-observer',timeout=240)
    assert before['dependencies']['pipeline']['data']['observations']==[{'count':'257','state':'pending'}]
    proof=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/receipt-proof.ts'],'real-receipt-proof')
    assert proof['status']=='PASS' and len(proof['proof']['processed'])==32
    r.start('observer');r.wait(r.status,lambda s:s['backfill']['phase']=='complete','receipt-drain',timeout=180)
    r.compose(['stop','-t','20','capture','backfill','publisher','consumer','es-worker','observer'],'quiesce',timeout=180)
    exported=r.out/'state';exported.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name)
        shutil.copyfile(log,exported/(name+'.jsonl'))
    (exported/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
    oracle=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count','257','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle')
    reconciliation=json.loads(oracle.read_text());assert reconciliation['status']=='PASS'
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report.update(status='PASS',transaction_proof=proof,reconciliation=reconciliation)
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
