"""Old-image populated page-bound upgrade, actual rollback and final reconciliation."""
import json,os,shutil,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime
r=Runtime(257)
r.report.update(mode='page-bound-proof',scope='Actual 16-to-64 opt-in page upgrade and independent source-derived content proof',old_code='ab0b8e4b7b8755b50ed9c96b85409c87bd935d89')
new_image='kill-it-twice-runtime:page-'+r.head[:12]
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-ab0b8e4b7b87'
(r.out/'verification.json').write_text(json.dumps({'services':{'inspect':{'image':new_image},'backfill':{'image':new_image,'environment':{'BACKFILL_PAGE_RECORDS':'64'}}}}))
requests=[{'command_id':str(uuid.uuid4()),'operation':'create','entity_id':None,'payload_json':json.dumps({'name':'Byte boundary '+str(i),'country':'GE','padding':'x'*50000},separators=(',',':'))} for i in range(6)]
journal=r.out/'journal.jsonl';journal.write_text(''.join(json.dumps(q)+'\n' for q in requests))
request_file=r.out/'declared-commands.json';request_file.write_text(json.dumps(requests))
(r.out/'rejections.json').write_text('[]')
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.run(['docker','image','inspect',r.env['KIT_IMAGE']],'old-image')
    r.compose(['build','inspect'],'build-proof',timeout=300)
    r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    workers=['capture','backfill','es-worker','publisher','consumer','observer']
    r.compose(['stop','-t','20',*workers],'pause-for-upgrade',timeout=180)
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','257'],'old-seed',timeout=300)
    result=r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/capacity/page-proof.ts'],'actual-page-proof',input_file=request_file,timeout=180)
    assert result['status']=='PASS' and result['page_records']==64 and result['old_page_records']==16
    assert result['controlled_rollback'] and result['replay_same_content']
    r.report['page_proof']=result;r.save()
    r.compose(['start',*workers],'resume-real-workers')
    r.wait(r.status,lambda s:isinstance(s.get('backfill'),dict) and s['backfill']['phase']=='complete' and s['dependencies']['consumer']['data']['processed']=='263','completed-source-derived-fixture',timeout=240)
    r.compose(['stop','-t','20',*workers],'quiesce-oracle',timeout=180)
    exported=r.out/'state';exported.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name)
        os.link(log,exported/(name+'.jsonl'))
    (exported/'failures.jsonl').write_text('')
    proof=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count','257','--journal',str(journal),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=180)
    r.report['reconciliation']=json.loads(proof.read_text());assert r.report['reconciliation']['status']=='PASS' and r.report['reconciliation']['mutation_effects']==6
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report['status']='PASS'
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
