"""Real target-lock contention reproduction and isolated candidate; not acceptance."""
import sys,json,time,signal,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime,now
r=Runtime(4096);r.env['KIT_IMAGE']='kill-it-twice-runtime:final-8d5a9f350a1e';r.report.update(mode='claim-contention-reproduction',scope='4096 real staged baselines; controlled target lock reproduces bounded claim cancellation; populated forward migration with restricted-role contention, cooldown and fencing checks; not million acceptance')
(r.out/'verification.json').write_text(json.dumps({'services':{'pipeline':{'command':['postgres','-c','log_min_error_statement=panic']}}}))
base=['exec','-T','pipeline','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','pipeline_admin','-d','pipeline_m2b']
def sql(q,label,timeout=60,expected=(0,)):
 p=r.out/(label+'.sql');p.write_text(q+'\n');return r.compose(base,label,input_file=p,timeout=timeout,expected=expected)
def interrupt(*a):raise KeyboardInterrupt('Diagnostic deadline')
signal.signal(signal.SIGTERM,interrupt)
print(r.out,flush=True)
holder=None
try:
 assert shutil.disk_usage(ROOT).free>80*1024**3
 r.compose(['up','-d','--no-build'],'start',timeout=300)
 r.compose(['stop','-t','20','es-worker','publisher','consumer','observer','control-api'],'hold-sinks',timeout=120)
 r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','4096'],'seed',timeout=300)
 r.wait(lambda:int(sql('SELECT count(*) FROM pipeline.events','staged-count').read_text()),lambda n:n==4096,'staged',timeout=300,interval=5)
 r.compose(['stop','-t','20','backfill','capture'],'quiesce',timeout=120)
 sql('ANALYZE;','analyze')
 fingerprint="SELECT md5(string_agg(to_jsonb(d)::text,',' ORDER BY event_id,kind)) FROM pipeline.delivery_intents d; SELECT count(*) FROM pipeline.es_attempts;"
 for table,order in [('events','event_id'),('backfill_runs','run_id'),('backfill_ranges','run_id,range_no'),('backfill_batches','batch_id'),('backfill_members','run_id,event_id'),('consumer_observations','event_id')]:
  fingerprint+="SELECT md5(string_agg(to_jsonb(d)::text,',' ORDER BY "+order+")) FROM pipeline."+table+" d;"
 before=sql(fingerprint,'before').read_text()
 metadata="SELECT jsonb_build_object('oid',oid,'owner',proowner,'acl',proacl,'config',proconfig,'security',prosecdef) FROM pg_proc WHERE oid='pipeline.es_claim(uuid,bigint,uuid,integer,integer)'::regprocedure;"
 catalog=sql(metadata,'metadata-before').read_text()
 claim="BEGIN; SET LOCAL ROLE pipeline_es;SET LOCAL statement_timeout=12000;SELECT count(*) FROM pipeline.es_identity() t CROSS JOIN LATERAL pipeline.es_claim(t.destination_id,t.generation,gen_random_uuid(),500,30000);ROLLBACK;"
 cases=[]
 for mode in ['original','candidate']:
  if mode=='candidate':
   sql('BEGIN;'+(ROOT/'migrations/pipeline/019-nonwaiting-es-claim.sql').read_text()+'COMMIT;','candidate-definition')
   assert sql(metadata,'metadata-after').read_text()==catalog
   assert sql(fingerprint,'after-migration').read_text()==before
  out=r.out/(mode+'-holder.stdout.log');err=r.out/(mode+'-holder.stderr.log')
  with out.open('wb') as h,err.open('wb') as eh:
   holder=subprocess.Popen(r.compose_args+base,cwd=ROOT,env=r.env,stdin=subprocess.PIPE,stdout=h,stderr=eh)
   holder.stdin.write(b"BEGIN; SET LOCAL statement_timeout=20000;SELECT destination_id FROM pipeline.es_target FOR UPDATE;\n\\echo LOCK_HELD\nSELECT pg_sleep(14);ROLLBACK;\n");holder.stdin.close()
   r.wait(lambda:out.read_text(),lambda s:'LOCK_HELD' in s,'holder-ready',timeout=10,interval=.05)
   t=time.monotonic();result=sql(claim,mode+'-contended-claim',timeout=20,expected=(0,3));elapsed=time.monotonic()-t;cmd=r.report['commands'][-1];error=(r.out/cmd['stderr']).read_text()
   if mode=='original':assert cmd['exit']==3 and '57014' in error and elapsed>=11
   else:assert cmd['exit']==0 and result.read_text().strip()=='0' and elapsed<2
   holder.wait(timeout=20);assert holder.returncode==0;holder=None
   assert sql(fingerprint,mode+'-after-contention').read_text()==before
   cases.append({'mode':mode,'claim_exit':cmd['exit'],'claim_seconds':elapsed,'result':result.read_text().strip(),'sqlstate57014':'57014' in error,'canonical_intents_and_attempts_unchanged':True})
  if mode=='candidate':
   result=sql(claim,'candidate-uncontended-500',timeout=20);assert result.read_text().strip()=='500'
   assert sql(fingerprint,'after-uncontended-rollback').read_text()==before
   result=sql("BEGIN;SET LOCAL ROLE pipeline_es;SELECT * FROM pipeline.es_claim(gen_random_uuid(),1,gen_random_uuid(),500,30000);ROLLBACK;",'missing-target',expected=(3,));assert 'P0002' in (r.out/r.report['commands'][-1]['stderr']).read_text()
   restricted="SET LOCAL ROLE pipeline_es;SELECT count(*) FROM pipeline.es_identity() t CROSS JOIN LATERAL pipeline.es_claim(t.destination_id,t.generation,gen_random_uuid(),500,30000);"
   value=sql("BEGIN;UPDATE pipeline.es_target SET mode='blocked',reason='test';"+restricted+"ROLLBACK;",'blocked-target').read_text().strip();assert value=='0'
   value=sql("BEGIN;UPDATE pipeline.es_target SET mode='cooldown',next_probe_at=clock_timestamp()-interval '1 second',probe_until=NULL;"+restricted+restricted+"ROLLBACK;",'single-cooldown-probe').read_text().strip().splitlines();assert value==['1','0'],value
   sql("BEGIN;SET LOCAL ROLE pipeline_es;SELECT count(*) FROM pipeline.es_identity() t CROSS JOIN LATERAL pipeline.es_claim(t.destination_id,t.generation+1,gen_random_uuid(),500,30000);ROLLBACK;",'wrong-generation',expected=(3,));assert 'P5001' in (r.out/r.report['commands'][-1]['stderr']).read_text()
   sql("BEGIN;SET LOCAL ROLE pipeline_stager;SELECT * FROM pipeline.es_claim(gen_random_uuid(),1,gen_random_uuid(),500,30000);ROLLBACK;",'wrong-role',expected=(3,));assert '42501' in (r.out/r.report['commands'][-1]['stderr']).read_text()
   assert sql(fingerprint,'final-preservation').read_text()==before
 r.report.update(status='PASS',cases=cases,uncontended_claim_500='PASS',missing_target_still_errors='PASS',metadata_preserved=True,blocked_target='PASS',single_cooldown_probe='PASS',generation_fence='PASS',wrong_role_denied='PASS');r.save();print(json.dumps(cases),flush=True)
except BaseException as e:r.report.update(status='FAIL',error={'type':type(e).__name__,'message':str(e)});r.save()
finally:
 if holder is not None:
  try:holder.wait(timeout=25)
  except subprocess.TimeoutExpired:holder.terminate();holder.wait(timeout=5)
 r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
