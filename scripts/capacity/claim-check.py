"""Populated selector-only upgrade with actual rollback claims and independent equality."""
import json,os,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime,sha
r=Runtime(4096);r.report.update(mode='due-selector-upgrade',scope='Existing partial index and equivalent due-set proof, not a capacity result',base_code='ff7e115d6d39b2ad04da58e54b001fb6525c6c33')
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-ff7e115d6d39'
(r.out/'verification.json').write_text('{"services":{}}')
def sql(text,label):
 file=r.out/(label+'.sql');file.write_text(text+'\n')
 return r.compose(['exec','-T','pipeline','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-U','pipeline_admin','-d','pipeline_m2b'],label,input_file=file)
def snapshot(label):
 return sql("BEGIN READ ONLY;SELECT jsonb_build_object('kind','intent','row',to_jsonb(d)) FROM pipeline.delivery_intents d ORDER BY event_id COLLATE \"C\",kind;SELECT jsonb_build_object('kind','event','row',to_jsonb(e)) FROM pipeline.events e ORDER BY event_id COLLATE \"C\";ROLLBACK;",label)
def claim(kind,label):
 target='es_target' if kind=='es' else 'rabbit_target'
 role='pipeline_es' if kind=='es' else 'pipeline_rabbit'
 return sql(f"BEGIN;SET LOCAL ROLE {role};SELECT destination_id,generation FROM pipeline.{kind}_identity() \\gset\nSELECT json_agg(jsonb_build_object('event_id',event_id,'generation',generation) ORDER BY ordinality) FROM pipeline.{kind}_claim(:'destination_id'::uuid,:'generation'::bigint,'11111111-1111-4111-8111-111111111111'::uuid,128,30000) WITH ORDINALITY;ROLLBACK;",label)
def plan(kind, explicit, label):
 sink='elasticsearch' if kind=='es' else 'rabbitmq'
 predicate=" AND i.state IN ('pending','leased','retry_wait')" if explicit else ''
 query=f"SELECT i.event_id FROM pipeline.delivery_intents i WHERE i.destination_id=:'target'::uuid AND i.kind='{sink}'{predicate} AND ((i.state IN ('pending','retry_wait') AND i.next_retry_at<=:'at'::timestamptz) OR (i.state='leased' AND i.lease_until<=:'at'::timestamptz)) ORDER BY i.next_retry_at,i.event_id LIMIT 128"
 path=sql(f"BEGIN READ ONLY;SELECT destination_id AS target FROM pipeline.{kind}_target \\gset\nSELECT clock_timestamp() AS at \\gset\nEXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {query};ROLLBACK;",label)
 value=json.loads(path.read_text());assert value[0]['Plan']['Actual Rows']==128
 if explicit:assert ('es_due' if kind=='es' else 'rabbit_due') in json.dumps(value)
 return {'file':path.name,'sha256':sha(path),'execution_ms':value[0]['Execution Time'],'explicit_predicate':explicit}

print(str(r.out),flush=True)
try:
 r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
 r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
 address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
 r.stop('es-worker');r.stop('publisher')
 r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','4096'],'seed',timeout=300)
 r.wait(r.status,lambda x:isinstance(x.get('backfill'),dict) and x['backfill']['phase']=='draining','staged-no-sinks',timeout=300)
 r.compose(['stop','-t','20','capture','backfill','consumer','observer'],'quiesce',timeout=180)
 before=snapshot('before')
 catalog_sql="SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT p.oid,proowner,proacl,proconfig,prosecdef,provolatile,pg_get_function_result(p.oid) result FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pipeline' AND proname IN ('es_claim','rabbit_claim'))x;"
 catalog=sql(catalog_sql,'metadata-before')
 originals={kind:claim(kind,kind+'-old-rollback') for kind in ('es','rabbit')}
 old_plans={kind:plan(kind,False,kind+'-old-selection-plan') for kind in ('es','rabbit')}
 assert snapshot('after-old-rollback').read_bytes()==before.read_bytes()
 definition=ROOT/'migrations/pipeline/015-explicit-due-predicates.sql'
 sql('BEGIN;\n'+definition.read_text()+'\nCOMMIT;','forward-migration')
 assert snapshot('after-migration').read_bytes()==before.read_bytes()
 assert sql(catalog_sql,'metadata-after').read_bytes()==catalog.read_bytes()
 claims=[]
 for kind in ('es','rabbit'):
  revised=claim(kind,kind+'-new-rollback')
  assert revised.read_bytes()==originals[kind].read_bytes(),kind+' exact ordered membership'
  rows=json.loads(revised.read_text());assert len(rows)==128 and all(x['generation']=='1' for x in rows)
  assert snapshot(kind+'-after-new-rollback').read_bytes()==before.read_bytes()
  claims.append({'kind':kind,'selected':128,'ordered_membership_sha256':sha(revised),'before_plan':old_plans[kind],'after_plan':plan(kind,True,kind+'-explicit-selection-plan')})
 truth=sql("WITH cases AS (SELECT state,due,expired FROM (VALUES('pending'),('retry_wait'),('leased'),('satisfied'),('dead_letter'),(NULL::text))s(state) CROSS JOIN (VALUES(true),(false),(NULL::boolean))d(due) CROSS JOIN (VALUES(true),(false),(NULL::boolean))e(expired)) SELECT jsonb_build_object('cases',count(*),'differences',count(*) FILTER(WHERE (((state IN ('pending','retry_wait') AND due) OR (state='leased' AND expired)) IS TRUE) IS DISTINCT FROM ((state IN ('pending','leased','retry_wait') AND ((state IN ('pending','retry_wait') AND due) OR (state='leased' AND expired))) IS TRUE))) FROM cases;",'eligibility-truth-table')
 table=json.loads(truth.read_text());assert table=={'cases':54,'differences':0}
 for role,fn in [('pipeline_es','es_claim'),('pipeline_rabbit','rabbit_claim')]:
  denied=r.compose(['exec','-T','pipeline','psql','-X','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','pipeline_admin','-d','pipeline_m2b','-c',f'BEGIN;SET LOCAL ROLE {role};ALTER FUNCTION pipeline.{fn}(uuid,bigint,uuid,integer,integer) COST 1;ROLLBACK;'],role+'-denied-ddl',expected=(1,))
  assert '42501' in denied.with_name(denied.name.replace('stdout','stderr')).read_text()
 assert snapshot('after-denials').read_bytes()==before.read_bytes()
 r.compose(['start','capture','backfill','es-worker','publisher','consumer','observer'],'resume-real-workers')
 r.wait(r.status,lambda s:isinstance(s.get('backfill'),dict) and s['backfill']['phase']=='complete','complete',timeout=300)
 r.compose(['stop','-t','20','capture','backfill','es-worker','publisher','consumer','observer'],'quiesce-export',timeout=180)
 exported=r.out/'state';exported.mkdir()
 for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
  path=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name,timeout=300)
  os.link(path,exported/(name+'.jsonl'))
 (exported/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
 result=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count','4096','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=300)
 reconciliation=json.loads(result.read_text());assert reconciliation['status']=='PASS'
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
 r.report.update(status='PASS',claims=claims,truth_table=table,unchanged_data_sha256=sha(before),unchanged_catalog_sha256=sha(catalog),reconciliation=reconciliation)
except BaseException as error:
 r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
