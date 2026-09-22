"""Real populated index-only upgrades: identical data/catalogs, export rows and plans."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime, sha
r=Runtime(4096)
r.report.update(scope='Three index-only populated upgrades and exact export preservation; not a scale result',mode='navigation-upgrade',base_code='493af791e8192df5220c4746f76dd22919e28074')
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-493af791e819'
(r.out/'verification.json').write_text('{"services":{}}')
cases=[
 ('source','source_m1','source_admin','source_owner','source','011-baseline-navigation.sql','baseline_entity_navigation','baselines',"SELECT entity_id,source_epoch,entity_version,payload::text FROM source.baseline_revisions WHERE entity_id>2048 ORDER BY entity_id LIMIT 32",['source_backfill','source_reader','source_command']),
 ('pipeline','pipeline_m2b','pipeline_admin','pipeline_owner','pipeline','pipeline/013-event-navigation.sql','events_ordinal_navigation','pipeline',"SELECT e.event_id,e.content_sha256,d.state FROM pipeline.events e LEFT JOIN pipeline.delivery_intents d ON d.event_id=e.event_id AND d.kind='elasticsearch' WHERE e.event_id COLLATE \"C\">(SELECT source_epoch::text||':2048:1' FROM pipeline.source_binding) COLLATE \"C\" ORDER BY e.event_id COLLATE \"C\" LIMIT 32",['pipeline_capture','pipeline_es','pipeline_rabbit']),
 ('pipeline','consumer_m4','pipeline_admin','consumer_owner','consumer','consumer/005-event-navigation.sql','processed_events_ordinal_navigation','consumer',"SELECT e.event_id,e.content_sha256,p.event_id AS projection FROM consumer.processed_events e LEFT JOIN consumer.entity_projection p ON p.event_id=e.event_id WHERE e.event_id COLLATE \"C\">(SELECT source_epoch::text||':2048:1' FROM consumer.identity) COLLATE \"C\" ORDER BY e.event_id COLLATE \"C\" LIMIT 32",['consumer_runtime','consumer_receipt_reader'])]
def sql(case, text, label, expected=(0,)):
    service,db,admin=case[:3];file=r.out/(label+'.sql');file.write_text(text+'\n')
    return r.compose(['exec','-T',service,'psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U',admin,'-d',db],label,input_file=file,expected=expected)
def metadata(case):
    schema=case[4]
    return f"SELECT jsonb_build_object('functions',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT p.oid,proname,proowner,proacl,proconfig,prosecdef,provolatile,pg_get_functiondef(p.oid) AS definition FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='{schema}')x),'tables',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT c.oid,relname,relowner,relacl FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema}' AND c.relkind='r')x),'constraints',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT c.oid,conname,convalidated,conenforced,pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='{schema}')x),'triggers',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT t.oid,tgenabled,pg_get_triggerdef(t.oid) AS definition FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema}')x));"
def snapshot(case,label):
    schema=case[4]
    tables=json.loads(sql(case,f"SELECT json_agg(relname ORDER BY relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema}' AND relkind='r';",label+'-tables').read_text())
    assert tables and all(name.replace('_','').isalnum() for name in tables)
    statements=['BEGIN READ ONLY;']
    for name in tables: statements.append(f"SELECT jsonb_build_object('table','{schema}.{name}','row',to_jsonb(v)) FROM {schema}.{name} v ORDER BY to_jsonb(v)::text COLLATE \"C\";")
    statements.append('ROLLBACK;')
    return sql(case,'\n'.join(statements),label)
def export(name,label):
    return r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],label,timeout=300)
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    image=json.loads(r.run(['docker','image','inspect',r.env['KIT_IMAGE']],'old-image').read_text())[0];r.report['base_image_id']=image['Id']
    r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    assert r.inspect()['source']['entities']=='0'
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','4096'],'seed',timeout=300)
    r.wait(r.status,lambda s:isinstance(s['backfill'],dict) and s['backfill']['phase']=='complete','old-complete',timeout=300)
    r.compose(['stop','-t','20','capture','backfill','publisher','consumer','es-worker','observer'],'quiesce',timeout=180)
    results=[]
    for case in cases:
        service,db,admin,owner,schema,migration,index,collection,query,roles=case
        existing=sql(case,f"SELECT count(*) FROM pg_indexes WHERE schemaname='{schema}' AND indexname='{index}';",schema+'-index-absent');assert existing.read_text().strip()=='0'
        before=snapshot(case,schema+'-before');catalog=sql(case,metadata(case),schema+'-metadata-before');original=export(collection,schema+'-export-before')
        plan_before=sql(case,'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query+';',schema+'-plan-before');rows_before=sql(case,query+';',schema+'-rows-before')
        file=ROOT/'migrations'/migration
        sql(case,'BEGIN;\n'+file.read_text()+'\nCOMMIT;',schema+'-migration')
        after=snapshot(case,schema+'-after');new_catalog=sql(case,metadata(case),schema+'-metadata-after');new_export=export(collection,schema+'-export-after')
        assert before.read_bytes()==after.read_bytes(),schema+' row preservation'
        assert catalog.read_bytes()==new_catalog.read_bytes(),schema+' catalog preservation'
        assert original.read_bytes()==new_export.read_bytes(),schema+' complete export preservation'
        rows_after=sql(case,query+';',schema+'-rows-after');assert rows_before.read_bytes()==rows_after.read_bytes()
        plan_after=sql(case,'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query+';',schema+'-plan-after')
        parsed=json.loads(plan_after.read_text());assert index in json.dumps(parsed),schema+' actual index plan'
        definition=sql(case,f"SELECT jsonb_build_object('definition',pg_get_indexdef(c.oid),'owner',pg_get_userbyid(c.relowner),'valid',i.indisvalid,'ready',i.indisready,'unique',i.indisunique) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema}' AND c.relname='{index}';",schema+'-definition')
        observed=json.loads(definition.read_text());assert observed['valid'] and observed['ready'] and not observed['unique'] and observed['owner']==owner
        for role in roles:
            denied=sql(case,f"BEGIN;SET LOCAL ROLE {role};DROP INDEX {schema}.{index};COMMIT;",schema+'-denied-'+role,(3,))
            assert '42501' in denied.with_name(denied.name.replace('stdout','stderr')).read_text()
        assert snapshot(case,schema+'-after-denials').read_bytes()==before.read_bytes()
        results.append({'schema':schema,'migration':migration,'migration_sha256':sha(file),'rows_sha256':sha(before),'catalog_sha256':sha(catalog),'export_sha256':sha(original),'before_execution_ms':json.loads(plan_before.read_text())[0]['Execution Time'],'after_execution_ms':parsed[0]['Execution Time'],'index':observed,'denied_roles':roles})
    state=r.out/'state';state.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        shutil.copyfile(export(name,'final-export-'+name),state/(name+'.jsonl'))
    (state/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
    proof=r.run([sys.executable,'-B','tests/final/reconcile.py',str(state),'--count','4096','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=300)
    reconciliation=json.loads(proof.read_text());assert reconciliation['status']=='PASS'
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report.update(status='PASS',upgrades=results,reconciliation=reconciliation)
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
