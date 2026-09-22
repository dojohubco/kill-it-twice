"""Populated index-only consumer upgrade with unchanged full exports and evidence."""
import json,os,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime,sha
r=Runtime(4096)
r.report.update(mode='consumer-navigation-upgrade',scope='Actual populated index preservation and query plans; no scale throughput claim',base_code='9c63b8bd95bb001307a6bbc78bacee2112a190e3')
r.env['KIT_IMAGE']='kill-it-twice-runtime:capacity-9c63b8bd95bb'
(r.out/'verification.json').write_text('{"services":{}}')
def sql(text,label,expected=(0,)):
    file=r.out/(label+'.sql');file.write_text(text+'\n')
    return r.compose(['exec','-T','pipeline','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','pipeline_admin','-d','consumer_m4'],label,input_file=file,expected=expected)
def snapshot(label):
    tables=json.loads(sql("SELECT json_agg(relname ORDER BY relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='consumer' AND relkind='r';",label+'-tables').read_text())
    assert tables and all(name.replace('_','').isalnum() for name in tables)
    statements=['BEGIN READ ONLY;']
    for name in tables:statements.append(f"SELECT jsonb_build_object('table','{name}','row',to_jsonb(v)) FROM consumer.{name} v ORDER BY to_jsonb(v)::text COLLATE \"C\";")
    return sql('\n'.join(statements+['ROLLBACK;']),label)
def metadata(label):
    return sql("""SELECT jsonb_build_object(
'functions',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT p.oid,proname,proowner,proacl,proconfig,prosecdef,provolatile,pg_get_functiondef(p.oid) definition FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='consumer')x),
'constraints',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT c.oid,conname,convalidated,conenforced,pg_get_constraintdef(c.oid) definition FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='consumer')x),
'tables',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT c.oid,relname,relowner,relacl FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='consumer' AND relkind='r')x),
'triggers',(SELECT jsonb_agg(to_jsonb(x) ORDER BY x.oid) FROM (SELECT t.oid,tgenabled,pg_get_triggerdef(t.oid) definition FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='consumer')x));""",label)
def export(name,label):
    return r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],label,timeout=300)
queries={
 'totals':('entity_totals_navigation','SELECT entity_id::text AS cursor,source_epoch,entity_id::text,units::text FROM consumer.entity_totals WHERE entity_id>2048::bigint ORDER BY consumer.entity_totals.entity_id LIMIT 32'),
 'projection':('entity_projection_navigation','SELECT entity_id::text AS cursor,source_epoch,entity_id::text,entity_version::text,event_id FROM consumer.entity_projection WHERE entity_id>2048::bigint ORDER BY consumer.entity_projection.entity_id LIMIT 32')}
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.run(['docker','image','inspect',r.env['KIT_IMAGE']],'base-image')
    r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    assert r.inspect()['source']['entities']=='0'
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','4096'],'seed',timeout=300)
    r.wait(r.status,lambda s:isinstance(s.get('backfill'),dict) and s['backfill']['phase']=='complete','before-complete',timeout=300)
    r.compose(['stop','-t','20','capture','backfill','es-worker','publisher','consumer','observer'],'quiesce',timeout=180)
    before=snapshot('before');catalog=metadata('catalog-before')
    exports={name:export(name,name+'-export-before') for name in queries}
    rows={name:sql(query+';',name+'-rows-before') for name,(_,query) in queries.items()}
    old_plans={name:json.loads(sql('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query+';',name+'-plan-before').read_text()) for name,(_,query) in queries.items()}
    assert sql("SELECT count(*) FROM pg_indexes WHERE schemaname='consumer' AND indexname IN ('entity_totals_navigation','entity_projection_navigation');",'indexes-absent').read_text().strip()=='0'
    migration=ROOT/'migrations/consumer/006-projection-navigation.sql'
    sql('BEGIN;\n'+migration.read_text()+'\nCOMMIT;','forward-migration')
    assert before.read_bytes()==snapshot('after').read_bytes()
    assert catalog.read_bytes()==metadata('catalog-after').read_bytes()
    observed=[]
    for name,(index,query) in queries.items():
        assert exports[name].read_bytes()==export(name,name+'-export-after').read_bytes()
        assert rows[name].read_bytes()==sql(query+';',name+'-rows-after').read_bytes()
        plan=json.loads(sql('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query+';',name+'-plan-after').read_text())
        assert index in json.dumps(plan) and plan[0]['Plan']['Actual Rows']==32
        definition=json.loads(sql(f"SELECT jsonb_build_object('definition',pg_get_indexdef(c.oid),'owner',pg_get_userbyid(c.relowner),'valid',i.indisvalid,'ready',i.indisready,'unique',i.indisunique) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='consumer' AND c.relname='{index}';",name+'-definition').read_text())
        assert definition['owner']=='consumer_owner' and definition['valid'] and definition['ready'] and not definition['unique']
        for role in ['consumer_runtime','consumer_receipt_reader','consumer_operator']:
            denied=sql(f'BEGIN;SET LOCAL ROLE {role};DROP INDEX consumer.{index};ROLLBACK;',name+'-denied-'+role,expected=(3,))
            assert '42501' in denied.with_name(denied.name.replace('stdout','stderr')).read_text()
        observed.append({'collection':name,'index':definition,'before_execution_ms':old_plans[name][0]['Execution Time'],'after_execution_ms':plan[0]['Execution Time'],'export_sha256':sha(exports[name]),'ordered_page_sha256':sha(rows[name])})
    assert before.read_bytes()==snapshot('after-denials').read_bytes()
    state=r.out/'state';state.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        path=export(name,'final-export-'+name);os.link(path,state/(name+'.jsonl'))
    (state/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
    proof=r.run([sys.executable,'-B','tests/final/reconcile.py',str(state),'--count','4096','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=300)
    reconciliation=json.loads(proof.read_text());assert reconciliation['status']=='PASS'
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report.update(status='PASS',indexes=observed,reconciliation=reconciliation,migration_sha256=sha(migration),unchanged_rows_sha256=sha(before),unchanged_catalog_sha256=sha(catalog))
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
