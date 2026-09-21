"""Real populated source upgrade/negative checks in a fresh owned old-version runtime."""
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from verification.runtime import Runtime,ROOT,sha
r=Runtime(257)
r.report.update(scope='Source validation/index forward upgrade with exact preservation and real negative SQL; not capacity',mode='source-index-upgrade')
r.env['KIT_IMAGE']='kill-it-twice-runtime:final-545b2c8dc877'
(r.out/'verification.json').write_text('{"services":{}}')
def sql(text,label,expected=(0,)):
    file=r.out/(label+'.sql');file.write_text(text+'\n')
    return r.compose(['exec','-T','source','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','source_admin','-d','source_m1'],label,input_file=file,expected=expected)
rows=['source_identity','bootstrap_manifest','bootstrap_chunks','entities','baseline_revisions','outbox','command_receipts','capture_binding','capture_work','backfill_fences','backfill_fence_members']
def snapshot(label):
    commands=['BEGIN READ ONLY;']
    for table in rows:
        commands.append(f"SELECT jsonb_build_object('table','{table}','rows',COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb)) FROM source.{table} r;")
    commands.append('ROLLBACK;')
    return sql('\n'.join(commands),label)
def catalog(label):
    return sql("SELECT jsonb_agg(jsonb_build_object('oid',oid,'name',proname,'owner',proowner,'acl',proacl,'config',proconfig,'definer',prosecdef) ORDER BY oid) FROM pg_proc WHERE oid IN ('source.require_current_baseline()'::regprocedure,'source.require_command_completion()'::regprocedure,'source.require_bootstrap_chunk()'::regprocedure,'source.require_bootstrap_progress()'::regprocedure);",label)
print(str(r.out),flush=True)
try:
    r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','257'],'seed',timeout=300)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();r.url='http://'+address
    r.wait(r.status,lambda s:s['backfill']['phase']=='complete','old-complete',timeout=240)
    r.compose(['stop','-t','20','capture','backfill','publisher','consumer','es-worker','observer'],'quiesce',timeout=180)
    before=snapshot('before-rows');before_catalog=catalog('before-functions')
    plan="BEGIN READ ONLY; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT count(*) FROM source.baseline_revisions WHERE bootstrap_key=(SELECT bootstrap_key FROM source.bootstrap_manifest) AND chunk_first=1; ROLLBACK;"
    sql(plan,'plan-before')
    files=[ROOT/'migrations'/name for name in ['009-source-validation-indexes.sql','010-source-chunk-validation.sql']]
    sql('BEGIN;\n'+'\n'.join(file.read_text() for file in files)+'\nCOMMIT;','apply-forward')
    after=snapshot('after-rows');after_catalog=catalog('after-functions')
    assert before.read_bytes()==after.read_bytes(),'Populated business evidence changed'
    old_metadata=json.loads(before_catalog.read_text());new_metadata=json.loads(after_catalog.read_text())
    assert len(old_metadata)==len(new_metadata)==4
    for old,new in zip(old_metadata,new_metadata):
        assert {k:v for k,v in old.items() if k!='acl'}=={k:v for k,v in new.items() if k!='acl'}
        assert old['acl'] in (None,['source_owner=X/source_owner'])
        assert new['acl']==['source_owner=X/source_owner'],'PUBLIC may only be narrowed, never expanded'
    sql(plan,'plan-after')
    columns='source_epoch,command_id,contract_version,operation,target_id,request_payload,completed,result_entity_id,result_version,result_change_id,result_recorded_at,result_deleted,result_payload'
    invalid=f"BEGIN; INSERT INTO source.command_receipts({columns}) SELECT source_epoch,gen_random_uuid(),1,'update',entity_id,payload,true,entity_id,1,NULL,recorded_at,false,'{{}}'::jsonb FROM source.baseline_revisions WHERE entity_id=1; COMMIT;"
    out=sql(invalid,'wrong-baseline-receipt',(3,))
    assert 'P2003' in out.with_name(out.name.replace('stdout','stderr')).read_text()
    missing=f"BEGIN; INSERT INTO source.command_receipts({columns}) SELECT source_epoch,gen_random_uuid(),1,'update',9223372036854775807,payload,true,9223372036854775807,1,NULL,recorded_at,false,payload FROM source.baseline_revisions WHERE entity_id=1; COMMIT;"
    out=sql(missing,'missing-baseline-receipt',(3,))
    assert 'P2003' in out.with_name(out.name.replace('stdout','stderr')).read_text()
    # Scoped SQL trigger probes exercise the actual replacement function, not a mock.
    for label,column,value in [('wrong-row-recipe','payload',"'{}'::jsonb"),('wrong-row-chunk','chunk_first','999999')]:
        probe=f"BEGIN; CREATE TEMP TABLE proof(LIKE source.baseline_revisions INCLUDING DEFAULTS); CREATE CONSTRAINT TRIGGER proof_guard AFTER INSERT ON proof DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source.require_bootstrap_chunk(); INSERT INTO proof SELECT source_epoch,entity_id,entity_version,change_id,recorded_at,is_deleted,{value if column=='payload' else 'payload'},bootstrap_key,ordinal,{value if column=='chunk_first' else 'chunk_first'} FROM source.baseline_revisions WHERE entity_id=1; COMMIT;"
        out=sql(probe,label,(3,));assert 'P7003' in out.with_name(out.name.replace('stdout','stderr')).read_text()
    assert snapshot('after-rejected-transactions').read_bytes()==before.read_bytes()
    r.report.update(status='PASS',migration_sha256={file.name:sha(file) for file in files},preserved_rows_sha256=sha(before),preserved_function_metadata_sha256=sha(before_catalog),negative_cases=['wrong retained payload','missing historical identity','wrong row recipe via scoped actual trigger','wrong row chunk via scoped actual trigger'])
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
