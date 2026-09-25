"""Populated observation/progress upgrade; negative controls are rollback-only."""
import json,subprocess,sys,os,argparse
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime,sha
parser=argparse.ArgumentParser()
upgrade=parser.add_mutually_exclusive_group()
upgrade.add_argument('--metadata-upgrade',action='store_true')
upgrade.add_argument('--delivery-upgrade',action='store_true')
upgrade.add_argument('--counts-upgrade',action='store_true')
options=parser.parse_args()
base='cbf7bf56e1679d40bfb797e8cf1808a14ccb45f4' if options.counts_upgrade else 'ef5f63393eb0207598dcc92b8c935ee67b7c224e' if options.delivery_upgrade else ('2738166ba3ee952ec68abbf8526b199cdb476276' if options.metadata_upgrade else '26548e9c07f47efab84c75175145b59f443bd5e2')
r=Runtime(4096);r.report.update(mode='observation-proof',base_code=base)
r.env['KIT_IMAGE']='kill-it-twice-runtime:final-cbf7bf56e167' if options.counts_upgrade else 'kill-it-twice-runtime:final-ef5f63393eb0' if options.delivery_upgrade else ('kill-it-twice-runtime:final-2738166ba3ee' if options.metadata_upgrade else 'kill-it-twice-runtime:capacity-26548e9c07f4')
(r.out/'verification.json').write_text('{"services":{}}')
def sql(text,label,expected=(0,)):
    file=r.out/(label+'.sql');file.write_text(text+'\n')
    return r.compose(['exec','-T','pipeline','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','pipeline_admin','-d','pipeline_m2b'],label,input_file=file,expected=expected)
def snapshot(label):
    return sql("BEGIN READ ONLY;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.backfill_runs ORDER BY run_id)x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.backfill_ranges ORDER BY run_id,range_no)x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.backfill_batches ORDER BY batch_id)x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.backfill_members ORDER BY run_id,event_id COLLATE \"C\")x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.events ORDER BY event_id COLLATE \"C\")x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.delivery_intents ORDER BY event_id COLLATE \"C\",kind)x;SELECT row_to_json(x)::text FROM (SELECT * FROM pipeline.consumer_observations ORDER BY event_id COLLATE \"C\")x;ROLLBACK;",label)
def operational_snapshot(query,label):
    value=json.loads(sql("BEGIN READ ONLY;SET LOCAL ROLE pipeline_operator;SET LOCAL statement_timeout=2500;"+query+";ROLLBACK;",label).read_text())
    value.pop('observed_at')
    # SQL grouped arrays have no ordering contract. Preserve every field/value.
    for key in ('deliveries','observations','attempts','settled'):
        value[key].sort(key=lambda row:json.dumps(row,sort_keys=True))
    return value
original=(ROOT/'migrations/pipeline/006-backfill.sql').read_text()
start=original.index('CREATE FUNCTION pipeline.backfill_progress_valid(');end=original.index('\n$$;',start)+4
reference=original[start:end].replace('pipeline.backfill_progress_valid(', 'pg_temp.reference_progress(')
compare="SELECT jsonb_build_object('old',pg_temp.reference_progress(run_id),'new',pipeline.backfill_progress_valid(run_id),'full',pipeline.backfill_counts(run_id),'observed',pipeline.backfill_observed_counts(run_id)) FROM pipeline.backfill_runs;"
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.run(['docker','image','inspect',r.env['KIT_IMAGE']],'original-image')
    r.compose(['up','-d','--no-build'],'old-runtime',timeout=300)
    address=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert address.startswith('127.0.0.1:');r.url='http://'+address
    assert r.inspect()['source']['entities']=='0'
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts','4096'],'seed',timeout=300)
    r.wait(r.status,lambda s:isinstance(s.get('backfill'),dict) and s['backfill']['phase']=='complete','before-complete',timeout=400)
    r.compose(['stop','-t','20','capture','backfill','es-worker','publisher','consumer','observer'],'quiesce',timeout=180)
    before=snapshot('before')
    query=json.loads(r.run(['node','--input-type=module','-e',"import {queries} from './src/operations/queries.ts';console.log(JSON.stringify(queries.pipeline.snapshot))"],'operational-query').read_text())
    prior_query=query
    if options.metadata_upgrade:
        prior_module=r.out/'prior-queries.ts'
        prior_module.write_text(subprocess.check_output(['git','show',base+':src/operations/queries.ts'],cwd=ROOT,text=True))
        prior_query=json.loads(r.run(['node','--input-type=module','-e',"const {queries}=await import("+json.dumps(prior_module.as_uri())+");console.log(JSON.stringify(queries.pipeline.snapshot))"],'prior-operational-query').read_text())
    operational_before=operational_snapshot(prior_query,'operational-before')
    metadata="SELECT jsonb_build_object('oid',oid,'owner',proowner,'acl',proacl,'config',proconfig,'security',prosecdef,'volatility',provolatile,'parallel',proparallel,'strict',proisstrict,'source',prosrc) FROM pg_proc WHERE oid IN ('pipeline.backfill_progress_valid(uuid)'::regprocedure,'pipeline.backfill_observed_counts(uuid)'::regprocedure,'pipeline.backfill_observation(uuid)'::regprocedure) ORDER BY oid;" if options.delivery_upgrade or options.counts_upgrade else "SELECT jsonb_build_object('oid',oid,'owner',proowner,'acl',proacl,'config',proconfig,'security',prosecdef) FROM pg_proc WHERE oid='pipeline.backfill_progress_valid(uuid)'::regprocedure;"
    if options.counts_upgrade:
        metadata=metadata.replace("'source',prosrc","'source',CASE WHEN proname='backfill_observed_counts' THEN NULL ELSE prosrc END")
    catalog=sql(metadata,'metadata-before')
    count_definition_query="SELECT jsonb_build_object('language',(SELECT lanname FROM pg_language WHERE oid=prolang),'source',prosrc) FROM pg_proc WHERE oid='pipeline.backfill_observed_counts(uuid)'::regprocedure;"
    if options.counts_upgrade:
        count_definition_before=json.loads(sql(count_definition_query,'counts-definition-before').read_text())
        assert count_definition_before['language']=='sql'

    migration=ROOT/('migrations/pipeline/022-plan-observation-counts.sql' if options.counts_upgrade else 'migrations/pipeline/021-delivery-observation-cover.sql' if options.delivery_upgrade else ('migrations/pipeline/018-observation-metadata.sql' if options.metadata_upgrade else 'migrations/pipeline/016-observation-and-progress.sql'))
    sql('BEGIN;\n'+migration.read_text()+'\nCOMMIT;','upgrade')
    indexes=ROOT/'migrations/pipeline/017-observation-indexes.sql'
    if not options.metadata_upgrade and not options.delivery_upgrade and not options.counts_upgrade:sql('BEGIN;\n'+indexes.read_text()+'\nCOMMIT;','observation-index-upgrade')
    if options.counts_upgrade:
        installed_counts=json.loads(sql(count_definition_query,'counts-definition-after').read_text())
        assert installed_counts['language']=='plpgsql'
        assert installed_counts['source']==migration.read_text().split('AS $$',1)[1].split('$$;',1)[0]
        assert installed_counts['source'].split('$count$')[1].replace('m.run_id= $1','m.run_id=id').strip()==count_definition_before['source'].strip(), 'Aggregate query changed'
        r.report['aggregate_query_unchanged']=True
        r.report['count_language_transition']={'before':'sql','after':'plpgsql'}
    if options.delivery_upgrade:
        installed=json.loads(sql("SELECT jsonb_build_object('valid',indisvalid,'definition',pg_get_indexdef(indexrelid)) FROM pg_index WHERE indexrelid='pipeline.delivery_member_observation'::regclass;",'delivery-index').read_text())
        assert installed['valid'] and '(kind, event_id) INCLUDE (state)' in installed['definition'],installed
    assert before.read_bytes()==snapshot('after').read_bytes()
    assert operational_before==operational_snapshot(query,'operational-after')
    assert catalog.read_bytes()==sql(metadata,'metadata-after').read_bytes()
    if options.counts_upgrade:
        old=(ROOT/'migrations/pipeline/016-observation-and-progress.sql').read_text()
        start=old.index('CREATE FUNCTION pipeline.backfill_observed_counts(');end=old.index('\n$$;',start)+4
        reference+='\n'+old[start:end].replace('pipeline.backfill_observed_counts(', 'pg_temp.reference_counts(')
        compare=compare.replace("'observed',pipeline.backfill_observed_counts(run_id)","'observed',pipeline.backfill_observed_counts(run_id),'prior_observed',pg_temp.reference_counts(run_id)")
    checked=[]
    for label,alter in [
        ('healthy',''),
        ('missing_member',"ALTER TABLE pipeline.backfill_members DISABLE TRIGGER USER;DELETE FROM pipeline.backfill_members WHERE ctid=(SELECT ctid FROM pipeline.backfill_members LIMIT 1);"),
        ('wrong_checkpoint',"ALTER TABLE pipeline.backfill_ranges DISABLE TRIGGER USER;UPDATE pipeline.backfill_ranges SET checkpoint=checkpoint-1 WHERE range_no=1;"),
        ('wrong_batch_hash',"ALTER TABLE pipeline.backfill_batches DISABLE TRIGGER USER;UPDATE pipeline.backfill_batches SET items=jsonb_set(items,'{0,hash}',to_jsonb(repeat('0',64))) WHERE batch_id=(SELECT batch_id FROM pipeline.backfill_batches WHERE jsonb_array_length(items)>0 LIMIT 1);")]:
        value=json.loads(sql('BEGIN;\n'+reference+'\n'+alter+'\n'+compare+'\nROLLBACK;',label).read_text())
        assert value['old']==value['new']==(label=='healthy'),value
        if options.counts_upgrade:assert value['observed']==value['prior_observed'],value
        assert value['observed']['invalid'] is None
        assert {k:v for k,v in value['full'].items() if k!='invalid'}=={k:v for k,v in value['observed'].items() if k!='invalid'}
        checked.append({'case':label,**value})
    assert before.read_bytes()==snapshot('after-negative-controls').read_bytes()
    observation=json.loads(sql("BEGIN READ ONLY;SET LOCAL ROLE pipeline_operator;SET LOCAL statement_timeout=2500;SELECT pipeline.backfill_observation(run_id) FROM pipeline.backfill_runs;ROLLBACK;",'restricted-observation').read_text())
    assert observation['evidence_scope']=='durable_state_observation_not_revalidation' and observation['counts']['invalid'] is None and observation['historical_terminal_proof'] is True
    denied=sql("BEGIN;SET LOCAL ROLE pipeline_operator;ALTER FUNCTION pipeline.backfill_progress_valid(uuid) RENAME TO bypass;ROLLBACK;",'denied',expected=(3,));assert '42501' in denied.with_name(denied.name.replace('stdout','stderr')).read_text()
    plan=None if options.delivery_upgrade or options.counts_upgrade else sql(reference+'BEGIN READ ONLY;'+"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT pg_temp.reference_progress(run_id) FROM pipeline.backfill_runs;EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT pipeline.backfill_progress_valid(run_id) FROM pipeline.backfill_runs;ROLLBACK;",'plans')
    exported=r.out/'state';exported.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name,timeout=300)
        os.link(log,exported/(name+'.jsonl'))
    (exported/'failures.jsonl').write_text('');(r.out/'journal.jsonl').write_text('');(r.out/'rejections.json').write_text('[]')
    proof=r.run([sys.executable,'-B','tests/final/reconcile.py',str(exported),'--count','4096','--journal',str(r.out/'journal.jsonl'),'--rejections',str(r.out/'rejections.json')],'independent-oracle',timeout=300)
    r.report.update(status='PASS',reconciliation=json.loads(proof.read_text()),comparison=checked,preserved_sha256=sha(before),metadata_sha256=sha(catalog),migration_sha256=sha(migration),index_migration_sha256=sha(indexes),operational_snapshot_preserved=True,plans_file=plan.name if plan else None,scope='Real populated function/index upgrade preservation, old/new progress equivalence, rollback-only negative controls and exact content reconciliation; no large-scale timing claim')
    assert subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)==r.dirty
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
except BaseException as error:r.report.update(status='FAIL',error={'type':type(error).__name__,'message':str(error)})
finally:r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
