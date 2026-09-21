"""Integrated real faults on the retained application; explicit functional/scale scope."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from verification.runtime import Runtime, ROOT, now, sha

arguments=argparse.ArgumentParser()
arguments.add_argument('--count',type=int,default=1024)
options=arguments.parse_args()
assert 257 <= options.count <= 2000000
r=Runtime(options.count)
workers=['capture','backfill','es-worker','publisher','consumer','observer']
journal=r.out/'journal.jsonl';journal.write_text('')
rejections=r.out/'rejections.json';rejections.write_text('[]')
seen=set()
def request(operation,entity=None,payload=None):
    return {'command_id':str(uuid.uuid4()),'operation':operation,'entity_id':entity,'payload_json':json.dumps(payload,ensure_ascii=False,separators=(',',':')) if payload is not None else None}
def changes(items):
    with journal.open('a') as stream:
        for item in items:stream.write(json.dumps(item,separators=(',',':'))+'\n')
    path=r.out/('workload-'+str(len(seen))+'-'+uuid.uuid4().hex[:6]+'.jsonl')
    path.write_text(''.join(json.dumps(item,separators=(',',':'))+'\n' for item in items))
    log=r.compose(['run','--rm','--no-deps','-T','writer','node','scripts/verification/commands.ts'],'source-workload',input_file=path,timeout=300)
    replies=[json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    assert len(replies)==len(items)
    for item,reply in zip(items,replies):
        assert reply['command_id']==item['command_id'];seen.add(item['command_id'])
    return replies

def settled(rejected=0,es=True,timeout=360):
    total=options.count+len(seen)
    def accept(s):
        dependencies=s['dependencies'];p=dependencies['pipeline']['data'];c=dependencies['consumer']['data']
        if p['staged']!=str(total) or c['processed']!=str(total) or c['effects']!=str(len(seen)):return False
        states={(x['sink'],x['state']):int(x['count']) for x in p['deliveries']}
        if states.get(('rabbitmq','satisfied'),0)!=total:return False
        if es and (states.get(('elasticsearch','satisfied'),0)!=total-rejected or states.get(('elasticsearch','dead_letter'),0)!=rejected):return False
        if sum(int(x['count']) for x in p['observations'] if x['state']=='processed')!=total:return False
        return dependencies['source']['data']['counts']['acknowledged']==str(len(seen)) and s['backfill']['phase']=='complete'
    return r.wait(r.status,accept,'settle',timeout)

def event_key(reply):
    e=reply['result'];return f"{e['source_epoch']}:{e['entity_id']}:{e['entity_version']}"
def event(key):return r.json_command(['run','--rm','--no-deps','-T','inspect','node','scripts/verification/inspect.ts','event',key],'event')
def trace(role):
    p=r.out/(role+'-trace.jsonl')
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()] if p.exists() else []
def start_role(role,boundary='',record=False):
    r.stop(role);token=r.control(role,boundary,record);r.start(role);return token
def release(token): (r.out/(token+'.release')).write_text(token)
def export_all(destination):
    destination.mkdir()
    for name in ('baselines','source','mutations','commands','work','pipeline','consumer','totals','projection','receiver'):
        log=r.compose(['run','--rm','--no-deps','-T','inspect','node','scripts/runtime/inspect.ts',name],'export-'+name,timeout=3600,maximum=max(64*1024*1024,options.count*8192))
        shutil.copyfile(log,destination/(name+'.jsonl'))
    values=r.inspect('failures')
    (destination/'failures.jsonl').write_text(''.join(json.dumps(v,separators=(',',':'))+'\n' for v in values))
def oracle(directory,expected=(0,)):
    return r.run([sys.executable,'-B','tests/final/reconcile.py',str(directory),'--count',str(options.count),'--journal',str(journal),'--rejections',str(rejections)],'independent-oracle',timeout=7200,expected=expected)

token=r.control('backfill','backfill.before_page_commit')
services={}
for role in ('backfill','consumer','publisher','es-worker'):
    if role!='backfill':r.control(role)
    services[role]={'restart':'no','command':['node','scripts/verification/fault-worker.ts',role],'volumes':[str(r.out)+':/verification:rw,z']}
(r.out/'verification.json').write_text(json.dumps({'services':services},indent=2))
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
inputs=[{'path':p,'sha256':sha(ROOT/p)} for p in sorted(set(filter(None,files))) if (ROOT/p).is_file()]
(r.out/'inputs.json').write_text(json.dumps(inputs,indent=2));r.report['input_sha256']=hashlib.sha256(json.dumps(inputs,separators=(',',':')).encode()).hexdigest();r.save()
print(str(r.out),flush=True)
try:
    r.run(['node','scripts/runtime-preflight.ts'],'prerequisite')
    r.compose(['config','--quiet'],'compose-configuration')
    r.compose(['up','-d','--build'],'cold-up',timeout=600)
    port=r.compose(['port','ui','4200'],'gateway').read_text().strip();assert port.startswith('127.0.0.1:');r.url='http://'+port;r.report['gateway']=r.url
    initial=r.inspect();assert initial['source']['entities']==initial['pipeline']['events']=='0'
    r.compose(['run','--rm','--no-deps','-T','seed','node','scripts/runtime/seed.ts',str(options.count)],'seed',timeout=7200)
    healthy=r.boundary(token);claim=healthy['data']['request']['claim'];before=r.inspect('backfill')
    old=next(q for q in before if q['run_id']==claim['runId'] and q['range_no']==claim['range'])
    assert old['checkpoint']==claim['checkpoint'] and int(old['checkpoint'])>=32 and int(old['committed_batches'])>=2
    release(token)
    healthy_after=r.wait(lambda:r.inspect('backfill'),lambda rows:any(q['run_id']==claim['runId'] and q['range_no']==claim['range'] and int(q['checkpoint'])>int(claim['checkpoint']) for q in rows),'healthy-page')
    token=start_role('backfill','backfill.before_page_commit')
    hit=r.boundary(token);claim=hit['data']['request']['claim'];before=r.inspect('backfill');sessions=r.inspect('sessions')
    old=next(q for q in before if q['run_id']==claim['runId'] and q['range_no']==claim['range'])
    assert old['checkpoint']==claim['checkpoint'] and int(old['checkpoint'])>=32
    assert any(s['state']=='idle in transaction' and s['backend_xid'] and 'backfill_page' in s['query'] for s in sessions)
    killed=r.kill('backfill',hit)
    after=r.inspect('backfill');current=next(q for q in after if q['run_id']==old['run_id'] and q['range_no']==old['range_no'])
    assert (current['checkpoint'],current['last_batch'],current['committed_batches'])==(old['checkpoint'],old['last_batch'],old['committed_batches'])
    update=request('update','1',{'name':'Concurrent update','country':'GE','loyalty_points':42})
    changed=changes([update,update,request('delete','2'),request('restore','2',{'name':'Restored','country':'FR','loyalty_points':8}),request('delete','3'),request('create',payload={'name':'Concurrent insert','country':'GE','loyalty_points':9})])
    assert changed[0]['result']==changed[1]['result'] and changed[1]['replayed'] is True
    r.control('backfill');r.start('backfill');complete=settled(timeout=1800)
    r.gate('G1',{'healthy':healthy,'healthy_progress':healthy_after,'fault':killed,'checkpoint_before':old,'checkpoint_after_kill':current,'open_sessions':sessions,'completed_run':complete['backfill'],'concurrent_mutations':len(seen)})

    healthy_token=start_role('consumer','consumer.after_commit.before_ack',True)
    one=changes([request('create',payload={'name':'Healthy consumer control','country':'GE','loyalty_points':1})])[0]
    healthy=r.boundary(healthy_token);key=event_key(one);record=event(key)
    assert len(record['consumer']['inbox'])==len(record['consumer']['effects'])==1
    release(healthy_token);settled()
    token=start_role('consumer','consumer.after_commit.before_ack',True)
    one=changes([request('create',payload={'name':'Consumer commit ambiguity','country':'GE','loyalty_points':2})])[0]
    hit=r.boundary(token);key=event_key(one);before=event(key)
    assert len(before['consumer']['inbox'])==len(before['consumer']['effects'])==1
    consumer_kill=r.kill('consumer',hit);r.control('consumer',record=True);r.start('consumer');settled()
    after=event(key);assert before['consumer']==after['consumer']
    delivered=[x['data'] for x in trace('consumer') if x['type']=='consumer-delivery' and x['data']['message_id']==key]
    assert len(delivered)>=2 and len({x['wire_sha256'] for x in delivered})==1 and any(x['redelivered'] for x in delivered)
    consumer_evidence={'fault':consumer_kill,'event':key,'deliveries':delivered,'retained_effect':after['consumer'],'healthy':healthy}
    healthy_token=start_role('publisher','publisher.after_confirm.before_local_commit',True)
    one=changes([request('create',payload={'name':'Healthy publisher control','country':'GE','loyalty_points':3})])[0]
    healthy=r.boundary(healthy_token);release(healthy_token);settled()
    token=start_role('publisher','publisher.after_confirm.before_local_commit',True)
    one=changes([request('create',payload={'name':'Publisher confirm ambiguity','country':'GE','loyalty_points':4})])[0]
    hit=r.boundary(token);key=event_key(one)
    before=r.wait(lambda:event(key),lambda v:len(v['consumer']['effects'])==1,'consumer-before-publisher-settlement',timeout=15)
    assert next(v for v in before['pipeline']['deliveries'] if v['kind']=='rabbitmq')['state']=='leased'
    publication_kill=r.kill('publisher',hit);r.control('publisher',record=True);r.start('publisher');settled()
    after=event(key);assert before['consumer']==after['consumer']
    delivered=r.wait(lambda:[x['data'] for x in trace('consumer') if x['type']=='consumer-delivery' and x['data']['message_id']==key],lambda v:len(v)>=2,'duplicate-publication')
    assert len({x['wire_sha256'] for x in delivered})==1
    r.gate('G2',{'consumer':consumer_evidence,'publisher':{'fault':publication_kill,'event':key,'deliveries':delivered,'one_effect':after['consumer'],'healthy':healthy}})

    attempts_before=r.inspect('attempts');es_id=r.worker_id('elasticsearch')
    r.compose(['stop','-t','20','elasticsearch'],'elasticsearch-stop',timeout=60);outage_start=time.monotonic()
    down=json.loads(r.run(['docker','inspect',es_id],'elasticsearch-stopped').read_text())[0];assert not down['State']['Running']
    changes([request('create',payload={'name':f'Outage change {i}','country':'GE','loyalty_points':i}) for i in range(10)])
    progressed=settled(es=False);during_states={(v['sink'],v['state']):int(v['count']) for v in progressed['dependencies']['pipeline']['data']['deliveries']}
    assert during_states.get(('elasticsearch','satisfied'),0)==options.count+len(seen)-10
    (r.out/'outage-status.json').write_text(json.dumps(progressed,indent=2))
    while time.monotonic()-outage_start<60:time.sleep(min(.25,60-(time.monotonic()-outage_start)))
    still_down=json.loads(r.run(['docker','inspect',es_id],'elasticsearch-still-stopped').read_text())[0];assert not still_down['State']['Running']
    attempts_after=r.inspect('attempts');attempt_delta=sum(int(x['count']) for x in attempts_after)-sum(int(x['count']) for x in attempts_before);assert 1<=attempt_delta<=150
    duration=time.monotonic()-outage_start;r.start('elasticsearch');recovered=settled(timeout=360)
    r.gate('G3',{'stopped_container':es_id,'down_seconds':duration,'mutations_while_down':10,'broker_consumer_progress':True,'attempt_delta':attempt_delta,'attempt_bound':150,'recovered_observation':recovered['observed_at'] if 'observed_at' in recovered else recovered.get('timestamp')})
    r.stop('es-worker')
    batch=[request('create',payload={'name':f'Bulk record {i}','country':'GE','loyalty_points':'not-a-number' if i in (17,233,499) else i}) for i in range(500)]
    declared=[batch[i]['command_id'] for i in (17,233,499)];rejections.write_text(json.dumps(declared,indent=2))
    replies=changes(batch);bad={event_key(reply) for reply in replies if reply['command_id'] in declared}
    settled(es=False)
    r.control('es-worker',record=True);r.start('es-worker');settled(rejected=3)
    responses=[x['data'] for x in trace('es-worker') if x['type']=='bulk-response']
    assert len(responses)==1 and responses[0]['operations']==500
    actual=responses[0]['response'];assert actual['errors'] is True and len(actual['items'])==500
    accepted=[x['index'] for x in actual['items'] if x['index']['status'] in (200,201)]
    rejected=[x['index'] for x in actual['items'] if x['index']['status']==400]
    assert len(accepted)==497 and len(rejected)==3
    assert {x['_id']+':1' for x in rejected}==bad
    assert all(x['error']['type'] in ('document_parsing_exception','mapper_parsing_exception') for x in rejected)
    failures=r.inspect('failures');assert {x['event_id'] for x in failures}==bad and len(failures)==3 and all(x['current'] for x in failures)
    request_count=len(responses);time.sleep(2);assert len([x for x in trace('es-worker') if x['type']=='bulk-response'])==request_count
    r.gate('G4',{'operations':500,'actual_applied':497,'actual_mapper_rejections':rejected,'dead_letters':failures,'request_sha256':responses[0]['request_sha256'],'declared_source_command_ids':declared})

    snapshot=settled(rejected=3);metrics=r.http('/metrics')
    (r.out/'g5-status.json').write_text(json.dumps(snapshot,indent=2));(r.out/'g5-metrics.prom').write_text(metrics)
    assert snapshot['backfill']['phase']=='complete'
    assert snapshot['dependencies']['source']['data']['counts']['acknowledged']==str(len(seen))
    assert 'pipeline_dlq_open{kind="elasticsearch"} 3' in metrics
    assert 'pipeline_source_pending 0' in metrics and 'pipeline_events_staged_total' in metrics
    browser=r.run(['node','scripts/runtime-browser.ts',r.url,str(r.out),str(options.count+len(seen))],'real-ui',timeout=90)
    browser_result=json.loads(browser.read_text());assert browser_result['mutations']==0 and browser_result['interceptedResponses']==0 and not browser_result['pageErrors']
    r.gate('G5',{'snapshot':'g5-status.json','metrics':'g5-metrics.prom','browser':browser_result,'declared_open_dlq':3,'source_pending':0,'backfill_phase':'complete'})
    r.compose(['stop','-t','20',*workers],'quiesce-final',timeout=180)
    exported=r.out/'final-state';export_all(exported)
    oracle_result=json.loads(oracle(exported).read_text());r.report['reconciliation']=oracle_result;r.save()
    negative_results=[]
    for kind in ('missing','extra','payload','version','effect'):
        directory=r.out/('negative-'+kind);directory.mkdir()
        target='consumer.jsonl' if kind=='effect' else 'receiver.jsonl'
        for file in exported.glob('*.jsonl'):
            if file.name!=target:os.symlink(file,directory/file.name)
        changed=False;first=None
        with (exported/target).open() as source,(directory/target).open('w') as output:
            for line in source:
                row=json.loads(line);first=first or row
                if not changed and (kind!='effect' or row['has_effect']):
                    changed=True
                    if kind=='missing':continue
                    if kind=='payload':row['source']['search_fields']['name']='Tampered with copied canonical hash'
                    elif kind=='version':row['receiver_version']=int(row['receiver_version'])+1
                    elif kind=='effect':row['has_effect']=False
                    line=json.dumps(row,separators=(',',':'))+'\n'
                output.write(line)
            if kind=='extra':
                assert first is not None;first['document_id']+='-unexpected';output.write(json.dumps(first)+'\n')
        assert changed
        oracle(directory,expected=(1,));negative_results.append(kind)
    r.report['negative_controls']=negative_results
    assert [g['id'] for g in r.report['gates']]==['G1','G2','G3','G4','G5']
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)==r.dirty
    assert all(sha(ROOT/x['path'])==x['sha256'] for x in inputs)
    r.report['status']='PASS'
    r.report['scale']={'requested_baselines':options.count,'two_million_profile':'EXECUTED' if options.count==2000000 else 'NOT RUN','performance_claim':'No extrapolation from this fixture'}
except BaseException as error:
    r.report['status']='FAIL';r.report['error']={'type':type(error).__name__,'message':str(error)}
finally:
    r.cleanup()
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
