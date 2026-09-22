"""Interpret bounded current observations; last-known data is never current progress."""
import datetime

def sample(snapshot, elapsed):
    assert isinstance(snapshot,dict) and isinstance(snapshot['dependencies'],dict)
    sections={};values={}
    for name in ('source','pipeline','consumer'):
        item=snapshot['dependencies'][name]
        assert isinstance(item,dict)
        sections[name]={key:item.get(key) for key in ('observed_at','freshness','health','dependency_health','data_health')}
        value=item['data']
        if value is not None:assert isinstance(value,dict)
        values[name]=value if item['freshness']=='fresh' else None
    pipeline=values['pipeline'];consumer=values['consumer'];backfill=snapshot['backfill']
    assert backfill is None or isinstance(backfill,dict)
    return {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_after_seed':elapsed,
        'staged':pipeline['staged'] if pipeline is not None else None,
        'deliveries':pipeline['deliveries'] if pipeline is not None else None,
        'observations':pipeline['observations'] if pipeline is not None else None,
        'consumer':consumer,'backfill_phase':backfill['phase'] if backfill is not None else None,
        'sections':sections,'source_counts':values['source'].get('counts') if values['source'] is not None else None}

def complete(value,count):
    if value['backfill_phase']!='complete' or value['staged']!=str(count) or value['consumer'] is None:return False
    if any(item['freshness']!='fresh' for item in value['sections'].values()):return False
    consumer=value['consumer']
    if consumer['processed']!=str(count) or consumer['effects']!='0' or consumer['quarantine']!='0':return False
    deliveries={(x['sink'],x['state']):int(x['count']) for x in value['deliveries']}
    observations={x['state']:int(x['count']) for x in value['observations']}
    return deliveries=={('elasticsearch','satisfied'):count,('rabbitmq','satisfied'):count} and observations=={'processed':count}

def unavailable(elapsed,error):
    return {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_after_seed':elapsed,
        'staged':None,'deliveries':None,'observations':None,'consumer':None,'backfill_phase':None,
        'sections':{name:{'freshness':'unknown','observed_at':None,'health':'unknown'} for name in ('source','pipeline','consumer')},
        'source_counts':None,'observation_error':type(error).__name__}
