"""Independent availability accounting for the finite active G1 drain window."""
import datetime
import math


def timestamp(value):
    try:
        return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
    except (AttributeError,ValueError,TypeError):
        return float('nan')


def metrics_fresh(record):
    if 'error' in record:return False
    samples={}
    for line in record.get('metrics','').splitlines():
        if not line or line.startswith('#'):continue
        fields=line.split()
        if len(fields)!=2:return False
        samples.setdefault(fields[0],[]).append(fields[1])
    key='pipeline_observation_fresh_timestamp_seconds{component="pipeline"}'
    try:
        values=samples[key]
        if len(values)!=1:return False
        at=float(values[0]);end=timestamp(record['finished_at'])
        if not math.isfinite(at) or not math.isfinite(end) or abs(end-at)>30:return False
    except (KeyError,ValueError,TypeError):return False
    staged=samples.get('pipeline_events_staged_total',[])
    if len(staged)!=1 or not staged[0].isascii() or not staged[0].isdecimal():return False
    return samples.get('pipeline_dependency_health{component="pipeline",state="unavailable"}')==['0'] and len(samples.get('pipeline_events_staged_total',[]))==1


def summarize(statuses,metrics,count):
    # Deliberate worker/receiver faults are outside the sustained G1 drain.
    # G1 pipeline itself is not intentionally stopped after this window begins.
    statuses=[x for x in statuses if x.get('phase')=='G1']
    metrics=[x for x in metrics if x.get('phase')=='G1']
    def fresh(row):
        if 'error' in row:return False
        pipeline=row.get('data',{}).get('dependencies',{}).get('pipeline',{})
        at=timestamp(pipeline.get('observed_at'));end=timestamp(row.get('finished_at'))
        return pipeline.get('freshness')=='fresh' and isinstance(pipeline.get('data'),dict) and math.isfinite(at) and math.isfinite(end) and abs(end-at)<=30
    def inventory(rows,check):
        latencies=sorted(float(r['elapsed_ms']) for r in rows)
        times=[timestamp(r['at']) for r in rows]
        finite=all(math.isfinite(t) for t in times) and all(math.isfinite(x) and x>=0 for x in latencies) and all(b>=a for a,b in zip(times,times[1:]))
        return {'valid_timing':finite,'samples':len(rows),'unavailable_or_stale':sum(not check(r) for r in rows),'request_failures':sum('error' in r for r in rows),'first_at':rows[0]['at'] if rows else None,'last_at':rows[-1]['at'] if rows else None,'max_gap_seconds':max((b-a for a,b in zip(times,times[1:])),default=0),'latency_ms':{'p50':latencies[(len(latencies)-1)//2] if latencies else None,'p95':latencies[math.ceil(len(latencies)*.95)-1] if latencies else None,'max':max(latencies,default=None)}}
    status=inventory(statuses,fresh);scrapes=inventory(metrics,metrics_fresh)
    staged=[]
    for row in statuses:
        try:staged.append(int(row['data']['dependencies']['pipeline']['data']['staged']))
        except (KeyError,TypeError,ValueError):pass
    result={'scope':'Every recorded status and independent metrics scrape during this finite G1 drain; not continuous or unbounded availability','status':status,'metrics':scrapes,'first_staged':staged[0] if staged else None,'last_staged':staged[-1] if staged else None}
    result['passed']=bool(statuses and metrics and status['valid_timing'] and scrapes['valid_timing'] and status['unavailable_or_stale']==0 and scrapes['unavailable_or_stale']==0 and status['max_gap_seconds']<=60 and scrapes['max_gap_seconds']<=60 and staged and staged[0]<count and staged[-1]>=count)
    if count>=1000000:result['passed']=result['passed'] and staged[0]<count//4 and status['samples']>=10 and scrapes['samples']>=3
    return result
