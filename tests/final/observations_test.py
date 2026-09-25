"""Reject incomplete, stale and selectively successful availability evidence."""
import copy
import datetime
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts'))
from verification.observations import summarize

class AvailabilityTests(unittest.TestCase):
    def records(self):
        start=datetime.datetime(2026,1,1,tzinfo=datetime.timezone.utc)
        statuses=[];metrics=[]
        for i in range(11):
            at=(start+datetime.timedelta(seconds=i*30)).isoformat()
            common={'phase':'G1','at':at,'finished_at':at,'elapsed_ms':10}
            statuses.append({**common,'data':{'dependencies':{'pipeline':{'observed_at':at,'freshness':'fresh','data':{'staged':str(i*100000)}}}}})
            text='pipeline_dependency_health{component="pipeline",state="unavailable"} 0\npipeline_events_staged_total '+str(i*100000)+'\npipeline_observation_fresh_timestamp_seconds{component="pipeline"} '+str(start.timestamp()+i*30)+'\n'
            metrics.append({**common,'metrics':text})
        return statuses,metrics
    def test_complete_finite_active_window_passes(self):
        status,metrics=self.records();result=summarize(status,metrics,1000000)
        self.assertTrue(result['passed']);self.assertEqual(result['status']['samples'],11)
        self.assertEqual(result['metrics']['max_gap_seconds'],30)
    def test_intermittent_unavailable_is_not_hidden_by_final_success(self):
        status,metrics=self.records();status[4]['data']['dependencies']['pipeline']['freshness']='unavailable'
        result=summarize(status,metrics,1000000);self.assertFalse(result['passed']);self.assertEqual(result['status']['unavailable_or_stale'],1)
    def test_http_failure_stays_in_denominator(self):
        for which in ('status','metrics'):
            status,metrics=self.records();rows=status if which=='status' else metrics
            rows[4].pop('data' if which=='status' else 'metrics');rows[4]['error']={'type':'TimeoutError'}
            result=summarize(status,metrics,1000000)
            self.assertFalse(result['passed']);self.assertEqual(result[which]['samples'],11);self.assertEqual(result[which]['request_failures'],1)
    def test_timestamp_label_cannot_make_stale_observation_fresh(self):
        for kind in ('status','metrics'):
            status,metrics=self.records()
            if kind=='status':status[-1]['data']['dependencies']['pipeline']['observed_at']=status[0]['at']
            else:metrics[-1]['metrics']=metrics[0]['metrics']
            self.assertFalse(summarize(status,metrics,1000000)['passed'])
    def test_omitted_or_duplicate_pipeline_metric_fails(self):
        status,metrics=self.records()
        for text in ('',metrics[0]['metrics']+metrics[0]['metrics'],metrics[0]['metrics'].replace('unavailable"} 0','unavailable"} 1')):
            changed=copy.deepcopy(metrics);changed[0]['metrics']=text
            self.assertFalse(summarize(status,changed,1000000)['passed'])
    def test_missing_load_window_or_large_sampling_gap_fails(self):
        status,metrics=self.records()
        for s,m in (([],metrics),(status,[]),(status[7:],metrics[7:]),(status[:2]+status[5:],metrics),(status,metrics[:2]+metrics[5:]),(status[:-1],metrics)):
            self.assertFalse(summarize(s,m,1000000)['passed'])

if __name__=='__main__':unittest.main()
