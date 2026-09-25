"""Ownership refusal tests; these do not substitute for real fault acceptance."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from verification.runtime import Runtime, cleanup_owned_resources, validate_compose_identity

class StatusEvidenceTests(unittest.TestCase):
    def test_records_success_and_request_failure_without_hiding_or_retrying_it(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Runtime.__new__(Runtime);r.out=Path(directory)
            value={'observed_at':'original-timestamp','dependencies':{'pipeline':{'freshness':'unavailable'}}}
            with patch.object(r,'status',return_value=value) as call:
                self.assertIs(r.recorded_status('G1'),value)
                call.assert_called_once_with()
            failure=TimeoutError('private endpoint must not be recorded')
            with patch.object(r,'status',side_effect=failure) as call:
                with self.assertRaises(TimeoutError) as caught:r.recorded_status('G1')
                self.assertIs(caught.exception,failure)
                call.assert_called_once_with()
            path=r.out/'status-observations.jsonl'
            records=[json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(records),2)
            self.assertEqual(records[0]['data'],value)
            self.assertEqual(records[1]['error'],{'type':'TimeoutError'})
            self.assertNotIn('data',records[1])
            self.assertNotIn('private endpoint',path.read_text())
            for record in records:
                self.assertEqual(record['phase'],'G1')
                self.assertGreaterEqual(record['elapsed_ms'],0)

class OwnershipTests(unittest.TestCase):
    def runtime(self,directory,project='kit-final-test'):
        r=Runtime.__new__(Runtime)
        r.project=project;r.out=Path(directory);r.report={'status':'RUNNING'}
        r.compose_args=['docker','compose','-p',project];r.compose_project_validated=False
        return r
    def test_demo_name_refuses_all_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            r=self.runtime(directory,'kit-server-dev');calls=[]
            r.run=lambda *a,**k:calls.append(a)
            r.cleanup()
            self.assertEqual(calls,[])
            self.assertEqual(r.report['cleanup']['status'],'FAIL')
    def test_foreign_label_refuses_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            r=self.runtime(directory);calls=[]
            def run(args,label,**options):
                calls.append(args);p=Path(directory)/label
                if label=='cleanup-list-container':p.write_text('abc\n')
                elif label=='cleanup-identities-container':p.write_text(json.dumps([{'Config':{'Labels':{'com.docker.compose.project':'kit-server-dev'}}}]))
                else:self.fail('Unexpected operation: '+label)
                return p
            r.run=run;r.cleanup()
            self.assertEqual(r.report['cleanup']['status'],'FAIL')
            self.assertFalse(any('stop' in a or 'rm' in a for a in calls))
    def test_wrong_resolved_project_refuses_compose_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            r=self.runtime(directory);calls=[];p=Path(directory)/'config.json';p.write_text(json.dumps({'name':'kit-server-dev'}))
            def run(args,label,**options):calls.append(args);return p
            r.run=run
            with self.assertRaises(AssertionError):r.compose(['up','-d'],'must-not-run')
            self.assertEqual(len(calls),1)
            self.assertEqual(calls[0][-3:],['config','--format','json'])
    def test_external_volume_refuses_compose_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            r=self.runtime(directory);p=Path(directory)/'config.json';p.write_text(json.dumps({'name':r.project,'volumes':{'v':{'name':r.project+'_v','external':True}}}))
            calls=[]
            def run(args,label,**options):calls.append(args);return p
            r.run=run
            with self.assertRaises(AssertionError):r.compose(['up','-d'],'must-not-run')
            self.assertEqual(len(calls),1)
    def test_retained_runtime_foreign_label_refuses_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            project='kit-verify-test';calls=[]
            def run(args,label,**options):
                calls.append(args);p=Path(directory)/label
                if label=='cleanup-list-container':p.write_text('abc\n')
                elif label=='cleanup-identities-container':p.write_text(json.dumps([{'Config':{'Labels':{'com.docker.compose.project':'kit-server-dev'}}}]))
                else:self.fail('Unexpected operation: '+label)
                return p
            result=cleanup_owned_resources(project,run)
            self.assertEqual(result['status'],'FAIL')
            self.assertFalse(any('stop' in a or 'rm' in a for a in calls))
    def test_retained_runtime_wrong_project_refuses_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            project='kit-verify-test';calls=[];p=Path(directory)/'config.json'
            p.write_text(json.dumps({'name':'kit-server-dev'}))
            def run(args,label,**options):calls.append(args);return p
            with self.assertRaises(AssertionError):
                validate_compose_identity(project,['docker','compose','-p',project],run)
            self.assertEqual(len(calls),1)
            self.assertEqual(calls[0][-3:],['config','--format','json'])
class ConsumerRedeliveryTests(unittest.TestCase):
    key='epoch:7:1'
    def delivery(self,redelivered=False,key=None,digest='same-wire'):
        return {'type':'consumer-delivery','data':{'message_id':key or self.key,'redelivered':redelivered,'wire_sha256':digest}}
    def commit(self,status='processed',key=None):
        return {'type':'consumer-commit','data':{'result':[{'event_id':key or self.key,'status':status}]}}
    def run_observation(self,directory,stages):
        r=Runtime.__new__(Runtime);r.out=Path(directory);r.health_check=lambda:None
        clock=[0.0];path=r.out/'consumer-trace.jsonl'
        def write(rows):path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        write(stages[0])
        def sleep(_):
            clock[0]+=1
            write(stages[min(int(clock[0]),len(stages)-1)])
        with patch('verification.runtime.time.monotonic',lambda:clock[0]),patch('verification.runtime.time.sleep',sleep):
            return r.consumer_redelivery(self.key),clock[0]
    def test_durable_original_commit_does_not_replace_delayed_replay_commit(self):
        original=[self.delivery(),self.commit()]
        delivered=original+[self.delivery(True)]
        replayed=delivered+[self.commit('already_processed')]
        with tempfile.TemporaryDirectory() as directory:
            evidence,elapsed=self.run_observation(directory,[original,original,delivered,replayed])
            self.assertEqual(elapsed,3)
            self.assertEqual(len(evidence['deliveries']),2)
            self.assertEqual(evidence['duplicate_commits'],[{'event_id':self.key,'status':'already_processed'}])
    def test_missing_replay_fails_at_finite_deadline_with_last_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AssertionError,'Deadline: consumer-redelivery'):
                self.run_observation(directory,[[self.delivery(),self.commit()]])
            last=json.loads((Path(directory)/'consumer-redelivery-last.json').read_text())
            self.assertEqual(len(last['deliveries']),1)
            self.assertEqual(last['duplicate_commits'],[])
    def test_deliveries_without_completed_duplicate_commit_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AssertionError,'Deadline: consumer-redelivery'):
                self.run_observation(directory,[[self.delivery(),self.commit(),self.delivery(True)]])
    def test_other_event_replay_cannot_satisfy_target(self):
        with tempfile.TemporaryDirectory() as directory:
            rows=[self.delivery(),self.commit(),self.delivery(True,'other:8:1'),self.commit('already_processed','other:8:1')]
            with self.assertRaisesRegex(AssertionError,'Deadline: consumer-redelivery'):
                self.run_observation(directory,[rows])
    def test_changed_wire_still_fails_after_duplicate_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            rows=[self.delivery(),self.commit(),self.delivery(True,digest='changed'),self.commit('already_processed')]
            with self.assertRaisesRegex(AssertionError,'wire bytes changed'):
                self.run_observation(directory,[rows])
    def test_missing_broker_redelivery_flag_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            rows=[self.delivery(),self.commit(),self.delivery(),self.commit('already_processed')]
            with self.assertRaisesRegex(AssertionError,'Deadline: consumer-redelivery'):
                self.run_observation(directory,[rows])

class G5MetricsTests(unittest.TestCase):
    total=1000519
    def metrics(self):
        lines=['# HELP pipeline_events_staged_total Immutable staged events.',
               f'pipeline_events_staged_total {self.total}',
               'pipeline_source_pending 0','pipeline_dlq_open{kind="elasticsearch"} 3']
        for component in ('source','pipeline','consumer'):
            lines.extend([f'pipeline_dependency_health{{component="{component}",state="unavailable"}} 0',
                          f'pipeline_observation_fresh_timestamp_seconds{{component="{component}"}} 1000.25'])
        return '\n'.join(lines)+'\n'
    def run_observations(self,directory,stages):
        r=Runtime.__new__(Runtime);r.out=Path(directory);r.health_check=lambda:None
        clock=[0.0];observed=[]
        def status():
            value={'observed_at':str(clock[0])};observed.append(value);return value
        def http(path):
            self.assertEqual(path,'/metrics')
            return stages[min(int(clock[0]),len(stages)-1)]
        def sleep(seconds):clock[0]+=seconds
        r.http=http
        with patch('verification.runtime.time.monotonic',lambda:clock[0]),patch('verification.runtime.time.sleep',sleep):
            result=r.wait(status,lambda s:r.g5_metrics(s,self.total),'settle-G5',timeout=360,interval=5)
        return result,observed,clock[0]
    def test_unavailable_second_observation_requires_new_pair(self):
        available=self.metrics()
        unavailable=available.replace('pipeline_dlq_open{kind="elasticsearch"} 3\n','').replace('component="pipeline",state="unavailable"} 0','component="pipeline",state="unavailable"} 1')
        with tempfile.TemporaryDirectory() as directory:
            result,observed,elapsed=self.run_observations(directory,[unavailable]*5+[available])
            self.assertEqual(elapsed,5)
            self.assertEqual(observed,[{'observed_at':'0.0'},{'observed_at':'5.0'}])
            self.assertEqual(result,observed[-1])
            self.assertEqual(json.loads((Path(directory)/'g5-status.json').read_text()),result)
            self.assertEqual((Path(directory)/'g5-metrics.prom').read_text(),available)
            history=[json.loads(x) for x in (Path(directory)/'g5-metrics-observations.jsonl').read_text().splitlines()]
            self.assertEqual([x['metrics'] for x in history],[unavailable,available])
    def test_permanent_unavailability_fails_existing_deadline(self):
        unavailable=self.metrics().replace('component="pipeline",state="unavailable"} 0','component="pipeline",state="unavailable"} 1')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                self.run_observations(directory,[unavailable])
            history=(Path(directory)/'g5-metrics-observations.jsonl').read_text().splitlines()
            self.assertEqual(len(history),72)
            self.assertEqual(json.loads((Path(directory)/'settle-G5-last.json').read_text())['observed_at'],'355.0')
    def test_slow_fresh_metrics_cannot_pass_after_settle_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Runtime.__new__(Runtime);r.out=Path(directory);r.health_check=lambda:None;clock=[0.0]
            def http(_):clock[0]=360.0;return self.metrics()
            r.http=http
            with patch('verification.runtime.time.monotonic',lambda:clock[0]),patch('verification.runtime.time.sleep',lambda _:None):
                with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                    r.wait(lambda:{'observed_at':'0'},lambda s:r.g5_metrics(s,self.total),'settle-G5',timeout=360)
            self.assertEqual((r.out/'g5-metrics.prom').read_text(),self.metrics())
    def test_incorrect_or_duplicate_dlq_never_passes(self):
        good=self.metrics()
        for bad in [good.replace('elasticsearch"} 3','elasticsearch"} '+n) for n in ('0','2','4')]+[good+'pipeline_dlq_open{kind="elasticsearch"} 3\n']:
            with self.subTest(metrics=bad),tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                    self.run_observations(directory,[bad])
    def test_header_or_wrong_staged_count_never_passes(self):
        good=self.metrics()
        for bad in (good.replace(f'pipeline_events_staged_total {self.total}\n',''),good.replace(str(self.total),'1000518')):
            with self.subTest(metrics=bad),tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                    self.run_observations(directory,[bad])
    def test_pending_source_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                self.run_observations(directory,[self.metrics().replace('pipeline_source_pending 0','pipeline_source_pending 1')])
    def test_missing_or_invalid_freshness_cannot_pass(self):
        good=self.metrics();key='pipeline_observation_fresh_timestamp_seconds{component="pipeline"}'
        for bad in (good.replace(key+' 1000.25\n',''),good.replace(key+' 1000.25',key+' NaN'),good.replace(key+' 1000.25',key+' 0')):
            with self.subTest(metrics=bad),tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(AssertionError,'Deadline: settle-G5'):
                    self.run_observations(directory,[bad])

if __name__=='__main__':unittest.main()
