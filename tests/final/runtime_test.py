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

if __name__=='__main__':unittest.main()
