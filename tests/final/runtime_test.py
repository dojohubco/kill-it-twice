"""Ownership refusal tests; these do not substitute for real fault acceptance."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
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
if __name__=='__main__':unittest.main()
