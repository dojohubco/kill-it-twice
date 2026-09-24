"""Pure resource-report parsing/ownership tests; not measured hardware results."""
from pathlib import Path
import json
import sys
import tempfile
import subprocess
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/capacity'))
from resources import ResourceSampler,memory_fields

class ResourceTests(unittest.TestCase):
    def test_byte_units_and_missing_values(self):
        value=memory_fields('Name: node\nVmRSS: 123 kB\nVmHWM: 456 kB\nMemAvailable: 1024 kB\nSwapFree: unknown\nVmSize: 999 kB\n')
        self.assertEqual(value,{'VmRSS':125952,'VmHWM':466944,'MemAvailable':1048576})
        self.assertEqual(memory_fields('VmRSS: 22 MB\nVmHWM: -1 kB\n'),{})
    def test_foreign_container_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler=ResourceSampler('kit-final-fixture',directory)
            outputs=[b'owned\n',json.dumps([{'Config':{'Labels':{'com.docker.compose.project':'other-project'}}}]).encode()]
            with patch.object(sampler,'command',side_effect=outputs):
                with self.assertRaises(AssertionError):sampler.sample()
            self.assertFalse((Path(directory)/'resources.jsonl').exists())
    def test_failure_remains_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler=ResourceSampler('kit-final-fixture',directory)
            sampler.errors.append({'type':'PermissionError','phase':'read_only_sample'})
            result=sampler.finish();self.assertEqual(result['status'],'FAIL');self.assertEqual(result['samples'],0)
    def test_exact_missing_container_is_recorded(self):
        identity='a'*64
        with tempfile.TemporaryDirectory() as directory:
            sampler=ResourceSampler('kit-final-fixture',directory);missing=[]
            result=subprocess.CompletedProcess([],1,b'[]',('error: no such object: '+identity+'\n').encode())
            with patch('resources.subprocess.run',return_value=result):
                self.assertEqual(sampler.command(['docker','inspect',identity],disappeared=missing),b'[]')
            self.assertEqual(missing,[identity])
    def test_other_inspection_errors_still_fail(self):
        identity='a'*64
        errors=[(1,b'permission denied'),(1,b'Cannot connect to the Docker daemon'),
                (1,('error: no such object: '+'b'*64).encode()),
                (1,('error: no such object: '+identity+'\npermission denied').encode()),
                (2,('error: no such object: '+identity).encode())]
        with tempfile.TemporaryDirectory() as directory:
            sampler=ResourceSampler('kit-final-fixture',directory)
            for code,stderr in errors:
                with self.subTest(stderr=stderr,code=code),patch('resources.subprocess.run',return_value=subprocess.CompletedProcess([],code,b'[]',stderr)):
                    with self.assertRaises(RuntimeError):sampler.command(['docker','inspect',identity],disappeared=[])
    def test_incomplete_success_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler=ResourceSampler('kit-final-fixture',directory)
            with patch.object(sampler,'command',side_effect=[('a'*64).encode(),b'[]']):
                with self.assertRaisesRegex(AssertionError,'Incomplete'):sampler.sample()
            self.assertFalse((Path(directory)/'resources.jsonl').exists())
if __name__=='__main__':unittest.main()
