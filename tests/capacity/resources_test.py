"""Pure resource-report parsing/ownership tests; not measured hardware results."""
from pathlib import Path
import json
import sys
import tempfile
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
if __name__=='__main__':unittest.main()
