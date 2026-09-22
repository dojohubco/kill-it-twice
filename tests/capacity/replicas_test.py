"""Pure replica-bound and no-service-launch controls, not a throughput result."""
from pathlib import Path
import subprocess
import sys
import unittest
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/capacity'))
from replicas import replica_counts, compose_scales

class Replicas(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(replica_counts(1, 1), {'backfill':1, 'es-worker':1, 'publisher':1, 'consumer':1, 'observer':1})
    def test_bounded_parallel_set(self):
        self.assertEqual(replica_counts(4, 2), {'backfill':4, 'es-worker':2, 'publisher':2, 'consumer':2, 'observer':2})
    def test_invalid_scanners(self):
        for value in (0, 5, -1, 1.0, '1', None, True):
            with self.subTest(value=value), self.assertRaises(ValueError): replica_counts(value, 1)
    def test_invalid_sinks(self):
        for value in (0, 3, -1, 1.0, '1', None, True):
            with self.subTest(value=value), self.assertRaises(ValueError): replica_counts(1, value)
    def test_compose_arguments(self):
        self.assertEqual(compose_scales(4, 2), ['--scale','backfill=4','--scale','es-worker=2','--scale','publisher=2','--scale','consumer=2','--scale','observer=2'])
    def test_invalid_cli_creates_no_fixture(self):
        before=set((ROOT/'artifacts/final').glob('kit-final-*'))
        for value in ('0', '3', '2.5', 'unlimited'):
            result=subprocess.run([sys.executable,'-B','scripts/capacity/pilot.py','--sink-workers',value],cwd=ROOT,capture_output=True,text=True,timeout=5)
            self.assertNotEqual(result.returncode,0)
            self.assertEqual(result.stdout,'')
        self.assertEqual(set((ROOT/'artifacts/final').glob('kit-final-*')),before)
if __name__ == '__main__': unittest.main()
