"""Synthetic contract fixtures only; not hosted-kernel or service evidence."""
from pathlib import Path
import copy,sys,unittest,subprocess
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts/capacity'))
from observations import sample,complete,unavailable

def snapshot():
    def section(data):return {'freshness':'fresh','observed_at':'2026-09-22T10:00:00Z','health':'healthy','data':data,'last_known':None}
    return {'backfill':{'phase':'complete'},'dependencies':{
      'source':section({'counts':{'pending':'0'}}),
      'pipeline':section({'staged':'32768','deliveries':[{'sink':'elasticsearch','state':'satisfied','count':'32768'},{'sink':'rabbitmq','state':'satisfied','count':'32768'}],'observations':[{'state':'processed','count':'32768'}]}),
      'consumer':section({'processed':'32768','effects':'0','quarantine':'0'})}}

class ObservationContract(unittest.TestCase):
    def test_cli_admits_explicit_scanner_option_without_starting_services(self):
        script=Path(__file__).resolve().parents[2]/'scripts/capacity/pilot.py'
        help_result=subprocess.run([sys.executable,'-B',str(script),'--help'],capture_output=True,text=True,timeout=5)
        self.assertEqual(help_result.returncode,0);self.assertIn('--scanners',help_result.stdout)
        invalid=subprocess.run([sys.executable,'-B',str(script),'--scanners','0'],capture_output=True,text=True,timeout=5)
        self.assertNotEqual(invalid.returncode,0);self.assertNotIn('kit-final-',invalid.stdout)
    def test_exact_completion(self):
        value=sample(snapshot(),1)
        self.assertTrue(complete(value,32768))
        self.assertFalse(complete(value,8192))
        changed=copy.deepcopy(value);changed['deliveries'][0]['count']='32767'
        self.assertFalse(complete(changed,32768))
    def test_valid_unavailable_section(self):
        value=snapshot();value['dependencies']['pipeline'].update(data=None,freshness='unknown',health='unknown')
        result=sample(value,5)
        self.assertIsNone(result['staged']);self.assertIsNone(result['deliveries'])
        self.assertEqual(result['consumer']['processed'],'32768')
        self.assertFalse(complete(result,32768))
    def test_last_known_is_not_current(self):
        value=snapshot();old=copy.deepcopy(value['dependencies']['pipeline']['data'])
        value['dependencies']['pipeline'].update(data=None,freshness='stale',last_known={'data':old})
        result=sample(value,8)
        self.assertIsNone(result['staged']);self.assertFalse(complete(result,32768))
        value['dependencies']['pipeline']['data']=old
        self.assertIsNone(sample(value,9)['staged'])
    def test_network_error_is_explicit_unknown(self):
        value=unavailable(10,TimeoutError('synthetic timeout'))
        self.assertEqual(value['observation_error'],'TimeoutError')
        self.assertIsNone(value['staged']);self.assertFalse(complete(value,32768))
    def test_malformed_contract_is_not_hidden(self):
        value=snapshot();del value['dependencies']['pipeline']['data']['staged']
        with self.assertRaises(KeyError):sample(value,10)
    def test_terminal_run_alone_is_not_current_convergence(self):
        value=sample(snapshot(),11);value['consumer']['processed']='32767'
        self.assertFalse(complete(value,32768))
        value=sample(snapshot(),11);value['observations'][0]['state']='pending'
        self.assertFalse(complete(value,32768))

if __name__=='__main__':unittest.main()
