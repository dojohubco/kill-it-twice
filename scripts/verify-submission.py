"""Single final entry point: current checks execute; historical summaries never confer PASS."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from verification.runtime import Runtime, ROOT, now, sha

parser=argparse.ArgumentParser()
parser.add_argument('--count',type=int,choices=(1000000,2000000),default=1000000)
options=parser.parse_args()
r=Runtime(options.count)
r.report.update(scope='Final submission acceptance; real G1-G5 use the selected large dataset',mode='submission',required_phases=['quality','ui-build','ui-fixtures','retained-runtime','large-faults'],phases=[],gate_report={g:'NOT RUN' for g in ('G1','G2','G3','G4','G5')})
r.save();print('Submission manifest: '+str(r.out/'run.json'),flush=True)
phase='admission'
try:
    assert not r.dirty,'make verify requires clean committed code'
    for phase,args,timeout in [
        # Shared-server component measurements total about 41-45 minutes.
        ('quality',['make','quality'],3600),
        ('ui-build',['npm','run','build:ui'],300),
        ('ui-fixtures',['npm','run','test:ui'],600),
        ('retained-runtime',[sys.executable,'-B','scripts/verify-runtime.py'],1200),
        ('large-faults',[sys.executable,'-B','scripts/verify-final.py','--count',str(options.count),'--page-records','64'],21600*(options.count//1000000)+14700),
    ]:
        print('PHASE '+phase,flush=True)
        log=r.run(args,phase,timeout=timeout,graceful=240)
        result={'phase':phase,'status':'PASS','command':args,'log':log.name}
        if phase in ('retained-runtime','large-faults'):
            manifests=[line[6:] for line in log.read_text().splitlines() if line.startswith('PASS: ') and line.endswith('/run.json')]
            assert len(manifests)==1,'Missing unique completed child manifest'
            manifest=Path(manifests[0]);child=json.loads(manifest.read_text())
            assert child['status']=='PASS' and child['cleanup']['status']=='PASS' and child['head']==r.head and not child['developmental']
            result.update(manifest=str(manifest.relative_to(ROOT)),sha256=sha(manifest))
            if phase=='large-faults':
                assert child['count']==options.count and child['reconciliation']['baselines']==options.count
                assert child['gate_report']=={g:'PASS' for g in r.report['gate_report']}
                assert len(child['negative_controls'])==5 and child['resources']['status']=='OBSERVED'
                r.report['gate_report']=child['gate_report']
        r.report['phases'].append(result);r.save()
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==r.head
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    r.report['status']='PASS'
except BaseException as error:
    r.report.update(status='FAIL',error={'phase':phase,'type':type(error).__name__,'message':str(error)})
    r.report['phases'].append({'phase':phase,'status':'FAIL'})
    # Recover only this invocation's child report, including FAIL versus NOT RUN.
    if phase=='large-faults':
        candidates=sorted(r.out.glob('*large-faults.stdout.log'))
        if candidates:
            lines=candidates[-1].read_text().splitlines()
            for line in lines:
                path=Path(line)
                if path.parent==ROOT/'artifacts/final' and (path/'run.json').is_file():
                    child=json.loads((path/'run.json').read_text())
                    r.report['gate_report']=child.get('gate_report',r.report['gate_report'])
                    r.report['failed_child']=str((path/'run.json').relative_to(ROOT))
finally:
    done={p['phase'] for p in r.report['phases']}
    r.report['phases'] += [{'phase':p,'status':'NOT RUN'} for p in r.report['required_phases'] if p not in done]
    r.report.update(finished_at=now(),cleanup={'status':'DELEGATED','scope':'Each service-owning child must independently prove cleanup PASS'})
    r.save()
for gate,status in r.report['gate_report'].items():print(gate+' '+status)
print(r.report['status']+': '+str(r.out/'run.json'),flush=True)
sys.exit(0 if r.report['status']=='PASS' else 1)
