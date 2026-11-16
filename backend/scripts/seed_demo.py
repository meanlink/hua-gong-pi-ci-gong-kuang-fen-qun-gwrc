import time, json
from sqlalchemy import select, func
from src.config.database import Session
from migrations.initial import migrate
from src.models import User, Dataset, Job, Report, Delivery, Audit, AnalysisPlan, PlanRun, PlanStepRun
from src.services.business import metadata, seed_records, analyze
from src.services.reports import deliver
from src.services import plans as plan_service
from src.utils.security import hash_password
from src.utils.serialization import checksum
def seed():
    migrate()
    with Session() as db:
        for role,name in [('admin','林知 · 业务负责人'),('analyst','陈舟 · 分析员'),('viewer','周禾 · 观察员')]:
            email=role+'@demo.local'
            if not db.scalar(select(User).where(User.email==email)):
                db.add(User(email=email,name=name,role=role,password=hash_password('Demo123!')))
        db.flush()
        m=metadata()
        d=db.scalar(select(Dataset).where(Dataset.name==m['dataset_name']))
        if not d:
            rows=seed_records()
            d=Dataset(name=m['dataset_name'],source=m['short']+'-202611.csv',records=rows,checksum=checksum(rows),created_at=time.time())
            db.add(d); db.flush()
        if not db.scalar(select(Job).where(Job.name=='基线复核 · '+m['name'])):
            params={p['key']:p['default'] for p in m.get('parameters',[])}
            result=analyze(d.records,params)
            json.dumps(result,ensure_ascii=False,allow_nan=False)
            admin=db.scalar(select(User).where(User.role=='admin'))
            j=Job(name='基线复核 · '+m['name'],dataset_id=d.id,params=params,status='completed',result=result,created_by=admin.id,created_at=time.time()-3600,finished_at=time.time()-3595,approved=True)
            db.add(j);db.flush()
            r=Report(name='每日业务复核简报',job_id=j.id,destination='业务复核组 · 本地收件箱',interval_seconds=86400,next_run=time.time()+86400,enabled=True)
            db.add(r);db.flush();deliver(db,r)
            db.add(Audit(actor='seed',action='demo.baseline',detail={'dataset':d.id},at=time.time()))
        db.flush()
        plan=db.scalar(select(AnalysisPlan).where(AnalysisPlan.name=='工况分支汇合方案'))
        if not plan:
            admin=db.scalar(select(User).where(User.role=='admin'))
            definition={'output_step':'merge','steps':[
                {'key':'base','name':'全批工况分群','kind':'cluster','params':{'clusters':4}},
                {'key':'high','name':'高温窗口支线','kind':'cluster','parents':['base'],'select':{'base':[3,4]},'params':{'clusters':3}},
                {'key':'low','name':'低温窗口支线','kind':'cluster','parents':['base'],'select':{'base':[1,2]},'params':{'clusters':2}},
                {'key':'merge','name':'支线批次汇合','kind':'merge','parents':['high','low'],'params':{}},
            ]}
            plan=AnalysisPlan(name='工况分支汇合方案',dataset_id=d.id,definition=plan_service.normalize_definition(definition),output_step='merge',version=1,created_by=admin.id,created_at=time.time(),updated_at=time.time())
            db.add(plan);db.flush()
            db.add(Audit(actor='seed',action='plan.create',detail={'id':plan.id},at=time.time()))
        db.commit()
        counts={model.__tablename__:db.scalar(select(func.count()).select_from(model)) for model in [User,Dataset,Job,Report,Delivery,Audit,AnalysisPlan,PlanRun,PlanStepRun]}
        print(json.dumps(counts,ensure_ascii=False))
        return counts
if __name__=='__main__':seed()
