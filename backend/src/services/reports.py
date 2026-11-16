import time, os, csv, io, hashlib, json
from fastapi import HTTPException
from sqlalchemy import select
from src.models import Job, Report, Delivery, PlanRun, PlanStepRun
from src.config.settings import ARTIFACT_DIR

def create(db, data):
    if bool(data.job_id)==bool(data.run_id) or (not data.job_id and not data.run_id):
        raise HTTPException(422,'简报须且仅须绑定一个已批准的分析结果或方案运行')
    if data.run_id:
        run=db.get(PlanRun,data.run_id)
        if not run or run.status!='completed' or not run.approved:
            raise HTTPException(409,'请先选择已完成且获批准的方案运行')
        r=Report(name=data.name, job_id=None, run_id=run.id, destination=data.destination, interval_seconds=data.interval_seconds, next_run=time.time()+data.delay_seconds, enabled=True)
        db.add(r); db.flush()
        return r
    j=db.get(Job,data.job_id)
    if not j or j.status!='completed' or not j.approved:
        raise HTTPException(409,'请先选择已完成且获批准的分析结果')
    r=Report(name=data.name, job_id=j.id, run_id=None, destination=data.destination, interval_seconds=data.interval_seconds, next_run=time.time()+data.delay_seconds, enabled=True)
    db.add(r); db.flush()
    return r

def source(db, report):
    """简报来源快照；失效或未获批准时返回 None，绝不输出过期结论。"""
    if report.run_id:
        run=db.get(PlanRun,report.run_id)
        if not run or run.status!='completed' or not run.approved: return None
        steps=list(db.scalars(select(PlanStepRun).where(PlanStepRun.run_id==run.id)))
        if not steps or any(s.status!='completed' for s in steps): return None
        step=next((s for s in steps if s.step_key==run.output_step),None)
        if not step: return None
        return {'name':f'{run.plan_name} · 运行#{run.id}', 'params':{'plan_version':run.plan_version,'output_step':run.output_step},
                'result':step.result, 'kind':'plan'}
    j=db.get(Job,report.job_id)
    if not j or not j.approved or j.status!='completed' or not j.result: return None
    return {'name':j.name, 'params':j.params, 'result':j.result, 'kind':'job'}

def deliver(db, report):
    src=source(db,report)
    if not src:
        # 结果已失效：暂停计划，避免继续输出过期结论。
        if report.enabled:
            report.enabled=False
            from src.services.audit import record
            record(db,'worker','report.paused_invalid',{'id':report.id,'run':report.run_id,'job':report.job_id})
        return
    j=src
    out=io.StringIO()
    out.write('\ufeff')
    table=j['result'].get('table',{})
    cols=table.get('columns',[])
    writer=csv.writer(out)
    def safe(v):
        if isinstance(v,(int,float)) and not isinstance(v,bool): return v
        text=str(v)
        return "'"+text if text.startswith(('=','+','-','@','\t','\r')) else text
    writer.writerow([safe(c['label']) for c in cols])
    for row in table.get('rows',[]): writer.writerow([safe(row.get(c['key'],'')) for c in cols])
    data=out.getvalue().encode('utf-8')
    delivery=Delivery(report_id=report.id, created_at=time.time(), checksum=hashlib.sha256(data).hexdigest(), filename='', destination=report.destination)
    db.add(delivery); db.flush()
    delivery.filename=f'report-{report.id}-delivery-{delivery.id}.csv'
    os.makedirs(ARTIFACT_DIR,exist_ok=True)
    with open(os.path.join(ARTIFACT_DIR,delivery.filename),'wb') as f:f.write(data)
    with open(os.path.join(ARTIFACT_DIR,delivery.filename+'.json'),'w') as f: json.dump({'job':j['name'],'params':j['params'],'result':j['result'],'checksum':delivery.checksum,'source':j['kind']},f,ensure_ascii=False,allow_nan=False)
    report.next_run=time.time()+report.interval_seconds
    return delivery
