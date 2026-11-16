import time, json, logging
from pathlib import Path
from sqlalchemy import select, update
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report, PlanRun, PlanStepRun
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
from src.services import plans
from migrations.initial import migrate
logging.basicConfig(level=logging.INFO)
PLAN_QUEUE = plans.PLAN_QUEUE

def execute(job_id):
    with Session() as db:
        j=db.get(Job,job_id)
        if not j or j.status!='queued':return
        j.status='running'; db.commit()
        try:
            d=db.get(Dataset,j.dataset_id)
            result=analyze(d.records,j.params)
            json.dumps(result,allow_nan=False)
            j.result=result;j.status='completed';j.finished_at=time.time()
            record(db,'worker','job.completed',{'id':j.id})
        except Exception as e:
            logging.exception('Analysis failed')
            j.status='failed';j.error=str(e)[:990];j.finished_at=time.time()
            record(db,'worker','job.failed',{'id':j.id,'error':j.error})
        db.commit()

def claim_step(step_run_id):
    """原子领取：返回已固定的输入批次、参数与尝试序号；并发消费者只有一个能领取成功。"""
    with Session() as db:
        step=db.get(PlanStepRun,step_run_id)
        if not step or step.status!='queued':return None
        run=db.get(PlanRun,step.run_id)
        if not run or run.status=='superseded':
            step.status='invalidated';step.error='运行已被取代，步骤取消'
            record(db,'worker','plan.step.invalidated',{'run':step.run_id,'step':step.step_key})
            db.commit();return None
        attempt=step.attempt+1
        claimed=db.execute(update(PlanStepRun).where(PlanStepRun.id==step.id,PlanStepRun.status=='queued',PlanStepRun.attempt==step.attempt).values(status='running',attempt=attempt,started_at=time.time())).rowcount
        db.commit()
        if claimed!=1:return None
        return {'run_id':run.id,'step_id':step.id,'step_key':step.step_key,'attempt':attempt,
                'input_batch_ids':list(step.input_batch_ids),'params':dict(step.params),
                'dataset_id':run.dataset_id}

def commit_step(claim,result,count=0):
    """条件提交：步骤必须仍是本次领取的 running 状态且尝试序号一致，否则视为迟到结果拒收。"""
    with Session() as db:
        written=db.execute(update(PlanStepRun).where(
            PlanStepRun.id==claim['step_id'],PlanStepRun.status=='running',
            PlanStepRun.attempt==claim['attempt']).values(
            result=result,status='completed',error='',finished_at=time.time())).rowcount
        if written!=1:
            record(db,'worker','plan.step.late_rejected',
                   {'run':claim['run_id'],'step':claim['step_key'],'attempt':claim['attempt']})
            db.commit();return False
        record(db,'worker','plan.step.completed',{'run':claim['run_id'],'step':claim['step_key'],
                                                  'inputs':count,'attempt':claim['attempt']})
        db.commit()
        run=db.get(PlanRun,claim['run_id'])
        if run:
            plans.advance_run(db,run,'worker');db.commit()
        return True

def fail_step(claim,message):
    with Session() as db:
        written=db.execute(update(PlanStepRun).where(
            PlanStepRun.id==claim['step_id'],PlanStepRun.status=='running',
            PlanStepRun.attempt==claim['attempt']).values(
            status='failed',error=message[:990],finished_at=time.time())).rowcount
        if written!=1:
            record(db,'worker','plan.step.late_rejected',
                   {'run':claim['run_id'],'step':claim['step_key'],'attempt':claim['attempt']})
            db.commit();return False
        record(db,'worker','plan.step.failed',{'run':claim['run_id'],'step':claim['step_key'],'error':message[:990]})
        db.commit()
        run=db.get(PlanRun,claim['run_id'])
        if run:
            plans.advance_run(db,run,'worker');db.commit()
        return True

def execute_step(step_run_id,claim=None):
    claim=claim or claim_step(step_run_id)
    if not claim:return
    try:
        with Session() as db:
            d=db.get(Dataset,claim['dataset_id'])
            if not d:raise ValueError('数据批次已不存在')
            wanted=set(str(b) for b in claim['input_batch_ids'])
            records=[r for r in d.records if str(r.get('batch_id')) in wanted]
        if len(records)!=len(wanted):raise ValueError('所选上游族群的部分原始批次已不在数据批次中')
        result=analyze(records,claim['params'])
        json.dumps(result,allow_nan=False)
        commit_step(claim,result,len(records))
    except Exception as e:
        logging.exception('Plan step failed')
        fail_step(claim,str(e))

def resume():
    """断点恢复：中断的步骤回到排队，未入队步骤补投，失败/失效向下游传播。"""
    with Session() as db:
        recovered=plans.resume_all(db)
        db.commit()
        queued=list(db.scalars(select(PlanStepRun.id).where(PlanStepRun.status=='queued').limit(20)))
    for step_id in queued:execute_step(step_id)
    return recovered

def tick():
    with Session() as db:
        queued=list(db.scalars(select(Job.id).where(Job.status=='queued').limit(3)))
    for job_id in queued:execute(job_id)
    resume()
    with Session() as db:
        reports=list(db.scalars(select(Report).where(Report.enabled==True,Report.next_run<=time.time())))
        for r in reports:
            try:
                d=deliver(db,r)
                if d:record(db,'worker','report.delivered',{'id':r.id,'delivery':d.id})
                db.commit()
            except Exception:
                logging.exception('Delivery failed');db.rollback()
                record(db,'worker','report.failed',{'id':r.id})
                db.commit()

def main():
    # 幂等建表，避免与 backend 启动竞争时缺表；Compose 中 worker 仍等待 backend 健康。
    migrate()
    # This Compose stack has a single worker. A crash must not strand its jobs.
    with Session() as db:
        for j in db.scalars(select(Job).where(Job.status=='running')):
            j.status='queued';record(db,'worker','job.recovered',{'id':j.id})
        db.commit()
        # 启动即恢复：崩溃时正在执行的步骤立即回到排队，沿用已建立的输入，不重复创建。
        plans.resume_all(db,stale_after=0);db.commit()
    r=None; next_connect=0
    while True:
        Path('/tmp/heartbeat').touch()
        item=None
        if r is None and time.time()>=next_connect:
            try: r=Redis.from_url(REDIS_URL,socket_timeout=3)
            except Exception: r=None
        if r is not None:
            try:
                item=r.blpop(['analysis:queue',PLAN_QUEUE],timeout=1)
            except Exception:
                # Redis 不可用时 SQL 排队状态兜底：断开连接，稍后重连并继续扫描。
                logging.warning('Queue unavailable; falling back to SQL scan')
                r=None; next_connect=time.time()+15
        if item:
            try:
                queue=item[0].decode() if isinstance(item[0],bytes) else item[0]
                value=int(item[1])
                if queue==PLAN_QUEUE:execute_step(value)
                else:execute(value)
            except Exception:
                logging.exception('Dequeued task failed')
        elif r is None:
            time.sleep(0.5)  # 无队列连接时由 SQL 轮询兜底，避免忙循环
        try:
            tick()
        except Exception:
            logging.exception('Worker tick failed');time.sleep(1)
if __name__=='__main__':main()
