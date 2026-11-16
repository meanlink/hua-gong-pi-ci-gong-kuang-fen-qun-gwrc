from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from src.config.database import get_db
from src.models import AnalysisPlan, PlanRun, Dataset, PlanStepRun
from src.schemas.plan import PlanInput
from src.middlewares.auth import current_user, can_write, admin_only
from src.services import plans
from src.services.audit import record
router = APIRouter()


def _plan(db, plan_id):
    plan = db.get(AnalysisPlan, plan_id)
    if not plan:
        raise HTTPException(404, '分析方案不存在')
    return plan


def _run(db, run_id):
    run = db.get(PlanRun, run_id)
    if not run:
        raise HTTPException(404, '方案运行不存在')
    return run


def _runs_for(db, plan_id, limit=20):
    return list(db.scalars(select(PlanRun).where(PlanRun.plan_id == plan_id)
                           .order_by(PlanRun.id.desc()).limit(limit)))


@router.get('/plans')
def list_plans(db=Depends(get_db), u=Depends(current_user)):
    result = []
    for plan in db.scalars(select(AnalysisPlan).order_by(AnalysisPlan.id.desc())):
        result.append(plans.plan_dict(plan, _runs_for(db, plan.id, 1)))
    return result


@router.post('/plans', status_code=201)
def create_plan(data: PlanInput, db=Depends(get_db), u=Depends(can_write)):
    plan = plans.create_plan(db, data, u)
    record(db, u.email, 'plan.create', {'id': plan.id, 'steps': len(plan.definition['steps'])})
    db.commit()
    return plans.plan_dict(plan)


@router.get('/plans/{id}')
def plan_detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    plan = _plan(db, id)
    runs = _runs_for(db, id)
    return plans.plan_dict(plan, runs)


@router.put('/plans/{id}')
def update_plan(id: int, data: PlanInput, db=Depends(get_db), u=Depends(can_write)):
    plan = _plan(db, id)
    if plan.created_by != u.id and u.role != 'admin':
        raise HTTPException(403, '仅方案创建者或负责人可以修改方案')
    plans.update_plan(db, plan, data)
    record(db, u.email, 'plan.update', {'id': plan.id, 'version': plan.version})
    db.commit()
    return plans.plan_dict(plan)


@router.post('/plans/{id}/impact')
def impact(id: int, data: PlanInput, db=Depends(get_db), u=Depends(current_user)):
    """预演方案改动：对比最近一次运行，标出沿用、重算与因上游失效而重算的步骤。"""
    plan = _plan(db, id)
    definition = plans.normalize_definition(data.definition)
    dataset_id = data.dataset_id or plan.dataset_id
    dataset = db.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(404, '数据批次不存在')
    prints = plans.fingerprints(definition, dataset.checksum)
    runs = _runs_for(db, id, 5)
    baseline = next((r for r in runs if r.definition), None)
    old = {}
    if baseline:
        old = {s.step_key: s for s in db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == baseline.id))}
    steps = []
    for step in definition['steps']:
        previous = old.get(step['key'])
        if baseline and baseline.dataset_checksum == dataset.checksum and previous and previous.fingerprint == prints[step['key']] and previous.status == 'completed':
            action, reason = 'reuse', '输入与参数未变，沿用上次成功结果'
        elif previous and previous.fingerprint != prints[step['key']]:
            action, reason = 'recompute', '本步参数或上游结果变化，需重算'
        elif not previous:
            action, reason = 'recompute', '新步骤'
        else:
            action, reason = 'recompute', '上次步骤未成功，本次重新计算'
        steps.append({'step_key': step['key'], 'name': step['name'], 'action': action, 'reason': reason,
                      'fingerprint': prints[step['key']], 'parents': step['parents']})
    return {'dataset_id': dataset_id, 'dataset_checksum': dataset.checksum,
            'baseline_run': plans.run_summary(baseline) if baseline else None, 'steps': steps}


@router.post('/plans/{id}/runs', status_code=201)
def start_run(id: int, db=Depends(get_db), u=Depends(can_write)):
    plan = _plan(db, id)
    run = plans.create_run(db, plan, u)
    record(db, u.email, 'plan.run.create', {'plan': id, 'run': run.id})
    db.commit()
    return plans.run_dict(db, run, detail=True)


@router.get('/runs')
def list_runs(plan_id: int | None = None, db=Depends(get_db), u=Depends(current_user)):
    query = select(PlanRun).order_by(PlanRun.id.desc()).limit(60)
    if plan_id:
        query = select(PlanRun).where(PlanRun.plan_id == plan_id).order_by(PlanRun.id.desc()).limit(60)
    return [plans.run_dict(db, r) for r in db.scalars(query)]


@router.get('/runs/{id}')
def run_detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    return plans.run_dict(db, _run(db, id), detail=True)


@router.post('/runs/{id}/approve')
def approve_run(id: int, db=Depends(get_db), u=Depends(admin_only)):
    run = _run(db, id)
    plans.approve_run(db, run, u)
    db.commit()
    return plans.run_dict(db, run, detail=True)


@router.post('/runs/{id}/retry')
def retry_run(id: int, db=Depends(get_db), u=Depends(can_write)):
    run = _run(db, id)
    plans.retry_run(db, run, u)
    db.commit()
    return plans.run_dict(db, run, detail=True)
