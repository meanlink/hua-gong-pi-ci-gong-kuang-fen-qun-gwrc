"""可分叉汇合的分析方案：DAG 校验、运行快照、逐步调度、失效重算与断点恢复。"""
import hashlib
import json
import re
import time

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import select

from src.config.settings import REDIS_URL
from src.models import AnalysisPlan, Dataset, PlanRun, PlanStepRun
from src.services.audit import record

PLAN_QUEUE = 'plan:queue'
KEY_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9_-]{0,39}$')
KINDS = ('cluster', 'merge')
MAX_STEPS = 20
ACTIVE = ('pending', 'queued', 'running')


def _fail(status, message):
    raise HTTPException(status, message)


def _norm_int(value, label, low, high):
    try:
        number = float(value)
        integer = int(number)
    except (TypeError, ValueError, OverflowError):
        _fail(422, f'{label} 必须为 {low}~{high} 的整数')
    if number != integer or not low <= integer <= high:
        _fail(422, f'{label} 必须为 {low}~{high} 的整数')
    return integer


def normalize_definition(definition):
    """校验并归一化方案定义；悬空上游、非法选取和循环依赖都会被拒绝。"""
    if not isinstance(definition, dict):
        _fail(422, '方案定义须为对象')
    raw_steps = definition.get('steps')
    if not isinstance(raw_steps, list) or not raw_steps:
        _fail(422, '方案至少需要一个分析步骤')
    if len(raw_steps) > MAX_STEPS:
        _fail(422, f'方案最多 {MAX_STEPS} 个步骤')
    steps = []
    keys = set()
    for index, raw in enumerate(raw_steps):
        if not isinstance(raw, dict):
            _fail(422, f'第 {index + 1} 个步骤格式不正确')
        key = str(raw.get('key', '')).strip()
        if not KEY_RE.match(key):
            _fail(422, f'步骤键「{key or "空"}」须以字母开头，仅含字母、数字、下划线或连字符，最长40字符')
        if key in keys:
            _fail(422, f'步骤键「{key}」重复')
        keys.add(key)
        name = str(raw.get('name', '')).strip() or key
        if len(name) > 160:
            _fail(422, f'步骤「{key}」名称过长')
        kind = str(raw.get('kind', 'cluster'))
        if kind not in KINDS:
            _fail(422, f'步骤「{key}」类型须为 cluster 或 merge')
        parents = raw.get('parents') or []
        if not isinstance(parents, list) or any(not isinstance(p, str) for p in parents):
            _fail(422, f'步骤「{key}」的上游步骤须为步骤键列表')
        parents = [p.strip() for p in parents if p.strip()]
        if len(parents) != len(set(parents)):
            _fail(422, f'步骤「{key}」重复声明了上游步骤')
        if key in parents:
            _fail(422, f'步骤「{key}」不能依赖自身')
        select_raw = raw.get('select') or {}
        if not isinstance(select_raw, dict):
            _fail(422, f'步骤「{key}」的上游族群选取格式不正确')
        select = {}
        for parent, groups in select_raw.items():
            if parent not in parents:
                _fail(422, f'步骤「{key}」选取了未声明为上游的步骤「{parent}」')
            if groups in (None, '', []):
                continue
            if not isinstance(groups, list) or len(groups) > 6:
                _fail(422, f'步骤「{key}」对「{parent}」的族群选取须为族群编号列表')
            picked = []
            for group in groups:
                g = _norm_int(group, f'步骤「{key}」的族群编号', 1, 6)
                if g not in picked:
                    picked.append(g)
            if picked:
                select[parent] = sorted(picked)
        params = raw.get('params') or {}
        if not isinstance(params, dict):
            _fail(422, f'步骤「{key}」参数格式不正确')
        if kind == 'merge':
            params = {}
        else:
            clusters = _norm_int(params.get('clusters', 3), f'步骤「{key}」的工况族群数量K', 2, 6)
            params = {'clusters': clusters}
        if kind == 'cluster' and len(parents) > 1:
            _fail(422, f'步骤「{key}」有多个上游，请改用汇合步骤（merge）以合并分支')
        if kind == 'merge' and len(parents) < 2:
            _fail(422, f'汇合步骤「{key}」至少需要两个上游分支')
        steps.append({'key': key, 'name': name, 'kind': kind, 'parents': parents,
                      'select': select, 'params': params})

    by_key = {s['key']: s for s in steps}
    for step in steps:
        for parent in step['parents']:
            if parent not in by_key:
                _fail(422, f'步骤「{step["key"]}」引用了不存在的上游步骤「{parent}」')
            parent_step = by_key[parent]
            if step['select'].get(parent) and parent_step['kind'] != 'cluster':
                _fail(422, f'步骤「{step["key"]}」不能按族群选取汇合步骤「{parent}」')
            if step['select'].get(parent):
                limit = parent_step['params'].get('clusters', 6)
                bad = [g for g in step['select'][parent] if g > limit]
                if bad:
                    _fail(422, f'步骤「{step["key"]}」选取了「{parent}」不存在的族群编号 {bad}')
    order = _topological(steps)
    output_step = str(definition.get('output_step') or '').strip()
    if not output_step:
        output_step = order[-1]['key']
    if output_step not in by_key:
        _fail(422, f'输出步骤「{output_step}」不存在')
    children = {k: [] for k in keys}
    for step in steps:
        for parent in step['parents']:
            children[parent].append(step['key'])
    if children[output_step]:
        _fail(422, f'输出步骤「{output_step}」还有下游步骤，请选择方案末端的步骤作为输出')
    return {'steps': order, 'output_step': output_step}


def _topological(steps):
    by_key = {s['key']: s for s in steps}
    pending = {s['key']: len(s['parents']) for s in steps}
    ready = [s['key'] for s in steps if not s['parents']]
    order = []
    while ready:
        key = ready.pop(0)
        order.append(by_key[key])
        for step in steps:
            if key in step['parents']:
                pending[step['key']] -= 1
                if pending[step['key']] == 0:
                    ready.append(step['key'])
    if len(order) != len(steps):
        cycle = sorted(k for k, v in pending.items() if v > 0)
        _fail(422, f'方案存在循环依赖，无法执行：{", ".join(cycle)}')
    return order


def fingerprints(definition, dataset_checksum):
    """按拓扑序计算每步输入指纹；参数或上游变化会沿依赖链传导。"""
    result = {}
    for step in definition['steps']:
        payload = {
            'key': step['key'], 'kind': step['kind'], 'params': step['params'],
            'parents': step['parents'], 'select': step['select'],
            'dataset': dataset_checksum,
            'upstream': {p: result[p] for p in step['parents']},
        }
        result[step['key']] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return result


def create_plan(db, data, user):
    definition = normalize_definition(data.definition)
    dataset = db.get(Dataset, data.dataset_id)
    if not dataset:
        _fail(404, '数据批次不存在')
    now = time.time()
    plan = AnalysisPlan(name=data.name.strip(), dataset_id=dataset.id, definition=definition,
                        output_step=definition['output_step'], version=1, created_by=user.id,
                        created_at=now, updated_at=now)
    db.add(plan)
    db.flush()
    return plan


def _invalidated_steps(old_definition, old_checksum, new_definition, new_checksum):
    old_prints = fingerprints(old_definition, old_checksum) if old_definition and old_definition.get('steps') else {}
    new_prints = fingerprints(new_definition, new_checksum)
    affected = {k for k in new_prints if old_prints.get(k) != new_prints[k]}
    grew = True
    while grew:
        grew = False
        for step in new_definition['steps']:
            if step['key'] not in affected and any(p in affected for p in step['parents']):
                affected.add(step['key'])
                grew = True
    return sorted(affected), new_prints


def update_plan(db, plan, data):
    definition = normalize_definition(data.definition)
    dataset = db.get(Dataset, data.dataset_id)
    if not dataset:
        _fail(404, '数据批次不存在')
    previous = list(db.scalars(select(PlanRun).where(PlanRun.plan_id == plan.id,
                                                    PlanRun.status != 'superseded').order_by(PlanRun.id.desc())))
    affected, _ = _invalidated_steps(plan.definition, previous[0].dataset_checksum if previous else None,
                                     definition, dataset.checksum)
    plan.name = data.name.strip()
    plan.dataset_id = dataset.id
    plan.definition = definition
    plan.output_step = definition['output_step']
    plan.version += 1
    plan.updated_at = time.time()
    if previous and affected:
        # 上游参数或数据变化：受影响步骤及其下游失效；无关分支保留，重算时沿用。
        for old in previous:
            old.status = 'superseded'
            old.approved = False
            old.approved_by = ''
            old.approved_at = None
            old.finished_at = old.finished_at or time.time()
            for step in db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == old.id)):
                if step.step_key in affected or step.status in ACTIVE:
                    step.status = 'invalidated'
                    step.error = step.error or '方案上游变化，结果已失效'
            record(db, 'system', 'plan.run.invalidated',
                   {'run': old.id, 'plan': plan.id, 'steps': affected, 'version': plan.version})
    db.flush()
    return plan


def plan_dict(plan, runs=None):
    latest = runs[0] if runs else None
    return {
        'id': plan.id, 'name': plan.name, 'dataset_id': plan.dataset_id,
        'definition': plan.definition, 'output_step': plan.output_step,
        'version': plan.version, 'created_at': plan.created_at, 'updated_at': plan.updated_at,
        'latest_run': run_summary(latest) if latest else None,
    }


def run_summary(run):
    if not run:
        return None
    return {k: getattr(run, k) for k in ['id', 'plan_id', 'plan_name', 'plan_version', 'status',
                                          'approved', 'created_at', 'started_at', 'finished_at', 'output_step']}


def step_dict(step, detail=False):
    data = {k: getattr(step, k) for k in ['id', 'step_key', 'name', 'status', 'attempt', 'fingerprint',
                                           'definition', 'params', 'input_count', 'selection_signature',
                                           'error', 'reused_from_run_id', 'started_at', 'finished_at']}
    data['kind'] = (step.definition or {}).get('kind', 'cluster')
    data['parents'] = (step.definition or {}).get('parents', [])
    if detail:
        data['input_batch_ids'] = step.input_batch_ids
        data['provenance'] = step.provenance
        data['result'] = step.result
    else:
        data['provenance'] = {k: step.provenance.get(k) for k in ('parents', 'unique_batches', 'dataset_checksum')}
    return data


def run_dict(db, run, detail=False):
    steps = list(db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id).order_by(PlanStepRun.id)))
    output = next((s for s in steps if s.step_key == run.output_step), None)
    data = {**run_summary(run), 'dataset_id': run.dataset_id, 'dataset_checksum': run.dataset_checksum,
            'definition': run.definition, 'approved_by': run.approved_by, 'approved_at': run.approved_at,
            'error': run.error, 'steps': [step_dict(s) for s in steps]}
    data['progress'] = {'total': len(steps), 'completed': sum(1 for s in steps if s.status == 'completed'),
                        'failed': sum(1 for s in steps if s.status == 'failed'),
                        'blocked': sum(1 for s in steps if s.status == 'blocked'),
                        'pending': sum(1 for s in steps if s.status in ACTIVE),
                        'reused': sum(1 for s in steps if s.reused_from_run_id is not None)}
    data['valid'] = run_valid(db, run)
    if output:
        data['output'] = {'step_key': output.step_key, 'status': output.status,
                          'reused_from_run_id': output.reused_from_run_id,
                          'result': output.result if detail else {}}
    if detail:
        data['step_details'] = [step_dict(s, True) for s in steps]
        data['impact'] = {
            'reused': [s.step_key for s in steps if s.reused_from_run_id is not None],
            'recomputed': [s.step_key for s in steps if s.status == 'completed' and s.reused_from_run_id is None],
            'failed': [s.step_key for s in steps if s.status == 'failed'],
            'blocked': [s.step_key for s in steps if s.status == 'blocked'],
            'invalidated': [s.step_key for s in steps if s.status == 'invalidated'],
        }
    return data


def create_run(db, plan, user):
    """固定方案版本与数据快照，建立全部步骤；同方案的历史运行被取代。"""
    dataset = db.get(Dataset, plan.dataset_id)
    if not dataset:
        _fail(409, '方案绑定的数据批次已不存在')
    definition = normalize_definition(plan.definition)
    now = time.time()
    previous = list(db.scalars(select(PlanRun).where(PlanRun.plan_id == plan.id,
                                                    PlanRun.status != 'superseded')))
    run = PlanRun(plan_id=plan.id, plan_name=plan.name, plan_version=plan.version, definition=definition,
                  dataset_id=dataset.id, dataset_checksum=dataset.checksum, output_step=definition['output_step'],
                  status='running', created_by=user.id, created_at=now, started_at=now)
    db.add(run)
    db.flush()
    for old in previous:
        old.status = 'superseded'
        old.approved = False
        old.approved_by = ''
        old.approved_at = None
        old.finished_at = old.finished_at or now
        for step in db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == old.id)):
            if step.status != 'completed':
                step.status = 'invalidated'
        record(db, user.email, 'plan.run.superseded', {'run': old.id, 'plan': plan.id, 'by_run': run.id})
    prints = fingerprints(definition, dataset.checksum)
    for step in definition['steps']:
        db.add(PlanStepRun(run_id=run.id, step_key=step['key'], name=step['name'], status='pending',
                           definition=step, params=step['params'], fingerprint=prints[step['key']],
                           input_batch_ids=[], provenance={}, result={}))
    db.flush()
    record(db, user.email, 'plan.run.start', {'run': run.id, 'plan': plan.id, 'version': plan.version,
                                              'dataset': dataset.id, 'checksum': dataset.checksum[:16]})
    advance_run(db, run, user.email)
    return run


def parent_batches(result):
    groups = result.get('groups') or []
    if groups:
        ids = []
        for group in groups:
            ids.extend(str(b) for b in group.get('batch_ids', []))
        return ids, groups
    return [str(b) for b in result.get('batch_ids', [])], []


def collect_inputs(step, parent_steps):
    """按父步骤选取族群并汇合原始批次；同一批次号只纳入一次。"""
    seen, ordered, provenance = set(), [], []
    for parent_key in step['parents']:
        parent = parent_steps[parent_key]
        all_ids, groups = parent_batches(parent.result)
        wanted = step['select'].get(parent_key) or []
        if wanted:
            by_number = {int(g.get('cluster')): g for g in groups}
            missing = [g for g in wanted if g not in by_number]
            if missing:
                raise ValueError(f'上游步骤「{parent_key}」不存在族群 {missing}，请检查方案选取')
            selected = [str(b) for g in wanted for b in by_number[g].get('batch_ids', [])]
        else:
            selected = all_ids
        added = duplicates = 0
        for batch in selected:
            if batch in seen:
                duplicates += 1
                continue
            seen.add(batch)
            ordered.append(batch)
            added += 1
        provenance.append({'step': parent_key, 'run': parent.run_id, 'groups': wanted,
                           'provided': len(selected), 'added': added, 'duplicates': duplicates,
                           'batch_ids': selected})
    signature = hashlib.sha256(json.dumps(provenance, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return ordered, provenance, signature


def _reusable(db, run, step):
    """同一方案、同一数据快照下，指纹一致的已完成步骤可沿用，无需重复计算。"""
    return db.execute(
        select(PlanStepRun, PlanRun)
        .join(PlanRun, PlanRun.id == PlanStepRun.run_id)
        .where(PlanStepRun.step_key == step.step_key,
               PlanStepRun.fingerprint == step.fingerprint,
               PlanStepRun.status == 'completed',
               PlanRun.plan_id == run.plan_id,
               PlanRun.dataset_checksum == run.dataset_checksum,
               PlanStepRun.run_id != run.id)
        .order_by(PlanStepRun.id.desc())
    ).first()


def advance_run(db, run, actor='worker'):
    """幂等推进：上游全部成功才入队；失败或失效阻断下游；可安全重复调用以恢复中断。"""
    db.flush()
    if run.status == 'superseded':
        return False
    steps = {s.step_key: s for s in db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id))}
    changed = False
    for step_def in run.definition['steps']:
        step = steps.get(step_def['key'])
        if not step or step.status != 'pending':
            continue
        parents = [steps[p] for p in step_def['parents']]
        if any(p.status in ('failed', 'blocked', 'invalidated') for p in parents):
            step.status = 'blocked'
            step.error = '上游步骤未成功，已阻断'
            record(db, actor, 'plan.step.blocked', {'run': run.id, 'step': step.step_key})
            changed = True
            continue
        if any(p.status != 'completed' for p in parents):
            continue
        try:
            batch_ids, provenance, signature = collect_inputs(step_def, {p.step_key: p for p in parents})
            if not step_def['parents']:
                dataset = db.get(Dataset, run.dataset_id)
                if not dataset:
                    raise ValueError('方案绑定的数据批次已不存在')
                batch_ids = [str(row.get('batch_id', i)) for i, row in enumerate(dataset.records)]
                provenance = [{'step': None, 'groups': [], 'provided': len(batch_ids),
                               'added': len(batch_ids), 'duplicates': 0}]
                signature = hashlib.sha256(json.dumps(batch_ids, ensure_ascii=False).encode()).hexdigest()
        except ValueError as exc:
            step.status = 'failed'
            step.error = str(exc)[:990]
            step.finished_at = time.time()
            record(db, actor, 'plan.step.failed', {'run': run.id, 'step': step.step_key, 'error': step.error})
            changed = True
            continue
        if not batch_ids:
            step.status = 'failed'
            step.error = '所选上游族群没有可用原始批次，无法分析'
            step.finished_at = time.time()
            record(db, actor, 'plan.step.failed', {'run': run.id, 'step': step.step_key, 'error': step.error})
            changed = True
            continue
        reuse = _reusable(db, run, step)
        if reuse:
            source_step, source_run = reuse
            step.result = source_step.result
            step.input_count = source_step.input_count
            step.input_batch_ids = list(source_step.input_batch_ids)
            step.provenance = dict(source_step.provenance or {})
            step.provenance['reused'] = {'run': source_run.id, 'step_run': source_step.id}
            step.selection_signature = source_step.selection_signature
            step.status = 'completed'
            step.reused_from_run_id = source_run.id
            step.started_at = step.started_at or time.time()
            step.finished_at = time.time()
            record(db, actor, 'plan.step.reused', {'run': run.id, 'step': step.step_key,
                                                   'from_run': source_run.id})
        else:
            step.input_count = len(batch_ids)
            step.input_batch_ids = batch_ids
            step.provenance = {'dataset_id': run.dataset_id, 'dataset_checksum': run.dataset_checksum,
                               'parents': provenance, 'unique_batches': len(batch_ids)}
            step.selection_signature = signature
            step.status = 'queued'
            step.error = ''
            step.started_at = None
            enqueue_step(step.id)
            record(db, actor, 'plan.step.queued', {'run': run.id, 'step': step.step_key,
                                                    'inputs': len(batch_ids)})
        changed = True
    db.flush()
    all_steps = list(db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id)))
    if all(s.status == 'completed' for s in all_steps):
        if run.status != 'completed':
            run.status = 'completed'
            run.finished_at = time.time()
            record(db, actor, 'plan.run.completed', {'run': run.id})
            changed = True
    elif all(s.status in ('completed', 'failed', 'blocked', 'invalidated') for s in all_steps):
        if run.status != 'failed':
            run.status = 'failed'
            run.finished_at = time.time()
            run.error = '; '.join(s.error for s in all_steps if s.status == 'failed')[0:900]
            record(db, actor, 'plan.run.failed', {'run': run.id})
            changed = True
    return changed


def enqueue_step(step_run_id):
    try:
        redis = Redis.from_url(REDIS_URL)
        redis.rpush(PLAN_QUEUE, step_run_id)
        redis.close()
    except Exception:
        pass  # SQL queued 状态是持久兜底，worker 每轮会扫描


def run_valid(db, run):
    if not run or run.status != 'completed':
        return False
    steps = list(db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id)))
    return bool(steps) and all(s.status == 'completed' for s in steps)


def approve_run(db, run, user):
    if run.status == 'superseded':
        _fail(409, '该运行已被更新方案取代，结果已失效，不能批准')
    if run.status != 'completed':
        _fail(409, '仅全部步骤成功的方案运行可以批准')
    if not run_valid(db, run):
        _fail(409, '仍有失效或未完成步骤，不能批准')
    if run.approved:
        return run
    run.approved = True
    run.approved_by = user.email
    run.approved_at = time.time()
    record(db, user.email, 'plan.run.approve', {'run': run.id})
    return run


def retry_run(db, run, user):
    if run.status == 'superseded':
        _fail(409, '该运行已被更新方案取代，不能重试')
    if run.status not in ('failed', 'completed'):
        _fail(409, '运行仍在执行，无需重试')
    steps = list(db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id)))
    if not any(s.status in ('failed', 'blocked') for s in steps):
        _fail(409, '没有失败或阻断的步骤需要重试')
    for step in steps:
        if step.status in ('failed', 'blocked'):
            step.status = 'pending'
            step.error = ''
            step.result = {}
            step.started_at = None
            step.finished_at = None
    run.status = 'running'
    run.error = ''
    run.finished_at = None
    run.approved = False
    run.approved_by = ''
    run.approved_at = None
    record(db, user.email, 'plan.run.retry', {'run': run.id})
    advance_run(db, run, user.email)
    return run


def resume_all(db, stale_after=90):
    """worker 启动或周期扫描：重置中断步骤并补齐未入队步骤。

    运行中的步骤超过 stale_after 秒未完成视为执行中断；重排沿用该步已建立的分析
    （输入批次与参数已随步骤持久化），不会重复创建步骤或重复纳入批次。
    """
    recovered = 0
    now = time.time()
    runs = list(db.scalars(select(PlanRun).where(PlanRun.status == 'running')))
    for run in runs:
        if run.status == 'superseded':
            continue
        for step in db.scalars(select(PlanStepRun).where(PlanStepRun.run_id == run.id,
                                                         PlanStepRun.status == 'running')):
            if now - (step.started_at or 0) < stale_after:
                continue
            step.status = 'queued'
            step.started_at = None
            record(db, 'worker', 'plan.step.recovered', {'run': run.id, 'step': step.step_key,
                                                          'inputs': step.input_count})
            recovered += 1
        advance_run(db, run, 'worker')
        if run.status == 'running' and not db.scalar(select(PlanStepRun).where(
                PlanStepRun.run_id == run.id, PlanStepRun.status.in_(ACTIVE))):
            run.status = 'failed'
            run.error = run.error or '运行没有可推进的步骤'
            run.finished_at = now
            record(db, 'worker', 'plan.run.failed', {'run': run.id, 'reason': 'no-progress'})
    return recovered
