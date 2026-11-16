import {api} from '../api/client.js';
import {datasets} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {chart} from '../components/common/chart.js';
import {table} from '../components/common/table.js';
import {status} from '../components/common/status.js';
import {esc,fmt,date,notify} from '../utils/dom.js';

const KIND={cluster:'分群步骤',merge:'汇合步骤'};
const STEP_STATUS={pending:'等待上游',queued:'排队中',running:'计算中',completed:'已完成',failed:'失败',blocked:'已阻断',invalidated:'已失效'};
const state={list:[],ds:[],plan:null,runs:[],run:null,draft:null,draftPlanId:undefined,impact:null};

const newStep=(index)=>({key:'step'+(index+1),name:'工况分群 '+(index+1),kind:'cluster',parents:[],select:{},clusters:3});
function draftFrom(plan){return plan?plan.definition.steps.map(s=>({key:s.key,name:s.name,kind:s.kind,parents:[...s.parents],select:JSON.parse(JSON.stringify(s.select||{})),clusters:(s.params||{}).clusters??3})):[newStep(0)];}

function stepChip(s){
 if(!s)return '';
 return '<span class="step-chip '+esc(s.status)+'">'+esc(s.step_key)+' · '+esc(STEP_STATUS[s.status]||s.status)+(s.reused_from_run_id?' · 沿用#'+s.reused_from_run_id:'')+'</span>';
}

function editor(steps){
 return '<section class="panel plan-editor"><div class="section-title"><h2>方案编排</h2><span>分支各自计算，汇合时按批次号去重</span></div>'+
  '<div id="step-editor">'+steps.map((s,i)=>{
    const others=steps.filter((_,j)=>j!==i).map(x=>x.key);
    const parents=others.map(key=>'<label class="inline-check"><input type="checkbox" data-parent="'+i+'" value="'+esc(key)+'" '+(s.parents.includes(key)?'checked':'')+'> '+esc(key)+'</label>').join('')||'<span class="hint">根步骤：读取整批数据</span>';
    const selects=others.map(key=>'<div class="select-pair"><span>'+esc(key)+'</span><input data-select="'+i+'" data-select-parent="'+esc(key)+'" placeholder="族群编号，如 1,3；留空=全部" value="'+esc((s.select?.[key]||[]).join(','))+'"></div>').join('');
    return '<article class="step-card" data-step="'+i+'"><header><div><span class="step-index">'+String(i+1).padStart(2,'0')+'</span><strong>'+esc(s.key)+'</strong></div><button type="button" class="text-button" data-remove-step="'+i+'" '+(steps.length<2?'disabled':'')+'>删除此步</button></header>'+
      '<div class="step-fields"><label>步骤键<input data-field="key" value="'+esc(s.key)+'" maxlength="40"></label><label>步骤名称<input data-field="name" value="'+esc(s.name)+'" maxlength="160"></label><label>步骤类型<select data-field="kind"><option value="cluster" '+(s.kind==='cluster'?'selected':'')+'>分群（产生族群）</option><option value="merge" '+(s.kind==='merge'?'selected':'')+'>汇合（合并多支线）</option></select></label><label>族群数量K<input type="number" min="2" max="6" data-field="clusters" value="'+esc(s.clusters)+'"></label></div>'+
      '<div class="step-wiring"><div><span class="mini-label">上游步骤</span><div class="checks">'+parents+'</div></div><div><span class="mini-label">按族群选取（仅对分群上游生效）</span>'+selects+'</div></div></article>';
  }).join('')+'</div>'+
  '<div class="editor-actions"><button type="button" class="button" id="add-step">＋ 增加步骤</button><label class="inline-field">输出步骤<select id="output-step">'+steps.map(s=>'<option value="'+esc(s.key)+'" '+(state.plan?.output_step===s.key?'selected':'')+'>'+esc(s.key)+'</option>').join('')+'</select></label><button type="button" class="button" id="preview-impact" '+(state.plan?'':'disabled')+'>预演改动影响</button><button type="button" class="primary" id="save-plan">'+(state.plan?'保存为新版本':'保存方案')+'</button>'+(state.plan?'<button type="button" class="primary" id="start-run">按当前版本运行</button>':'')+'</div>'+
  '<p class="hint">步骤键不可重复；存在循环依赖、悬空上游或选取不存在的族群时后端拒绝保存。汇合步骤把各上游所选族群的原始批次合并，同一批次只纳入一次。</p></section>';
}

function readDraft(){
 const steps=[...document.querySelectorAll('[data-step]')].map(card=>{
  const index=card.dataset.step;
  const field=n=>card.querySelector('[data-field="'+n+'"]').value.trim();
  const parents=[...card.querySelectorAll('[data-parent="'+index+'"]')].filter(b=>b.checked).map(b=>b.value);
  const select={};
  card.querySelectorAll('[data-select="'+index+'"]').forEach(input=>{
    const p=input.dataset.selectParent, raw=input.value.trim();
    if(!raw||!parents.includes(p))return;
    select[p]=raw.split(/[,，\s]+/).filter(Boolean).map(Number);
  });
  return {key:field('key'),name:field('name')||field('key'),kind:field('kind'),parents,select,clusters:Number(field('clusters'))};
 });
 return {steps,output_step:document.querySelector('#output-step')?.value};
}

function impactView(impact){
 if(!impact)return '';
 return '<section class="panel"><div class="section-title"><h2>改动影响预演</h2><span>对比运行 #'+esc(impact.baseline_run?.id||'—')+'</span></div><div class="impact-grid">'+impact.steps.map(s=>'<div class="impact-item '+esc(s.action)+'"><strong>'+esc(s.name)+' <small>'+esc(s.step_key)+'</small></strong><span>'+(s.action==='reuse'?'沿用上次结果':'失效重算')+'</span><p>'+esc(s.reason)+'</p></div>').join('')+'</div><p class="hint">仅上游变化波及的步骤与下游会重算；无关分支在下次运行时直接沿用。</p></section>';
}

function runView(run){
 if(!run)return '';
 const r=run.output?.result||{};
 const metrics=(r.metrics||[]).map(m=>'<div><span>'+esc(m.label)+'</span><strong>'+esc(fmt(m.value))+'</strong></div>').join('');
 const steps=run.step_details||run.steps||[];
 const impact=run.impact||{};
 const impactText=[impact.reused?.length?'沿用 '+impact.reused.length:null,impact.recomputed?.length?'重算 '+impact.recomputed.length:null,impact.failed?.length?'失败 '+impact.failed.length:null,impact.blocked?.length?'阻断 '+impact.blocked.length:null].filter(Boolean).join(' · ');
 return '<section class="panel"><div class="section-title"><h2>运行 #'+run.id+' · '+esc(run.plan_name)+'</h2><span>方案 v'+run.plan_version+' · 固定数据指纹 '+esc((run.dataset_checksum||'').slice(0,12))+'…</span></div>'+
  '<div class="run-banner '+esc(run.status)+'"><div>'+status(run.status)+'<strong>'+esc(run.progress.completed)+'/'+esc(run.progress.total)+' 步完成'+(impactText?' · '+esc(impactText):'')+'</strong></div><div class="run-actions">'+(session.user.role!=='viewer'&&run.status==='failed'?'<button class="button small" id="retry-run">重试失败步骤</button>':'')+(session.user.role==='admin'&&run.valid&&!run.approved?'<button class="primary small" id="approve-run">批准此方案结论</button>':'')+(run.approved?'<span class="approval">已由 '+esc(run.approved_by)+' 批准</span>':'')+(run.status==='superseded'?'<span class="warning">该运行已被更新方案取代，结果不再供批准或简报使用</span>':'')+'</div></div>'+
  '<p class="hint">各步只在上游全部成功后运行；失败会阻断其下游。方案或数据变化时相关下游失效重算，无关分支保留。</p>'+
  '<div class="run-steps">'+steps.map(s=>{
    const prov=(s.provenance?.parents||[]).map(p=>'<li><strong>'+esc(p.step||'数据批次')+'</strong> '+(p.step?'族群 '+esc((p.groups||[]).join('、')||'全部')+' · ':'')+'提供 '+esc(p.provided)+' 批，新纳入 '+esc(p.added)+' 批，去重跳过 '+esc(p.duplicates)+' 批</li>').join('');
    return '<article class="run-step '+esc(s.status)+'"><header><div>'+stepChip(s)+'<span>'+esc(s.name)+' · '+esc(KIND[s.kind]||s.kind)+'</span></div><small>输入 '+esc(s.input_count)+' 批 · 尝试 '+esc(s.attempt)+' 次</small></header>'+
      (s.error?'<p class="step-error">'+esc(s.error)+'</p>':'')+
      '<details><summary>逐步输入来源</summary><div class="provenance"><p>数据批次 #'+esc(s.provenance?.dataset_id??run.dataset_id)+' · 指纹 '+esc((s.provenance?.dataset_checksum||'').slice(0,16))+'… · 去重后 '+esc(s.provenance?.unique_batches??s.input_count)+' 批</p><ul>'+(prov||'<li>读取方案绑定的整批数据</li>')+'</ul>'+(s.reused_from_run_id?'<p class="hint">沿用运行 #'+esc(s.reused_from_run_id)+' 的已完成结果，未重复计算。</p>':'')+(s.input_batch_ids?.length?'<p class="batch-ids">批次样本：'+esc(s.input_batch_ids.slice(0,12).join(' · '))+(s.input_batch_ids.length>12?' …':'')+'</p>':'')+'</div></details></article>';
  }).join('')+'</div>'+
  (r.summary?'<div class="result-lead compact"><p class="eyebrow">汇合结果</p><h2>'+esc(r.summary)+'</h2><div class="metrics">'+metrics+'</div></div><div class="chart-panel">'+chart(r.chart)+'</div>'+(r.table?'<div class="section-title"><h2>结果明细</h2><span>'+r.table.rows.length+' 行</span></div>'+table(r.table.columns,r.table.rows):''):'')+
  '</section>';
}

export async function planPage(planId,runId){
 const [list,dss]=await Promise.all([api('/plans'),datasets.list()]);
 state.list=list;state.ds=dss;
 let plan=planId?await api('/plans/'+planId):null;
 state.plan=plan;
 if(plan){state.runs=(await api('/runs?plan_id='+plan.id));}
 const runs=plan?state.runs:[];
 const targetRun=runId?await api('/runs/'+runId):(runs[0]?await api('/runs/'+runs[0].id):null);
 state.run=targetRun;
 if(!state.draft||state.draftPlanId!==(plan?.id??null)){state.draft=draftFrom(plan);state.draftPlanId=plan?.id??null;state.impact=null;}
 const canWrite=session.user.role!=='viewer';
 const planList='<div class="plan-list">'+list.map(p=>'<a class="plan-item '+(p.id===plan?.id?'active':'')+'" href="#/plans/'+p.id+'"><strong>'+esc(p.name)+'</strong><span>v'+p.version+' · '+esc(p.definition.steps.length)+' 步</span><small>'+esc(p.latest_run?('最近运行 #'+p.latest_run.id+' · '+p.latest_run.status):'尚未运行')+'</small></a>').join('')+(!list.length?'<p class="empty">还没有分析方案。在右侧编排分叉与汇合步骤后保存。</p>':'')+'</div>';
 const runList='<div class="run-list">'+runs.map(r=>'<a class="run-item '+(r.id===targetRun?.id?'active':'')+'" href="#/plans/'+plan.id+'/'+r.id+'"><span>运行 #'+r.id+' · v'+r.plan_version+'</span>'+status(r.status)+'<small>'+esc(date(r.created_at))+(r.approved?' · 已批准':'')+'</small></a>').join('')+(!runs.length?'<p class="hint">保存方案后点击“按当前版本运行”。</p>':'')+'</div>';
 return heading('06 / PLAN GRAPH','分析方案编排','把工况分群拆成分支实验，再汇合各支线选取的原始批次。每次运行固定方案版本与数据指纹，逐步留痕。')+
  '<div class="plan-grid"><section><div class="section-title"><h2>分析方案</h2><span>'+list.length+' 个方案</span></div>'+planList+
  '<div class="section-title"><h2>运行记录</h2><span>'+(plan?esc(plan.name):'选择方案')+'</span></div>'+runList+'</section><section>'+
  heading('','方案定义',plan?('方案 v'+plan.version+' · 绑定数据批次 #'+plan.dataset_id):'新建方案，选择数据批次并编排步骤。',
    canWrite?'':'<span class="hint">观察员只读</span>').replace('class="page-heading"','class="page-heading sub"')+
  '<div class="plan-form"><label>方案名称<input id="plan-name" value="'+esc(plan?.name||'工况分叉汇合方案')+'" maxlength="160"></label><label>数据批次<select id="plan-dataset">'+dss.map(d=>'<option value="'+d.id+'" '+(d.id===(plan?.dataset_id??dss[0]?.id)?'selected':'')+'>'+esc(d.name)+' · '+d.rows+' 条</option>').join('')+'</select></label></div>'+
  (canWrite?editor(state.draft):'<section class="panel"><p class="hint">观察员可查看方案与运行记录，不能编排或运行。</p></section>')+
  impactView(state.impact)+runView(targetRun)+'</section></div>';
}

function toDefinition(){const {steps,output_step}=readDraft();return {steps:steps.map(s=>({key:s.key,name:s.name,kind:s.kind,parents:s.parents,select:s.select,params:{clusters:s.clusters}})),output_step};}
function parseSelects(){return readDraft();}

export function bindPlans(refresh){
 const rerender=(planId,runId)=>{location.hash='#/plans/'+(planId??state.plan?.id??'')+(runId?'/'+runId:'');};
 const syncDraft=()=>{try{state.draft=parseSelects().steps;}catch(e){}};
 document.querySelector('#add-step')?.addEventListener('click',()=>{syncDraft();state.draft.push(newStep(state.draft.length));state.impact=null;refresh();});
 document.querySelectorAll('[data-remove-step]').forEach(b=>b.addEventListener('click',()=>{syncDraft();state.draft.splice(Number(b.dataset.removeStep),1);state.impact=null;refresh();}));
 document.querySelectorAll('[data-parent]').forEach(b=>b.addEventListener('change',()=>{syncDraft();state.impact=null;refresh();}));
 document.querySelectorAll('[data-field="kind"]').forEach(el=>el.addEventListener('change',()=>{syncDraft();state.impact=null;refresh();}));
 document.querySelector('#preview-impact')?.addEventListener('click',async()=>{
   try{state.draft=parseSelects().steps;const d=toDefinition();state.impact=await api('/plans/'+state.plan.id+'/impact',{method:'POST',body:JSON.stringify({name:document.querySelector('#plan-name').value,dataset_id:Number(document.querySelector('#plan-dataset').value),definition:d})});refresh();}catch(e){notify(e.message,true);}
 });
 document.querySelector('#save-plan')?.addEventListener('click',async()=>{
   try{const d=toDefinition();const body={name:document.querySelector('#plan-name').value,dataset_id:Number(document.querySelector('#plan-dataset').value),definition:d};
     const saved=state.plan?await api('/plans/'+state.plan.id,{method:'PUT',body:JSON.stringify(body)}):await api('/plans',{method:'POST',body:JSON.stringify(body)});
     state.impact=null;notify(state.plan?'方案已保存为新版本':'方案已保存');state.draft=null;rerender(saved.id);refresh();}catch(e){notify(e.message,true);}
 });
 document.querySelector('#start-run')?.addEventListener('click',async()=>{
   try{const run=await api('/plans/'+state.plan.id+'/runs',{method:'POST'});notify('方案运行已建立，各步将在上游成功后执行');location.hash='#/plans/'+state.plan.id+'/'+run.id;refresh();}catch(e){notify(e.message,true);}
 });
 document.querySelector('#retry-run')?.addEventListener('click',async()=>{
   try{await api('/runs/'+state.run.id+'/retry',{method:'POST'});notify('失败与阻断步骤已重排');refresh();}catch(e){notify(e.message,true);}
 });
 document.querySelector('#approve-run')?.addEventListener('click',async()=>{
   try{await api('/runs/'+state.run.id+'/approve',{method:'POST'});notify('方案结论已批准');refresh();}catch(e){notify(e.message,true);}
 });
}
