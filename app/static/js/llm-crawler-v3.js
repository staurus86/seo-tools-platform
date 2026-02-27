const V3_API = '/api/tools/llm-crawler';

function fmt(n) { if (n === undefined || n === null) return '-'; const v=Number(n); return Number.isFinite(v)?v:'-'; }
function pct(n) { const v=fmt(n); return v==='-'?'-':`${v}%`; }
function meter(value) {
  const val = Math.max(0, Math.min(100, Number(value)||0));
  const color = val>=80?'#22c55e':val>=50?'#f59e0b':'#ef4444';
  return `<div class="h-2 bg-slate-200 rounded-full overflow-hidden"><div class="h-2" style="width:${val}%;background:${color};transition:width .3s"></div></div>`;
}

async function fetchJob(jobId){
  const r=await fetch(`${V3_API}/jobs/${encodeURIComponent(jobId)}`); if(!r.ok) throw new Error(`HTTP ${r.status}`); return r.json();
}

function renderHero(result){
  const score = fmt(result.score?.total);
  const projected = fmt(result.projected_score_after_fixes);
  const loss = pct(result.content_loss_percent);
  document.getElementById('v3-hero').innerHTML = `
    <div class="bg-white rounded-2xl shadow p-6 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
      <div>
        <div class="text-sm text-slate-500">AI Visibility Score</div>
        <div class="text-4xl font-bold text-slate-900">${score}</div>
        <div class="text-sm text-slate-500">Projected after fixes: <span class="font-semibold text-emerald-600">${projected}</span></div>
      </div>
      <div class="grid grid-cols-2 gap-3 text-sm">
        <div class="bg-slate-50 rounded-xl p-3">Citation probability <div class="font-semibold">${pct(result.citation_probability)}</div>${meter(result.citation_probability)}</div>
        <div class="bg-slate-50 rounded-xl p-3">Trust signals <div class="font-semibold">${pct(result.trust_signal_score)}</div>${meter(result.trust_signal_score)}</div>
        <div class="bg-slate-50 rounded-xl p-3">Content loss <div class="font-semibold">${loss}</div>${meter(100 - (result.content_loss_percent||0))}</div>
        <div class="bg-slate-50 rounded-xl p-3">Discoverability <div class="font-semibold">${pct(result.discoverability?.discoverability_score)}</div>${meter(result.discoverability?.discoverability_score)}</div>
      </div>
    </div>`;
}

function renderWhatAI(result){
  const ai = result.ai_understanding || {};
  const signals = result.nojs?.signals || {};
  document.getElementById('v3-what-ai').innerHTML = `
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-brain text-indigo-500"></i><h3 class="text-lg font-semibold">What AI actually understands</h3></div>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="bg-slate-50 rounded-xl p-3">
          <div class="text-xs text-slate-500">Topic detected</div>
          <div class="font-semibold text-slate-800">${ai.topic || 'Not detected'}</div>
          <div class="mt-2 text-xs text-slate-500">Confidence</div>
          ${meter(ai.score||0)}
        </div>
        <div class="bg-slate-50 rounded-xl p-3">
          <div class="text-xs text-slate-500">Detected entities</div>
          <div class="text-sm">Organization: ${signals.author_present?'✅':'❌'}</div>
          <div class="text-sm">Product: ${(ai.entities||[])[0]?'✅':'❌'}</div>
          <div class="text-sm">Author: ${signals.author_present?'✅':'❌'}</div>
          <div class="text-sm mt-2">Content clarity: ${pct(ai.content_clarity)}</div>
        </div>
      </div>
    </div>`;
}

function renderLoss(result){
  const content = result.nojs?.content || {};
  const total = (result.rendered?.content?.main_text_length) || content.main_text_length || 0;
  const extracted = content.main_text_length || 0;
  const loss = fmt(result.content_loss_percent);
  document.getElementById('v3-loss').innerHTML = `
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-scissors text-rose-500"></i><h3 class="text-lg font-semibold">Content loss</h3></div>
      <div class="grid grid-cols-3 gap-3 text-sm">
        <div class="bg-slate-50 rounded-xl p-3">HTML text: <span class="font-semibold">${total}</span></div>
        <div class="bg-slate-50 rounded-xl p-3">Extracted: <span class="font-semibold">${extracted}</span></div>
        <div class="bg-slate-50 rounded-xl p-3">Lost: <span class="font-semibold">${loss}%</span>${meter(100 - (result.content_loss_percent||0))}</div>
      </div>
    </div>`;
}

function renderCitation(result){
  const cb = result.citation_breakdown || {};
  const labels = ['Schema','Author','Content','Accessibility','Structure'];
  const data = [
    cb.schema||0, cb.author||0, cb.content_clarity||0, cb.bot_accessibility||0, cb.structure||0
  ];
  const ctxId='v3-citation-chart';
  document.getElementById('v3-citation').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-quote-left text-emerald-500"></i><h3 class="text-lg font-semibold">Citation readiness</h3></div>
      <canvas id="${ctxId}" height="200"></canvas>
    </div>`;
  new Chart(document.getElementById(ctxId), {type:'radar', data:{labels, datasets:[{label:'Readiness', data, backgroundColor:'rgba(16,185,129,0.2)', borderColor:'#10b981'}]}, options:{scales:{r:{beginAtZero:true,max:100}}}});
}

function renderTrust(result){
  const signals = result.nojs?.signals || {};
  const schema = result.nojs?.schema || {};
  const trust = fmt(result.trust_signal_score);
  document.getElementById('v3-trust').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-shield-alt text-sky-500"></i><h3 class="text-lg font-semibold">Trust signals</h3></div>
      <div class="grid grid-cols-2 gap-3 text-sm">
        <div class="bg-slate-50 rounded-xl p-3">Author: ${signals.author_present?'✅':'❌'}</div>
        <div class="bg-slate-50 rounded-xl p-3">Organization: ${(schema.jsonld_types||[]).includes('Organization')?'✅':'❌'}</div>
        <div class="bg-slate-50 rounded-xl p-3">Contact info: ${(result.nojs?.meta?.canonical)?'✅':'❌'}</div>
        <div class="bg-slate-50 rounded-xl p-3">Schema: ${schema.jsonld_types?.length? '✅':'❌'}</div>
      </div>
      <div class="mt-3 text-sm">Trust completeness: ${trust}%</div>
      ${meter(trust)}
    </div>`;
}

function renderReasons(result){
  const items = [
    {txt:'Missing schema', impact: result.citation_breakdown?.schema<50?-15:0},
    {txt:'Missing author', impact: (result.citation_breakdown?.author||0)===0?-10:0},
    {txt:'Content loss', impact: -(result.content_loss_percent||0)/5},
  ].filter(x=>x.impact!==0).sort((a,b)=>a.impact-b.impact).slice(0,3);
  const rows = items.map(i=>`<div class="flex justify-between text-sm"><span>${i.txt}</span><span class="text-rose-600">${i.impact.toFixed(0)}</span></div>`).join('');
  document.getElementById('v3-reasons').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-arrow-down text-rose-500"></i><h3 class="text-lg font-semibold">Why score is low</h3></div>
      ${rows || '<div class="text-sm text-slate-500">No major negatives</div>'}
    </div>`;
}

function renderFix(result){
  const projected = fmt(result.projected_score_after_fixes);
  const current = fmt(result.score?.total);
  document.getElementById('v3-fix').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-magic text-indigo-500"></i><h3 class="text-lg font-semibold">Fix impact simulation</h3></div>
      <div class="text-sm">Current: ${current}</div>
      <div class="text-sm text-emerald-600">Projected after fixes: ${projected}</div>
      ${meter(projected)}
    </div>`;
}

function renderPreview(result){
  const ap = result.ai_answer_preview || {};
  document.getElementById('v3-preview').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-robot text-slate-500"></i><h3 class="text-lg font-semibold">AI search preview</h3></div>
      <div class="text-xs text-slate-500 mb-1">When user asks:</div>
      <div class="font-semibold text-slate-800 mb-2">"${ap.question||'What is this page about?'}"</div>
      <div class="text-sm text-slate-700">${ap.answer||'Not enough content'}</div>
    </div>`;
}

function renderDiscover(result){
  const d = result.discoverability || {};
  document.getElementById('v3-discover').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-compass text-emerald-500"></i><h3 class="text-lg font-semibold">Discoverability</h3></div>
      <div class="text-sm">Score: ${pct(d.discoverability_score)}</div>
      <div class="text-sm">Depth estimate: ${d.click_depth_estimate||'-'} clicks</div>
      ${meter(d.discoverability_score||0)}
    </div>`;
}

function renderSchema(result){
  const schema = result.nojs?.schema || {};
  const checklist = ['Organization','Article','Product','Breadcrumb'];
  const items = checklist.map(t=>`<div class="flex items-center gap-2 text-sm">${(schema.jsonld_types||[]).includes(t)?'✅':'❌'} ${t}</div>`).join('');
  document.getElementById('v3-schema').innerHTML=`
    <div class="bg-white rounded-2xl shadow p-6">
      <div class="flex items-center gap-2 mb-3"><i class="fas fa-database text-sky-500"></i><h3 class="text-lg font-semibold">Schema visualization</h3></div>
      <div class="grid grid-cols-2 gap-2">${items}</div>
    </div>`;
}

async function initV3(jobId){
  const data = await fetchJob(jobId);
  if (data.status !== 'done') {
    const root=document.querySelector('.max-w-6xl'); root.innerHTML='<div class="bg-amber-50 border border-amber-200 text-amber-800 p-4 rounded-xl">Задача ещё выполняется. Повторите позже.</div>';
    return;
  }
  const result = data.result || {};
  renderHero(result);
  renderWhatAI(result);
  renderLoss(result);
  renderCitation(result);
  renderTrust(result);
  renderReasons(result);
  renderFix(result);
  renderPreview(result);
  renderDiscover(result);
  renderSchema(result);
  document.getElementById('v3-export-json').onclick = () => {
    const blob = new Blob([JSON.stringify(result, null, 2)], {type:'application/json'});
    const href=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=href; a.download=`llm_crawler_${jobId}.json`; a.click(); URL.revokeObjectURL(href);
  };
  document.getElementById('v3-export-docx').onclick = () => window.location.href = `${V3_API}/jobs/${encodeURIComponent(jobId)}/report.docx`;
}

document.addEventListener('DOMContentLoaded', () => {
  if (window.llmJobId) initV3(window.llmJobId);
});
