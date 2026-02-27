const V2_API = '/api/tools/llm-crawler';

function riskColor(val) {
  if (val >= 80) return 'text-emerald-600';
  if (val >= 50) return 'text-amber-500';
  return 'text-rose-600';
}

function clampScore(n) {
  if (n === null || n === undefined) return '-';
  const v = Number(n);
  if (Number.isNaN(v)) return '-';
  return Math.max(0, Math.min(100, v));
}

async function fetchJob(jobId) {
  const resp = await fetch(`${V2_API}/jobs/${encodeURIComponent(jobId)}`);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

function renderHero(result) {
  const score = clampScore((result.score || {}).total);
  const citation = clampScore(result.citation_probability);
  const eeat = clampScore((result.eeat_score || {}).score);
  const ingestion = clampScore((result.llm_ingestion || {}).avg_chunk_quality);
  const jsRisk = clampScore((result.js_dependency || {}).score);
  const color = score >= 80 ? 'text-emerald-600' : score >= 50 ? 'text-amber-500' : 'text-rose-600';
  const ring = score >= 80 ? 'stroke-emerald-500' : score >= 50 ? 'stroke-amber-500' : 'stroke-rose-500';
  document.getElementById('v2-hero').innerHTML = `
    <div class="hero-score rounded-2xl p-6 shadow card">
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-2 text-slate-600">
          <i class="fas fa-brain text-slate-500"></i>
          <span class="font-semibold">AI Visibility Score</span>
        </div>
        <div class="pill ${score>=80?'pill-success':score>=50?'pill-warn':'pill-error'}">${score>=80?'Excellent':score>=50?'Needs work':'Poor'}</div>
      </div>
      <div class="mt-4 flex items-center gap-6">
        <div class="relative w-32 h-32">
          <svg viewBox="0 0 36 36" class="w-32 h-32">
            <path class="text-slate-200" stroke-width="4" stroke="currentColor" fill="none" d="M18 2a16 16 0 1 1 0 32 16 16 0 0 1 0-32"></path>
            <path class="${ring}" stroke-width="4" stroke-linecap="round" fill="none"
              d="M18 2a16 16 0 1 1 0 32 16 16 0 0 1 0-32"
              stroke-dasharray="${score},100"></path>
          </svg>
          <div class="absolute inset-0 flex flex-col items-center justify-center">
            <div class="text-3xl font-bold ${color}">${score}</div>
            <div class="text-xs text-slate-500">/100</div>
          </div>
        </div>
        <div class="grid grid-cols-2 gap-3 text-sm text-slate-700">
          <div class="card p-3 rounded-xl border border-slate-200">
            <div class="text-xs text-slate-500">Citation probability</div>
            <div class="text-lg font-semibold ${riskColor(citation)}">${citation}%</div>
          </div>
          <div class="card p-3 rounded-xl border border-slate-200">
            <div class="text-xs text-slate-500">EEAT score</div>
            <div class="text-lg font-semibold ${riskColor(eeat)}">${eeat}</div>
          </div>
          <div class="card p-3 rounded-xl border border-slate-200">
            <div class="text-xs text-slate-500">LLM ingestion quality</div>
            <div class="text-lg font-semibold ${riskColor(ingestion)}">${ingestion}</div>
          </div>
          <div class="card p-3 rounded-xl border border-slate-200">
            <div class="text-xs text-slate-500">JS dependency risk</div>
            <div class="text-lg font-semibold ${riskColor(100-jsRisk)}">${jsRisk === '-'?'-':jsRisk}</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderCritical(result) {
  const issues = (result.score?.top_issues || []).slice(0,6);
  const good = issues.length === 0 ? ['No critical issues detected'] : [];
  const cards = issues.map(t=> `<div class="card bg-rose-50 border border-rose-200 rounded-xl p-3 text-rose-800 text-sm flex gap-2"><i class="fas fa-alert text-rose-500"></i><span>${t}</span></div>`).join('') +
    good.map(t=> `<div class="card bg-emerald-50 border border-emerald-200 rounded-xl p-3 text-emerald-800 text-sm flex gap-2"><i class="fas fa-check text-emerald-600"></i><span>${t}</span></div>`).join('');
  document.getElementById('v2-critical').innerHTML = cards;
}

function renderBotMatrix(result) {
  const matrix = result.bot_matrix || [];
  const rows = matrix.map(r=>{
    const allowed = r.allowed;
    const icon = allowed ? '<i class="fas fa-check text-emerald-500"></i>' : '<i class="fas fa-ban text-rose-500"></i>';
    return `<tr class="border-t border-slate-100">
      <td class="p-2 font-semibold">${r.profile}</td>
      <td class="p-2">${icon} ${allowed?'Allowed':'Blocked'}</td>
      <td class="p-2">${r.reason||'-'}</td>
    </tr>`;
  }).join('');
  document.getElementById('v2-bot-matrix').innerHTML = `
    <div class="flex items-center justify-between mb-2">
      <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100">Bot Access Matrix</h3>
    </div>
    <div class="overflow-auto">
      <table class="min-w-full text-sm">
        <thead class="text-left text-slate-500 uppercase text-xs">
          <tr><th class="p-2">Bot</th><th class="p-2">Access</th><th class="p-2">Reason</th></tr>
        </thead>
        <tbody>${rows || '<tr><td class="p-2" colspan="3">No data</td></tr>'}</tbody>
      </table>
    </div>
  `;
}

function renderContentVis(result) {
  const content = (result.nojs?.content) || {};
  const ratio = Number(content.main_content_ratio || 0);
  const boiler = Number(content.boilerplate_ratio || 0);
  const ctxId = 'donut-ratio';
  document.getElementById('v2-content-vis').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Content Extraction</h3>
    <canvas id="${ctxId}" height="160"></canvas>
    <div class="mt-3 text-sm text-slate-700 grid grid-cols-2 gap-2">
      <div class="card p-3 border border-slate-200 rounded-xl">Text length: <span class="font-semibold">${content.main_text_length||0}</span></div>
      <div class="card p-3 border border-slate-200 rounded-xl">Chunks: <span class="font-semibold">${(content.chunks||[]).length}</span></div>
    </div>
    <div class="mt-3 text-xs text-slate-500 line-clamp-5">${(content.main_text_preview||'').slice(0,400)}</div>
  `;
  const ctx = document.getElementById(ctxId);
  if (ctx) {
    new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Main content','Boilerplate'],
        datasets: [{
          data: [Math.round(ratio*100), Math.round(boiler*100)],
          backgroundColor: ['#10b981', '#cbd5e1'],
        }]
      },
      options: {responsive:true, plugins:{legend:{display:true, position:'bottom'}}}
    });
  }
}

function renderLLMSim(result) {
  const llm = result.llm || {};
  const scores = llm.scores || {};
  document.getElementById('v2-llm-sim').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">LLM Simulation</h3>
    <div class="card border border-slate-200 rounded-xl p-3 mb-2">
      <div class="text-xs text-slate-500 mb-1">Summary</div>
      <div class="text-sm text-slate-800">${llm.summary || '—'}</div>
    </div>
    <div class="text-sm text-slate-700">Citation likelihood: ${scores.citation_likelihood||'-'}%</div>
    <div class="text-sm text-slate-700">Hallucination risk: ${scores.hallucination_risk||'-'}%</div>
    <div class="text-sm text-slate-700">Answer quality: ${scores.answer_quality_score||'-'}%</div>
  `;
}

function renderCloakingJs(result) {
  const cloaking = result.cloaking || {};
  const js = result.js_dependency || {};
  document.getElementById('v2-cloaking').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Cloaking & JS Dependency</h3>
    <div class="grid grid-cols-1 gap-2 text-sm">
      <div class="card border border-slate-200 rounded-xl p-3">
        <div class="text-xs text-slate-500">Cloaking risk</div>
        <div class="text-base font-semibold">${cloaking.risk || 'n/a'}</div>
        <div class="text-xs text-slate-500">Browser vs GPTBot: ${cloaking.similarity_scores?.browser_vs_gptbot ?? '-'}</div>
        <div class="text-xs text-slate-500">Browser vs Googlebot: ${cloaking.similarity_scores?.browser_vs_googlebot ?? '-'}</div>
      </div>
      <div class="card border border-slate-200 rounded-xl p-3">
        <div class="text-xs text-slate-500">JS dependency score</div>
        <div class="text-base font-semibold">${js.score ?? '-'}</div>
        <div class="text-xs text-slate-500">Failed requests: ${js.failures ?? '-'}</div>
        <div class="text-xs text-slate-500">Blocked scripts/css: ${js.blocked ?? '-'}</div>
      </div>
    </div>
  `;
}

function renderEntityEeat(result) {
  const eeat = result.eeat_score || {};
  const eg = result.entity_graph || {};
  const org = (eg.organizations || [])[0] || '—';
  const author = ((result.nojs?.signals?.author_samples)||[])[0] || '—';
  document.getElementById('v2-entity-eeat').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Entities & EEAT</h3>
    <div class="text-sm text-slate-700 space-y-1">
      <div>Organization: <span class="font-semibold">${org}</span></div>
      <div>Author: <span class="font-semibold">${author}</span></div>
      <div>EEAT score: <span class="font-semibold">${eeat.score ?? '-'}</span></div>
      <div class="text-xs text-slate-500">Entities: ${(eg.organizations||[]).slice(0,5).join(', ')}</div>
    </div>
  `;
}

function renderSchema(result) {
  const schema = result.nojs?.schema || {};
  document.getElementById('v2-schema').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Structured Data</h3>
    <div class="text-sm text-slate-700 space-y-1">
      <div>Types: ${(schema.jsonld_types||[]).join(', ') || '—'}</div>
      <div>Coverage: ${schema.coverage_score ?? '-'}%</div>
      <div>Microdata: ${(schema.microdata_types||[]).slice(0,5).join(', ')}</div>
    </div>
  `;
}

function renderLinks(result) {
  const links = result.nojs?.links || {};
  document.getElementById('v2-links').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Link Discovery</h3>
    <div class="grid grid-cols-2 gap-2 text-sm">
      <div class="card p-3 border rounded-xl">Internal links: <span class="font-semibold">${links.count||0}</span></div>
      <div class="card p-3 border rounded-xl">JS-only links: <span class="font-semibold">${links.js_only_count||0}</span></div>
      <div class="card p-3 border rounded-xl">Anchor quality: <span class="font-semibold">${links.anchor_quality_score||0}%</span></div>
    </div>
  `;
}

function renderAccess(result) {
  const res = result.nojs?.resources || {};
  document.getElementById('v2-access').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Access Barriers</h3>
    <div class="grid grid-cols-1 gap-2 text-sm">
      <div class="card p-3 border rounded-xl ${res.cookie_wall?'bg-amber-50':''}">Cookie wall: ${res.cookie_wall?'Yes':'No'}</div>
      <div class="card p-3 border rounded-xl ${res.paywall?'bg-amber-50':''}">Paywall: ${res.paywall?'Yes':'No'}</div>
      <div class="card p-3 border rounded-xl ${res.csp_strict?'bg-amber-50':''}">Strict CSP: ${res.csp_strict?'Yes':'No'}</div>
    </div>
  `;
}

function renderTech(result) {
  const dbg = (result.rendered?.render_debug) || {};
  document.getElementById('v2-tech').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Technical Diagnostics</h3>
    <div class="text-sm text-slate-700 space-y-1">
      <div>Console errors: ${(dbg.console_errors||[]).length}</div>
      <div>Failed requests: ${(dbg.failed_requests||[]).length}</div>
    </div>
  `;
}

function renderChunks(result) {
  const chunks = result.nojs?.content?.chunks || [];
  const items = chunks.map(c=> `<div class="card border rounded-xl p-3"><div class="text-xs text-slate-500">Chunk ${c.idx}</div><div class="text-sm text-slate-800 line-clamp-3">${c.text.slice(0,180)}</div></div>`).join('');
  document.getElementById('v2-chunks').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Chunks</h3>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">${items || '<p class="text-sm text-slate-500">No chunks</p>'}</div>
  `;
}

function renderRecs(result) {
  const recs = result.recommendations || [];
  const items = recs.map(r=> `<div class="card border rounded-xl p-3">
    <div class="text-xs uppercase text-slate-500">${r.priority||''}</div>
    <div class="text-sm font-semibold">${r.title||''}</div>
    <div class="text-xs text-slate-500">${r.area||''}</div>
  </div>`).join('');
  document.getElementById('v2-recs').innerHTML = `
    <h3 class="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-3">Recommendations</h3>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">${items || '<p class="text-sm text-slate-500">No recommendations</p>'}</div>
  `;
}

async function initV2(jobId) {
  try {
    const data = await fetchJob(jobId);
    if (data.status !== 'done') {
      const root = document.querySelector('.llm-v2');
      if (root) root.innerHTML = `<div class="bg-amber-50 border border-amber-200 text-amber-800 p-4 rounded-xl">Задача ещё выполняется (status=${data.status}). Обновите страницу позже.</div>`;
      return;
    }
    const result = data.result || {};
    renderHero(result);
    renderCritical(result);
    renderBotMatrix(result);
    renderContentVis(result);
    renderLLMSim(result);
    renderCloakingJs(result);
    renderEntityEeat(result);
    renderSchema(result);
    renderLinks(result);
    renderAccess(result);
    renderTech(result);
    renderChunks(result);
    renderRecs(result);
    // export buttons
    document.getElementById('v2-export-json').onclick = () => {
      const blob = new Blob([JSON.stringify(result, null, 2)], {type:'application/json'});
      const href = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = href; a.download = `llm_crawler_${jobId}.json`; a.click(); URL.revokeObjectURL(href);
    };
    document.getElementById('v2-export-docx').onclick = () => window.location.href = `${V2_API}/jobs/${encodeURIComponent(jobId)}/report.docx`;
    document.getElementById('v2-export-html').onclick = () => window.location.href = `${V2_API}/jobs/${encodeURIComponent(jobId)}/report`;
  } catch (e) {
    console.error(e);
    const root = document.querySelector('.max-w-6xl');
    if (root) root.innerHTML = `<div class="bg-rose-50 border border-rose-200 text-rose-700 p-4 rounded-xl">Ошибка загрузки результатов: ${e}</div>`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  if (window.Chart && window.Chart.defaults) {
    window.Chart.defaults.color = '#94a3b8';
  }
  if (window.llmJobId) {
    initV2(window.llmJobId);
  }
});
