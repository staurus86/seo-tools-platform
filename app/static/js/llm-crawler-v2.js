const V2_API = '/api/tools/llm-crawler';

function _esc(v) {
  return String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function _pct(v, fallback = '-') {
  if (v === null || v === undefined || v === '') return fallback;
  const n = _num(v, NaN);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, Math.round(n * 100) / 100));
}

function _riskClass(score) {
  if (score >= 80) return 'risk-low';
  if (score >= 50) return 'risk-mid';
  return 'risk-high';
}

function _scoreColor(score) {
  if (score >= 80) return '#16a34a';
  if (score >= 50) return '#d97706';
  return '#dc2626';
}

function _meter(value) {
  const val = Math.max(0, Math.min(100, _num(value, 0)));
  const color = val >= 80 ? '#16a34a' : val >= 50 ? '#d97706' : '#dc2626';
  return `<div class="meter-track"><div class="meter-fill" style="width:${val}%;background:${color}"></div></div>`;
}

function _setHTML(id, html) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = html;
}

function _lucide() {
  if (window.lucide && typeof window.lucide.createIcons === 'function') {
    window.lucide.createIcons();
  }
}

async function _fetchJob(jobId) {
  const resp = await fetch(`${V2_API}/jobs/${encodeURIComponent(jobId)}`);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

function _wowEnabled() {
  return true;
}

function _isNotEvaluated(module) {
  return module && String(module.status || '').toLowerCase() === 'not_evaluated';
}

function _metricValue(value, fallback = '—') {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function _copyText(txt) {
  const text = String(txt || '');
  if (!text) return;
  navigator.clipboard?.writeText(text).catch(() => {});
}

function _renderHero(result) {
  const score = _pct(result?.score?.total, 0);
  const projected = _pct(result?.projected_score_after_fixes, score);
  const citation = _pct(result?.citation_probability, 0);
  const eeatModule = result?.eeat_score || {};
  const ingestModule = result?.llm_ingestion || {};
  const eeat = _isNotEvaluated(eeatModule) ? '—' : _pct(result?.eeat_score?.score, 0);
  const ingest = _isNotEvaluated(ingestModule) ? '—' : _pct(result?.llm_ingestion?.avg_chunk_quality, 0);
  const js = _pct(result?.js_dependency?.score, 0);
  const pillClass = _riskClass(score);
  const pillText = score >= 80 ? 'Excellent' : score >= 50 ? 'Needs work' : 'Critical';
  const ringColor = _scoreColor(score);
  const ringDeg = Math.round((score / 100) * 360);
  _setHTML('v2-hero', `
    <div class="panel p-5 md:p-6">
      <div class="flex items-start justify-between gap-5 flex-wrap">
        <div class="flex gap-4">
          <div class="score-ring" style="background: conic-gradient(${ringColor} ${ringDeg}deg, #dbe5f3 0deg);">
            <div class="score-ring-inner">
              <div class="score-ring-value">${score}</div>
              <div class="score-ring-label">Score</div>
            </div>
          </div>
          <div>
            <div class="section-title flex items-center gap-2"><i data-lucide="sparkles" class="w-4 h-4"></i>AI Visibility Overview</div>
            <div class="metric-kpi">${score}<span class="text-xl font-semibold subtle">/100</span></div>
            <div class="subtle text-sm mt-1">Projected after fixes: <span class="font-semibold text-emerald-700">${projected}/100</span></div>
          </div>
        </div>
        <span class="risk-pill ${pillClass}">${pillText}</span>
      </div>
      <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mt-5">
        <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">Citation probability</div><div class="font-bold text-lg">${citation}%</div>${_meter(citation)}</div>
        <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">EEAT score</div><div class="font-bold text-lg">${eeat}${eeat === '—' ? '' : '%'}</div>${eeat === '—' ? `<div class="text-xs subtle mt-1">Not evaluated: ${_esc(eeatModule.reason || 'unknown')}</div>` : _meter(eeat)}</div>
        <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">Ingestion quality</div><div class="font-bold text-lg">${ingest}${ingest === '—' ? '' : '%'}</div>${ingest === '—' ? `<div class="text-xs subtle mt-1">Not evaluated: ${_esc(ingestModule.reason || 'unknown')}</div>` : _meter(ingest)}</div>
        <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">JS dependency risk</div><div class="font-bold text-lg">${js}%</div>${_meter(100 - js)}</div>
      </div>
    </div>
  `);
}

function _renderWhatAI(result) {
  const ai = result?.ai_understanding || {};
  const signals = result?.nojs?.signals || {};
  const graph = result?.entity_graph || {};
  const schemaTypes = result?.nojs?.schema?.jsonld_types || [];
  const hasOrg = Array.isArray(graph.organizations) ? graph.organizations.length > 0 : schemaTypes.includes('Organization');
  const topic = ai.topic || result?.llm?.summary || 'Topic not detected';
  const confidence = _pct(ai.topic_confidence ?? ai.score, 0);
  const entities = Array.isArray(ai.entities) ? ai.entities.slice(0, 8) : [];
  const clarity = ai.content_clarity;
  const clarityKnown = !(ai.content_clarity_status === 'not_evaluated' || clarity === null || clarity === undefined);
  _setHTML('v2-ai-understands', `
    <div class="section-title flex items-center gap-2"><i data-lucide="brain" class="w-4 h-4"></i>What AI Actually Understands</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="card p-4 bg-slate-50 border-slate-200">
        <div class="subtle text-xs">Topic detected</div>
        <div class="font-semibold mt-1">${_esc(topic)}</div>
        ${ai.topic_fallback_used ? '<div class="text-xs subtle mt-1">Fallback mode: title/H1/keywords heuristic</div>' : ''}
        <div class="subtle text-xs mt-3">Confidence</div>
        ${_meter(confidence)}
      </div>
      <div class="card p-4 bg-slate-50 border-slate-200 text-sm space-y-1">
        <div>Organization: ${hasOrg ? '✅' : '❌'}</div>
        <div>Product/entity detected: ${entities.length ? '✅' : '❌'}</div>
        <div>Author: ${signals.author_present ? '✅' : '❌'}</div>
        <div>Primary intent: ${_esc(ai.intent || 'informational')}</div>
        <div>Content clarity: <span class="font-semibold">${clarityKnown ? `${_pct(clarity, 0)}%` : '— Not evaluated'}</span></div>
        ${clarityKnown ? '' : `<div class="text-xs subtle">Reason: ${_esc(ai.content_clarity_reason || 'insufficient data')}</div>`}
        <div class="subtle text-xs mt-2">Detected entities: ${entities.length ? _esc(entities.join(', ')) : 'not enough data'}</div>
      </div>
    </div>
  `);
}

function _renderLoss(result) {
  const extracted = _num(result?.nojs?.content?.main_text_length, 0);
  const rendered = _num(result?.rendered?.content?.main_text_length, extracted);
  const loss = _pct(result?.content_loss_percent, Math.max(0, Math.round((1 - extracted / Math.max(1, rendered)) * 100)));
  const missing = Array.isArray(result?.diff?.missing) ? result.diff.missing : [];
  const m = result?.metrics_bytes || {};
  const htmlBytes = _metricValue(m.html_bytes, extracted ? extracted * 4 : 0);
  const textBytes = _metricValue(m.text_bytes, extracted);
  const ratio = _pct(_num(m.text_html_ratio, 0) * 100, 0);
  const mainRatio = _pct(_num(m.main_content_ratio, 0) * 100, 0);
  const boilRatio = _pct(_num(m.boilerplate_ratio, 0) * 100, 0);
  _setHTML('v2-loss', `
    <div class="section-title flex items-center gap-2"><i data-lucide="scissors" class="w-4 h-4"></i>Content Loss Visualization</div>
    <div class="grid grid-cols-3 gap-3 text-sm">
      <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">Total HTML text</div><div class="font-semibold">${rendered}</div></div>
      <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">Extracted main text</div><div class="font-semibold">${extracted}</div></div>
      <div class="card p-3 bg-slate-50 border-slate-200"><div class="subtle text-xs">Lost content</div><div class="font-semibold">${loss}%</div>${_meter(100 - loss)}</div>
    </div>
    <div class="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs mt-3">
      <div class="card p-2 bg-slate-50 border-slate-200"><div class="subtle">HTML bytes</div><div class="font-semibold">${htmlBytes}</div></div>
      <div class="card p-2 bg-slate-50 border-slate-200"><div class="subtle">Text bytes</div><div class="font-semibold">${textBytes}</div></div>
      <div class="card p-2 bg-slate-50 border-slate-200" title="${_esc(m.formula || 'text_html_ratio = text_bytes / html_bytes')}"><div class="subtle">Text/HTML ratio</div><div class="font-semibold">${ratio}%</div></div>
      <div class="card p-2 bg-slate-50 border-slate-200"><div class="subtle">Main content</div><div class="font-semibold">${mainRatio}%</div></div>
      <div class="card p-2 bg-slate-50 border-slate-200"><div class="subtle">Boilerplate</div><div class="font-semibold">${boilRatio}%</div></div>
    </div>
    <div class="text-xs subtle mt-3">Likely lost sections: ${_esc(missing.join(' | ') || 'navigation, footer, menu blocks')}</div>
  `);
}

function _renderCitation(result) {
  const b = result?.citation_breakdown || {};
  const labels = ['Schema', 'Author', 'Content', 'Accessibility', 'Structure'];
  const data = [_num(b.schema, 0), _num(b.author, 0), _num(b.content_clarity, 0), _num(b.bot_accessibility, 0), _num(b.structure, 0)];
  _setHTML('v2-citation', `
    <div class="section-title flex items-center gap-2"><i data-lucide="quote" class="w-4 h-4"></i>Citation Readiness Breakdown</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div><canvas id="v2-citation-radar" height="180"></canvas></div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Factor</th><th>Score</th></tr></thead>
          <tbody>
            ${labels.map((l, i) => `<tr><td>${l}</td><td>${data[i]}%</td></tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `);
  const ctx = document.getElementById('v2-citation-radar');
  if (ctx && window.Chart) {
    new Chart(ctx, {
      type: 'radar',
      data: {
        labels,
        datasets: [{
          label: 'Citation readiness',
          data,
          borderColor: '#0ea5e9',
          backgroundColor: 'rgba(14,165,233,0.16)',
          pointBackgroundColor: '#0284c7',
        }],
      },
      options: { scales: { r: { beginAtZero: true, max: 100 } }, plugins: { legend: { display: false } } },
    });
  }
}

function _renderTrust(result) {
  const trust = _pct(result?.trust_signal_score, 0);
  const schemaTypes = result?.nojs?.schema?.jsonld_types || [];
  const hasOrg = schemaTypes.includes('Organization');
  const hasArticle = schemaTypes.includes('Article');
  const hasProduct = schemaTypes.includes('Product');
  const hasAuthor = !!result?.nojs?.signals?.author_present;
  _setHTML('v2-trust', `
    <div class="section-title flex items-center gap-2"><i data-lucide="shield-check" class="w-4 h-4"></i>Trust Signal Detection</div>
    <div class="grid grid-cols-2 gap-3 text-sm">
      <div class="card p-3 bg-slate-50 border-slate-200">Author page: ${hasAuthor ? '✅' : '❌'}</div>
      <div class="card p-3 bg-slate-50 border-slate-200">Organization info: ${hasOrg ? '✅' : '❌'}</div>
      <div class="card p-3 bg-slate-50 border-slate-200">Structured schema: ${schemaTypes.length ? '✅' : '❌'}</div>
      <div class="card p-3 bg-slate-50 border-slate-200">Article/Product: ${(hasArticle || hasProduct) ? '✅' : '❌'}</div>
    </div>
    <div class="mt-3 text-sm">Trust completeness: <span class="font-semibold">${trust}%</span></div>
    ${_meter(trust)}
  `);
}

function _renderReasons(result) {
  const reasons = [
    { title: 'Missing schema', delta: (_num(result?.citation_breakdown?.schema, 0) < 50) ? -15 : 0 },
    { title: 'Missing author', delta: (_num(result?.citation_breakdown?.author, 0) === 0) ? -10 : 0 },
    { title: 'Content loss', delta: -Math.round(_num(result?.content_loss_percent, 0) / 5) },
  ].filter((x) => x.delta !== 0).sort((a, b) => a.delta - b.delta);
  _setHTML('v2-reasons', `
    <div class="section-title flex items-center gap-2"><i data-lucide="alert-triangle" class="w-4 h-4"></i>Why Score Is Low</div>
    <div class="space-y-2">
      ${reasons.length ? reasons.map((r, idx) => `
        <div class="card p-3 bg-slate-50 border-slate-200">
          <div class="flex items-center justify-between text-sm">
            <span>${idx + 1}. ${_esc(r.title)}</span>
            <span class="font-semibold text-rose-600">${r.delta} points</span>
          </div>
          ${_meter(Math.max(0, 100 + r.delta))}
        </div>
      `).join('') : '<div class="subtle text-sm">No strong negative factors detected.</div>'}
    </div>
  `);
}

function _renderFix(result) {
  const now = _pct(result?.score?.total, 0);
  const projected = _pct(result?.projected_score_after_fixes, now);
  const schemaGain = _num(result?.citation_breakdown?.schema, 0) < 50 ? 12 : 0;
  const authorGain = _num(result?.citation_breakdown?.author, 0) === 0 ? 8 : 0;
  const wf = result?.projected_score_waterfall || {};
  _setHTML('v2-fix', `
    <div class="section-title flex items-center gap-2"><i data-lucide="sparkles" class="w-4 h-4"></i>Fix Impact Simulation</div>
    <div class="text-sm">Current score: <span class="font-semibold">${now}/100</span></div>
    <div class="text-sm text-emerald-700">Projected score after key fixes: <span class="font-semibold">${projected}/100</span></div>
    <div class="mt-2">${_meter(projected)}</div>
    <div class="mt-3 text-xs subtle">Estimated lifts: Organization schema ${schemaGain ? `+${schemaGain}` : '+0'} | Author signals ${authorGain ? `+${authorGain}` : '+0'}</div>
    ${_wowEnabled() ? '<div class="mt-3"><canvas id="v2-waterfall" height="140"></canvas></div>' : ''}
  `);
  if (_wowEnabled() && window.Chart) {
    const ctx = document.getElementById('v2-waterfall');
    if (ctx) {
      const labels = ['Baseline', ...((wf.steps || []).map((s) => s.label || 'Step')), 'Projected'];
      const values = [wf.baseline ?? now, ...((wf.steps || []).map((s) => s.value || 0)), wf.target ?? projected];
      new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Score', data: values, backgroundColor: ['#64748b', ...labels.slice(1, -1).map(() => '#0ea5e9'), '#16a34a'] }] },
        options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, max: 100 } } },
      });
    }
  }
}

function _renderPreview(result) {
  const preview = result?.ai_answer_preview || {};
  const bullets = Array.isArray(preview.bullets) ? preview.bullets.slice(0, 3) : [];
  _setHTML('v2-preview', `
    <div class="section-title flex items-center gap-2"><i data-lucide="message-square-text" class="w-4 h-4"></i>AI Search Preview</div>
    <div class="card p-3 bg-slate-50 border-slate-200">
      <div class="subtle text-xs">When user asks</div>
      <div class="font-semibold mt-1">${_esc(preview.question || 'What is this page about?')}</div>
      <div class="text-sm mt-2">${_esc(preview.answer || 'Not enough content for stable answer')}</div>
      <div class="subtle text-xs mt-2">Mode: ${_esc(preview.preview_mode || result.preview_mode || 'extractive')} | Confidence: ${_pct(preview.confidence, '-')}</div>
      ${bullets.length ? `<ul class="list-disc pl-5 text-xs subtle mt-2">${bullets.map((b) => `<li>${_esc(b)}</li>`).join('')}</ul>` : ''}
    </div>
  `);
}

function _renderDiscoverability(result) {
  const d = result?.discoverability || {};
  const score = _pct(d.discoverability_score, 0);
  _setHTML('v2-discover', `
    <div class="section-title flex items-center gap-2"><i data-lucide="route" class="w-4 h-4"></i>Discoverability Score</div>
    <div class="text-sm">Internal link strength: <span class="font-semibold">${_pct(result?.nojs?.links?.anchor_quality_score, 0)}%</span></div>
    <div class="text-sm">Navigation depth estimate: <span class="font-semibold">${_esc(d.click_depth_estimate || '-')} clicks</span></div>
    <div class="text-sm">Discoverability score: <span class="font-semibold">${score}%</span></div>
    <div class="mt-2">${_meter(score)}</div>
  `);
}

function _renderDomDiff(result) {
  const browserText = String(result?.rendered?.content?.main_text_preview || '').slice(0, 1400);
  const aiText = String(result?.nojs?.content?.main_text_preview || '').slice(0, 1400);
  const missing = Array.isArray(result?.diff?.missing) ? result.diff.missing : [];
  _setHTML('v2-dom-diff', `
    <div class="section-title flex items-center gap-2"><i data-lucide="git-compare-arrows" class="w-4 h-4"></i>Visual DOM vs AI Text Diff</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
      <div>
        <div class="subtle text-xs mb-1">Browser view (rendered)</div>
        <div class="diff-box">${_esc(browserText || 'No rendered text')}</div>
      </div>
      <div>
        <div class="subtle text-xs mb-1">AI extracted view</div>
        <div class="diff-box">${_esc(aiText || 'No extracted text')}</div>
      </div>
    </div>
    <div class="mt-3 text-xs subtle">Likely removed sections: ${_esc(missing.join(' | ') || 'navigation / footer / utility blocks')}</div>
  `);
}

function _renderCritical(result) {
  const issues = Array.isArray(result?.score?.top_issues) ? result.score.top_issues.slice(0, 6) : [];
  const blocks = issues.length ? issues.map((txt, idx) => {
    const cls = idx < 2 ? 'risk-high' : idx < 4 ? 'risk-mid' : 'risk-low';
    const icon = idx < 2 ? 'alert-octagon' : idx < 4 ? 'alert-triangle' : 'check-circle-2';
    return `<button type="button" data-target="v2-reasons" class="card w-full text-left p-3 ${cls === 'risk-high' ? 'bg-red-50' : cls === 'risk-mid' ? 'bg-amber-50' : 'bg-emerald-50'} border-slate-200">
      <div class="flex items-start gap-2 text-sm">
        <i data-lucide="${icon}" class="w-4 h-4 mt-0.5"></i>
        <span>${_esc(txt)}</span>
      </div>
    </button>`;
  }).join('') : `<div class="card p-3 bg-emerald-50 text-emerald-800 text-sm">No critical issues detected.</div>`;
  _setHTML('v2-critical', blocks);
  document.querySelectorAll('#v2-critical [data-target]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = document.getElementById(btn.getAttribute('data-target'));
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    });
  });
}

function _renderBotMatrix(result) {
  const matrix = Array.isArray(result?.bot_matrix) ? result.bot_matrix : [];
  const rows = matrix.map((r) => {
    const access = r.allowed ? '✅ Allowed' : '❌ Blocked';
    const quality = _pct(result?.score?.total, 0);
    const risk = r.allowed ? 'Low' : 'High';
    return `<tr>
      <td>${_esc(r.profile)}</td>
      <td>${access}</td>
      <td>${quality}%</td>
      <td>${risk}</td>
    </tr>`;
  }).join('');
  _setHTML('v2-bot-matrix', `
    <div class="section-title flex items-center gap-2"><i data-lucide="bot" class="w-4 h-4"></i>Bot Access Matrix</div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Bot</th><th>Access</th><th>Content quality</th><th>Risk</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4">No bot matrix data</td></tr>'}</tbody>
      </table>
    </div>
  `);
}

function _renderContentExtraction(result) {
  const content = result?.nojs?.content || {};
  const ratio = _pct(_num(content.main_content_ratio, 0) * 100, 0);
  const boiler = _pct(_num(content.boilerplate_ratio, 0) * 100, 0);
  _setHTML('v2-content-vis', `
    <div class="section-title flex items-center gap-2"><i data-lucide="file-text" class="w-4 h-4"></i>Content Extraction Visualization</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div><canvas id="v2-content-donut" height="170"></canvas></div>
      <div class="space-y-2 text-sm">
        <div class="card p-3 bg-slate-50 border-slate-200">Main content: <span class="font-semibold">${ratio}%</span></div>
        <div class="card p-3 bg-slate-50 border-slate-200">Boilerplate: <span class="font-semibold">${boiler}%</span></div>
        <div class="card p-3 bg-slate-50 border-slate-200">Text length: <span class="font-semibold">${_num(content.main_text_length, 0)}</span></div>
        <div class="card p-3 bg-slate-50 border-slate-200">Chunks: <span class="font-semibold">${(content.chunks || []).length}</span></div>
      </div>
    </div>
    <details class="mt-3">
      <summary class="cursor-pointer text-sm font-medium">Extracted text preview</summary>
      <div class="diff-box mt-2">${_esc((content.main_text_preview || '').slice(0, 1500) || 'No preview')}</div>
    </details>
  `);
  const ctx = document.getElementById('v2-content-donut');
  if (ctx && window.Chart) {
    new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Main content', 'Boilerplate'],
        datasets: [{ data: [ratio, boiler], backgroundColor: ['#22c55e', '#cbd5e1'] }],
      },
      options: { plugins: { legend: { position: 'bottom' } } },
    });
  }
}

function _renderLLMSim(result) {
  const llm = result?.llm || {};
  if (!llm || llm.enabled === false) {
    _setHTML('v2-llm-sim', `
      <div class="section-title flex items-center gap-2"><i data-lucide="messages-square" class="w-4 h-4"></i>LLM Simulation</div>
      <div class="card p-3 bg-slate-50 border-slate-200 text-sm">
        <div class="font-semibold">— Not evaluated</div>
        <div class="subtle text-xs mt-1">Reason: LLM_SIMULATION disabled or no extractable content</div>
      </div>
    `);
    return;
  }
  const scores = llm?.scores || {};
  const spans = Array.isArray(llm?.citation_spans) ? llm.citation_spans.slice(0, 3) : [];
  const rank = Array.isArray(result?.chunk_ranking_debug) ? result.chunk_ranking_debug : [];
  _setHTML('v2-llm-sim', `
    <div class="section-title flex items-center gap-2"><i data-lucide="messages-square" class="w-4 h-4"></i>LLM Simulation</div>
    <div class="card p-3 bg-slate-50 border-slate-200">
      <div class="subtle text-xs mb-1">Summary</div>
      <div class="text-sm">${_esc(llm.summary || 'No summary')}</div>
    </div>
    <div class="grid grid-cols-1 gap-2 mt-3 text-sm">
      <div>Citation likelihood: <span class="font-semibold">${_pct(scores.citation_likelihood, '-')}%</span>${_meter(_num(scores.citation_likelihood, 0))}</div>
      <div>Hallucination risk: <span class="font-semibold">${_pct(scores.hallucination_risk, '-')}%</span>${_meter(100 - _num(scores.hallucination_risk, 0))}</div>
      <div>Answer quality: <span class="font-semibold">${_pct(scores.answer_quality_score, '-')}%</span>${_meter(_num(scores.answer_quality_score, 0))}</div>
    </div>
    <div class="mt-3 text-xs subtle">Citation spans: ${_esc(spans.map((s) => s.text || '').join(' | ') || 'none')}</div>
    ${rank.length ? `<div class="mt-2 text-xs subtle">Top chunks: ${_esc(rank.map((r) => `#${r.idx}:${r.score}`).join(' | '))}</div>` : ''}
  `);
}

function _renderCloakingJs(result) {
  const cloak = result?.cloaking || {};
  const js = result?.js_dependency || {};
  const status = String(cloak.status || '');
  const executed = status === 'executed';
  const evidence = Array.isArray(cloak.evidence) ? cloak.evidence.slice(0, 3) : [];
  _setHTML('v2-cloaking', `
    <div class="section-title flex items-center gap-2"><i data-lucide="shield-alert" class="w-4 h-4"></i>Cloaking & JS Dependency</div>
    <div class="grid grid-cols-1 gap-2 text-sm">
      <div class="card p-3 bg-slate-50 border-slate-200">
        <div class="subtle text-xs">Cloaking risk</div>
        <div class="font-semibold">${executed ? _esc(cloak.risk || 'unknown') : 'Not executed'}</div>
        ${executed ? `
          <div class="subtle text-xs mt-1">Browser vs GPTBot: ${_pct(cloak?.similarity_scores?.browser_vs_gptbot, '-')}</div>
          <div class="subtle text-xs">Browser vs Googlebot: ${_pct(cloak?.similarity_scores?.browser_vs_googlebot, '-')}</div>
          ${evidence.length ? `<ul class="list-disc pl-5 text-xs subtle mt-2">${evidence.map((e) => `<li>${_esc(e)}</li>`).join('')}</ul>` : ''}
        ` : `
          <div class="subtle text-xs mt-1">Reason: ${_esc(cloak.reason || 'profiles not requested')}</div>
          ${(cloak.can_run && _wowEnabled()) ? '<button id="v2-run-cloaking" class="mt-2 v2-btn v2-btn-primary text-xs">Run cloaking check</button>' : ''}
        `}
      </div>
      <div class="card p-3 bg-slate-50 border-slate-200">
        <div class="subtle text-xs">JS dependency score</div>
        <div class="font-semibold">${_pct(js.score, '-')}</div>
        <div class="subtle text-xs">Failed resources: ${_num(js.failures, 0)}</div>
        <div class="subtle text-xs">Blocked scripts/css: ${_num(js.blocked, 0)}</div>
      </div>
    </div>
  `);
  const runBtn = document.getElementById('v2-run-cloaking');
  if (runBtn) {
    runBtn.addEventListener('click', () => _runCloakingCheck(result));
  }
}

function _renderEntityEeat(result) {
  const eeat = result?.eeat_score || {};
  const graph = result?.entity_graph || {};
  const notEvaluated = _isNotEvaluated(eeat);
  _setHTML('v2-entity-eeat', `
    <div class="section-title flex items-center gap-2"><i data-lucide="network" class="w-4 h-4"></i>Entity & EEAT</div>
    <div class="text-sm space-y-1">
      <div>EEAT score: <span class="font-semibold">${notEvaluated ? '— Not evaluated' : _pct(eeat.score, '-')}</span></div>
      ${notEvaluated ? `<div class="text-xs subtle">Reason: ${_esc(eeat.reason || 'unknown')}</div>` : ''}
      <div>Organizations: ${_esc((graph.organizations || []).slice(0, 4).join(', ') || '—')}</div>
      <div>Persons: ${_esc((graph.persons || []).slice(0, 4).join(', ') || '—')}</div>
      <div>Products: ${_esc((graph.products || []).slice(0, 4).join(', ') || '—')}</div>
    </div>
  `);
}

function _renderSchema(result) {
  const schema = result?.nojs?.schema || {};
  const found = new Set((schema.jsonld_types || []).map((s) => String(s)));
  const checklist = ['Organization', 'Article', 'Product', 'Breadcrumb'];
  _setHTML('v2-schema', `
    <div class="section-title flex items-center gap-2"><i data-lucide="database" class="w-4 h-4"></i>Schema Visualization</div>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-2 text-sm">
      ${checklist.map((k) => `<div class="card p-3 bg-slate-50 border-slate-200">${found.has(k) ? '✅' : '❌'} ${k}</div>`).join('')}
    </div>
    <div class="subtle text-xs mt-3">Coverage: ${_pct(schema.coverage_score, 0)}% | Microdata: ${_esc((schema.microdata_types || []).slice(0, 6).join(', ') || 'none')}</div>
  `);
}

function _renderLinks(result) {
  const links = result?.nojs?.links || {};
  _setHTML('v2-links', `
    <div class="section-title flex items-center gap-2"><i data-lucide="link" class="w-4 h-4"></i>Link Discovery</div>
    <div class="grid grid-cols-2 gap-2 text-sm">
      <div class="card p-3 bg-slate-50 border-slate-200">Internal links: <span class="font-semibold">${_num(links.count, 0)}</span></div>
      <div class="card p-3 bg-slate-50 border-slate-200">JS-only links: <span class="font-semibold">${_num(links.js_only_count, 0)}</span></div>
      <div class="card p-3 bg-slate-50 border-slate-200">Anchor clarity: <span class="font-semibold">${_pct(links.anchor_quality_score, 0)}%</span></div>
      <div class="card p-3 bg-slate-50 border-slate-200">Unique links: <span class="font-semibold">${_num(links.unique_count, 0)}</span></div>
    </div>
  `);
}

function _renderAccess(result) {
  const r = result?.nojs?.resources || {};
  _setHTML('v2-access', `
    <div class="section-title flex items-center gap-2"><i data-lucide="lock" class="w-4 h-4"></i>Access Barriers</div>
    <div class="grid grid-cols-1 gap-2 text-sm">
      <div class="card p-3 ${r.cookie_wall ? 'bg-amber-50' : 'bg-slate-50'} border-slate-200">Cookie wall: ${r.cookie_wall ? '⚠ detected' : '✅ not detected'}</div>
      <div class="card p-3 ${r.paywall ? 'bg-amber-50' : 'bg-slate-50'} border-slate-200">Paywall: ${r.paywall ? '⚠ detected' : '✅ not detected'}</div>
      <div class="card p-3 ${r.login_wall ? 'bg-amber-50' : 'bg-slate-50'} border-slate-200">Login wall: ${r.login_wall ? '⚠ detected' : '✅ not detected'}</div>
      <div class="card p-3 ${r.csp_strict ? 'bg-amber-50' : 'bg-slate-50'} border-slate-200">Strict CSP: ${r.csp_strict ? '⚠ strict' : '✅ ok'}</div>
    </div>
  `);
}

function _renderTech(result) {
  const dbg = result?.rendered?.render_debug || {};
  const errors = Array.isArray(dbg.console_errors) ? dbg.console_errors : [];
  const failed = Array.isArray(dbg.failed_requests) ? dbg.failed_requests : [];
  _setHTML('v2-tech', `
    <div class="section-title flex items-center gap-2"><i data-lucide="terminal-square" class="w-4 h-4"></i>Technical Diagnostics</div>
    <div class="grid grid-cols-2 gap-2 text-sm mb-3">
      <div class="card p-3 bg-slate-50 border-slate-200">Console errors: <span class="font-semibold">${errors.length}</span></div>
      <div class="card p-3 bg-slate-50 border-slate-200">Failed requests: <span class="font-semibold">${failed.length}</span></div>
    </div>
    <details>
      <summary class="cursor-pointer text-sm font-medium">View console / network logs</summary>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3 mt-2">
        <div class="diff-box">${_esc(errors.join('\n') || 'No console errors')}</div>
        <div class="diff-box">${_esc(failed.map((x) => `[${x.resource_type || '-'}] ${x.url || ''} ${x.failure_text || ''}`).join('\n') || 'No failed requests')}</div>
      </div>
    </details>
  `);
}

function _renderChunks(result) {
  const chunks = Array.isArray(result?.nojs?.content?.chunks) ? result.nojs.content.chunks : [];
  const cards = chunks.slice(0, 12).map((c, idx) => {
    const text = String(c.text || '');
    const tokens = Math.max(1, Math.round(text.length / 4));
    const hasEntity = /[A-ZА-Я][a-zа-я]+/.test(text);
    return `<div class="card p-3 bg-slate-50 border-slate-200">
      <div class="flex items-center justify-between text-xs subtle"><span>Chunk ${idx + 1}</span><span>~${tokens} tokens</span></div>
      <div class="text-sm mt-1">${_esc(text.slice(0, 180))}${text.length > 180 ? '...' : ''}</div>
      <div class="text-xs mt-2">${hasEntity ? 'Contains key info ✅' : 'Low-signal chunk ⚠'}</div>
    </div>`;
  }).join('');
  _setHTML('v2-chunks', `
    <div class="section-title flex items-center gap-2"><i data-lucide="blocks" class="w-4 h-4"></i>Chunk Visualization</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">${cards || '<div class="subtle text-sm">No chunks available.</div>'}</div>
  `);
}

function _renderRecs(result) {
  const recs = Array.isArray(result?.recommendations) ? result.recommendations : [];
  const items = recs.map((r) => {
    const pri = String(r.priority || 'P2').toUpperCase();
    const badge = pri === 'P0' ? 'badge badge-p0' : pri === 'P1' ? 'badge badge-p1' : 'badge badge-p2';
    const impact = r.expected_lift || (pri === 'P0' ? '+12% AI visibility' : pri === 'P1' ? '+7% AI visibility' : '+3% AI visibility');
    const evidence = Array.isArray(r.evidence) ? r.evidence.slice(0, 3) : [];
    const source = Array.isArray(r.source) ? r.source.join(', ') : (r.source || '-');
    const snippet = r.snippet || '';
    const snippetBtn = snippet ? `<button type="button" class="v2-btn v2-btn-neutral text-xs mt-2" data-snippet="${_esc(snippet)}">Copy snippet</button>` : '';
    return `<div class="card p-3 bg-slate-50 border-slate-200">
      <div class="flex items-center justify-between">
        <span class="${badge}">${pri}</span>
        <span class="text-xs subtle">${impact}</span>
      </div>
      <div class="text-sm font-semibold mt-2">${_esc(r.title || 'Recommendation')}</div>
      <div class="text-xs subtle mt-1">Area: ${_esc(r.area || '-')}</div>
      ${evidence.length ? `<ul class="list-disc pl-5 text-xs subtle mt-2">${evidence.map((e) => `<li>${_esc(e)}</li>`).join('')}</ul>` : ''}
      <div class="text-xs subtle mt-1">Source: ${_esc(source)}</div>
      ${snippetBtn}
    </div>`;
  }).join('');
  const snippetsBlock = _wowEnabled() ? `
    <details class="mt-3">
      <summary class="cursor-pointer text-sm font-medium">Copy-paste templates</summary>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mt-2">
        ${Object.entries(result?.snippet_library || {}).map(([k, v]) => `
          <div class="card p-2 bg-slate-50 border-slate-200">
            <div class="text-xs font-semibold">${_esc(k)}</div>
            <div class="diff-box mt-1">${_esc(String(v).slice(0, 500))}</div>
            <button type="button" class="v2-btn v2-btn-neutral text-xs mt-2" data-snippet="${_esc(v)}">Copy snippet</button>
          </div>
        `).join('')}
      </div>
    </details>` : '';
  _setHTML('v2-recs', `
    <div class="section-title flex items-center gap-2"><i data-lucide="list-checks" class="w-4 h-4"></i>Recommendations</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">${items || '<div class="subtle text-sm">No recommendations.</div>'}</div>
    ${snippetsBlock}
  `);
  document.querySelectorAll('#v2-recs [data-snippet]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const txt = btn.getAttribute('data-snippet') || '';
      _copyText(txt);
      btn.textContent = 'Copied';
      setTimeout(() => { btn.textContent = 'Copy snippet'; }, 1200);
    });
  });
}

function _wireExports(jobId, result) {
  const jsonBtn = document.getElementById('v2-export-json');
  const docxBtn = document.getElementById('v2-export-docx');
  const htmlBtn = document.getElementById('v2-export-html');
  if (jsonBtn) {
    jsonBtn.onclick = () => {
      const blob = new Blob([JSON.stringify(result, null, 2)], { type: 'application/json' });
      const href = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = href;
      a.download = `llm_crawler_${jobId}.json`;
      a.click();
      URL.revokeObjectURL(href);
    };
  }
  if (docxBtn) docxBtn.onclick = () => { window.location.href = `${V2_API}/jobs/${encodeURIComponent(jobId)}/report.docx`; };
  if (htmlBtn) htmlBtn.onclick = () => { window.location.href = `${V2_API}/jobs/${encodeURIComponent(jobId)}/report`; };
}

async function _runCloakingCheck(result) {
  const url = result?.final_url || result?.requested_url;
  if (!url) return;
  try {
    const payload = {
      url,
      options: {
        renderJs: true,
        timeoutMs: 20000,
        profile: ['gptbot', 'google-extended', 'search-bot'],
        showHeaders: false,
        runCloaking: true,
      },
    };
    const resp = await fetch(`${V2_API}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !data.jobId) {
      throw new Error(data?.detail?.message || data?.detail || `HTTP ${resp.status}`);
    }
    window.location.href = `/llm-crawler/results/${encodeURIComponent(data.jobId)}`;
  } catch (err) {
    alert(`Failed to start cloaking check: ${err?.message || err}`);
  }
}

function _renderPending(status, progress, message) {
  const p = _pct(progress, 0);
  _setHTML('v2-hero', `
    <div class="panel p-5">
      <div class="section-title flex items-center gap-2"><i data-lucide="loader-circle" class="w-4 h-4 animate-spin"></i>LLM Crawler is running</div>
      <div class="text-sm subtle">Current status: <span class="font-semibold text-slate-700">${_esc(message || status)}</span></div>
      <div class="mt-3">${_meter(p)}</div>
      <div class="subtle text-xs mt-1">Progress: ${p}%</div>
    </div>
  `);
  _lucide();
}

function _renderError(message) {
  const root = document.querySelector('.llm-v2');
  if (!root) return;
  root.innerHTML = `<div class="card p-5 bg-red-50 border-red-200 text-red-700">Error loading results: ${_esc(message)}</div>`;
}

async function initV2(jobId) {
  try {
    const data = await _fetchJob(jobId);
    if (data.status !== 'done') {
      _renderPending(data.status || 'queued', data.progress, data.status_message);
      setTimeout(() => initV2(jobId), 2500);
      return;
    }

    const result = data.result || {};
    _renderHero(result);
    _renderWhatAI(result);
    _renderLoss(result);
    _renderCitation(result);
    _renderTrust(result);
    _renderReasons(result);
    _renderFix(result);
    _renderPreview(result);
    _renderDiscoverability(result);
    _renderDomDiff(result);
    _renderCritical(result);
    _renderBotMatrix(result);
    _renderContentExtraction(result);
    _renderLLMSim(result);
    _renderCloakingJs(result);
    _renderEntityEeat(result);
    _renderSchema(result);
    _renderLinks(result);
    _renderAccess(result);
    _renderTech(result);
    _renderChunks(result);
    _renderRecs(result);
    _wireExports(jobId, result);
    _lucide();
  } catch (err) {
    console.error(err);
    _renderError(err?.message || String(err));
  }
}

document.addEventListener('DOMContentLoaded', () => {
  if (window.llmJobId) {
    initV2(window.llmJobId);
  }
});
