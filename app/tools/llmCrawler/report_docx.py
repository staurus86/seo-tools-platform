from __future__ import annotations

import datetime as dt
from io import BytesIO
from typing import Any, Dict


def _safe(result: Dict[str, Any], path: list[str], default=""):
    cur = result
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return default
    return cur if cur is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _pct(value: Any, default: str = "-") -> str:
    try:
        return f"{round(float(value), 2)}%"
    except Exception:
        return default


def _yesno(value: Any) -> str:
    return "✅ Yes" if bool(value) else "❌ No"


def _words(text: Any, limit: int = 25) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    parts = raw.split()
    if len(parts) <= limit:
        return raw
    return " ".join(parts[:limit]) + "..."


def build_docx_v2(job: Dict[str, Any], job_id: str, wow_enabled: bool = True) -> BytesIO:
    try:
        from docx import Document  # type: ignore
        from docx.enum.text import WD_PARAGRAPH_ALIGNMENT  # type: ignore
        from docx.shared import Inches, Pt, RGBColor  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"python-docx unavailable: {exc}")

    result = job.get("result") or {}
    doc = Document()
    normal = doc.styles["Normal"].font
    normal.name = "Calibri"
    normal.size = Pt(11)

    for section in doc.sections:
        section.left_margin = Inches(0.8)
        section.right_margin = Inches(0.8)
        section.top_margin = Inches(0.7)
        section.bottom_margin = Inches(0.7)

    def add_heading(title: str, level: int = 1, color: str = "0F4C81") -> None:
        p = doc.add_heading("", level=level)
        run = p.add_run(title)
        run.font.color.rgb = RGBColor.from_string(color)

    def add_subtitle(text: str) -> None:
        p = doc.add_paragraph(text)
        p.runs[0].font.size = Pt(10)
        p.runs[0].font.color.rgb = RGBColor.from_string("475569")

    def add_kv(label: str, value: Any) -> None:
        p = doc.add_paragraph()
        r1 = p.add_run(f"{label}: ")
        r1.bold = True
        r1.font.color.rgb = RGBColor.from_string("0F172A")
        r2 = p.add_run(str(value))
        r2.font.color.rgb = RGBColor.from_string("111827")

    def add_table(headers, rows):
        table = doc.add_table(rows=1, cols=len(headers))
        table.style = "Table Grid"
        hdr_cells = table.rows[0].cells
        for i, h in enumerate(headers):
            hdr_cells[i].text = str(h)
            if hdr_cells[i].paragraphs and hdr_cells[i].paragraphs[0].runs:
                hdr_cells[i].paragraphs[0].runs[0].bold = True
        for row in rows:
            cells = table.add_row().cells
            for i, val in enumerate(row):
                cells[i].text = str(val)
        return table

    final_url = _safe(result, ["final_url"], _safe(result, ["requested_url"], "-"))
    score_total = _num(_safe(result, ["score", "total"], 0))
    projected = _num(_safe(result, ["projected_score_after_fixes"], score_total), score_total)
    citation = _num(_safe(result, ["citation_probability"], 0))
    eeat = _num(_safe(result, ["eeat_score", "score"], 0))
    trust = _num(_safe(result, ["trust_signal_score"], 0))
    ai_understanding = result.get("ai_understanding") or {}
    citation_breakdown = result.get("citation_breakdown") or {}
    discoverability = result.get("discoverability") or {}
    preview = result.get("ai_answer_preview") or {}
    llm = result.get("llm") or {}
    nojs = result.get("nojs") or {}
    nojs_content = nojs.get("content") or {}
    rendered = result.get("rendered") or {}
    rendered_content = (rendered.get("content") or {}) if isinstance(rendered, dict) else {}
    schema = nojs.get("schema") or {}
    resources = nojs.get("resources") or {}
    recommendations = result.get("recommendations") or []
    top_issues = (result.get("score") or {}).get("top_issues") or []
    bot_matrix = result.get("bot_matrix") or []
    diagnostics = (rendered.get("render_debug") or {}) if isinstance(rendered, dict) else {}
    js_dependency = result.get("js_dependency") or {}
    entity_graph = result.get("entity_graph") or {}
    metrics_bytes = result.get("metrics_bytes") or {}
    snippet_library = result.get("snippet_library") or {}
    projected_wf = result.get("projected_score_waterfall") or {}
    cloaking = result.get("cloaking") or {}
    content_loss = _num(result.get("content_loss_percent"), 0)
    main_text_len = int(_num(nojs_content.get("main_text_length"), 0))
    rendered_text_len = int(_num(rendered_content.get("main_text_length"), main_text_len))

    # PAGE 1: COVER
    add_heading("AI Crawlability & LLM Visibility Report", 0, color="0B3B63")
    add_subtitle("Enterprise LLM Crawler Simulation Report")
    doc.add_paragraph("")
    add_kv("URL analyzed", final_url)
    add_kv("Generated", f"{dt.datetime.now(dt.timezone.utc).isoformat()}")
    add_kv("Job ID", job_id)
    doc.add_paragraph("")
    score_p = doc.add_paragraph()
    score_p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    score_run = score_p.add_run(f"{round(score_total, 2)} / 100")
    score_run.bold = True
    score_run.font.size = Pt(34)
    score_run.font.color.rgb = RGBColor.from_string("0F4C81")
    lbl = doc.add_paragraph("AI Visibility Score")
    lbl.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    if lbl.runs:
        lbl.runs[0].font.color.rgb = RGBColor.from_string("475569")
    doc.add_page_break()

    # PAGE 2: EXECUTIVE SUMMARY
    add_heading("Executive Summary", 1, color="0B3B63")
    add_kv("Overall score", f"{round(score_total, 2)} / 100")
    add_kv("Projected score after fixes", f"{round(projected, 2)} / 100")
    add_kv("Citation likelihood", _pct(citation))
    if (result.get("eeat_score") or {}).get("status") == "not_evaluated":
        add_kv("EEAT score", f"— Not evaluated ({(result.get('eeat_score') or {}).get('reason', 'unknown')})")
    else:
        add_kv("EEAT score", _pct(eeat))
    add_kv("Trust completeness", _pct(trust))
    if projected_wf and wow_enabled:
        add_heading("Projected Impact Waterfall", 2, color="0E7490")
        rows = [["Baseline", projected_wf.get("baseline", score_total)]]
        for step in projected_wf.get("steps") or []:
            rows.append([f"{step.get('label', 'Step')} (+{step.get('delta', 0)})", step.get("value", "-")])
        rows.append(["Projected", projected_wf.get("target", projected)])
        add_table(["Step", "Score"], rows)
    doc.add_paragraph("")
    add_heading("⚠ Key Findings", 2, color="9A3412")
    if top_issues:
        for issue in top_issues[:6]:
            doc.add_paragraph(str(issue), style="List Bullet")
    else:
        doc.add_paragraph("No critical findings detected.")
    doc.add_page_break()

    # PAGE 3: WHAT AI UNDERSTANDS
    add_heading("AI Understanding Summary", 1, color="0B3B63")
    add_kv("Topic detected", _safe(ai_understanding, ["topic"], llm.get("summary", "Not detected")))
    add_kv("AI understanding score", _pct(_safe(ai_understanding, ["score"], 0)))
    add_kv("Topic confidence", _pct(_safe(ai_understanding, ["topic_confidence"], 0)))
    add_kv("Topic fallback used", _yesno(_safe(ai_understanding, ["topic_fallback_used"], False)))
    add_kv("Primary intent", _safe(ai_understanding, ["intent"], "informational"))
    if _safe(ai_understanding, ["content_clarity_status"], "") == "not_evaluated":
        add_kv("Content clarity", f"— Not evaluated ({_safe(ai_understanding, ['content_clarity_reason'], 'unknown')})")
    else:
        add_kv("Content clarity", _pct(_safe(ai_understanding, ["content_clarity"], 0)))
    add_kv("Discoverability score", _pct(discoverability.get("discoverability_score", 0)))
    add_kv("Click depth estimate", discoverability.get("click_depth_estimate", "-"))
    doc.add_paragraph("")
    add_heading("Detected Entities", 2, color="0E7490")
    entities = _safe(ai_understanding, ["entities"], [])
    if entities:
        for entity in entities[:20]:
            doc.add_paragraph(str(entity), style="List Bullet")
    else:
        doc.add_paragraph("No stable entities detected.")
    doc.add_page_break()

    # PAGE 4: CONTENT LOSS ANALYSIS
    add_heading("Content Loss Analysis", 1, color="0B3B63")
    add_kv("Total HTML text length", rendered_text_len)
    add_kv("Extracted main text length", main_text_len)
    add_kv("Lost content percent", _pct(content_loss))
    add_kv("HTML bytes", metrics_bytes.get("html_bytes", rendered_text_len))
    add_kv("Text bytes", metrics_bytes.get("text_bytes", main_text_len))
    add_kv("Text/HTML ratio", _pct(_num(metrics_bytes.get("text_html_ratio"), 0) * 100))
    add_kv("Main content ratio", _pct(_num(nojs_content.get("main_content_ratio"), 0) * 100))
    add_kv("Boilerplate ratio", _pct(_num(nojs_content.get("boilerplate_ratio"), 0) * 100))
    add_kv("Chunks count", len(nojs_content.get("chunks") or []))
    if wow_enabled and (nojs_content.get("chunks") or []):
        add_heading("Top Chunk Excerpts", 2, color="0E7490")
        for ch in (nojs_content.get("chunks") or [])[:3]:
            doc.add_paragraph(f"Chunk {ch.get('idx', '-')}: {_words(ch.get('text', ''), 35)}", style="List Bullet")
    add_heading("Extracted Text Preview", 2, color="0E7490")
    doc.add_paragraph(str(nojs_content.get("main_text_preview", ""))[:1200] or "No preview.")
    doc.add_page_break()

    # PAGE 5: CITATION READINESS BREAKDOWN
    add_heading("Citation Readiness Breakdown", 1, color="0B3B63")
    add_table(
        ["Factor", "Score"],
        [
            ["Schema", _pct(citation_breakdown.get("schema", 0))],
            ["Author", _pct(citation_breakdown.get("author", 0))],
            ["Content clarity", _pct(citation_breakdown.get("content_clarity", 0))],
            ["Bot accessibility", _pct(citation_breakdown.get("bot_accessibility", 0))],
            ["Structure", _pct(citation_breakdown.get("structure", 0))],
        ],
    )
    add_heading("Projected Improvement", 2, color="0E7490")
    add_kv("Current score", f"{round(score_total, 2)} / 100")
    add_kv("Projected score", f"{round(projected, 2)} / 100")
    doc.add_page_break()

    # PAGE 6: BOT ACCESS
    add_heading("Bot Access Matrix", 1, color="0B3B63")
    matrix_rows = []
    for m in bot_matrix:
        matrix_rows.append(
            [
                m.get("profile", "-"),
                "✅ Allowed" if m.get("allowed") else "❌ Blocked",
                m.get("reason", "-"),
            ]
        )
    add_table(["Bot", "Access", "Reason"], matrix_rows or [["-", "-", "-"]])
    add_heading("Directives", 2, color="0E7490")
    add_kv("meta robots", _safe(result, ["policies", "meta", "meta_robots"], "-"))
    add_kv("x-robots-tag", _safe(result, ["policies", "meta", "x_robots_tag"], "-"))
    doc.add_page_break()

    # PAGE 7: LLM SIMULATION
    add_heading("LLM Simulation", 1, color="0B3B63")
    add_kv("Summary", llm.get("summary", "Not available"))
    key_facts = llm.get("key_facts") or []
    if key_facts:
        add_heading("Key Facts", 2, color="0E7490")
        for fact in key_facts[:10]:
            doc.add_paragraph(str(fact), style="List Bullet")
    add_heading("Citation Spans", 2, color="0E7490")
    spans = llm.get("citation_spans") or []
    if spans:
        for span in spans[:12]:
            quoted = _words((span or {}).get("text", ""), 25)
            if quoted:
                doc.add_paragraph(f"\"{quoted}\"", style="List Bullet")
    else:
        doc.add_paragraph("No citation spans.")
    add_heading("AI Answer Preview", 2, color="0E7490")
    add_kv("Question", preview.get("question", "What is this page about?"))
    add_kv("Answer", preview.get("answer", "Not enough content"))
    add_kv("Confidence", _pct(preview.get("confidence"), "-"))
    doc.add_page_break()

    # PAGE 8: STRUCTURED DATA
    add_heading("Structured Data & Site Structure", 1, color="0B3B63")
    add_kv("Schema coverage", _pct(schema.get("coverage_score"), "-"))
    add_kv("Organization schema", _yesno("Organization" in (schema.get("jsonld_types") or [])))
    add_kv("Article schema", _yesno("Article" in (schema.get("jsonld_types") or [])))
    add_kv("Product schema", _yesno("Product" in (schema.get("jsonld_types") or [])))
    add_kv("Breadcrumb schema", _yesno("BreadcrumbList" in (schema.get("jsonld_types") or [])))
    add_kv("Microdata types", ", ".join((schema.get("microdata_types") or [])[:8]) or "-")
    add_kv("RDFa types", ", ".join((schema.get("rdfa_types") or [])[:8]) or "-")
    doc.add_page_break()

    # PAGE 9: TECHNICAL DIAGNOSTICS
    add_heading("Technical Diagnostics", 1, color="0B3B63")
    add_kv("JS dependency score", _pct(js_dependency.get("score"), "-"))
    add_kv("Failed resources", js_dependency.get("failures", 0))
    add_kv("Blocked scripts/styles", js_dependency.get("blocked", 0))
    add_kv("Cloaking status", cloaking.get("status", "not_executed"))
    if cloaking.get("status") == "executed":
        add_kv("Cloaking risk", cloaking.get("risk", "unknown"))
        add_kv("Similarity browser vs gptbot", _pct(_safe(cloaking, ["similarity_scores", "browser_vs_gptbot"], "-")))
        add_kv("Similarity browser vs googlebot", _pct(_safe(cloaking, ["similarity_scores", "browser_vs_googlebot"], "-")))
    else:
        add_kv("Cloaking reason", cloaking.get("reason", "not_run"))
    add_kv("Console errors", len(diagnostics.get("console_errors") or []))
    add_kv("Failed requests", len(diagnostics.get("failed_requests") or []))
    if diagnostics.get("console_errors"):
        add_heading("Top Console Errors", 2, color="9A3412")
        for row in (diagnostics.get("console_errors") or [])[:8]:
            doc.add_paragraph(str(row)[:220], style="List Bullet")
    doc.add_page_break()

    # PAGE 10: ACCESS BARRIERS
    add_heading("Access Barriers", 1, color="0B3B63")
    add_kv("Cookie wall", _yesno(resources.get("cookie_wall")))
    add_kv("Paywall", _yesno(resources.get("paywall")))
    add_kv("Login wall", _yesno(resources.get("login_wall")))
    add_kv("Strict CSP", _yesno(resources.get("csp_strict")))
    add_kv("Mixed content count", resources.get("mixed_content_count", 0))
    add_page = doc.add_paragraph("")
    if add_page.runs:
        add_page.runs[0].font.size = Pt(1)
    doc.add_page_break()

    # PAGE 11: ENTITY + RECOMMENDATIONS
    add_heading("Entity Graph & Recommendations", 1, color="0B3B63")
    add_kv("Organizations", ", ".join((entity_graph.get("organizations") or [])[:12]) or "-")
    add_kv("Persons", ", ".join((entity_graph.get("persons") or [])[:12]) or "-")
    add_kv("Products", ", ".join((entity_graph.get("products") or [])[:12]) or "-")
    add_kv("Locations", ", ".join((entity_graph.get("locations") or [])[:12]) or "-")
    doc.add_paragraph("")
    add_heading("🔧 Prioritized Fix Backlog", 2, color="9A3412")
    if recommendations:
        for rec in recommendations[:20]:
            pri = str(rec.get("priority", "P2")).upper()
            area = rec.get("area", "-")
            title = rec.get("title", "-")
            expected = rec.get("expected_lift") or ("+12 score" if pri == "P0" else "+7 score" if pri == "P1" else "+3 score")
            doc.add_paragraph(f"{pri} | {area} | {title} | expected impact {expected}", style="List Bullet")
            evidence = rec.get("evidence") or []
            if evidence:
                for ev in evidence[:3]:
                    doc.add_paragraph(f"Evidence: {ev}", style="List Bullet")
    else:
        doc.add_paragraph("No recommendations.")

    if wow_enabled and snippet_library:
        doc.add_page_break()
        add_heading("Appendix: Copy-Paste Snippets", 1, color="0B3B63")
        for key, snippet in snippet_library.items():
            add_heading(str(key), 2, color="0E7490")
            doc.add_paragraph(str(snippet)[:1800])

    bio = BytesIO()
    doc.save(bio)
    bio.seek(0)
    return bio
