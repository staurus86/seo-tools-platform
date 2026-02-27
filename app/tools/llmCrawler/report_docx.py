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


def build_docx_v2(job: Dict[str, Any], job_id: str) -> BytesIO:
    try:
        from docx import Document  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"python-docx unavailable: {exc}")

    result = job.get("result") or {}
    doc = Document()
    doc.styles["Normal"].font.name = "Arial"

    def add_heading(title: str, level: int = 1):
        doc.add_heading(title, level=level)

    def add_table(headers, rows):
        table = doc.add_table(rows=1, cols=len(headers))
        hdr_cells = table.rows[0].cells
        for i, h in enumerate(headers):
            hdr_cells[i].text = str(h)
        for row in rows:
            cells = table.add_row().cells
            for i, val in enumerate(row):
                cells[i].text = str(val)
        return table

    # COVER
    add_heading("AI Crawlability & LLM Visibility Report", 0)
    doc.add_paragraph(f"URL: {_safe(result, ['final_url'], _safe(result, ['requested_url'], '-'))}")
    doc.add_paragraph(f"Generated: {dt.datetime.utcnow().isoformat()}Z")
    doc.add_paragraph(f"Job ID: {job_id}")
    doc.add_paragraph("")
    doc.add_heading(str(_safe(result, ['score','total'],'-')), 1)
    doc.add_paragraph("AI-ready score")
    doc.add_page_break()

    # EXEC SUMMARY
    add_heading("Executive Summary", 1)
    doc.add_paragraph(f"Citation likelihood: {_safe(result, ['citation_probability'], '-')}")
    doc.add_paragraph(f"EEAT score: {_safe(result, ['eeat_score','score'], '-')}")
    for issue in (result.get("score") or {}).get("top_issues", [])[:5]:
        doc.add_paragraph(issue, style="List Bullet")
    doc.add_page_break()

    # BOT ACCESS
    add_heading("Bot Access", 1)
    matrix = result.get("bot_matrix") or []
    add_table(["Bot","Access","Reason"], [[m.get("profile"), "Allowed" if m.get("allowed") else "Blocked", m.get("reason","-")] for m in matrix])
    doc.add_page_break()

    # CONTENT
    add_heading("Content Extraction", 1)
    content = _safe(result, ["nojs","content"], {})
    doc.add_paragraph(f"Main content ratio: {content.get('main_content_ratio','-')}")
    doc.add_paragraph(f"Chunks: {len(content.get('chunks') or [])}")
    doc.add_paragraph(content.get("main_text_preview","")[:800])
    doc.add_page_break()

    # LLM SIM
    add_heading("LLM Simulation", 1)
    llm = result.get("llm") or {}
    doc.add_paragraph(llm.get("summary",""))
    for fact in llm.get("key_facts") or []:
        doc.add_paragraph(fact, style="List Bullet")
    doc.add_page_break()

    # STRUCTURED DATA
    add_heading("Structured Data", 1)
    schema = _safe(result, ["nojs","schema"], {})
    doc.add_paragraph(f"Coverage: {schema.get('coverage_score','-')}%")
    for t in schema.get("jsonld_types") or []:
        doc.add_paragraph(t, style="List Bullet")
    doc.add_page_break()

    # TECH
    add_heading("Technical Diagnostics", 1)
    js = result.get("js_dependency") or {}
    doc.add_paragraph(f"JS dependency score: {js.get('score','-')}")
    render_dbg = _safe(result, ["rendered","render_debug"], {})
    doc.add_paragraph(f"Console errors: {len(render_dbg.get('console_errors') or [])}")
    doc.add_paragraph(f"Failed requests: {len(render_dbg.get('failed_requests') or [])}")
    doc.add_page_break()

    # ACCESS BARRIERS
    add_heading("Access Barriers", 1)
    res = _safe(result, ["nojs","resources"], {})
    for key in ["cookie_wall","paywall","csp_strict"]:
        doc.add_paragraph(f"{key}: {res.get(key, False)}")
    doc.add_page_break()

    # RECOMMENDATIONS
    add_heading("Recommendations", 1)
    for rec in result.get("recommendations") or []:
        doc.add_paragraph(f"{rec.get('priority','')} {rec.get('area','')}: {rec.get('title','')}", style="List Bullet")

    bio = BytesIO()
    doc.save(bio)
    bio.seek(0)
    return bio
