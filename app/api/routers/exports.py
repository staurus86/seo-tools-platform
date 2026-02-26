"""
Export endpoints for all SEO tool reports (DOCX / XLSX).

Every handler reads the task from the store, delegates to the matching
report generator, and streams the file as an attachment.
"""
import os
import re
from datetime import datetime
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

from app.api.routers._task_store import append_task_artifact, get_task_result

router = APIRouter(tags=["Exports"])


class ExportRequest(BaseModel):
    task_id: str


# ─── helpers ───────────────────────────────────────────────────────────────


def _safe_domain(url: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", (urlparse(url).netloc or "site"))


def _ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d_%H-%M")


def _file_response(filepath: str, media_type: str, filename: str) -> Response:
    with open(filepath, "rb") as f:
        content = f.read()
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# ─── robots ────────────────────────────────────────────────────────────────


@router.post("/export/robots")
async def export_robots_word(data: ExportRequest):
    """Export robots.txt analysis to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "robots_check":
            return {"error": f"Unsupported task type for DOCX export: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_robots_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}

        return _file_response(filepath, _DOCX, f"robots_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


# ─── sitemap ───────────────────────────────────────────────────────────────


@router.post("/export/sitemap-xlsx")
async def export_sitemap_xlsx(data: ExportRequest):
    """Export sitemap validation report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "sitemap_validate":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = xlsx_generator.generate_sitemap_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}

        return _file_response(filepath, _XLSX, f"sitemap_report_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/sitemap-docx")
async def export_sitemap_docx(data: ExportRequest):
    """Export sitemap validation report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "sitemap_validate":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_sitemap_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}

        return _file_response(filepath, _DOCX, f"sitemap_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


# ─── bot checker ───────────────────────────────────────────────────────────


@router.post("/export/bot-xlsx")
async def export_bot_xlsx(data: ExportRequest):
    """Export bot check report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "bot_check":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = xlsx_generator.generate_bot_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}

        return _file_response(filepath, _XLSX, f"bot_report_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/bot-docx")
async def export_bot_docx(data: ExportRequest):
    """Export bot check report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "bot_check":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_bot_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}

        return _file_response(filepath, _DOCX, f"bot_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


# ─── mobile check ──────────────────────────────────────────────────────────


@router.post("/export/mobile-docx")
async def export_mobile_docx(data: ExportRequest, request: Request):
    """Export mobile check report to DOCX."""
    import traceback

    try:
        from app.reports.docx_generator import docx_generator

        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "mobile_check":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {
            "url": url,
            "results": task_result.get("results", task_result),
            "server_base_url": str(request.base_url),
        }

        filepath = docx_generator.generate_mobile_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _DOCX, f"mobile_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        print(f"[mobile-docx] export failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/export/mobile-xlsx")
async def export_mobile_xlsx(data: ExportRequest, request: Request):
    """Export mobile check report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "mobile_check":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        results = task_result.get("results", task_result) or {}
        issues_count = results.get("issues_count") or len(results.get("issues", []) or [])
        if issues_count <= 0:
            return {"error": "Проблемы не найдены, XLSX-отчет не формируется"}

        payload = {"url": url, "results": results, "server_base_url": str(request.base_url)}
        filepath = xlsx_generator.generate_mobile_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _XLSX, f"mobile_issues_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


# ─── render audit ──────────────────────────────────────────────────────────


@router.post("/export/render-docx")
async def export_render_docx(data: ExportRequest):
    """Export render audit report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "render_audit":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_render_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _DOCX, f"render_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/render-xlsx")
async def export_render_xlsx(data: ExportRequest):
    """Export render issues to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "render_audit":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        results = task_result.get("results", task_result) or {}
        issues_count = results.get("issues_count") or len(results.get("issues", []) or [])
        if issues_count <= 0:
            return {"error": "Проблемы не найдены, XLSX-отчет не формируется"}

        payload = {"url": url, "results": results}
        filepath = xlsx_generator.generate_render_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _XLSX, f"render_issues_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


# ─── onpage audit ──────────────────────────────────────────────────────────


@router.post("/export/onpage-docx")
async def export_onpage_docx(data: ExportRequest):
    """Export OnPage audit report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "onpage_audit":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_onpage_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _DOCX, f"onpage_report_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/onpage-xlsx")
async def export_onpage_xlsx(data: ExportRequest):
    """Export OnPage audit report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "onpage_audit":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = xlsx_generator.generate_onpage_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _XLSX, f"onpage_report_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


# ─── redirect checker ──────────────────────────────────────────────────────


@router.post("/export/redirect-checker-docx")
async def export_redirect_checker_docx(data: ExportRequest):
    """Export Redirect Checker report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "redirect_checker":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {}) or {}
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_redirect_checker_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(
            filepath, _DOCX, f"redirect_checker_report_{_safe_domain(url)}_{_ts()}.docx"
        )
    except Exception as e:
        return {"error": str(e)}


# ─── core web vitals ───────────────────────────────────────────────────────


@router.post("/export/core-web-vitals-docx")
async def export_core_web_vitals_docx(data: ExportRequest):
    """Export Core Web Vitals report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "core_web_vitals":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {}) or {}
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_core_web_vitals_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(
            filepath, _DOCX, f"core_web_vitals_report_{_safe_domain(url)}_{_ts()}.docx"
        )
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/core-web-vitals-xlsx")
async def export_core_web_vitals_xlsx(data: ExportRequest):
    """Export Core Web Vitals report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "core_web_vitals":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {}) or {}
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = xlsx_generator.generate_core_web_vitals_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(
            filepath, _XLSX, f"core_web_vitals_report_{_safe_domain(url)}_{_ts()}.xlsx"
        )
    except Exception as e:
        return {"error": str(e)}


# ─── site audit pro ────────────────────────────────────────────────────────


@router.post("/export/site-audit-pro-docx")
async def export_site_audit_pro_docx(data: ExportRequest):
    """Export Site Audit Pro report to DOCX."""
    from app.config import settings
    from app.reports.docx_generator import docx_generator

    try:
        if not getattr(settings, "SITE_AUDIT_PRO_ENABLED", True):
            return {"error": "Site Audit Pro is disabled by feature flag"}

        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "site_audit_pro":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_site_audit_pro_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _DOCX, f"site_audit_pro_{_safe_domain(url)}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/site-audit-pro-xlsx")
async def export_site_audit_pro_xlsx(data: ExportRequest):
    """Export Site Audit Pro report to XLSX."""
    from app.config import settings
    from app.reports.xlsx_generator import xlsx_generator

    try:
        if not getattr(settings, "SITE_AUDIT_PRO_ENABLED", True):
            return {"error": "Site Audit Pro is disabled by feature flag"}

        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Задача не найдена", "task_id": data.task_id}
        if task.get("task_type") != "site_audit_pro":
            return {"error": f"Неподдерживаемый тип задачи: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result) or {}}

        filepath = xlsx_generator.generate_site_audit_pro_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Не удалось сформировать отчет"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _XLSX, f"site_audit_pro_{_safe_domain(url)}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


# ─── clusterizer ───────────────────────────────────────────────────────────


@router.post("/export/clusterizer-xlsx")
async def export_clusterizer_xlsx(data: ExportRequest):
    """Export keyword clusterizer report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "clusterizer":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        payload = {
            "url": task.get("url", "") or task_result.get("url", ""),
            "results": task_result.get("results", task_result),
        }

        filepath = xlsx_generator.generate_clusterizer_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        return _file_response(filepath, _XLSX, f"clusterizer_report_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}


# ─── link profile ──────────────────────────────────────────────────────────


@router.post("/export/link-profile-docx")
async def export_link_profile_docx(data: ExportRequest):
    """Export link profile audit report to DOCX."""
    from app.reports.docx_generator import docx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "link_profile_audit":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = docx_generator.generate_link_profile_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        domain = re.sub(r"[^a-zA-Z0-9._-]+", "_", (urlparse(url).netloc or str(url) or "site"))
        return _file_response(filepath, _DOCX, f"link_profile_report_{domain}_{_ts()}.docx")
    except Exception as e:
        return {"error": str(e)}


@router.post("/export/link-profile-xlsx")
async def export_link_profile_xlsx(data: ExportRequest):
    """Export link profile audit report to XLSX."""
    from app.reports.xlsx_generator import xlsx_generator

    try:
        task = get_task_result(data.task_id)
        if not task:
            return {"error": "Task not found", "task_id": data.task_id}
        if task.get("task_type") != "link_profile_audit":
            return {"error": f"Unsupported task type: {task.get('task_type')}"}

        task_result = task.get("result", {})
        url = task.get("url", "") or task_result.get("url", "")
        payload = {"url": url, "results": task_result.get("results", task_result)}

        filepath = xlsx_generator.generate_link_profile_report(data.task_id, payload)
        if not filepath or not os.path.exists(filepath):
            return {"error": "Failed to generate report"}
        append_task_artifact(data.task_id, filepath, kind="export")

        domain = re.sub(r"[^a-zA-Z0-9._-]+", "_", (urlparse(url).netloc or str(url) or "site"))
        return _file_response(filepath, _XLSX, f"link_profile_report_{domain}_{_ts()}.xlsx")
    except Exception as e:
        return {"error": str(e)}
