"""FastAPI routes for LLM Crawler Simulation."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
import uuid

from fastapi import APIRouter, HTTPException, Request

from app.config import settings

from .feature_gate import is_llm_crawler_enabled_for_request, request_subject
from .queue import (
    check_rate_limit,
    create_job_record,
    enqueue_job,
    get_job_record,
    get_worker_heartbeat,
    get_redis_client,
    new_job_id,
    queue_depth,
    update_job_record,
    inc_subject,
)
from .schemas import LlmCrawlerJobStatusResponse, LlmCrawlerRunRequest
from .service import run_llm_crawler_simulation
from fastapi.responses import HTMLResponse
from fastapi.responses import Response
from io import BytesIO
import datetime as dt


router = APIRouter(prefix="/api/tools/llm-crawler", tags=["LLM Crawler Simulation"])


def _ensure_feature_enabled(request: Request) -> None:
    if not is_llm_crawler_enabled_for_request(request):
        raise HTTPException(
            status_code=404,
            detail="LLM Crawler Simulation is disabled by feature flag",
        )


def _request_id(request: Request) -> str:
    header_id = str(request.headers.get("x-request-id", "") or "").strip()
    return header_id or f"req-{uuid.uuid4().hex[:12]}"


def _heartbeat_age_sec(heartbeat: Dict[str, Any] | None) -> int | None:
    if not heartbeat or not heartbeat.get("updatedAt"):
        return None
    try:
        ts = datetime.fromisoformat(str(heartbeat["updatedAt"]).replace("Z", "+00:00"))
        return int((datetime.now(timezone.utc) - ts).total_seconds())
    except Exception:
        return None


def _worker_is_healthy() -> bool:
    heartbeat = get_worker_heartbeat()
    age_sec = _heartbeat_age_sec(heartbeat)
    if heartbeat is None or age_sec is None:
        return False
    ttl = max(30, int(getattr(settings, "LLM_CRAWLER_WORKER_HEARTBEAT_TTL_SEC", 120) or 120))
    return age_sec <= max(60, ttl * 2)


def _ensure_worker_available() -> None:
    if not bool(getattr(settings, "LLM_CRAWLER_REQUIRE_HEALTHY_WORKER", True)):
        return
    if _worker_is_healthy():
        return
    raise HTTPException(
        status_code=503,
        detail="LLM worker is unavailable or stale. Check worker deployment and REDIS_URL, then retry.",
    )


def _job_age_sec(job: Dict[str, Any]) -> int | None:
    raw = job.get("createdAt") or job.get("updatedAt")
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return int((datetime.now(timezone.utc) - ts).total_seconds())
    except Exception:
        return None


def _normalize_targets(payload: LlmCrawlerRunRequest) -> list[str]:
    mode = str(payload.mode or "single_url").lower()
    urls: list[str] = []
    if mode == "url_list":
        urls = [str(u).strip() for u in (payload.urls or []) if str(u).strip()]
    elif mode == "sitemap":
        if payload.sitemap_url:
            try:
                import requests
                resp = requests.get(payload.sitemap_url, timeout=8)
                if resp.ok:
                    import re
                    urls = re.findall(r"<loc>([^<]+)</loc>", resp.text, flags=re.I)
            except Exception:
                urls = []
    else:
        if payload.url:
            urls = [str(payload.url).strip()]
    # Failsafe: unique, preserve order
    seen = set()
    normalized = []
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        normalized.append(u)
    return normalized


@router.post("/run")
async def run_llm_crawler(payload: LlmCrawlerRunRequest, request: Request) -> Dict[str, Any]:
    _ensure_feature_enabled(request)
    request_id = _request_id(request)

    limits_enabled = bool(getattr(settings, "LLM_CRAWLER_LIMITS_ENABLED", False))
    subject = request_subject(request)
    options = payload.options.model_dump()
    if limits_enabled:
        tool_limit = max(1, int(getattr(settings, "LLM_CRAWLER_RATE_LIMIT_PER_MINUTE", 999) or 999))
        tool_rate = check_rate_limit(subject, "tool-minute", tool_limit, 60)
        if not tool_rate.get("allowed", True):
            raise HTTPException(
                status_code=429,
                detail={
                    "message": "LLM crawler rate limit exceeded (tool).",
                    "reset_in": tool_rate.get("reset_in", 60),
                    "remaining": tool_rate.get("remaining", 0),
                },
            )
        if bool(options.get("renderJs")):
            render_limit = max(1, int(getattr(settings, "LLM_CRAWLER_RENDER_RATE_LIMIT_PER_DAY", 999) or 999))
            render_rate = check_rate_limit(subject, "render-day", render_limit, 86400)
            if not render_rate.get("allowed", True):
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Rendered fetch daily limit exceeded.",
                        "reset_in": render_rate.get("reset_in", 86400),
                        "remaining": render_rate.get("remaining", 0),
                    },
                )

    worker_ok = _worker_is_healthy()
    # Inline Playwright fallback запрещаем для изоляции: если воркер недоступен — вернём 503
    inline_allowed = False

    targets = _normalize_targets(payload)
    if not targets:
        raise HTTPException(status_code=400, detail="Provide url / urls or sitemap_url")

    # For now limits on batch size are disabled unless flag on; still cap extreme to 200 to protect service.
    hard_cap = 200
    if len(targets) > hard_cap:
        targets = targets[:hard_cap]

    job_ids: list[str] = []

    # Enforce per-subject rate limits when enabled
    if limits_enabled:
        per_minute_limit = max(1, int(getattr(settings, "MAX_JOBS_PER_MINUTE", 10) or 10))
        per_minute_rate = check_rate_limit(subject, "subject-minute", per_minute_limit, 60)
        if not per_minute_rate.get("allowed", True):
            raise HTTPException(
                status_code=429,
                detail={"message": "Too many jobs per minute for this subject", "retry_after": per_minute_rate.get("reset_in")},
            )

    if not worker_ok:
        raise HTTPException(
            status_code=503,
            detail="LLM worker unavailable; please retry when worker is healthy",
        )

    for idx, target_url in enumerate(targets):
        _ensure_worker_available()

        job_id = new_job_id()
        try:
            if limits_enabled:
                concurrent = inc_subject(subject)
                if concurrent > int(getattr(settings, "MAX_CONCURRENT_JOBS", 2) or 2):
                    dec_subject(subject)
                    raise HTTPException(
                        status_code=429,
                        detail={"message": "Too many concurrent jobs for subject"},
                    )
            job = create_job_record(
                job_id=job_id,
                request_id=request_id,
                requested_url=target_url,
                options=options,
                subject=subject,
                status_message="Queued",
            )
            enqueue_job(job)
            job_ids.append(job_id)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"LLM crawler temporarily unavailable: {exc}",
            ) from exc

    return {"jobId": job_ids[0], "jobIds": job_ids, "total": len(job_ids), "status": "queued"}


@router.get("/jobs/{job_id}", response_model=LlmCrawlerJobStatusResponse)
async def get_llm_crawler_job(job_id: str, request: Request) -> Dict[str, Any]:
    _ensure_feature_enabled(request)
    job = get_job_record(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status = str(job.get("status") or "queued")
    if status in {"queued", "running"}:
        timeout_sec = max(60, int(getattr(settings, "LLM_CRAWLER_STUCK_JOB_TIMEOUT_SEC", 300) or 300))
        age_sec = _job_age_sec(job)
        if age_sec is not None and age_sec >= timeout_sec and not _worker_is_healthy():
            job = update_job_record(
                job_id,
                status="error",
                progress=100,
                error="Job timed out in queue: worker unavailable.",
            )
    return {
        "jobId": str(job.get("jobId") or job_id),
        "requestId": str(job.get("requestId") or ""),
        "status": str(job.get("status") or "queued"),
        "progress": int(job.get("progress") or 0),
        "status_message": job.get("status_message"),
        "result": job.get("result"),
        "error": job.get("error"),
    }


@router.get("/worker-health")
async def llm_worker_health(request: Request) -> Dict[str, Any]:
    _ensure_feature_enabled(request)
    heartbeat = get_worker_heartbeat()
    queue_size = queue_depth()
    age_sec = _heartbeat_age_sec(heartbeat)
    return {
        "queue_depth": queue_size,
        "worker_heartbeat": heartbeat,
        "worker_heartbeat_age_sec": age_sec,
        "status": "healthy" if _worker_is_healthy() else "unknown",
    }


@router.get("/jobs/{job_id}/report", response_class=HTMLResponse)
async def llm_crawler_report(job_id: str, request: Request) -> HTMLResponse:
    _ensure_feature_enabled(request)
    if not bool(getattr(settings, "LLM_REPORT_HTML_ENABLED", False)):
        raise HTTPException(status_code=404, detail="HTML report disabled")
    job = get_job_record(job_id)
    if not job or not job.get("result"):
        raise HTTPException(status_code=404, detail="Job not found")
    result = job.get("result") or {}
    title = f"LLM Crawler Report — {result.get('final_url') or result.get('requested_url') or job_id}"
    score = (result.get("score") or {}).get("total", "-")
    summary = (result.get("llm") or {}).get("summary") or ""
    recs = result.get("recommendations") or []
    body = f"""
    <html><head><meta charset="utf-8"><title>{title}</title>
    <style>body{{font-family:Arial,sans-serif;max-width:960px;margin:32px auto;padding:0 12px;}}h1{{margin-bottom:4px;}}.card{{border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin:12px 0;}}</style>
    </head><body>
    <h1>{title}</h1>
    <p>Score: <strong>{score}</strong></p>
    <div class="card"><h3>Summary</h3><p>{summary or '—'}</p></div>
    <div class="card"><h3>Recommendations</h3><ul>
    {''.join([f"<li><strong>{r.get('priority','')}</strong> {r.get('area','')}: {r.get('title','')}</li>" for r in recs]) or '<li>None</li>'}
    </ul></div>
    <div class="card"><h3>What bots miss</h3><ul>
    {''.join([f"<li>{x}</li>" for x in (result.get('diff') or {}).get('missing', [])]) or '<li>None</li>'}
    </ul></div>
    </body></html>
    """
    return HTMLResponse(content=body)


@router.get("/jobs/{job_id}/report.docx")
async def llm_crawler_report_docx(job_id: str, request: Request) -> Response:
    _ensure_feature_enabled(request)
    if not (bool(getattr(settings, "LLM_REPORT_HTML_ENABLED", False)) or bool(getattr(settings, "LLM_REPORT_V2_ENABLED", False))):
        raise HTTPException(status_code=404, detail="DOCX report disabled")
    job = get_job_record(job_id)
    if not job or not job.get("result"):
        raise HTTPException(status_code=404, detail="Job not found")
    from app.tools.llmCrawler.report_docx import build_docx_v2
    payload = build_docx_v2(job, job_id)
    return Response(
        content=payload.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="llm_crawler_{job_id}.docx"'},
    )
