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
    new_job_id,
    queue_depth,
)
from .schemas import LlmCrawlerJobStatusResponse, LlmCrawlerRunRequest


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


@router.post("/run")
async def run_llm_crawler(payload: LlmCrawlerRunRequest, request: Request) -> Dict[str, str]:
    _ensure_feature_enabled(request)
    request_id = _request_id(request)

    subject = request_subject(request)
    tool_limit = max(1, int(getattr(settings, "LLM_CRAWLER_RATE_LIMIT_PER_MINUTE", 5) or 5))
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

    options = payload.options.model_dump()
    if bool(options.get("renderJs")):
        render_limit = max(1, int(getattr(settings, "LLM_CRAWLER_RENDER_RATE_LIMIT_PER_DAY", 20) or 20))
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

    job_id = new_job_id()
    try:
        job = create_job_record(
            job_id=job_id,
            request_id=request_id,
            requested_url=payload.url,
            options=options,
        )
        enqueue_job(job)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"LLM crawler temporarily unavailable: {exc}",
        ) from exc

    return {"jobId": job_id}


@router.get("/jobs/{job_id}", response_model=LlmCrawlerJobStatusResponse)
async def get_llm_crawler_job(job_id: str, request: Request) -> Dict[str, Any]:
    _ensure_feature_enabled(request)
    job = get_job_record(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "jobId": str(job.get("jobId") or job_id),
        "requestId": str(job.get("requestId") or ""),
        "status": str(job.get("status") or "queued"),
        "progress": int(job.get("progress") or 0),
        "result": job.get("result"),
        "error": job.get("error"),
    }


@router.get("/worker-health")
async def llm_worker_health(request: Request) -> Dict[str, Any]:
    _ensure_feature_enabled(request)
    heartbeat = get_worker_heartbeat()
    queue_size = queue_depth()
    age_sec = None
    if heartbeat and heartbeat.get("updatedAt"):
        try:
            ts = datetime.fromisoformat(str(heartbeat["updatedAt"]).replace("Z", "+00:00"))
            age_sec = int((datetime.now(timezone.utc) - ts).total_seconds())
        except Exception:
            age_sec = None
    return {
        "queue_depth": queue_size,
        "worker_heartbeat": heartbeat,
        "worker_heartbeat_age_sec": age_sec,
        "status": "healthy" if heartbeat else "unknown",
    }

