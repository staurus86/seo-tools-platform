"""
API Routes for SEO Tools
"""
from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from celery.result import AsyncResult
from typing import Optional
import os
from datetime import datetime, timedelta

from app.api.schemas import (
    SiteAnalyzeRequest, RobotsCheckRequest, SitemapValidateRequest,
    RenderAuditRequest, MobileCheckRequest, BotCheckRequest,
    TaskResponse, TaskResult, TaskStatus, TaskType,
    RateLimitInfo
)
from app.core.celery_app import celery_app
from app.core.rate_limiter import check_rate_limit_http, rate_limiter
from app.core.progress import progress_tracker
from app.config import settings


router = APIRouter(prefix="/api", tags=["SEO Tools"])


# Import tasks
from app.core.tasks import (
    analyze_site_task, check_robots_task, validate_sitemap_task,
    audit_render_task, check_mobile_task, check_bots_task
)


# Temporary: Disable rate limiting for testing
def no_rate_limit():
    return {"allowed": True, "remaining": 9999}

@router.post("/tasks/site-analyze", response_model=TaskResponse)
async def create_site_analyze_task(
    request: Request,
    data: SiteAnalyzeRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на анализ сайта"""
    task = analyze_site_task.delay(
        url=str(data.url),
        max_pages=data.max_pages,
        use_js=data.use_js,
        ignore_robots=data.ignore_robots,
        check_external=data.check_external
    )
    
    return TaskResponse(
        task_id=task.id,
        status="PENDING",
        message=f"Site analysis task created. Max pages: {data.max_pages}"
    )


@router.post("/tasks/robots-check", response_model=TaskResponse)
async def create_robots_check_task(
    request: Request,
    data: RobotsCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на проверку robots.txt"""
    try:
        # Try Celery first
        task = check_robots_task.delay(url=str(data.url))
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Robots.txt check task created"
        )
    except Exception as e:
        # Fallback: run synchronously
        print(f"Celery failed, running sync: {e}")
        result = check_robots_task.run(url=str(data.url))
        task_id = "sync-" + str(datetime.now().timestamp())
        sync_results[task_id] = result
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message="Task completed synchronously"
        )


@router.post("/tasks/sitemap-validate", response_model=TaskResponse)
async def create_sitemap_validate_task(
    request: Request,
    data: SitemapValidateRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на валидацию sitemap"""
    task = validate_sitemap_task.delay(url=str(data.url))
    
    return TaskResponse(
        task_id=task.id,
        status="PENDING",
        message="Sitemap validation task created"
    )


@router.post("/tasks/render-audit", response_model=TaskResponse)
async def create_render_audit_task(
    request: Request,
    data: RenderAuditRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на аудит рендеринга"""
    task = audit_render_task.delay(
        url=str(data.url),
        user_agent=data.user_agent
    )
    
    return TaskResponse(
        task_id=task.id,
        status="PENDING",
        message="Render audit task created"
    )


@router.post("/tasks/mobile-check", response_model=TaskResponse)
async def create_mobile_check_task(
    request: Request,
    data: MobileCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на проверку мобильной версии"""
    task = check_mobile_task.delay(
        url=str(data.url),
        devices=data.devices
    )
    
    return TaskResponse(
        task_id=task.id,
        status="PENDING",
        message="Mobile check task created"
    )


@router.post("/tasks/bot-check", response_model=TaskResponse)
async def create_bot_check_task(
    request: Request,
    data: BotCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Создает задачу на проверку доступности для ботов"""
    task = check_bots_task.delay(url=str(data.url))
    
    return TaskResponse(
        task_id=task.id,
        status="PENDING",
        message="Bot check task created"
    )


# Store for synchronous results
sync_results = {}

@router.get("/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    """Получает статус и результат задачи"""
    # Check if it's a sync result
    if task_id.startswith("sync-"):
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            task_type=TaskType.ROBOTS_CHECK,
            url="",
            created_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            result=sync_results.get(task_id, {}),
            error=None,
            can_continue=False
        )
    
    task_result = AsyncResult(task_id, app=celery_app)
    
    # Get progress if available
    progress = progress_tracker.get_progress(task_id)
    
    # Determine task type from result if available
    task_type = TaskType.SITE_ANALYZE  # Default
    url = ""
    
    result_data = None
    error_msg = None
    can_continue = False
    
    if task_result.state == "SUCCESS":
        result_data = task_result.result
        if isinstance(result_data, dict):
            task_type_str = result_data.get("task_type", "site_analyze")
            task_type = TaskType(task_type_str)
            url = result_data.get("url", "")
    elif task_result.state == "FAILURE":
        error_msg = str(task_result.result) if task_result.result else "Unknown error"
        # Check if it's a timeout error
        if error_msg and ("time limit" in error_msg.lower() or "timeout" in error_msg.lower()):
            can_continue = True
    
    return TaskResult(
        task_id=task_id,
        status=TaskStatus(task_result.state),
        task_type=task_type,
        url=url,
        created_at=datetime.fromtimestamp(task_result.date_done.timestamp()) if task_result.date_done else datetime.utcnow(),
        completed_at=datetime.fromtimestamp(task_result.date_done.timestamp()) if task_result.date_done and task_result.state == "SUCCESS" else None,
        progress=TaskProgress(**progress) if progress else None,
        result=result_data,
        error=error_msg,
        can_continue=can_continue
    )


@router.post("/tasks/{task_id}/continue")
async def continue_task(task_id: str):
    """Продолжает задачу, которая превысила лимит времени"""
    # Get old task info
    old_task = AsyncResult(task_id, app=celery_app)
    
    if old_task.state != "FAILURE":
        raise HTTPException(status_code=400, detail="Task can only be continued if it failed")
    
    # Get original arguments
    # Note: In production, you'd store these in Redis or DB
    # For now, we'll require the client to resubmit
    raise HTTPException(
        status_code=501,
        detail="Task continuation requires resubmission with same parameters"
    )


@router.get("/download/{task_id}/{format}")
async def download_report(task_id: str, format: str):
    """Скачивает отчет в указанном формате (xlsx или docx)"""
    if format not in ["xlsx", "docx"]:
        raise HTTPException(status_code=400, detail="Format must be 'xlsx' or 'docx'")
    
    # Check if file exists
    filename = f"{task_id}.{format}"
    filepath = os.path.join(settings.REPORTS_DIR, filename)
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Report not found or not ready")
    
    return FileResponse(
        filepath,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument." + 
                  ("spreadsheetml.sheet" if format == "xlsx" else "wordprocessingml.document")
    )


@router.get("/rate-limit", response_model=RateLimitInfo)
async def get_rate_limit(request: Request):
    """Получает информацию о rate limit для текущего IP"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.client.host
    
    result = rate_limiter.check_rate_limit(ip)
    
    return RateLimitInfo(
        allowed=result["allowed"],
        remaining=result["remaining"],
        reset_in=result["reset_in"],
        limit=rate_limiter.limit
    )
