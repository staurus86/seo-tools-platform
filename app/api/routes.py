"""
API Routes for SEO Tools - With synchronous fallback
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from celery.result import AsyncResult
from typing import Optional
import os
from datetime import datetime

from app.api.schemas import (
    SiteAnalyzeRequest, RobotsCheckRequest, SitemapValidateRequest,
    RenderAuditRequest, MobileCheckRequest, BotCheckRequest,
    TaskResponse, TaskResult, TaskStatus, TaskType
)
from app.core.progress import progress_tracker


router = APIRouter(prefix="/api", tags=["SEO Tools"])

# Import tasks
from app.core.tasks import (
    analyze_site_task, check_robots_task, validate_sitemap_task,
    audit_render_task, check_mobile_task, check_bots_task
)

# Storage for synchronous results
sync_results = {}


def check_celery_available():
    """Check if Celery worker is available"""
    try:
        from app.core.celery_app import celery_app
        # Try to get worker statistics
        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        return stats is not None
    except Exception as e:
        print(f"Celery not available: {e}")
        return False


def run_task_sync(task_func, **kwargs):
    """Run task synchronously and return result"""
    print(f"Running {task_func.__name__} synchronously...")
    result = task_func.run(**kwargs)
    task_id = f"sync-{datetime.now().timestamp()}"
    sync_results[task_id] = result
    return task_id, result


# Get celery_app only when needed
_celery_app = None

def get_celery_app():
    global _celery_app
    if _celery_app is None:
        from app.core.celery_app import celery_app
        _celery_app = celery_app
    return _celery_app


def get_task_result(task_id):
    """Get task result - from Celery or sync storage"""
    if task_id.startswith("sync-"):
        # Synchronous result
        if task_id in sync_results:
            return {
                "state": "SUCCESS",
                "result": sync_results[task_id],
                "date_done": datetime.utcnow()
            }
        return {"state": "PENDING", "result": None, "date_done": None}
    
    # Celery result
    try:
        app = get_celery_app()
        result = AsyncResult(task_id, app=app)
        return {
            "state": result.state,
            "result": result.result if hasattr(result, 'result') else None,
            "date_done": result.date_done
        }
    except Exception as e:
        print(f"Error getting Celery result: {e}")
        return {"state": "PENDING", "result": None, "date_done": None}


# Rate limiting disabled for testing
def no_rate_limit():
    return {"allowed": True, "remaining": 9999}


@router.post("/tasks/site-analyze", response_model=TaskResponse)
async def create_site_analyze_task(
    request: Request,
    data: SiteAnalyzeRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Анализ сайта"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
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
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(analyze_site_task,
            url=str(data.url),
            max_pages=data.max_pages,
            use_js=data.use_js,
            ignore_robots=data.ignore_robots,
            check_external=data.check_external
        )
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Analysis completed (sync mode)"
        )


@router.post("/tasks/robots-check", response_model=TaskResponse)
async def create_robots_check_task(
    request: Request,
    data: RobotsCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Проверка robots.txt"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
        task = check_robots_task.delay(url=str(data.url))
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Robots.txt check task created"
        )
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(check_robots_task, url=str(data.url))
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Robots check completed (sync mode)"
        )


@router.post("/tasks/sitemap-validate", response_model=TaskResponse)
async def create_sitemap_validate_task(
    request: Request,
    data: SitemapValidateRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Валидация sitemap"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
        task = validate_sitemap_task.delay(url=str(data.url))
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Sitemap validation task created"
        )
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(validate_sitemap_task, url=str(data.url))
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Sitemap validation completed (sync mode)"
        )


@router.post("/tasks/render-audit", response_model=TaskResponse)
async def create_render_audit_task(
    request: Request,
    data: RenderAuditRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Аудит рендеринга"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
        task = audit_render_task.delay(url=str(data.url), user_agent=data.user_agent)
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Render audit task created"
        )
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(audit_render_task, 
            url=str(data.url), 
            user_agent=data.user_agent
        )
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Render audit completed (sync mode)"
        )


@router.post("/tasks/mobile-check", response_model=TaskResponse)
async def create_mobile_check_task(
    request: Request,
    data: MobileCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Проверка мобильной версии"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
        task = check_mobile_task.delay(url=str(data.url), devices=data.devices)
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Mobile check task created"
        )
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(check_mobile_task, 
            url=str(data.url), 
            devices=data.devices
        )
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Mobile check completed (sync mode)"
        )


@router.post("/tasks/bot-check", response_model=TaskResponse)
async def create_bot_check_task(
    request: Request,
    data: BotCheckRequest,
    rate_limit: dict = Depends(no_rate_limit)
):
    """Проверка ботов"""
    try:
        if not check_celery_available():
            raise Exception("Celery not available")
        
        task = check_bots_task.delay(url=str(data.url))
        return TaskResponse(
            task_id=task.id,
            status="PENDING",
            message="Bot check task created"
        )
    except Exception as e:
        print(f"Celery failed: {e}, running sync")
        task_id, result = run_task_sync(check_bots_task, url=str(data.url))
        return TaskResponse(
            task_id=task_id,
            status="SUCCESS",
            message=f"Bot check completed (sync mode)"
        )


@router.get("/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    """Получает статус задачи"""
    result_data = get_task_result(task_id)
    state = result_data.get("state", "PENDING")
    result = result_data.get("result")
    date_done = result_data.get("date_done")
    
    task_type = TaskType.SITE_ANALYZE
    url = ""
    error_msg = None
    
    if state == "SUCCESS" and result:
        if isinstance(result, dict):
            task_type_str = result.get("task_type", "site_analyze")
            try:
                task_type = TaskType(task_type_str)
            except:
                pass
            url = result.get("url", "")
    
    return TaskResult(
        task_id=task_id,
        status=TaskStatus(state),
        task_type=task_type,
        url=url,
        created_at=date_done or datetime.utcnow(),
        completed_at=date_done,
        progress=None,
        result=result,
        error=error_msg,
        can_continue=False
    )


@router.get("/download/{task_id}/{format}")
async def download_report(task_id: str, format: str):
    """Скачать отчет"""
    from fastapi.responses import FileResponse
    import os
    from app.config import settings
    
    if format not in ["xlsx", "docx"]:
        return {"error": "Invalid format"}
    
    filepath = os.path.join(settings.REPORTS_DIR, f"{task_id}.{format}")
    if not os.path.exists(filepath):
        return {"error": "File not found"}
    
    return FileResponse(
        filepath,
        filename=f"seo-report.{format}",
        media_type="application/vnd.openxmlformats-officedocument." + 
                  ("spreadsheetml.sheet" if format == "xlsx" else "wordprocessingml.document")
    )


@router.get("/celery-status")
async def celery_status():
    """Проверить статус Celery"""
    available = check_celery_available()
    return {"celery_available": available}
