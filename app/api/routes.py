"""
SEO Tools API Routes - Fixed version with proper JSON handling
"""
from fastapi import APIRouter, Request, Form
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional, List
from datetime import datetime
import os

from app.api.schemas import TaskResponse, TaskResult, TaskStatus, TaskType

router = APIRouter(prefix="/api", tags=["SEO Tools"])

# Storage for results
task_results = {}


def create_task_result(task_id: str, task_type: str, url: str, result: dict):
    """Save task result"""
    task_results[task_id] = {
        "task_id": task_id,
        "task_type": task_type,
        "url": url,
        "result": result,
        "completed_at": datetime.utcnow().isoformat()
    }


# Request models
class RobotsCheckRequest(BaseModel):
    url: HttpUrl

class SitemapValidateRequest(BaseModel):
    url: HttpUrl

class BotCheckRequest(BaseModel):
    url: HttpUrl


# Simplified SEO checks
def check_robots_sync(url: str) -> dict:
    """Check robots.txt"""
    import requests
    
    try:
        robots_url = url.rstrip('/') + '/robots.txt'
        response = requests.get(robots_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": response.status_code == 200,
                "status_code": response.status_code,
                "content_length": len(response.text) if response.status_code == 200 else 0
            }
        }
    except Exception as e:
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e),
                "robots_txt_found": False
            }
        }


def check_sitemap_sync(url: str) -> dict:
    """Check sitemap"""
    import requests
    
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        
        if response.status_code != 200:
            return {
                "task_type": "sitemap_validate",
                "url": url,
                "completed_at": datetime.utcnow().isoformat(),
                "results": {
                    "valid": False,
                    "error": f"HTTP {response.status_code}"
                }
            }
        
        return {
            "task_type": "sitemap_validate",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "valid": True,
                "urls_count": response.text.count("<url"),
                "status": "completed"
            }
        }
    except Exception as e:
        return {
            "task_type": "sitemap_validate",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "valid": False,
                "error": str(e)
            }
        }


def check_bots_sync(url: str) -> dict:
    """Check bot accessibility"""
    import requests
    
    bots = ["Googlebot", "YandexBot", "Bingbot"]
    results = {}
    
    for bot in bots:
        try:
            response = requests.get(url, headers={"User-Agent": bot}, timeout=10)
            results[bot] = {
                "status": response.status_code,
                "accessible": response.status_code == 200
            }
        except Exception as e:
            results[bot] = {"error": str(e)}
    
    return {
        "task_type": "bot_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "bots_checked": bots,
            "bot_results": results,
            "status": "completed"
        }
    }


# Task endpoints
@router.post("/tasks/robots-check", response_model=TaskResponse)
async def create_robots_check(data: RobotsCheckRequest):
    """Check robots.txt"""
    url = str(data.url)
    
    print(f"[API] Checking robots.txt for: {url}")
    
    # Run sync
    result = check_robots_sync(url)
    
    task_id = f"robots-{datetime.now().timestamp()}"
    create_task_result(task_id, "robots_check", url, result)
    
    return TaskResponse(
        task_id=task_id,
        status="SUCCESS",
        message="Robots.txt check completed"
    )


@router.post("/tasks/sitemap-validate", response_model=TaskResponse)
async def create_sitemap_validate(data: SitemapValidateRequest):
    """Validate sitemap"""
    url = str(data.url)
    
    print(f"[API] Validating sitemap: {url}")
    
    result = check_sitemap_sync(url)
    
    task_id = f"sitemap-{datetime.now().timestamp()}"
    create_task_result(task_id, "sitemap_validate", url, result)
    
    return TaskResponse(
        task_id=task_id,
        status="SUCCESS",
        message="Sitemap validation completed"
    )


@router.post("/tasks/bot-check", response_model=TaskResponse)
async def create_bot_check(data: BotCheckRequest):
    """Check bot accessibility"""
    url = str(data.url)
    
    print(f"[API] Checking bots for: {url}")
    
    result = check_bots_sync(url)
    
    task_id = f"bots-{datetime.now().timestamp()}"
    create_task_result(task_id, "bot_check", url, result)
    
    return TaskResponse(
        task_id=task_id,
        status="SUCCESS",
        message="Bot check completed"
    )


@router.get("/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    """Get task result"""
    print(f"[API] Getting status for: {task_id}")
    
    if task_id in task_results:
        data = task_results[task_id]
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            task_type=TaskType(data["task_type"]),
            url=data["url"],
            created_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            result=data["result"],
            error=None,
            can_continue=False
        )
    
    return TaskResult(
        task_id=task_id,
        status=TaskStatus.PENDING,
        task_type=TaskType.SITE_ANALYZE,
        url="",
        created_at=datetime.utcnow(),
        completed_at=None,
        result=None,
        error="Task not found",
        can_continue=False
    )


@router.get("/rate-limit")
async def get_rate_limit():
    """Rate limit info"""
    return {
        "allowed": True,
        "remaining": 999,
        "reset_in": 3600,
        "limit": 10
    }


@router.get("/celery-status")
async def celery_status():
    """Check celery status"""
    return {"celery_available": False, "mode": "synchronous"}
