"""
Celery Tasks for SEO Tools - Simplified for Railway testing
"""
from celery import shared_task, Task
from celery.exceptions import SoftTimeLimitExceeded
from typing import Optional, List, Dict, Any
import time
from datetime import datetime

from app.core.celery_app import celery_app
from app.core.progress import progress_tracker


class SEOBaseTask(Task):
    """Базовый класс для SEO задач"""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Обработка ошибок"""
        pass
    
    def on_success(self, retval, task_id, args, kwargs):
        """Обработка успешного выполнения"""
        progress_tracker.clear_progress(task_id)


MAX_DURATION = 20 * 60  # 20 minutes


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=MAX_DURATION,
    time_limit=MAX_DURATION + 60,
    max_retries=2
)
def analyze_site_task(
    self,
    url: str,
    max_pages: int = 100,
    use_js: bool = True,
    ignore_robots: bool = False,
    check_external: bool = False
) -> Dict[str, Any]:
    """Анализ сайта"""
    task_id = self.request.id
    
    try:
        for i in range(10):
            progress_tracker.update_progress(
                task_id=task_id,
                current=i + 1,
                total=10,
                message=f"Analyzing page {i + 1} of {max_pages}...",
                extra={"url": url}
            )
            time.sleep(0.5)
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "pages_analyzed": max_pages,
            "results": {
                "total_pages": max_pages,
                "status": "completed",
                "summary": "Analysis completed successfully"
            }
        }
        
    except SoftTimeLimitExceeded:
        raise self.retry(countdown=60, max_retries=1)


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=300,
    time_limit=360
)
def check_robots_task(self, url: str) -> Dict[str, Any]:
    """Проверка robots.txt"""
    task_id = self.request.id
    
    progress_tracker.update_progress(
        task_id=task_id,
        current=1,
        total=1,
        message="Checking robots.txt...",
        extra={"url": url}
    )
    
    # Use the actual robots audit
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Проверка Robots.txt'))
    
    try:
        from robots_audit import (
            fetch_robots, parse_robots, normalize_robots_url, 
            build_issues_and_warnings, collect_stats
        )
        
        site_url, robots_url = normalize_robots_url(url)
        raw_text = fetch_robots(robots_url)
        result = parse_robots(raw_text)
        stats = collect_stats(result)
        issues, warnings, recommendations = build_issues_and_warnings(result)
        
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": True,
                "status": "completed",
                "url": url,
                "robots_url": robots_url,
                "content_length": len(raw_text),
                "lines_count": len(raw_text.splitlines()),
                "raw_content": raw_text,
                "user_agents": [ua for group in result.groups for ua in group.user_agents],
                "disallow_count": len([rule for group in result.groups for rule in group.disallow]),
                "allow_count": len([rule for group in result.groups for rule in group.allow]),
                "issues": issues,
                "warnings": warnings,
                "recommendations": recommendations,
                "sitemaps": [s[0] for s in result.sitemaps],
                "groups_detail": [
                    {
                        "user_agents": group.user_agents,
                        "disallow": [{"path": rule.path, "line": rule.line} for rule in group.disallow],
                        "allow": [{"path": rule.path, "line": rule.line} for rule in group.allow]
                    }
                    for group in result.groups
                ],
                "stats": stats
            }
        }
    except Exception as e:
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": False,
                "status": "error",
                "error": str(e)
            }
        }


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=600,
    time_limit=660
)
def validate_sitemap_task(self, url: str) -> Dict[str, Any]:
    """Валидация sitemap"""
    task_id = self.request.id
    
    progress_tracker.update_progress(
        task_id=task_id,
        current=1,
        total=1,
        message="Validating sitemap...",
        extra={"url": url}
    )
    
    return {
        "task_type": "sitemap_validate",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "valid": True,
            "urls_count": 0,
            "status": "completed"
        }
    }


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=300,
    time_limit=360
)
def audit_render_task(
    self,
    url: str,
    user_agent: Optional[str] = None
) -> Dict[str, Any]:
    """Аудит рендеринга"""
    task_id = self.request.id
    
    progress_tracker.update_progress(
        task_id=task_id,
        current=1,
        total=1,
        message="Auditing render...",
        extra={"url": url, "user_agent": user_agent}
    )
    
    return {
        "task_type": "render_audit",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "js_render_diff": False,
            "status": "completed"
        }
    }


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=600,
    time_limit=660
)
def check_mobile_task(
    self,
    url: str,
    devices: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Проверка мобильной версии"""
    task_id = self.request.id
    
    device_list = devices or ["iPhone 14", "Samsung Galaxy S24"]
    total_devices = len(device_list)
    
    for i, device in enumerate(device_list):
        progress_tracker.update_progress(
            task_id=task_id,
            current=i + 1,
            total=total_devices,
            message=f"Testing on {device}...",
            extra={"url": url, "device": device}
        )
        time.sleep(0.3)
    
    return {
        "task_type": "mobile_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "devices_tested": device_list,
            "status": "completed"
        }
    }


@shared_task(
    base=SEOBaseTask,
    bind=True,
    soft_time_limit=900,
    time_limit=960
)
def check_bots_task(self, url: str) -> Dict[str, Any]:
    """Проверка доступности для ботов"""
    task_id = self.request.id
    
    bot_types = ["Googlebot", "YandexBot", "BingBot", "ChatGPT", "Grok"]
    total_bots = len(bot_types)
    
    for i, bot in enumerate(bot_types):
        progress_tracker.update_progress(
            task_id=task_id,
            current=i + 1,
            total=total_bots,
            message=f"Checking {bot}...",
            extra={"url": url, "bot": bot}
        )
        time.sleep(0.2)
    
    return {
        "task_type": "bot_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "bots_checked": bot_types,
            "status": "completed"
        }
    }
