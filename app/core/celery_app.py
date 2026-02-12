"""
Celery configuration and initialization
"""
import os
from celery import Celery

# Get Redis URL directly from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BROKER_URL = os.getenv("CELERY_BROKER_URL") or REDIS_URL
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND") or REDIS_URL

print(f"Celery: Using broker {BROKER_URL}")
print(f"Celery: Using backend {BACKEND_URL}")

# Create Celery app
celery_app = Celery(
    "seo_tools",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=["app.core.tasks"]
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_track_started=True,
    task_time_limit=settings.MAX_TASK_DURATION + 60,  # +60 sec buffer
    task_soft_time_limit=settings.MAX_TASK_DURATION,
    
    # Result settings
    result_expires=3600 * 24,  # 24 hours
    result_extended=True,
    
    # Worker settings
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
    
    # Task execution
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    
    # Timezone
    timezone="UTC",
    enable_utc=True,
)

# Queue configuration
celery_app.conf.task_routes = {
    "app.core.tasks.analyze_site": {"queue": "seo"},
    "app.core.tasks.check_robots": {"queue": "seo"},
    "app.core.tasks.validate_sitemap": {"queue": "seo"},
    "app.core.tasks.audit_render": {"queue": "seo"},
    "app.core.tasks.check_mobile": {"queue": "seo"},
    "app.core.tasks.check_bots": {"queue": "seo"},
}

# Task annotations for rate limiting
celery_app.conf.task_annotations = {
    "*": {
        "rate_limit": "10/m"  # Max 10 tasks per minute per worker
    }
}
