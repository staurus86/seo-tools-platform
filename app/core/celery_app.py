"""
Celery configuration and initialization
"""
import os
import sys

# CRITICAL: Use only REDIS_URL, ignore CELERY_BROKER_URL if it points to localhost
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Check if CELERY_BROKER_URL exists and doesn't point to localhost
CELERY_BROKER = os.getenv("CELERY_BROKER_URL")
if CELERY_BROKER and "localhost" not in CELERY_BROKER:
    BROKER_URL = CELERY_BROKER
else:
    BROKER_URL = REDIS_URL

CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
if CELERY_BACKEND and "localhost" not in CELERY_BACKEND:
    BACKEND_URL = CELERY_BACKEND
else:
    BACKEND_URL = REDIS_URL

print(f"[CELERY CONFIG] REDIS_URL from env: {REDIS_URL}", file=sys.stderr)
print(f"[CELERY CONFIG] CELERY_BROKER_URL from env: {CELERY_BROKER}", file=sys.stderr)
print(f"[CELERY CONFIG] Using BROKER_URL: {BROKER_URL}", file=sys.stderr)
print(f"[CELERY CONFIG] Using BACKEND_URL: {BACKEND_URL}", file=sys.stderr)

from celery import Celery

# Create Celery app with explicit broker/backend - NO IMPORTS BEFORE THIS
celery_app = Celery(
    "seo_tools",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=["app.core.tasks"]
)

print(f"[CELERY CONFIG] App broker_url: {celery_app.conf.broker_url}", file=sys.stderr)

# Hardcoded settings - no imports to avoid reconfiguration
MAX_TASK_DURATION = 20 * 60  # 20 minutes

# Celery configuration - only update specific settings, NOT broker/backend
celery_app.conf.update(
    # Task settings
    task_track_started=True,
    task_time_limit=MAX_TASK_DURATION + 60,
    task_soft_time_limit=MAX_TASK_DURATION,
    
    # Result settings
    result_expires=3600 * 24,
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
    
    # Retry on startup
    broker_connection_retry_on_startup=True,
)

print(f"[CELERY CONFIG] Final broker_url: {celery_app.conf.broker_url}", file=sys.stderr)

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
        "rate_limit": "10/m"
    }
}

print(f"[CELERY CONFIG] Configuration complete", file=sys.stderr)
