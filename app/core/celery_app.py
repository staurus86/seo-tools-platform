"""
Minimal Celery configuration - no external imports
"""
import os
import sys

# Get Redis URL
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

print(f"[CELERY] Starting with REDIS_URL: {REDIS_URL}", file=sys.stderr, flush=True)

from celery import Celery

# Create Celery app
celery_app = Celery(
    "seo_tools",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.core.tasks"]
)

print(f"[CELERY] App created with broker: {celery_app.conf.broker_url}", file=sys.stderr, flush=True)

# Hardcoded config
MAX_DURATION = 20 * 60  # 20 minutes

# Configuration
celery_app.conf.update(
    task_track_started=True,
    task_time_limit=MAX_DURATION + 60,
    task_soft_time_limit=MAX_DURATION,
    result_expires=3600 * 24,
    result_extended=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
)

# IMPORTANT: Do not allow any further changes to broker
celery_app.conf.broker_url = REDIS_URL
celery_app.conf.result_backend = REDIS_URL

print(f"[CELERY] Final broker: {celery_app.conf.broker_url}", file=sys.stderr, flush=True)

# Queues
celery_app.conf.task_routes = {
    "app.core.tasks.analyze_site": {"queue": "seo"},
    "app.core.tasks.check_robots": {"queue": "seo"},
    "app.core.tasks.validate_sitemap": {"queue": "seo"},
    "app.core.tasks.audit_render": {"queue": "seo"},
    "app.core.tasks.check_mobile": {"queue": "seo"},
    "app.core.tasks.check_bots": {"queue": "seo"},
}

# Rate limiting
celery_app.conf.task_annotations = {
    "*": {"rate_limit": "10/m"}
}

print(f"[CELERY] Ready!", file=sys.stderr, flush=True)
