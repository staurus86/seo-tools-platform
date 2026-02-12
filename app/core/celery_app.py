"""
Production Celery Configuration - Fixed for Railway
"""
import os
import sys

# CRITICAL: Read REDIS_URL FIRST, before anything else
REDIS_URL = os.environ.get("REDIS_URL", "")
if not REDIS_URL:
    print("[CELERY FATAL] REDIS_URL is not set!", file=sys.stderr)
    sys.exit(1)

print(f"[CELERY] REDIS_URL = {REDIS_URL[:50]}...", file=sys.stderr)

from celery import Celery

# Create Celery app with explicit broker/backend
celery_app = Celery(
    "seo_tools",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.core.tasks"]
)

print(f"[CELERY] Created app with broker: {celery_app.conf.broker_url}", file=sys.stderr)

# Print all config
print(f"[CELERY] Final config broker_url: {celery_app.conf.get('broker_url')}", file=sys.stderr)
print(f"[CELERY] Final config result_backend: {celery_app.conf.get('result_backend')}", file=sys.stderr)

# Export for other modules
CELERY_APP = celery_app
