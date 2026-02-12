#!/bin/bash
set -e

echo "============================================"
echo "SEO Tools Platform - Celery Worker"
echo "============================================"
echo "REDIS_URL: ${REDIS_URL:-not set}"
echo ""

# Check if Redis is available
if [ -z "$REDIS_URL" ]; then
    echo "WARNING: REDIS_URL not set. Worker may not function properly."
    echo "Please add REDIS_URL environment variable from your Redis service."
    exit 1
fi

echo "Starting Celery worker..."
echo "Queue: seo"
echo "Concurrency: 2"
echo ""

# Set PYTHONPATH
export PYTHONPATH=/app:$PYTHONPATH

# Start Celery worker
exec celery -A app.core.celery_app worker --loglevel=info --concurrency=2 -Q seo
