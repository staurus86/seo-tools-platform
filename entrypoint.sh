#!/bin/bash
set -e

echo "============================================"
echo "SEO Tools Platform - Railway Deployment"
echo "============================================"
echo "MODE: ${SERVICE_MODE:-web}"
echo "PORT: ${PORT:-8000}"
echo "REDIS_URL: ${REDIS_URL:-not set}"
echo "Current directory: $(pwd)"
echo ""

# Export PYTHONPATH
export PYTHONPATH=/app:$PYTHONPATH

# CRITICAL: Export REDIS_URL explicitly for Celery
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

echo "=== Environment ==="
echo "REDIS_URL=$REDIS_URL"
echo "PYTHONPATH=$PYTHONPATH"
echo ""

if [ "$SERVICE_MODE" = "worker" ]; then
    echo "Starting in WORKER mode..."
    echo ""
    
    if [ -z "$REDIS_URL" ] || [ "$REDIS_URL" = "redis://localhost:6379/0" ]; then
        echo "ERROR: REDIS_URL not properly set!"
        echo "REDIS_URL=$REDIS_URL"
        exit 1
    fi
    
    echo "Starting Celery worker..."
    echo "Queue: seo"
    echo "Concurrency: 2"
    echo ""
    
    exec celery -A app.core.celery_app worker --loglevel=info --concurrency=2 -Q seo
else
    echo "Starting in WEB mode..."
    echo ""
    
    exec python -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
fi
