#!/bin/bash
set -e

echo "============================================"
echo "SEO Tools Platform - Railway Deployment"
echo "============================================"
echo "MODE: ${SERVICE_MODE:-web}"
echo "PORT: ${PORT:-8000}"
echo "Current directory: $(pwd)"
echo ""

# Export PYTHONPATH
export PYTHONPATH=/app:$PYTHONPATH

# Show all env vars
echo "=== All Environment Variables ==="
env | grep -E '(REDIS|CELERY|SERVICE|PORT)' | sed 's/=.*@/=***@/' || echo "No matching vars found"
echo ""

if [ "$SERVICE_MODE" = "worker" ]; then
    echo "Starting in WORKER mode..."
    echo ""
    
    # Test environment first
    echo "=== Testing Environment ==="
    python3 test_env.py
    echo ""
    
    # Check REDIS_URL
    if [ -z "$REDIS_URL" ]; then
        echo "ERROR: REDIS_URL is empty!"
        exit 1
    fi
    
    echo "REDIS_URL is set to: ${REDIS_URL//:*@/:***@}"
    echo ""
    
    # Unset CELERY_* vars to prevent override
    unset CELERY_BROKER_URL
    unset CELERY_RESULT_BACKEND
    
    echo "Unset CELERY_BROKER_URL and CELERY_RESULT_BACKEND to prevent override"
    echo ""
    
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
