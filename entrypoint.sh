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

# Set PYTHONPATH
export PYTHONPATH=/app:$PYTHONPATH

# Check mode
if [ "$SERVICE_MODE" = "worker" ]; then
    echo "Starting in WORKER mode..."
    echo ""
    
    if [ -z "$REDIS_URL" ]; then
        echo "ERROR: REDIS_URL not set. Worker cannot start without Redis."
        echo "Please add REDIS_URL environment variable."
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
    echo "=== Directory structure ==="
    ls -la
    echo ""
    
    # Check if required files exist
    echo "Checking required files..."
    if [ -f "app/main.py" ]; then
        echo "✓ app/main.py exists"
    else
        echo "✗ app/main.py NOT FOUND"
    fi
    
    if [ -d "app/templates" ]; then
        echo "✓ app/templates directory exists"
    else
        echo "✗ app/templates directory NOT FOUND"
    fi
    
    echo ""
    echo "============================================"
    echo "Starting uvicorn server..."
    echo "============================================"
    
    exec python -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
fi
