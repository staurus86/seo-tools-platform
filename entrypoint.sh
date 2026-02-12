#!/bin/bash
set -e

echo "============================================"
echo "SEO Tools Platform - Railway Deployment"
echo "============================================"
echo "PORT: ${PORT:-8000}"
echo "Current directory: $(pwd)"
echo ""
echo "=== Directory structure ==="
ls -la
echo ""
echo "=== App directory ==="
ls -la app/ 2>/dev/null || echo "app/ not found"
echo ""
echo "=== App/templates directory ==="
ls -la app/templates/ 2>/dev/null || echo "app/templates/ not found"
echo ""

# Check if required files exist
echo "Checking required files..."
if [ -f "app/main.py" ]; then
    echo "✓ app/main.py exists"
else
    echo "✗ app/main.py NOT FOUND"
fi

if [ -f "requirements.txt" ]; then
    echo "✓ requirements.txt exists"
else
    echo "✗ requirements.txt NOT FOUND"
fi

if [ -d "app/static" ]; then
    echo "✓ app/static directory exists"
else
    echo "✗ app/static directory NOT FOUND"
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

# Set PYTHONPATH
export PYTHONPATH=/app:$PYTHONPATH

# Start the server with detailed logging
exec python -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
