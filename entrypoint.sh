#!/bin/bash
set -e

echo "============================================"
echo "SEO Tools Platform - Railway Deployment"
echo "============================================"
echo "PORT: ${PORT:-8000}"
echo "REDIS_URL: ${REDIS_URL:-not set}"
echo "Current directory: $(pwd)"
echo "Files in directory:"
ls -la
echo ""

# Check if required files exist
echo "Checking required files..."
if [ -f "app/main.py" ]; then
    echo "✓ app/main.py exists"
else
    echo "✗ app/main.py NOT FOUND"
    ls -la app/ || echo "app directory not found"
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

# Try to start uvicorn with error handling
python -c "
import sys
sys.path.insert(0, '/app')
try:
    from app.main import app
    print('✓ Successfully imported app')
except Exception as e:
    print(f'✗ Failed to import app: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

# Start the server
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
