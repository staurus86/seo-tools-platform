#!/bin/bash
set -e

echo "=== SEO Tools Platform Startup ==="
echo "PORT: ${PORT:-8000}"
echo "REDIS_URL: ${REDIS_URL:-not set}"

# Run startup check
echo "Running startup checks..."
python check_startup.py

# Start the application
echo "Starting uvicorn server on port ${PORT:-8000}..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
