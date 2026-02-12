#!/usr/bin/env python3
"""
Test script to check environment variables in Railway
"""
import os
import sys

print("=" * 60, file=sys.stderr)
print("ENVIRONMENT VARIABLES TEST", file=sys.stderr)
print("=" * 60, file=sys.stderr)

# Check all relevant variables
vars_to_check = [
    'REDIS_URL',
    'CELERY_BROKER_URL', 
    'CELERY_RESULT_BACKEND',
    'SERVICE_MODE',
    'PORT',
    'PYTHONPATH'
]

for var in vars_to_check:
    value = os.environ.get(var, 'NOT SET')
    # Hide password in output
    if 'redis' in str(value).lower() and '@' in str(value):
        parts = value.split('@')
        safe_value = parts[0].split('://')[0] + '://***@' + parts[1]
    else:
        safe_value = value
    print(f"{var}: {safe_value}", file=sys.stderr, flush=True)

print("=" * 60, file=sys.stderr)

# Try to import celery and check what it sees
try:
    print("\nTrying to import Celery...", file=sys.stderr)
    from celery import Celery
    
    # Test creating app
    test_app = Celery('test', broker=os.environ.get('REDIS_URL', 'redis://localhost:6379/0'))
    print(f"Test app broker: {test_app.conf.broker_url}", file=sys.stderr)
    
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)

print("\nTest complete!", file=sys.stderr)
