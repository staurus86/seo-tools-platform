"""
Startup check script - проверка перед запуском приложения
"""
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("=== Starting SEO Tools Platform ===")

# Check Python version
logger.info(f"Python version: {sys.version}")

# Check environment variables
import os
logger.info(f"PORT: {os.environ.get('PORT', 'not set')}")
logger.info(f"REDIS_URL: {os.environ.get('REDIS_URL', 'not set')}")
logger.info(f"PYTHONUNBUFFERED: {os.environ.get('PYTHONUNBUFFERED', 'not set')}")

# Try imports
try:
    logger.info("Checking FastAPI import...")
    from fastapi import FastAPI
    logger.info("✓ FastAPI imported successfully")
except Exception as e:
    logger.error(f"✗ FastAPI import failed: {e}")
    sys.exit(1)

try:
    logger.info("Checking Celery import...")
    from celery import Celery
    logger.info("✓ Celery imported successfully")
except Exception as e:
    logger.error(f"✗ Celery import failed: {e}")

try:
    logger.info("Checking Redis import...")
    import redis
    logger.info("✓ Redis imported successfully")
except Exception as e:
    logger.error(f"✗ Redis import failed: {e}")

try:
    logger.info("Checking app modules...")
    from app.config import settings
    logger.info(f"✓ Config loaded. PORT={settings.PORT}, HOST={settings.HOST}")
except Exception as e:
    logger.error(f"✗ Config import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    logger.info("Checking app.main...")
    from app.main import app
    logger.info("✓ App loaded successfully")
except Exception as e:
    logger.error(f"✗ App import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

logger.info("=== All checks passed! Starting server... ===")
