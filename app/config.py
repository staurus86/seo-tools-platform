"""
Конфигурация SEO Tools Platform
"""
import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # App
    APP_NAME: str = "SEO Tools Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Celery - use REDIS_URL from environment
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL") or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND") or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Timeouts (seconds)
    HTTP_TIMEOUT_NORMAL: int = 10
    HTTP_TIMEOUT_RENDER: int = 15
    MAX_TASK_DURATION: int = 20 * 60  # 20 minutes
    
    # Page Limits
    MAX_PAGES_DEFAULT: int = 100
    MAX_PAGES_LIMIT: int = 2000
    
    # Concurrent Tasks
    MAX_CONCURRENT_TASKS: int = 10
    
    # Rate Limiting
    RATE_LIMIT_PER_HOUR: int = 10
    RATE_LIMIT_WINDOW: int = 3600  # 1 hour in seconds
    
    # Reports
    REPORTS_DIR: str = "reports_output"
    MAX_REPORT_AGE_DAYS: int = 7

    # Bot check v2
    BOT_CHECK_ENGINE: str = os.getenv("BOT_CHECK_ENGINE", "legacy")
    BOT_CHECK_TIMEOUT: int = int(os.getenv("BOT_CHECK_TIMEOUT", "15"))
    BOT_CHECK_MAX_WORKERS: int = int(os.getenv("BOT_CHECK_MAX_WORKERS", "10"))

    # Mobile check v2
    MOBILE_CHECK_ENGINE: str = os.getenv("MOBILE_CHECK_ENGINE", "v2")
    MOBILE_CHECK_TIMEOUT: int = int(os.getenv("MOBILE_CHECK_TIMEOUT", "35"))
    MOBILE_CHECK_MODE: str = os.getenv("MOBILE_CHECK_MODE", "quick")
    
    # History
    HISTORY_SIZE: int = 10
    
    # Playwright
    PLAYWRIGHT_BROWSERS_PATH: str = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "0")
    
    class Config:
        env_file = ".env"


settings = Settings()
