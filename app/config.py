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

    # Sitemap validator
    SITEMAP_MAX_FILES: int = int(os.getenv("SITEMAP_MAX_FILES", "500"))
    SITEMAP_MAX_EXPORT_URLS: int = int(os.getenv("SITEMAP_MAX_EXPORT_URLS", "100000"))

    # Link Profile upload limits
    LINK_PROFILE_MAX_BACKLINK_FILES: int = int(os.getenv("LINK_PROFILE_MAX_BACKLINK_FILES", "20"))
    LINK_PROFILE_MAX_FILE_SIZE_BYTES: int = int(os.getenv("LINK_PROFILE_MAX_FILE_SIZE_BYTES", str(35 * 1024 * 1024)))
    LINK_PROFILE_MAX_BATCH_FILE_SIZE_BYTES: int = int(os.getenv("LINK_PROFILE_MAX_BATCH_FILE_SIZE_BYTES", str(5 * 1024 * 1024)))
    LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES: int = int(os.getenv("LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES", str(150 * 1024 * 1024)))

    # Clusterizer
    CLUSTERIZER_MAX_KEYWORDS: int = int(os.getenv("CLUSTERIZER_MAX_KEYWORDS", "2000"))

    # Core Web Vitals (Google PageSpeed Insights API)
    PAGESPEED_API_KEY: str = os.getenv("PAGESPEED_API_KEY", "")
    PAGESPEED_TIMEOUT_SEC: int = int(os.getenv("PAGESPEED_TIMEOUT_SEC", "60"))
    PAGESPEED_MAX_RETRIES: int = int(os.getenv("PAGESPEED_MAX_RETRIES", "3"))

    # LLM Crawler Simulation rollout and limits
    FEATURE_LLM_CRAWLER: bool = os.getenv("FEATURE_LLM_CRAWLER", "false").lower() == "true"
    LLM_CRAWLER_ALLOWLIST: str = os.getenv("LLM_CRAWLER_ALLOWLIST", "")
    LLM_CRAWLER_ALLOW_ADMIN: bool = os.getenv("LLM_CRAWLER_ALLOW_ADMIN", "true").lower() == "true"
    JOB_CONCURRENCY: int = int(os.getenv("JOB_CONCURRENCY", "2"))
    FETCH_TIMEOUT_MS: int = int(os.getenv("FETCH_TIMEOUT_MS", "20000"))
    MAX_HTML_BYTES: int = int(os.getenv("MAX_HTML_BYTES", "2000000"))
    LLM_CRAWLER_MAX_REDIRECT_HOPS: int = int(os.getenv("LLM_CRAWLER_MAX_REDIRECT_HOPS", "8"))
    LLM_CRAWLER_JOB_TTL_SEC: int = int(os.getenv("LLM_CRAWLER_JOB_TTL_SEC", str(72 * 3600)))
    LLM_CRAWLER_RATE_LIMIT_PER_MINUTE: int = int(os.getenv("LLM_CRAWLER_RATE_LIMIT_PER_MINUTE", "5"))
    LLM_CRAWLER_RENDER_RATE_LIMIT_PER_DAY: int = int(os.getenv("LLM_CRAWLER_RENDER_RATE_LIMIT_PER_DAY", "20"))
    LLM_CRAWLER_QUEUE_KEY: str = os.getenv("LLM_CRAWLER_QUEUE_KEY", "llmCrawler:queue")
    LLM_CRAWLER_WORKER_HEARTBEAT_KEY: str = os.getenv("LLM_CRAWLER_WORKER_HEARTBEAT_KEY", "llmCrawler:worker:heartbeat")
    LLM_CRAWLER_WORKER_HEARTBEAT_TTL_SEC: int = int(os.getenv("LLM_CRAWLER_WORKER_HEARTBEAT_TTL_SEC", "120"))
    LLM_CRAWLER_REQUIRE_HEALTHY_WORKER: bool = os.getenv("LLM_CRAWLER_REQUIRE_HEALTHY_WORKER", "true").lower() == "true"
    LLM_CRAWLER_STUCK_JOB_TIMEOUT_SEC: int = int(os.getenv("LLM_CRAWLER_STUCK_JOB_TIMEOUT_SEC", "300"))

    # Bot check v2
    BOT_CHECK_ENGINE: str = os.getenv("BOT_CHECK_ENGINE", "legacy")
    BOT_CHECK_TIMEOUT: int = int(os.getenv("BOT_CHECK_TIMEOUT", "15"))
    BOT_CHECK_MAX_WORKERS: int = int(os.getenv("BOT_CHECK_MAX_WORKERS", "10"))

    # Mobile check v2
    MOBILE_CHECK_ENGINE: str = os.getenv("MOBILE_CHECK_ENGINE", "v2")
    MOBILE_CHECK_TIMEOUT: int = int(os.getenv("MOBILE_CHECK_TIMEOUT", "35"))
    MOBILE_CHECK_MODE: str = os.getenv("MOBILE_CHECK_MODE", "quick")

    # Render audit v2
    RENDER_AUDIT_ENGINE: str = os.getenv("RENDER_AUDIT_ENGINE", "v2")
    RENDER_AUDIT_TIMEOUT: int = int(os.getenv("RENDER_AUDIT_TIMEOUT", "35"))
    RENDER_AUDIT_DEBUG: bool = os.getenv("RENDER_AUDIT_DEBUG", "false").lower() == "true"

    # Site Audit Pro rollout
    SITE_AUDIT_PRO_ENABLED: bool = os.getenv("SITE_AUDIT_PRO_ENABLED", "true").lower() == "true"
    SITE_AUDIT_PRO_DEFAULT_MODE: str = os.getenv("SITE_AUDIT_PRO_DEFAULT_MODE", "quick")
    SITE_AUDIT_PRO_MAX_PAGES_LIMIT: int = int(os.getenv("SITE_AUDIT_PRO_MAX_PAGES_LIMIT", "5"))
    SITE_AUDIT_PRO_MAX_PAGES_LIMIT_QUICK: int = int(os.getenv("SITE_AUDIT_PRO_MAX_PAGES_LIMIT_QUICK", "5"))
    SITE_AUDIT_PRO_MAX_PAGES_LIMIT_FULL: int = int(os.getenv("SITE_AUDIT_PRO_MAX_PAGES_LIMIT_FULL", "30"))
    SITE_AUDIT_PRO_INLINE_ISSUES_LIMIT: int = int(os.getenv("SITE_AUDIT_PRO_INLINE_ISSUES_LIMIT", "200"))
    SITE_AUDIT_PRO_INLINE_SEMANTIC_LIMIT: int = int(os.getenv("SITE_AUDIT_PRO_INLINE_SEMANTIC_LIMIT", "200"))
    SITE_AUDIT_PRO_INLINE_PAGES_LIMIT: int = int(os.getenv("SITE_AUDIT_PRO_INLINE_PAGES_LIMIT", "500"))
    
    # History
    HISTORY_SIZE: int = 10
    
    # Playwright
    PLAYWRIGHT_BROWSERS_PATH: str = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "0")
    
    class Config:
        env_file = ".env"


settings = Settings()
