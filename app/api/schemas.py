"""
Pydantic schemas for API
"""
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class TaskType(str, Enum):
    """Типы задач"""
    SITE_ANALYZE = "site_analyze"
    SITE_AUDIT_PRO = "site_audit_pro"
    ROBOTS_CHECK = "robots_check"
    SITEMAP_VALIDATE = "sitemap_validate"
    RENDER_AUDIT = "render_audit"
    MOBILE_CHECK = "mobile_check"
    BOT_CHECK = "bot_check"


class TaskStatus(str, Enum):
    """Статусы задач"""
    PENDING = "PENDING"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"


# Request Schemas
class SiteAnalyzeRequest(BaseModel):
    """Запрос на анализ сайта"""
    url: HttpUrl = Field(..., description="URL сайта для анализа")
    max_pages: int = Field(default=100, ge=1, le=2000, description="Максимальное количество страниц")
    use_js: bool = Field(default=True, description="Использовать JavaScript рендеринг")
    ignore_robots: bool = Field(default=False, description="Игнорировать robots.txt")
    check_external: bool = Field(default=False, description="Проверять внешние ссылки")


class RobotsCheckRequest(BaseModel):
    """Запрос на проверку robots.txt"""
    url: HttpUrl = Field(..., description="URL сайта")


class SitemapValidateRequest(BaseModel):
    """Запрос на валидацию sitemap"""
    url: HttpUrl = Field(..., description="URL sitemap.xml")


class RenderAuditRequest(BaseModel):
    """Запрос на аудит рендеринга"""
    url: HttpUrl = Field(..., description="URL страницы")
    user_agent: Optional[str] = Field(default=None, description="User-Agent (опционально)")


class MobileCheckRequest(BaseModel):
    """Запрос на проверку мобильной версии"""
    url: HttpUrl = Field(..., description="URL сайта")
    devices: Optional[List[str]] = Field(default=None, description="Список устройств (опционально)")


class BotCheckRequest(BaseModel):
    """Запрос на проверку доступности для ботов"""
    url: HttpUrl = Field(..., description="URL сайта")


# Response Schemas
class TaskResponse(BaseModel):
    """Ответ с ID задачи"""
    task_id: str
    status: str
    message: str = "Task created successfully"


class TaskProgress(BaseModel):
    """Прогресс задачи"""
    current: int
    total: int
    percentage: float
    message: str
    extra: Dict[str, Any]


class TaskResult(BaseModel):
    """Результат задачи"""
    task_id: str
    status: TaskStatus
    task_type: TaskType
    url: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    progress: Optional[TaskProgress] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    can_continue: bool = False  # Можно ли продолжить задачу


class RateLimitInfo(BaseModel):
    """Информация о rate limit"""
    allowed: bool
    remaining: int
    reset_in: int
    limit: int


class DownloadResponse(BaseModel):
    """Ответ для скачивания файла"""
    download_url: str
    filename: str
    expires_at: datetime


