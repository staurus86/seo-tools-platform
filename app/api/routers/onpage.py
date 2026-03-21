"""
OnPage Audit router.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, BackgroundTasks
from pydantic import field_validator

from app.validators import URLModel
from app.api.routers._task_store import create_task_pending, update_task_state

router = APIRouter(tags=["SEO Tools"])


def check_onpage_audit(
    *,
    url: str,
    keywords: Optional[List[str]] = None,
    language: str = "auto",
    min_word_count: int = 250,
    keyword_density_warn_pct: float = 3.0,
    keyword_density_critical_pct: float = 5.0,
    title_min_len: int = 30,
    title_max_len: int = 60,
    description_min_len: int = 120,
    description_max_len: int = 160,
    h1_required: bool = True,
    h1_max_count: int = 1,
    use_proxy: bool = False,
) -> Dict[str, Any]:
    """Single-page on-page audit."""
    from app.tools.onpage import OnPageAuditServiceV1

    service = OnPageAuditServiceV1()
    return service.run(
        url=url,
        keywords=keywords or [],
        language=language,
        min_word_count=min_word_count,
        keyword_density_warn_pct=keyword_density_warn_pct,
        keyword_density_critical_pct=keyword_density_critical_pct,
        title_min_len=title_min_len,
        title_max_len=title_max_len,
        description_min_len=description_min_len,
        description_max_len=description_max_len,
        h1_required=h1_required,
        h1_max_count=h1_max_count,
        use_proxy=use_proxy,
    )


class OnPageAuditRequest(URLModel):
    url: str
    keywords: Optional[List[str]] = None
    language: Optional[str] = "auto"
    min_word_count: int = 250
    keyword_density_warn_pct: float = 3.0
    keyword_density_critical_pct: float = 5.0
    title_min_len: int = 30
    title_max_len: int = 60
    description_min_len: int = 120
    description_max_len: int = 160
    h1_required: bool = True
    h1_max_count: int = 1
    use_proxy: bool = False

    @field_validator("keywords", mode="before")
    @classmethod
    def _normalize_keywords(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            raw = value.replace("\r", "\n")
            parts = []
            for chunk in raw.split("\n"):
                parts.extend([x.strip() for x in chunk.split(",") if x.strip()])
            return parts
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return []


@router.post("/tasks/onpage-audit")
async def create_onpage_audit(data: OnPageAuditRequest, background_tasks: BackgroundTasks):
    """OnPage audit queued as isolated background task."""
    url = (data.url or "").strip()
    task_id = f"onpage-{datetime.now().timestamp()}"
    create_task_pending(task_id, "onpage_audit", url, status_message="Задача поставлена в очередь")

    def _run_onpage_task() -> None:
        try:
            update_task_state(task_id, status="RUNNING", progress=10, status_message="Анализ on-page факторов")
            result = check_onpage_audit(
                url=url,
                keywords=data.keywords or [],
                language=(data.language or "auto"),
                min_word_count=int(data.min_word_count or 250),
                keyword_density_warn_pct=float(data.keyword_density_warn_pct or 3.0),
                keyword_density_critical_pct=float(data.keyword_density_critical_pct or 5.0),
                title_min_len=int(data.title_min_len or 30),
                title_max_len=int(data.title_max_len or 60),
                description_min_len=int(data.description_min_len or 120),
                description_max_len=int(data.description_max_len or 160),
                h1_required=bool(data.h1_required),
                h1_max_count=int(data.h1_max_count or 1),
                use_proxy=bool(data.use_proxy),
            )
            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="OnPage аудит завершен",
                result=result,
                error=None,
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="OnPage аудит завершился с ошибкой",
                error=str(exc),
            )

    background_tasks.add_task(_run_onpage_task)
    return {"task_id": task_id, "status": "PENDING", "message": "OnPage аудит запущен"}
