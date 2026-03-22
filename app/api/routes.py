"""
SEO Tools API Routes — thin router that aggregates all tool sub-routers.
"""
import asyncio
from datetime import datetime

from fastapi import APIRouter
from fastapi import HTTPException
from pydantic import BaseModel, field_validator

from app.validators import normalize_http_input

router = APIRouter(prefix="/api", tags=["SEO Tools"])

# ─── sub-routers (extracted modules) ──────────────────────────────────────
from app.api.routers import exports as _exports_mod          # noqa: E402
from app.api.routers import tasks as _tasks_mod              # noqa: E402
from app.api.routers import redirect as _redirect_mod        # noqa: E402
from app.api.routers import site_pro as _site_pro_mod        # noqa: E402
from app.api.routers import onpage as _onpage_mod            # noqa: E402
from app.api.routers import clusterizer as _clusterizer_mod  # noqa: E402
from app.api.routers import render as _render_mod            # noqa: E402
from app.api.routers import mobile as _mobile_mod            # noqa: E402
from app.api.routers import link_profile as _link_profile_mod  # noqa: E402
from app.api.routers import cwv as _cwv_mod                  # noqa: E402
from app.api.routers import site_analyze as _site_analyze_mod  # noqa: E402
from app.api.routers import robots as _robots_mod            # noqa: E402
from app.api.routers import unified as _unified_mod          # noqa: E402
from app.api.routers import batch as _batch_mod              # noqa: E402
from app.api.routers._task_store import (                    # noqa: E402
    create_task_pending,
    create_task_result,
    get_task_result,
    update_task_state,
)

# Backward-compatible re-exports for legacy imports and tests.
from app.api.routers.clusterizer import (                    # noqa: E402
    ClusterizerRequest,
    create_clusterizer_task,
)
from app.api.routers.cwv import (                            # noqa: E402
    _build_core_web_vitals_competitor_result,
    check_core_web_vitals,
    create_core_web_vitals as _create_core_web_vitals,
)
from app.api.routers.exports import (                        # noqa: E402
    ExportRequest,
    export_redirect_checker_docx,
    export_redirect_checker_xlsx,
)
from app.api.routers.link_profile import (                   # noqa: E402
    create_link_profile_audit,
)
from app.api.routers.redirect import (                       # noqa: E402
    check_redirect_checker_full,
    create_redirect_checker as _create_redirect_checker,
)
from app.api.routers.render import (                         # noqa: E402
    RenderAuditRequest,
    check_render_full,
    create_render_audit,
)
from app.api.routers.robots import (                         # noqa: E402
    RobotsCheckRequest,
    check_robots_full,
    create_robots_check as _create_robots_check,
)

router.include_router(_exports_mod.router)
router.include_router(_tasks_mod.router)
router.include_router(_redirect_mod.router)
router.include_router(_site_pro_mod.router)
router.include_router(_onpage_mod.router)
router.include_router(_clusterizer_mod.router)
router.include_router(_render_mod.router)
router.include_router(_mobile_mod.router)
router.include_router(_link_profile_mod.router)
router.include_router(_cwv_mod.router)
router.include_router(_site_analyze_mod.router)
router.include_router(_robots_mod.router)
router.include_router(_unified_mod.router)
router.include_router(_batch_mod.router)


class RedirectCheckerRequest(BaseModel):
    url: str
    user_agent: str | None = "googlebot_desktop"
    canonical_host_policy: str | None = "auto"
    trailing_slash_policy: str | None = "auto"
    enforce_lowercase: bool | None = True
    allowed_query_params: list[str] | None = None
    required_query_params: list[str] | None = None
    ignore_query_params: list[str] | None = None
    use_proxy: bool = False

    @field_validator("url", mode="before")
    @classmethod
    def _normalize_url(cls, value):
        normalized = normalize_http_input(str(value or ""))
        if not normalized:
            raise ValueError("Введите корректный URL сайта (домен или http/https URL).")
        return normalized


class CoreWebVitalsRequest(BaseModel):
    url: str | None = ""
    strategy: str | None = "desktop"
    scan_mode: str | None = "single"
    batch_urls: list[str] | None = None
    competitor_mode: bool = False
    combined: bool = False
    use_proxy: bool = False

    @field_validator("url", mode="before")
    @classmethod
    def _normalize_url(cls, value):
        if value in (None, ""):
            return ""
        normalized = normalize_http_input(str(value or ""))
        if not normalized:
            raise ValueError("Введите корректный URL сайта (домен или http/https URL).")
        return normalized


async def create_robots_check(data: RobotsCheckRequest):
    """Backward-compatible direct-call wrapper for legacy imports/tests."""
    url = normalize_http_input(str(data.url or ""))
    if not url:
        raise HTTPException(status_code=422, detail="Введите корректный домен или URL сайта.")

    result = check_robots_full(url)
    task_id = f"robots-{datetime.now().timestamp()}"
    create_task_result(task_id, "robots_check", url, result)
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Robots.txt analysis completed",
    }


async def create_redirect_checker(data: RedirectCheckerRequest):
    """Backward-compatible direct-call wrapper for legacy imports/tests."""
    url = normalize_http_input(data.url or "")
    if not url:
        raise HTTPException(status_code=422, detail="Введите корректный URL сайта (домен или http/https URL).")

    user_agent = str(data.user_agent or "googlebot_desktop").strip().lower()
    canonical_host_policy = str(data.canonical_host_policy or "auto").strip().lower()
    trailing_slash_policy = str(data.trailing_slash_policy or "auto").strip().lower()
    enforce_lowercase = bool(data.enforce_lowercase if data.enforce_lowercase is not None else True)
    allowed_query_params = data.allowed_query_params or []
    required_query_params = data.required_query_params or []
    ignore_query_params = data.ignore_query_params or []

    task_id = f"redirect-{datetime.now().timestamp()}"
    create_task_pending(task_id, "redirect_checker", url, status_message="Задача поставлена в очередь")

    async def _run_redirect_task() -> None:
        try:
            result = await asyncio.to_thread(
                check_redirect_checker_full,
                url,
                user_agent=user_agent,
                canonical_host_policy=canonical_host_policy,
                trailing_slash_policy=trailing_slash_policy,
                enforce_lowercase=enforce_lowercase,
                allowed_query_params=allowed_query_params,
                required_query_params=required_query_params,
                ignore_query_params=ignore_query_params,
                progress_callback=lambda payload: payload,
                use_proxy=bool(data.use_proxy),
            )
            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Redirect Checker завершен",
                result=result,
                error=None,
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Redirect Checker завершился с ошибкой",
                error=str(exc),
            )

    asyncio.create_task(_run_redirect_task())
    return {
        "task_id": task_id,
        "status": "PENDING",
        "message": "Redirect checker started",
    }


async def create_core_web_vitals(data: CoreWebVitalsRequest, background_tasks):
    """Backward-compatible direct-call wrapper for legacy imports/tests."""
    strategy = str(data.strategy or "desktop").strip().lower()
    if strategy not in {"mobile", "desktop"}:
        strategy = "desktop"
    scan_mode = str(data.scan_mode or "single").strip().lower()
    if scan_mode not in {"single", "batch"}:
        scan_mode = "single"
    competitor_mode = bool(data.competitor_mode)
    if competitor_mode:
        scan_mode = "batch"

    raw_batch_urls = [normalize_http_input(str(item or "")) for item in (data.batch_urls or [])]
    batch_urls = [item for item in raw_batch_urls if item]

    if scan_mode == "batch":
        if len(batch_urls) > 10:
            raise HTTPException(status_code=422, detail="Лимит batch Core Web Vitals: максимум 10 URL.")
        if competitor_mode and len(batch_urls) < 2:
            raise HTTPException(
                status_code=422,
                detail="Для режима анализа конкурентов укажите минимум 2 URL: первый — ваш сайт, далее конкуренты.",
            )
        if not batch_urls:
            raise HTTPException(status_code=422, detail="Добавьте хотя бы один URL для batch Core Web Vitals сканирования.")

        task_id = f"cwv-{datetime.now().timestamp()}"
        create_task_pending(task_id, "core_web_vitals", batch_urls[0], status_message="Задача поставлена в очередь")
        return {
            "task_id": task_id,
            "status": "PENDING",
            "message": "Core Web Vitals competitor scan queued" if competitor_mode else "Core Web Vitals batch scan queued",
        }

    url = normalize_http_input(str(data.url or ""))
    if not url:
        raise HTTPException(status_code=422, detail="Введите корректный URL сайта (домен или http/https URL).")

    task_id = f"cwv-{datetime.now().timestamp()}"
    create_task_pending(task_id, "core_web_vitals", url, status_message="Задача поставлена в очередь")
    return {
        "task_id": task_id,
        "status": "PENDING",
        "message": "Core Web Vitals scan queued",
    }
