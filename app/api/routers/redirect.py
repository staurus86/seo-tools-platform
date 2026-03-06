"""
Redirect Checker router.
"""
import asyncio
import re
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import field_validator

from app.validators import URLModel, normalize_http_input
from app.api.routers._task_store import create_task_pending, update_task_state

router = APIRouter(tags=["SEO Tools"])


def check_redirect_checker_full(
    url: str,
    user_agent: str = "googlebot_desktop",
    canonical_host_policy: str = "auto",
    trailing_slash_policy: str = "auto",
    enforce_lowercase: bool = True,
    allowed_query_params: Optional[List[str]] = None,
    required_query_params: Optional[List[str]] = None,
    ignore_query_params: Optional[List[str]] = None,
) -> dict:
    from app.tools.redirect_checker import run_redirect_checker

    return run_redirect_checker(
        url=url,
        user_agent_key=user_agent,
        canonical_host_policy=canonical_host_policy,
        trailing_slash_policy=trailing_slash_policy,
        enforce_lowercase=enforce_lowercase,
        allowed_query_params=allowed_query_params,
        required_query_params=required_query_params,
        ignore_query_params=ignore_query_params,
    )


class RedirectCheckerRequest(URLModel):
    url: str
    user_agent: Optional[str] = "googlebot_desktop"
    canonical_host_policy: Optional[str] = "auto"
    trailing_slash_policy: Optional[str] = "auto"
    enforce_lowercase: Optional[bool] = True
    allowed_query_params: Optional[List[str]] = None
    required_query_params: Optional[List[str]] = None
    ignore_query_params: Optional[List[str]] = None

    @field_validator("canonical_host_policy", mode="before")
    @classmethod
    def _normalize_host_policy(cls, value):
        token = str(value or "auto").strip().lower()
        return token if token in {"auto", "www", "non-www"} else "auto"

    @field_validator("trailing_slash_policy", mode="before")
    @classmethod
    def _normalize_trailing_policy(cls, value):
        token = str(value or "auto").strip().lower()
        return token if token in {"auto", "slash", "no-slash"} else "auto"

    @field_validator("allowed_query_params", "required_query_params", "ignore_query_params", mode="before")
    @classmethod
    def _normalize_params_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            return [x.strip() for x in re.split(r"[\r\n,;]+", value) if x.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return []


@router.post("/tasks/redirect-checker")
async def create_redirect_checker(data: RedirectCheckerRequest):
    """Run redirect checker as background task with task-store progress."""
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
    print(
        f"[API] Redirect checker for: {url}, ua={user_agent}, host_policy={canonical_host_policy}, "
        f"slash_policy={trailing_slash_policy}, lowercase={enforce_lowercase}"
    )

    task_id = f"redirect-{datetime.now().timestamp()}"
    create_task_pending(task_id, "redirect_checker", url, status_message="Задача поставлена в очередь")

    async def _run_redirect_task() -> None:
        try:
            update_task_state(
                task_id,
                status="RUNNING",
                progress=10,
                status_message="Запуск сценариев Redirect Checker",
                progress_meta={
                    "current_stage": "redirect_checks",
                    "scenario_count": 17,
                    "current_url": url,
                },
            )
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
            )
            summary = ((result or {}).get("results", {}) or {}).get("summary", {}) or {}
            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Redirect Checker завершен",
                result=result,
                error=None,
                progress_meta={
                    "current_stage": "done",
                    "scenario_count": summary.get("total_scenarios", 17),
                    "duration_ms": summary.get("duration_ms"),
                    "current_url": url,
                },
            )
        except ValueError as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Redirect Checker завершился с ошибкой валидации",
                error=str(exc),
                progress_meta={
                    "current_stage": "failed",
                    "scenario_count": 17,
                    "current_url": url,
                },
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Redirect Checker завершился с ошибкой",
                error=f"Redirect checker failed: {exc}",
                progress_meta={
                    "current_stage": "failed",
                    "scenario_count": 17,
                    "current_url": url,
                },
            )

    asyncio.create_task(_run_redirect_task())
    return {
        "task_id": task_id,
        "status": "PENDING",
        "message": "Redirect checker started",
    }
