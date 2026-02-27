"""Feature flag and canary access checks for LLM Crawler."""
from __future__ import annotations

from typing import Dict, Set

from fastapi import Request

from app.config import settings


def _normalize_token(value: str) -> str:
    token = str(value or "").strip()
    if len(token) >= 2 and ((token[0] == token[-1] == '"') or (token[0] == token[-1] == "'")):
        token = token[1:-1].strip()
    return token


def _allowlist_tokens() -> Set[str]:
    raw = str(getattr(settings, "LLM_CRAWLER_ALLOWLIST", "") or "").strip()
    if not raw:
        return set()
    tokens = {_normalize_token(token) for token in raw.split(",")}
    return {token for token in tokens if token}


def _request_identity(request: Request) -> Dict[str, str]:
    headers = request.headers
    user_id = str(headers.get("x-user-id", "") or "").strip()
    project_id = str(headers.get("x-project-id", "") or "").strip()
    role = str(headers.get("x-role", "") or "").strip().lower()
    forwarded = str(headers.get("x-forwarded-for", "") or "").strip()
    ip = forwarded.split(",")[0].strip() if forwarded else str(getattr(request.client, "host", "") or "")
    return {
        "user_id": user_id,
        "project_id": project_id,
        "role": role,
        "ip": ip,
    }


def is_llm_crawler_enabled_for_request(request: Request) -> bool:
    if bool(getattr(settings, "FEATURE_LLM_CRAWLER", False)):
        return True

    identity = _request_identity(request)
    if bool(getattr(settings, "LLM_CRAWLER_ALLOW_ADMIN", True)) and identity["role"] == "admin":
        return True

    allowlist = _allowlist_tokens()
    if not allowlist:
        return False
    if "*" in allowlist:
        return True

    return bool(
        identity["user_id"] in allowlist
        or identity["project_id"] in allowlist
        or identity["ip"] in allowlist
    )


def feature_context(request: Request) -> Dict[str, object]:
    identity = _request_identity(request)
    return {
        "enabled": is_llm_crawler_enabled_for_request(request),
        "feature_flag": bool(getattr(settings, "FEATURE_LLM_CRAWLER", False)),
        "user_id": identity["user_id"],
        "project_id": identity["project_id"],
        "role": identity["role"],
    }


def request_subject(request: Request) -> str:
    identity = _request_identity(request)
    if identity["user_id"]:
        return f"user:{identity['user_id']}"
    if identity["ip"]:
        return f"ip:{identity['ip']}"
    return "unknown"
