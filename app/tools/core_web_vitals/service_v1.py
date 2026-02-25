"""Core Web Vitals scanner via Google PageSpeed Insights API."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests

from app.config import settings


PSI_ENDPOINT = "https://pagespeedonline.googleapis.com/pagespeedonline/v5/runPagespeed"


def _normalize_http_input(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        value = f"https://{value}"
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    path = parsed.path or "/"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def _normalize_strategy(value: str) -> str:
    token = str(value or "").strip().lower()
    return token if token in {"mobile", "desktop"} else "desktop"


def _metric_status(metric: str, value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    if metric == "lcp":
        if value <= 2500:
            return "good"
        if value <= 4000:
            return "needs_improvement"
        return "poor"
    if metric == "inp":
        if value <= 200:
            return "good"
        if value <= 500:
            return "needs_improvement"
        return "poor"
    if metric == "cls":
        if value <= 0.1:
            return "good"
        if value <= 0.25:
            return "needs_improvement"
        return "poor"
    if metric == "fcp":
        if value <= 1800:
            return "good"
        if value <= 3000:
            return "needs_improvement"
        return "poor"
    if metric == "ttfb":
        if value <= 800:
            return "good"
        if value <= 1800:
            return "needs_improvement"
        return "poor"
    return "unknown"


def _field_metric(metrics: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        payload = metrics.get(key)
        if not isinstance(payload, dict):
            continue
        percentile = payload.get("percentile")
        try:
            if percentile is not None:
                return float(percentile)
        except Exception:
            continue
    return None


def run_core_web_vitals(
    *,
    url: str,
    strategy: str = "desktop",
    timeout: int = 60,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_url = _normalize_http_input(url)
    if not normalized_url:
        raise ValueError("Введите корректный URL сайта (http/https или домен).")

    selected_strategy = _normalize_strategy(strategy)
    token = str(api_key or getattr(settings, "PAGESPEED_API_KEY", "") or "").strip()

    params = {
        "url": normalized_url,
        "strategy": selected_strategy,
        "category": "performance",
    }
    if token:
        params["key"] = token

    configured_timeout = int(timeout or getattr(settings, "PAGESPEED_TIMEOUT_SEC", 60) or 60)
    configured_timeout = max(20, configured_timeout)
    max_retries = int(getattr(settings, "PAGESPEED_MAX_RETRIES", 3) or 3)
    max_retries = max(1, min(5, max_retries))

    response = None
    last_exception: Optional[Exception] = None
    retries_used = 0
    for attempt in range(max_retries):
        read_timeout = configured_timeout + (attempt * 15)
        try:
            response = requests.get(
                PSI_ENDPOINT,
                params=params,
                timeout=(10, read_timeout),
            )
            if response.status_code in (500, 502, 503, 504) and attempt < (max_retries - 1):
                retries_used += 1
                time.sleep(min(2.0, 0.35 * (2 ** attempt)))
                continue
            break
        except requests.Timeout as exc:
            last_exception = exc
            if attempt >= (max_retries - 1):
                break
            retries_used += 1
            time.sleep(min(2.0, 0.35 * (2 ** attempt)))
            continue
        except requests.RequestException as exc:
            raise RuntimeError(f"PSI request failed: {exc}") from exc

    if response is None:
        message = str(last_exception) if last_exception else "unknown timeout"
        raise RuntimeError(
            f"PSI request failed after {max_retries} attempts: {message}. "
            "Попробуйте повторить запрос или увеличить PAGESPEED_TIMEOUT_SEC."
        )

    if response.status_code != 200:
        error_text = ""
        try:
            payload = response.json()
            error_text = str((payload.get("error") or {}).get("message") or "")
        except Exception:
            error_text = response.text[:300]
        raise RuntimeError(f"PSI API error ({response.status_code}): {error_text or 'unknown error'}")

    try:
        data = response.json()
    except Exception as exc:
        raise RuntimeError(f"PSI response decode failed: {exc}") from exc

    lighthouse = data.get("lighthouseResult") or {}
    categories = lighthouse.get("categories") or {}
    audits = lighthouse.get("audits") or {}
    performance_score_raw = ((categories.get("performance") or {}).get("score"))
    performance_score = int(round(float(performance_score_raw) * 100)) if performance_score_raw is not None else None

    lcp_lab = ((audits.get("largest-contentful-paint") or {}).get("numericValue"))
    inp_lab = (
        (audits.get("interaction-to-next-paint") or {}).get("numericValue")
        or (audits.get("experimental-interaction-to-next-paint") or {}).get("numericValue")
    )
    cls_lab = ((audits.get("cumulative-layout-shift") or {}).get("numericValue"))
    fcp_lab = ((audits.get("first-contentful-paint") or {}).get("numericValue"))
    ttfb_lab = ((audits.get("server-response-time") or {}).get("numericValue"))

    loading_experience = data.get("loadingExperience") or {}
    field_metrics = loading_experience.get("metrics") or {}
    lcp_field = _field_metric(field_metrics, ["LARGEST_CONTENTFUL_PAINT_MS"])
    inp_field = _field_metric(field_metrics, ["INTERACTION_TO_NEXT_PAINT", "INTERACTION_TO_NEXT_PAINT_MS"])
    cls_field = _field_metric(field_metrics, ["CUMULATIVE_LAYOUT_SHIFT_SCORE"])

    metrics = {
        "lcp": {
            "lab_value_ms": float(lcp_lab) if lcp_lab is not None else None,
            "field_value_ms": lcp_field,
            "status": _metric_status("lcp", lcp_field if lcp_field is not None else (float(lcp_lab) if lcp_lab is not None else None)),
            "thresholds": {"good_max": 2500, "ni_max": 4000},
        },
        "inp": {
            "lab_value_ms": float(inp_lab) if inp_lab is not None else None,
            "field_value_ms": inp_field,
            "status": _metric_status("inp", inp_field if inp_field is not None else (float(inp_lab) if inp_lab is not None else None)),
            "thresholds": {"good_max": 200, "ni_max": 500},
        },
        "cls": {
            "lab_value": float(cls_lab) if cls_lab is not None else None,
            "field_value": cls_field / 100 if cls_field is not None and cls_field > 1 else cls_field,
            "status": _metric_status(
                "cls",
                (
                    (cls_field / 100 if cls_field is not None and cls_field > 1 else cls_field)
                    if cls_field is not None
                    else (float(cls_lab) if cls_lab is not None else None)
                ),
            ),
            "thresholds": {"good_max": 0.1, "ni_max": 0.25},
        },
        "fcp": {
            "lab_value_ms": float(fcp_lab) if fcp_lab is not None else None,
            "status": _metric_status("fcp", float(fcp_lab) if fcp_lab is not None else None),
            "thresholds": {"good_max": 1800, "ni_max": 3000},
        },
        "ttfb": {
            "lab_value_ms": float(ttfb_lab) if ttfb_lab is not None else None,
            "status": _metric_status("ttfb", float(ttfb_lab) if ttfb_lab is not None else None),
            "thresholds": {"good_max": 800, "ni_max": 1800},
        },
    }

    cwv_statuses = [metrics["lcp"]["status"], metrics["inp"]["status"], metrics["cls"]["status"]]
    if "poor" in cwv_statuses:
        cwv_grade = "poor"
    elif "needs_improvement" in cwv_statuses:
        cwv_grade = "needs_improvement"
    elif all(item == "good" for item in cwv_statuses):
        cwv_grade = "good"
    else:
        cwv_grade = "unknown"

    opportunities: List[Dict[str, Any]] = []
    for audit_id, audit in audits.items():
        if not isinstance(audit, dict):
            continue
        score = audit.get("score")
        display_mode = str(audit.get("scoreDisplayMode") or "")
        if score is None:
            continue
        try:
            score_value = float(score)
        except Exception:
            continue
        if display_mode not in {"metricSavings", "binary", "numeric", "informative"}:
            continue
        if score_value >= 0.9:
            continue
        opportunities.append(
            {
                "id": audit_id,
                "title": str(audit.get("title") or audit_id),
                "score": round(score_value, 3),
                "display_value": str(audit.get("displayValue") or ""),
                "description": str(audit.get("description") or ""),
            }
        )
    opportunities.sort(key=lambda item: item.get("score", 1.0))
    opportunities = opportunities[:8]

    recommendations: List[str] = []
    for item in opportunities:
        text = f"{item.get('title')}"
        if item.get("display_value"):
            text += f" ({item.get('display_value')})"
        recommendations.append(text)
    if not recommendations:
        recommendations = [
            "Поддерживайте текущий уровень производительности: регулярный аудит CWV после релизов.",
            "Проверьте кэширование статики и оптимизацию изображений для стабильного LCP.",
        ]

    fetched_at = datetime.now(timezone.utc).isoformat()
    return {
        "task_type": "core_web_vitals",
        "url": normalized_url,
        "results": {
            "strategy": selected_strategy,
            "source": "pagespeed_insights_api",
            "summary": {
                "performance_score": performance_score,
                "core_web_vitals_status": cwv_grade,
                "passed_metrics": sum(1 for key in ("lcp", "inp", "cls") if metrics[key].get("status") == "good"),
                "total_core_metrics": 3,
            },
            "metrics": metrics,
            "field_data": {
                "is_available": bool(field_metrics),
                "origin_fallback": bool((data.get("originLoadingExperience") or {}).get("metrics")),
            },
            "opportunities": opportunities,
            "recommendations": recommendations,
            "api": {
                "has_key": bool(token),
                "endpoint": PSI_ENDPOINT,
                "retries_used": retries_used,
                "timeout_sec": configured_timeout,
            },
            "checked_at": fetched_at,
        },
    }
