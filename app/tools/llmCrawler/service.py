"""Core execution pipeline for LLM Crawler Simulation."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests

from app.config import settings
from app.tools.http_text import decode_response_text

from .extraction import build_snapshot
from .policies import evaluate_profile_access, parse_robots_rules
from .scoring import compute_score
from .security import assert_safe_url, normalize_http_url, safe_redirect_target


REDIRECT_STATUSES = {301, 302, 303, 307, 308}
UA_NOJS = "Mozilla/5.0 (compatible; LLMCrawlerNoJS/1.0; +https://example.com/bot)"
UA_RENDER = "Mozilla/5.0 (compatible; LLMCrawlerRendered/1.0; +https://example.com/bot)"
MAX_BROWSER_AGE_SEC = 300


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(fallback)


def _read_limited_body(response: requests.Response, max_html_bytes: int) -> tuple[bytes, bool]:
    max_bytes = max(1024, int(max_html_bytes))
    total = 0
    chunks: List[bytes] = []
    truncated = False
    for chunk in response.iter_content(chunk_size=8192):
        if not chunk:
            continue
        next_total = total + len(chunk)
        if next_total > max_bytes:
            keep = max_bytes - total
            if keep > 0:
                chunks.append(chunk[:keep])
                total += keep
            truncated = True
            break
        chunks.append(chunk)
        total = next_total
    return b"".join(chunks), truncated


def _fetch_http(
    *,
    url: str,
    user_agent: str,
    timeout_ms: int,
    max_redirect_hops: int,
    max_html_bytes: int,
) -> Dict[str, Any]:
    from .security import get_allowed_ips_for_url
    
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    current = normalize_http_url(url)
    if not current:
        raise ValueError("Invalid URL")
    assert_safe_url(current)
    
    allowed_ips = get_allowed_ips_for_url(current)
    
    timeout_sec = max(3, int(timeout_ms) / 1000)
    chain: List[Dict[str, Any]] = []
    final_response: Optional[requests.Response] = None
    final_timing_ms = 0
    started_at = time.perf_counter()

    attempts = 3
    last_err: Optional[Exception] = None

    for _ in range(max(1, int(max_redirect_hops)) + 1):
        assert_safe_url(current)
        step_start = time.perf_counter()
        response = None
        for attempt in range(attempts):
            try:
                response = session.get(
                    current,
                    allow_redirects=False,
                    timeout=(5, timeout_sec),
                    stream=True,
                )
                break
            except Exception as exc:
                last_err = exc
                time.sleep(0.2 * (attempt + 1))
        if response is None:
            raise RuntimeError(f"HTTP fetch failed after retries: {last_err}")
        step_timing = int((time.perf_counter() - step_start) * 1000)
        status_code = int(response.status_code)
        location_raw = str(response.headers.get("Location") or "").strip()
        location_abs = ""
        if status_code in REDIRECT_STATUSES and location_raw:
            location_abs = safe_redirect_target(current, location_raw, allowed_ips)
        chain.append(
            {
                "url": current,
                "status_code": status_code,
                "location": location_abs or location_raw,
                "timing_ms": step_timing,
            }
        )
        if status_code in REDIRECT_STATUSES and location_abs:
            response.close()
            current = location_abs
            continue
        final_response = response
        final_timing_ms = step_timing
        break

    if final_response is None:
        raise RuntimeError("Redirect limit exceeded")

    body_bytes, truncated = _read_limited_body(final_response, max_html_bytes=max_html_bytes)
    # Override content with bounded payload for stable decoding and memory control.
    final_response._content = body_bytes
    text = decode_response_text(final_response)
    content_type = str(final_response.headers.get("Content-Type") or "")
    final_url = str(final_response.url or current)
    normalized_final = normalize_http_url(final_url)
    if normalized_final:
        assert_safe_url(normalized_final)
        final_url = normalized_final
    total_timing_ms = int((time.perf_counter() - started_at) * 1000)
    return {
        "status_code": int(final_response.status_code),
        "final_url": final_url,
        "headers": dict(final_response.headers or {}),
        "content_type": content_type,
        "body_text": text,
        "size_bytes": len(body_bytes),
        "truncated": truncated,
        "timing_ms": final_timing_ms,
        "total_timing_ms": total_timing_ms,
        "redirect_chain": chain,
    }


def _rendered_fetch(
    *,
    url: str,
    timeout_ms: int,
    max_html_bytes: int,
) -> Dict[str, Any]:
    from .security import get_allowed_ips_for_url
    
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - depends on runtime
        raise RuntimeError(f"Playwright is unavailable: {exc}") from exc

    started_at = time.perf_counter()
    timeout = max(3000, int(timeout_ms))
    
    initial_url = normalize_http_url(url)
    if not initial_url:
        raise ValueError("Invalid URL")
    assert_safe_url(initial_url)
    allowed_ips = get_allowed_ips_for_url(initial_url)

    with sync_playwright() as p:
        browser = _get_or_create_browser(p)
        try:
            context = browser.new_context(user_agent=UA_RENDER)
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=min(timeout, 12000))
            except Exception:
                pass
            html = page.content()
            raw_bytes = html.encode("utf-8", errors="ignore")
            truncated = len(raw_bytes) > max_html_bytes
            if truncated:
                raw_bytes = raw_bytes[:max_html_bytes]
                html = raw_bytes.decode("utf-8", errors="ignore")
            final_url = normalize_http_url(page.url) or normalize_http_url(url) or url
            final_allowed_ips = get_allowed_ips_for_url(final_url)
            if allowed_ips and final_allowed_ips and not allowed_ips.intersection(final_allowed_ips):
                raise ValueError("Redirect leads to different IP range (DNS rebinding blocked)")
            assert_safe_url(final_url)
            headers = response.headers if response else {}
            status_code = response.status if response else None
            chain = []
            if response is not None:
                req = response.request
                stack = []
                while req:
                    stack.append(req)
                    req = req.redirected_from
                for item in reversed(stack):
                    chain.append({"url": item.url, "status_code": None, "location": ""})
                if chain:
                    chain[-1]["status_code"] = status_code
                else:
                    chain.append({"url": final_url, "status_code": status_code, "location": ""})

            return {
                "status_code": status_code,
                "final_url": final_url,
                "headers": dict(headers or {}),
                "content_type": str((headers or {}).get("content-type") or "text/html"),
                "body_text": html,
                "size_bytes": len(raw_bytes),
                "truncated": bool(truncated),
                "timing_ms": int((time.perf_counter() - started_at) * 1000),
                "total_timing_ms": int((time.perf_counter() - started_at) * 1000),
                "redirect_chain": chain,
            }
        finally:
            try:
                context.close()
            except Exception:
                pass


def _build_diff(nojs: Dict[str, Any], rendered: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not rendered:
        return {
            "textCoverage": None,
            "linksDiff": {"added": 0, "removed": 0, "added_top": [], "removed_top": []},
            "headingsDiff": {"h1": 0, "h2": 0, "h3": 0},
            "note": "Rendered fetch disabled",
        }

    nojs_text = int(((nojs.get("content") or {}).get("main_text_length") or 0))
    rendered_text = int(((rendered.get("content") or {}).get("main_text_length") or 0))
    text_coverage = round(nojs_text / rendered_text, 4) if rendered_text > 0 else None

    nojs_links = set(((nojs.get("links") or {}).get("all_urls") or []))
    rendered_links = set(((rendered.get("links") or {}).get("all_urls") or []))
    added_links = sorted(list(rendered_links - nojs_links))
    removed_links = sorted(list(nojs_links - rendered_links))

    nojs_h = nojs.get("headings") or {}
    r_h = rendered.get("headings") or {}
    return {
        "textCoverage": text_coverage,
        "linksDiff": {
            "added": len(added_links),
            "removed": len(removed_links),
            "added_top": added_links[:20],
            "removed_top": removed_links[:20],
        },
        "headingsDiff": {
            "h1": int(r_h.get("h1") or 0) - int(nojs_h.get("h1") or 0),
            "h2": int(r_h.get("h2") or 0) - int(nojs_h.get("h2") or 0),
            "h3": int(r_h.get("h3") or 0) - int(nojs_h.get("h3") or 0),
        },
    }


def _policies_from_robots(
    *,
    final_url: str,
    requested_profiles: List[str],
    timeout_ms: int,
    max_html_bytes: int,
    max_redirect_hops: int,
) -> Dict[str, Any]:
    cached = _robots_cache_get(final_url)
    if cached:
        return cached

    parsed = urlparse(final_url)
    robots_url = urlunparse((parsed.scheme or "https", parsed.netloc, "/robots.txt", "", "", ""))
    robots_fetch = _fetch_http(
        url=robots_url,
        user_agent=UA_NOJS,
        timeout_ms=timeout_ms,
        max_redirect_hops=max_redirect_hops,
        max_html_bytes=max_html_bytes,
    )
    robots_text = str(robots_fetch.get("body_text") or "")
    rules = parse_robots_rules(robots_text)
    profiles: Dict[str, Any] = {}
    for profile in requested_profiles:
        profiles[profile] = evaluate_profile_access(rules=rules, profile=profile, url=final_url)
    payload = {
        "robots": {
            "url": robots_url,
            "final_url": robots_fetch.get("final_url"),
            "status_code": robots_fetch.get("status_code"),
            "redirect_chain": robots_fetch.get("redirect_chain") or [],
            "profiles": profiles,
            "rules_count": len(rules),
        },
    }
    _robots_cache_put(final_url, payload)
    return payload


# ─── helpers ──────────────────────────────────────────────────────────────

_browser_holder: Dict[str, Any] = {"browser": None, "created_at": 0.0}


def _get_or_create_browser(p) -> Any:
    now = time.time()
    browser = _browser_holder.get("browser")
    created = float(_browser_holder.get("created_at") or 0.0)
    if browser:
        try:
            browser.is_connected()
            if now - created < MAX_BROWSER_AGE_SEC:
                return browser
        except Exception:
            browser = None
    try:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        _browser_holder["browser"] = browser
        _browser_holder["created_at"] = now
        return browser
    except Exception:
        _browser_holder["browser"] = None
        _browser_holder["created_at"] = 0.0
        raise


_robots_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}
_ROBOTS_CACHE_TTL = 600  # seconds


def _robots_cache_get(url: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    entry = _robots_cache.get(url)
    if not entry:
        return None
    ts, payload = entry
    if now - ts > _ROBOTS_CACHE_TTL:
        _robots_cache.pop(url, None)
        return None
    return payload


def _robots_cache_put(url: str, payload: Dict[str, Any]) -> None:
    if len(_robots_cache) > 200:
        # drop oldest 50
        oldest = sorted(_robots_cache.items(), key=lambda kv: kv[1][0])[:50]
        for key, _ in oldest:
            _robots_cache.pop(key, None)
    _robots_cache[url] = (time.time(), payload)


def run_llm_crawler_simulation(
    *,
    requested_url: str,
    options: Dict[str, Any],
    request_id: str,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> Dict[str, Any]:
    started_at = time.perf_counter()

    def notify(progress: int, message: str) -> None:
        if callable(progress_callback):
            progress_callback(max(0, min(100, int(progress))), message)

    normalized_url = normalize_http_url(requested_url)
    if not normalized_url:
        raise ValueError("Введите корректный URL (http/https или домен)")
    assert_safe_url(normalized_url)

    timeout_ms = _safe_int(options.get("timeoutMs"), int(getattr(settings, "FETCH_TIMEOUT_MS", 20000)))
    timeout_ms = max(3000, min(timeout_ms, 120000))
    max_html_bytes = max(50_000, _safe_int(getattr(settings, "MAX_HTML_BYTES", 2_000_000), 2_000_000))
    max_redirect_hops = max(1, _safe_int(getattr(settings, "LLM_CRAWLER_MAX_REDIRECT_HOPS", 8), 8))
    render_js = bool(options.get("renderJs", False))
    show_headers = bool(options.get("showHeaders", False))
    profiles = list(options.get("profile") or ["generic-bot", "search-bot", "ai-bot", "gptbot", "google-extended"])

    timings: Dict[str, Any] = {}

    notify(8, "No-JS fetch started")
    t0 = time.perf_counter()
    nojs_http = _fetch_http(
        url=normalized_url,
        user_agent=UA_NOJS,
        timeout_ms=timeout_ms,
        max_redirect_hops=max_redirect_hops,
        max_html_bytes=max_html_bytes,
    )
    nojs_snapshot = build_snapshot(
        html=str(nojs_http.get("body_text") or ""),
        final_url=str(nojs_http.get("final_url") or normalized_url),
        status_code=nojs_http.get("status_code"),
        headers=nojs_http.get("headers") or {},
        timing_ms=int(nojs_http.get("timing_ms") or 0),
        redirect_chain=list(nojs_http.get("redirect_chain") or []),
        show_headers=show_headers,
        content_type=str(nojs_http.get("content_type") or ""),
        size_bytes=int(nojs_http.get("size_bytes") or 0),
        truncated=bool(nojs_http.get("truncated")),
    )
    timings["nojs_ms"] = int((time.perf_counter() - t0) * 1000)

    notify(40, "Robots and policy checks")
    t1 = time.perf_counter()
    policies = _policies_from_robots(
        final_url=str(nojs_snapshot.get("final_url") or normalized_url),
        requested_profiles=profiles,
        timeout_ms=timeout_ms,
        max_html_bytes=max_html_bytes,
        max_redirect_hops=max_redirect_hops,
    )
    policies["meta"] = {
        "meta_robots": ((nojs_snapshot.get("meta") or {}).get("meta_robots") or ""),
        "x_robots_tag": ((nojs_snapshot.get("meta") or {}).get("x_robots_tag") or ""),
    }
    timings["policies_ms"] = int((time.perf_counter() - t1) * 1000)

    rendered_snapshot: Optional[Dict[str, Any]] = None
    if render_js:
        notify(62, "Rendered fetch (Playwright)")
        t2 = time.perf_counter()
        rendered_http = _rendered_fetch(
            url=str(nojs_snapshot.get("final_url") or normalized_url),
            timeout_ms=timeout_ms,
            max_html_bytes=max_html_bytes,
        )
        rendered_snapshot = build_snapshot(
            html=str(rendered_http.get("body_text") or ""),
            final_url=str(rendered_http.get("final_url") or normalized_url),
            status_code=rendered_http.get("status_code"),
            headers=rendered_http.get("headers") or {},
            timing_ms=int(rendered_http.get("timing_ms") or 0),
            redirect_chain=list(rendered_http.get("redirect_chain") or []),
            show_headers=show_headers,
            content_type=str(rendered_http.get("content_type") or ""),
            size_bytes=int(rendered_http.get("size_bytes") or 0),
            truncated=bool(rendered_http.get("truncated")),
        )
        timings["rendered_ms"] = int((time.perf_counter() - t2) * 1000)
    else:
        timings["rendered_ms"] = 0

    notify(84, "Diff and scoring")
    t3 = time.perf_counter()
    diff = _build_diff(nojs_snapshot, rendered_snapshot)
    score = compute_score(
        nojs=nojs_snapshot,
        rendered=rendered_snapshot,
        diff=diff,
        policies=policies,
    )
    timings["analysis_ms"] = int((time.perf_counter() - t3) * 1000)
    timings["total_ms"] = int((time.perf_counter() - started_at) * 1000)

    notify(100, "Done")
    return {
        "result_version": "1.0",
        "requested_url": normalized_url,
        "final_url": str(
            (rendered_snapshot or {}).get("final_url")
            or (nojs_snapshot.get("final_url") or normalized_url)
        ),
        "request_id": request_id,
        "checked_at": _utc_now(),
        "timings": timings,
        "nojs": nojs_snapshot,
        "rendered": rendered_snapshot,
        "diff": diff,
        "policies": policies,
        "score": score,
        "engine": "llm_crawler_mvp_v1",
    }
