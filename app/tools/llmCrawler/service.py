"""Core execution pipeline for LLM Crawler Simulation."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests
import re
import math

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
            console_errors: list[str] = []
            failed_requests: list[dict[str, str | int | None]] = []
            try:
                page.on(
                    "console",
                    lambda msg: console_errors.append(f"{msg.type}: {msg.text}") if msg.type in {"error", "warning"} else None,
                )
                page.on(
                    "requestfailed",
                    lambda req: failed_requests.append(
                        {
                            "url": req.url[:500],
                            "resource_type": req.resource_type,
                            "failure_text": getattr(req, "failure", lambda: {})().get("errorText") if hasattr(req, "failure") else None,
                        }
                    ),
                )
            except Exception:
                pass
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
                "console_errors": console_errors[:50],
                "failed_requests": failed_requests[:50],
            }
        finally:
            try:
                context.close()
            except Exception:
                pass


def _build_diff(nojs: Dict[str, Any], rendered: Optional[Dict[str, Any]], render_error: Optional[str] = None) -> Dict[str, Any]:
    if not rendered:
        return {
            "textCoverage": None,
            "linksDiff": {"added": 0, "removed": 0, "added_top": [], "removed_top": []},
            "headingsDiff": {"h1": 0, "h2": 0, "h3": 0},
            "note": render_error or "Rendered fetch disabled",
            "missing": [],
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
    missing: List[str] = []
    if text_coverage is not None and text_coverage < 0.7:
        missing.append("Основной текст появляется только после JS (coverage < 0.7)")
    if len(added_links) > 0:
        missing.append("Часть ссылок доступна только с JS (добавленные в rendered)")
    if int(r_h.get("h1") or 0) > int(nojs_h.get("h1") or 0):
        missing.append("Заголовки H1/H2/H3 появляются только в rendered версии")
    if bool((rendered.get("content") or {}).get("readability_text")) and not bool((nojs.get("content") or {}).get("main_text_length")):
        missing.append("Main content извлекается только reader-алгоритмом (raw пустой)")
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
        "missing": missing,
        "note": render_error or "",
    }


def _text_cosine(a: str, b: str) -> float:
    def vec(text: str) -> Dict[str, int]:
        words = re.findall(r"[A-Za-zА-Яа-я0-9]{3,}", text.lower())
        v: Dict[str, int] = {}
        for w in words:
            v[w] = v.get(w, 0) + 1
        return v
    va = vec(a)
    vb = vec(b)
    if not va or not vb:
        return 0.0
    dot = sum(va.get(k, 0) * vb.get(k, 0) for k in va)
    na = math.sqrt(sum(v * v for v in va.values()))
    nb = math.sqrt(sum(v * v for v in vb.values()))
    if na == 0 or nb == 0:
        return 0.0
    return round(dot / (na * nb), 4)


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
    if bool(options.get("include_raw_html")):
        nojs_snapshot["raw_html"] = str(nojs_http.get("body_text") or "")[:200000]
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
    bot_matrix = []
    robots_profiles = (policies.get("robots") or {}).get("profiles") or {}
    for profile in profiles:
        bot_matrix.append(
            {
                "profile": profile,
                "allowed": bool((robots_profiles.get(profile) or {}).get("allowed", True)),
                "reason": (robots_profiles.get(profile) or {}).get("reason"),
            }
        )
    timings["policies_ms"] = int((time.perf_counter() - t1) * 1000)

    rendered_snapshot: Optional[Dict[str, Any]] = None
    render_error: Optional[str] = None
    if render_js:
        notify(62, "Rendered fetch (Playwright)")
        t2 = time.perf_counter()
        try:
            rendered_http = _rendered_fetch(
                url=str(nojs_snapshot.get("final_url") or normalized_url),
                timeout_ms=min(timeout_ms, 15000),
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
            render_debug = {
                "console_errors": rendered_http.get("console_errors") or [],
                "failed_requests": rendered_http.get("failed_requests") or [],
            }
            rendered_snapshot["render_debug"] = render_debug
            if bool(options.get("include_rendered_html")):
                rendered_snapshot["rendered_html"] = str(rendered_http.get("body_text") or "")[:200000]
        except Exception as exc:
            render_error = f"Rendered fetch failed: {exc}"
            notify(70, render_error)
            rendered_snapshot = None
        timings["rendered_ms"] = int((time.perf_counter() - t2) * 1000)
    else:
        timings["rendered_ms"] = 0

    notify(84, "Diff and scoring")
    t3 = time.perf_counter()
    diff = _build_diff(nojs_snapshot, rendered_snapshot, render_error=render_error)
    score = compute_score(
        nojs=nojs_snapshot,
        rendered=rendered_snapshot,
        diff=diff,
        policies=policies,
    )
    recommendations = _build_recommendations(nojs_snapshot, rendered_snapshot, policies, score)
    llm_sim = None
    if bool(getattr(settings, "LLM_SIMULATION_ENABLED", False)):
        llm_sim = _run_llm_simulation(nojs_snapshot)
    js_dep = _js_dependency_score(rendered_snapshot, diff)
    score["js_dependency_score"] = js_dep.get("score")
    cloaking_result = None
    if bool(getattr(settings, "LLM_CRAWLER_CLOAKING_ENABLED", False)) and gpt_snapshot and gbot_snapshot and rendered_snapshot:
        cloaking_result = _cloaking_analysis(rendered_snapshot, gpt_snapshot, gbot_snapshot)
    citation_prob = _compute_citation_probability(nojs_snapshot, score)
    entity_graph = _build_entity_graph(nojs_snapshot) if bool(getattr(settings, "LLM_CRAWLER_ENTITY_GRAPH_ENABLED", False)) else None
    if entity_graph:
        nojs_snapshot["entity_graph"] = entity_graph
    eeat = _compute_eeat(nojs_snapshot, score) if bool(getattr(settings, "LLM_CRAWLER_EEAT_ENABLED", False)) else None
    vector_score = _vector_quality_score(nojs_snapshot, entity_graph) if bool(getattr(settings, "LLM_CRAWLER_VECTOR_SCORE_ENABLED", False)) else None
    ingestion = _llm_ingestion(nojs_snapshot, diff) if bool(getattr(settings, "LLM_SIMULATION_ENABLED", False)) else None
    discoverability = _crawler_path_sim(nojs_snapshot)
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
        "bot_matrix": bot_matrix,
        "recommendations": recommendations,
        "llm": llm_sim,
        "js_dependency": js_dep,
        "cloaking": cloaking_result,
        "citation_probability": citation_prob,
        "entity_graph": entity_graph,
        "eeat_score": eeat,
        "vector_quality_score": vector_score,
        "llm_ingestion": ingestion,
        "discoverability": discoverability,
        "engine": "llm_crawler_mvp_v1",
    }


def _build_recommendations(nojs: Dict[str, Any], rendered: Optional[Dict[str, Any]], policies: Dict[str, Any], score: Dict[str, Any]) -> List[Dict[str, str]]:
    recs: List[Dict[str, str]] = []
    meta = (nojs.get("meta") or {})
    social = (nojs.get("social") or {})
    resources = (nojs.get("resources") or {})
    if "noindex" in str(meta.get("meta_robots") or "").lower():
        recs.append({"priority": "P0", "area": "crawlability", "title": "Уберите noindex для страниц, которые должны индексироваться ботами/LLM"})
    challenge = (nojs.get("challenge") or {})
    if challenge.get("is_challenge"):
        recs.append({"priority": "P0", "area": "access", "title": "WAF/челлендж блокирует ботов — ослабьте правила для известных AI-ботов"})
    if resources.get("cookie_wall"):
        recs.append({"priority": "P0", "area": "access", "title": "Cookie/consent wall перекрывает контент — добавьте бот-байпас или серверный рендер"})
    if resources.get("paywall") or resources.get("login_wall"):
        recs.append({"priority": "P0", "area": "access", "title": "Paywall/Login wall скрывает текст — предусмотрите публичный пререндер или открытый виджет"})
    if resources.get("csp_strict"):
        recs.append({"priority": "P1", "area": "access", "title": "Слишком строгий CSP (script-src 'none') может ломать JS — ослабьте для нужных скриптов"})
    if int(resources.get("mixed_content_count") or 0) > 0:
        recs.append({"priority": "P1", "area": "resources", "title": "Исправьте mixed content (http ресурсы на https странице)"})
    schema = (nojs.get("schema") or {})
    if not schema.get("jsonld_types"):
        recs.append({"priority": "P1", "area": "schema", "title": "Добавьте JSON-LD (Organization/Article/Product) для доверия и извлечения"})
    if float(schema.get("coverage_score") or 0) < 50:
        recs.append({"priority": "P1", "area": "schema", "title": "Увеличьте покрытие schema.org (Organization/Person/Article/Product)"})
    signals = (nojs.get("signals") or {})
    if not signals.get("author_present") or not signals.get("date_present"):
        recs.append({"priority": "P1", "area": "trust", "title": "Укажите автора/дату публикации — повышает понятность и доверие"})
    if not social.get("og_present") or not social.get("twitter_present"):
        recs.append({"priority": "P2", "area": "social", "title": "Добавьте OpenGraph/Twitter метатеги для консистентных сниппетов и LLM-карточек"})
    links = (nojs.get("links") or {})
    if int(links.get("js_only_count") or 0) > 0:
        recs.append({"priority": "P1", "area": "links", "title": "Избегайте JS-only ссылок — используйте href для навигации ботов"})
    if float(links.get("anchor_quality_score") or 0) < 50:
        recs.append({"priority": "P2", "area": "links", "title": "Улучшите анкоры ссылок — больше смысловых текстов вместо 'здесь/читать'"})
    content = (nojs.get("content") or {})
    if int(content.get("main_text_length") or 0) < 500:
        recs.append({"priority": "P2", "area": "content", "title": "Увеличьте основной текст/контент — сейчас он слишком короткий для извлечения"})
    if float(score.get("js_dependency_score", 0)) > 70:
        recs.append({"priority": "P1", "area": "js_dependency", "title": "Высокая зависимость от JS — обеспечьте SSR или пререндер"})
    return recs[:10]


def _run_llm_simulation(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    content = snapshot.get("content") or {}
    text = content.get("main_text_preview") or content.get("readability_text") or ""
    text = str(text or "")
    if not text:
        return {"enabled": False, "summary": "", "key_facts": [], "entities": [], "scores": {}}
    sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
    summary = " ".join(sentences[:2])[:400]
    key_facts = sentences[:5]
    words = re.findall(r"\b[A-ZА-Я][A-Za-zА-Яа-я0-9]+\b", text)
    entities = list(dict.fromkeys(words))[:10]
    citation_spans = []
    for fact in key_facts[:3]:
        start = text.find(fact)
        if start != -1:
            citation_spans.append({"text": fact[:120], "start": start, "end": start + len(fact)})
    scores = {
        "citation_likelihood": 70 if citation_spans else 40,
        "recommendation_likelihood": 60 if len(text) > 800 else 30,
        "hallucination_risk": 20 if citation_spans else 40,
        "answer_quality_score": 60 if len(text) > 800 else 40,
    }
    return {
        "enabled": True,
        "summary": summary,
        "key_facts": key_facts,
        "entities": entities,
        "citation_spans": citation_spans,
        "scores": scores,
    }


def _compute_citation_probability(snapshot: Dict[str, Any], score: Dict[str, Any]) -> float:
    schema = (snapshot.get("schema") or {})
    signals = (snapshot.get("signals") or {})
    headings = (snapshot.get("headings") or {})
    content = (snapshot.get("content") or {})
    base = 40
    if schema.get("coverage_score", 0) >= 50:
        base += 15
    if signals.get("author_present"):
        base += 10
    if signals.get("date_present"):
        base += 5
    if int(headings.get("h1") or 0) >= 1 and int(headings.get("h2") or 0) >= 2:
        base += 10
    ratio = float(content.get("main_content_ratio") or 0)
    if ratio >= 0.5:
        base += 10
    elif ratio < 0.25:
        base -= 10
    return float(max(0, min(100, base)))


def _build_entity_graph(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    schema = snapshot.get("schema") or {}
    text = ((snapshot.get("content") or {}).get("main_text_preview") or "") + " " + ((snapshot.get("meta") or {}).get("title") or "")
    orgs = set()
    persons = set()
    products = set()
    locations = set()
    for t in schema.get("jsonld_types", []):
        if "Organization" in t:
            orgs.add("schema:Organization")
        if "Person" in t:
            persons.add("schema:Person")
        if "Product" in t:
            products.add("schema:Product")
    for token in re.findall(r"\b[A-Z][A-Za-z]{2,}(?:\s+[A-Z][A-Za-z]{2,})+\b", text):
        if "inc" in token.lower() or "llc" in token.lower() or "ltd" in token.lower():
            orgs.add(token[:100])
        else:
            persons.add(token[:100])
    for token in re.findall(r"\b[A-Z][A-Za-z]{2,}\s+(Street|St\.|Ave|Road|City)\b", text):
        locations.add(token[:100])
    return {
        "organizations": sorted(orgs)[:20],
        "persons": sorted(persons)[:20],
        "products": sorted(products)[:20],
        "locations": sorted(locations)[:20],
    }


def _compute_eeat(snapshot: Dict[str, Any], score: Dict[str, Any]) -> Dict[str, Any]:
    signals = snapshot.get("signals") or {}
    schema = snapshot.get("schema") or {}
    meta = snapshot.get("meta") or {}
    total = 50
    if signals.get("author_present"):
        total += 10
    if signals.get("date_present"):
        total += 5
    if schema.get("coverage_score", 0) >= 50:
        total += 10
    if schema.get("coverage_score", 0) >= 75:
        total += 5
    if meta.get("canonical"):
        total += 3
    if signals.get("author_samples"):
        total += 2
    total = max(0, min(100, total))
    return {"score": total}


def _vector_quality_score(snapshot: Dict[str, Any], entity_graph: Dict[str, Any] | None) -> float:
    content = snapshot.get("content") or {}
    text = content.get("main_text_preview") or ""
    words = re.findall(r"[A-Za-zА-Яа-я0-9]+", text)
    unique = len(set(words))
    density = unique / max(1, len(words))
    entities = sum(len(v or []) for v in (entity_graph or {}).values()) if entity_graph else 0
    score = 50
    score += min(20, entities * 2)
    if density < 0.2:
        score -= 15
    elif density > 0.5:
        score += 10
    return float(max(0, min(100, round(score, 2))))


def _llm_ingestion(snapshot: Dict[str, Any], diff: Dict[str, Any]) -> Dict[str, Any]:
    content = snapshot.get("content") or {}
    chunks = content.get("chunks") or []
    chunks_count = len(chunks)
    avg_len = sum(len(c.get("text") or "") for c in chunks) / max(1, chunks_count)
    readability = float(content.get("readability_score") or 0)
    avg_quality = round(min(100, (readability / 100) * 40 + min(60, avg_len / 40)), 2)
    lost = 0.0
    if diff.get("textCoverage") is not None:
        try:
            lost = max(0.0, 1 - float(diff.get("textCoverage")))
        except Exception:
            lost = 0.0
    ingestion_risk = "low"
    if lost > 0.5 or avg_quality < 40:
        ingestion_risk = "high"
    elif lost > 0.3:
        ingestion_risk = "medium"
    return {
        "chunks_count": chunks_count,
        "avg_chunk_quality": avg_quality,
        "lost_content_percent": round(lost * 100, 2),
        "ingestion_risk": ingestion_risk,
    }


def _crawler_path_sim(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    links = snapshot.get("links") or {}
    internal_links = int(links.get("count") or 0)
    anchor_quality = float(links.get("anchor_quality_score") or 0)
    discoverability_score = min(100, internal_links) * 0.3 + anchor_quality * 0.4
    click_depth_estimate = 2 if internal_links > 20 else 3
    crawl_priority = "high" if discoverability_score >= 70 else "medium" if discoverability_score >= 40 else "low"
    return {
        "discoverability_score": round(min(100, discoverability_score), 2),
        "click_depth_estimate": click_depth_estimate,
        "crawl_priority": crawl_priority,
    }


def _js_dependency_score(rendered_snapshot: Dict[str, Any] | None, diff: Dict[str, Any]) -> Dict[str, Any]:
    if not rendered_snapshot:
        return {"score": 0, "failures": 0, "blocked": 0, "content_loaded_by_js_ratio": None}
    render_debug = rendered_snapshot.get("render_debug") or {}
    failed = len(render_debug.get("failed_requests") or [])
    blocked = len([x for x in (render_debug.get("failed_requests") or []) if str(x.get("resource_type") or "") in {"script", "stylesheet"}])
    text_coverage = diff.get("textCoverage")
    try:
        ratio = 1 - float(text_coverage or 0)
    except Exception:
        ratio = None
    score = 0
    if ratio is not None:
        score += min(70, ratio * 100)
    score += min(30, (failed + blocked) * 2)
    return {
        "score": round(min(100, score), 2),
        "failures": failed,
        "blocked": blocked,
        "content_loaded_by_js_ratio": ratio,
    }


def _cloaking_analysis(browser_snap: Dict[str, Any], gpt_snap: Dict[str, Any], gbot_snap: Dict[str, Any]) -> Dict[str, Any]:
    def text(s):
        return ((s or {}).get("content") or {}).get("main_text_preview") or ""
    sim_bg = _text_cosine(text(browser_snap), text(gpt_snap))
    sim_bb = _text_cosine(text(browser_snap), text(gbot_snap))
    len_b = len(text(browser_snap))
    len_gpt = len(text(gpt_snap))
    len_gb = len(text(gbot_snap))
    len_delta = max(abs(len_b - len_gpt) / max(1, len_b), abs(len_b - len_gb) / max(1, len_b))
    missing = []
    bh = (browser_snap.get("headings") or {})
    gh = (gpt_snap.get("headings") or {})
    ggh = (gbot_snap.get("headings") or {})
    if int(bh.get("h1") or 0) > int(gh.get("h1") or 0):
        missing.append("GPTBot missing H1/H2/H3 found in browser render")
    if int(bh.get("h1") or 0) > int(ggh.get("h1") or 0):
        missing.append("Googlebot missing H1/H2/H3 found in browser render")
    br_links = set(((browser_snap.get("links") or {}).get("all_urls") or [])[:200])
    gpt_links = set(((gpt_snap.get("links") or {}).get("all_urls") or [])[:200])
    gg_links = set(((gbot_snap.get("links") or {}).get("all_urls") or [])[:200])
    if len(br_links - gpt_links) > 10:
        missing.append("GPTBot missing significant set of links")
    if len(br_links - gg_links) > 10:
        missing.append("Googlebot missing significant set of links")
    risk = "low"
    if sim_bg < 0.6 or sim_bb < 0.6 or len_delta > 0.4:
        risk = "high"
    elif sim_bg < 0.75 or sim_bb < 0.75:
        risk = "medium"
    return {
        "risk": risk,
        "similarity_scores": {
            "browser_vs_gptbot": sim_bg,
            "browser_vs_googlebot": sim_bb,
        },
        "length_delta": round(len_delta, 3),
        "missing_sections": missing[:20],
    }
