"""Redirect checker service (11 scenarios)."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

REDIRECT_STATUSES = {301, 302, 303, 307, 308}
PERMANENT_REDIRECT_STATUSES = {301, 308}

UA_PRESETS: Dict[str, Dict[str, str]] = {
    "googlebot_desktop": {
        "label": "Googlebot Desktop",
        "value": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    },
    "googlebot_smartphone": {
        "label": "Googlebot Smartphone",
        "value": (
            "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36 "
            "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        ),
    },
    "yandex_bot": {
        "label": "Yandex Bot",
        "value": "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    },
}


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


def _build_netloc(hostname: str, port: Optional[int]) -> str:
    if not hostname:
        return ""
    if port is None:
        return hostname
    return f"{hostname}:{port}"


def _status_to_grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 45:
        return "D"
    return "E"


def _extract_canonical_from_response(response: requests.Response, current_url: str) -> Tuple[str, str]:
    link_header = str(response.headers.get("Link") or "")
    if link_header:
        m = re.search(r"<([^>]+)>\s*;\s*rel\s*=\s*\"?canonical\"?", link_header, flags=re.IGNORECASE)
        if m:
            return urljoin(current_url, m.group(1).strip()), "header"

    content_type = str(response.headers.get("Content-Type") or "").lower()
    raw_text = ""
    if "text/html" in content_type or "application/xhtml+xml" in content_type:
        raw_text = str(response.text or "")
    elif "<html" in str(response.text or "").lower():
        raw_text = str(response.text or "")

    if raw_text:
        soup = BeautifulSoup(raw_text, "html.parser")
        tag = soup.find("link", attrs={"rel": re.compile(r"canonical", re.IGNORECASE)})
        if tag:
            href = str(tag.get("href") or "").strip()
            if href:
                return urljoin(current_url, href), "html"
    return "", ""


def _trace_url(url: str, user_agent: str, timeout: int = 12, max_hops: int = 10, use_proxy: bool = False) -> Dict[str, Any]:
    session = requests.Session()
    if use_proxy:
        from app.proxy import get_requests_proxies
        _proxies = get_requests_proxies()
        if _proxies:
            session.proxies.update(_proxies)
    chain: List[Dict[str, Any]] = []
    visited: set[str] = set()
    current = url
    final_response: Optional[requests.Response] = None
    loop_detected = False
    started_at = time.perf_counter()
    error_text = ""

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    for _ in range(max_hops + 1):
        hop_start = time.perf_counter()
        try:
            response = session.get(current, allow_redirects=False, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            error_text = str(exc)
            break

        elapsed_ms = int((time.perf_counter() - hop_start) * 1000)
        final_response = response
        location_raw = str(response.headers.get("Location") or "").strip()
        location_abs = urljoin(current, location_raw) if location_raw else ""
        chain.append(
            {
                "url": current,
                "status_code": int(response.status_code),
                "location": location_abs or location_raw,
                "timing_ms": elapsed_ms,
                "headers": {
                    "server": response.headers.get("Server", ""),
                    "x-powered-by": response.headers.get("X-Powered-By", ""),
                    "content-type": response.headers.get("Content-Type", ""),
                },
            }
        )

        if int(response.status_code) in REDIRECT_STATUSES and location_abs:
            if location_abs in visited:
                loop_detected = True
                break
            visited.add(location_abs)
            current = location_abs
            continue
        break

    duration_ms = int((time.perf_counter() - started_at) * 1000)

    total_chain_ms = int((time.perf_counter() - started_at) * 1000)
    avg_hop_ms = total_chain_ms // max(1, len(chain))
    slowest_hop = max(chain, key=lambda x: x.get("timing_ms", 0)) if chain else None

    if final_response is None:
        return {
            "start_url": url,
            "final_url": current,
            "final_status_code": None,
            "hops": max(0, len(chain) - 1),
            "chain": chain,
            "error": error_text or "Request failed",
            "loop_detected": loop_detected,
            "duration_ms": duration_ms,
            "total_chain_ms": total_chain_ms,
            "avg_hop_ms": avg_hop_ms,
            "slowest_hop": slowest_hop,
            "content_type": "",
            "canonical_url": "",
            "canonical_source": "",
        }

    final_url = str(final_response.url or current)
    canonical_url, canonical_source = _extract_canonical_from_response(final_response, final_url)
    return {
        "start_url": url,
        "final_url": final_url,
        "final_status_code": int(final_response.status_code),
        "hops": max(0, len(chain) - 1),
        "chain": chain,
        "error": error_text,
        "loop_detected": loop_detected,
        "duration_ms": duration_ms,
        "total_chain_ms": total_chain_ms,
        "avg_hop_ms": avg_hop_ms,
        "slowest_hop": slowest_hop,
        "content_type": str(final_response.headers.get("Content-Type") or ""),
        "canonical_url": canonical_url,
        "canonical_source": canonical_source,
    }


def _trace_codes(trace: Dict[str, Any]) -> List[int]:
    return [int(item.get("status_code")) for item in (trace.get("chain") or []) if item.get("status_code") is not None]


def _chain_summary(trace: Dict[str, Any]) -> str:
    codes = _trace_codes(trace)
    return " -> ".join(str(c) for c in codes) if codes else "-"


def _normalize_for_compare(url: str) -> str:
    parsed = urlparse(str(url or ""))
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.hostname or "").lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return f"{scheme}://{host}{path}"


def _normalize_policy_choice(value: Any, allowed: set[str], default: str) -> str:
    token = str(value or default).strip().lower()
    return token if token in allowed else default


def _normalize_param_names(values: Optional[List[str]]) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()
    for raw in values or []:
        candidate = str(raw or "").strip().lower()
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def _sum_trace_durations(*traces: Optional[Dict[str, Any]]) -> int:
    total = 0
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        try:
            total += int(trace.get("duration_ms") or 0)
        except Exception:
            continue
    return total


def _apex_domain(hostname: str) -> str:
    host = str(hostname or "").strip().lower().strip(".")
    if not host:
        return ""
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return host
    return ".".join(parts[-2:])


def _expand_host_aliases(host: str) -> set[str]:
    value = str(host or "").strip().lower()
    if not value:
        return set()
    aliases = {value}
    if value.startswith("www."):
        aliases.add(value[4:])
    else:
        aliases.add(f"www.{value}")
    return aliases


def _make_scenario(
    *,
    sid: int,
    key: str,
    title: str,
    what_checked: str,
    status: str,
    expected: str,
    actual: str,
    recommendation: str,
    test_url: str,
    trace: Optional[Dict[str, Any]] = None,
    response_codes: Optional[List[int]] = None,
    final_url: Optional[str] = None,
    hops: Optional[int] = None,
    duration_ms: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    trace = trace or {}
    return {
        "id": sid,
        "key": key,
        "title": title,
        "what_checked": what_checked,
        "status": status,
        "expected": expected,
        "actual": actual,
        "recommendation": recommendation,
        "test_url": test_url,
        "response_codes": response_codes if response_codes is not None else _trace_codes(trace),
        "final_url": str(final_url if final_url is not None else trace.get("final_url") or ""),
        "hops": int(hops if hops is not None else trace.get("hops") or 0),
        "duration_ms": int(duration_ms if duration_ms is not None else trace.get("duration_ms") or 0),
        "chain": trace.get("chain") or [],
        "error": str(trace.get("error") or ""),
        "details": details or {},
    }


def _check_js_redirect(url: str, timeout: int = 15000, use_proxy: bool = False) -> Dict[str, Any]:
    """Detect JavaScript-triggered redirects using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"status": "skipped", "reason": "Playwright not available"}

    try:
        with sync_playwright() as p:
            launch_kwargs: Dict[str, Any] = {"headless": True, "args": ["--no-sandbox"]}
            if use_proxy:
                from app.proxy import get_playwright_proxy
                pw_proxy = get_playwright_proxy()
                if pw_proxy:
                    launch_kwargs["proxy"] = pw_proxy
            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
            )
            page = context.new_page()

            redirects: List[Dict[str, str]] = []
            page.on(
                "request",
                lambda req: redirects.append({"url": req.url, "method": req.method})
                if req.is_navigation_request()
                else None,
            )

            started = time.perf_counter()
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=min(timeout, 8000))
            except Exception:
                pass

            final_url = page.url
            elapsed_ms = int((time.perf_counter() - started) * 1000)

            # Check for meta refresh
            meta_refresh = page.evaluate(
                """
                () => {
                    const meta = document.querySelector('meta[http-equiv="refresh"]');
                    return meta ? meta.getAttribute('content') : null;
                }
            """
            )

            # Check for JS redirect patterns in source
            js_redirect_detected = page.evaluate(
                """
                () => {
                    const scripts = document.querySelectorAll('script');
                    let found = false;
                    scripts.forEach(s => {
                        const t = s.textContent || '';
                        if (t.includes('location.href') || t.includes('location.replace') ||
                            t.includes('window.location') || t.includes('location.assign')) {
                            found = true;
                        }
                    });
                    return found;
                }
            """
            )

            context.close()
            browser.close()

            is_js_redirect = final_url.rstrip("/") != url.rstrip("/")

            return {
                "status": "checked",
                "original_url": url,
                "final_url": final_url,
                "is_js_redirect": is_js_redirect,
                "js_redirect_code_detected": js_redirect_detected,
                "meta_refresh": meta_refresh,
                "navigation_requests": len(redirects),
                "redirect_chain": redirects[:10],
                "elapsed_ms": elapsed_ms,
            }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def run_redirect_checker(
    *,
    url: str,
    user_agent_key: str = "googlebot_desktop",
    timeout: int = 12,
    max_hops: int = 10,
    canonical_host_policy: str = "auto",
    trailing_slash_policy: str = "auto",
    enforce_lowercase: bool = True,
    allowed_query_params: Optional[List[str]] = None,
    required_query_params: Optional[List[str]] = None,
    ignore_query_params: Optional[List[str]] = None,
    use_proxy: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    started = time.perf_counter()
    total_scenarios = 18

    normalized_input = _normalize_http_input(url)
    if not normalized_input:
        raise ValueError("Введите корректный URL сайта (http/https или домен).")

    selected_key = user_agent_key if user_agent_key in UA_PRESETS else "googlebot_desktop"
    selected_ua = UA_PRESETS[selected_key]
    ua_value = selected_ua["value"]

    base_trace = _trace_url(normalized_input, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    canonical_from_base = str(base_trace.get("final_url") or normalized_input)
    parsed_canonical = urlparse(canonical_from_base)

    base_host = (parsed_canonical.hostname or urlparse(normalized_input).hostname or "").lower()
    base_port = parsed_canonical.port
    base_scheme = parsed_canonical.scheme if parsed_canonical.scheme in ("http", "https") else "https"
    base_netloc = _build_netloc(base_host, base_port)
    base_root_url = urlunparse((base_scheme, base_netloc, "/", "", "", ""))

    canonical_host_policy = _normalize_policy_choice(
        canonical_host_policy, {"auto", "www", "non-www"}, "auto"
    )
    trailing_slash_policy = _normalize_policy_choice(
        trailing_slash_policy, {"auto", "slash", "no-slash"}, "auto"
    )
    enforce_lowercase = bool(enforce_lowercase)

    ignore_defaults = [
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "gclid",
        "fbclid",
        "yclid",
        "mc_cid",
        "mc_eid",
    ]
    ignore_params = _normalize_param_names(ignore_query_params or ignore_defaults)
    allowed_params = _normalize_param_names(allowed_query_params or [])
    required_params = [
        p for p in _normalize_param_names(required_query_params or []) if p not in set(ignore_params)
    ]

    if canonical_host_policy == "www":
        target_host = base_host if base_host.startswith("www.") else (f"www.{base_host}" if base_host else "")
    elif canonical_host_policy == "non-www":
        target_host = base_host[4:] if base_host.startswith("www.") else base_host
    else:
        target_host = base_host

    scenarios: List[Dict[str, Any]] = []
    traces_for_chain: List[Tuple[str, Dict[str, Any]]] = [("base", base_trace)]

    def _notify_progress(index: int, key: str, title: str, test_url: str = "") -> None:
        if not progress_callback:
            return
        try:
            progress_callback(
                {
                    "current_scenario_index": index,
                    "scenario_count": total_scenarios,
                    "current_scenario_key": key,
                    "current_scenario_title": title,
                    "current_step": title,
                    "current_url": test_url or normalized_input,
                }
            )
        except Exception:
            pass

    # 1) HTTP -> HTTPS
    _notify_progress(1, "http_to_https", "HTTP -> HTTPS", normalized_input)
    http_url = urlunparse(("http", base_netloc, "/", "", "", ""))
    trace_http = _trace_url(http_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("http_to_https", trace_http))
    final_scheme_http = (urlparse(str(trace_http.get("final_url") or "")).scheme or "").lower()
    http_first_code = (_trace_codes(trace_http) or [None])[0]
    if trace_http.get("error"):
        status_http = "error"
        rec_http = "Проверьте DNS/SSL и настройте прямой 301 редирект с HTTP на HTTPS."
    elif final_scheme_http == "https" and int(trace_http.get("hops") or 0) >= 1 and http_first_code in PERMANENT_REDIRECT_STATUSES:
        status_http = "passed"
        rec_http = ""
    elif final_scheme_http == "https":
        status_http = "warning"
        rec_http = "Используйте постоянный редирект 301/308 с HTTP на HTTPS."
    else:
        status_http = "error"
        rec_http = "Настройте принудительный HTTPS: HTTP должен всегда вести на HTTPS."
    scenarios.append(
        _make_scenario(
            sid=1,
            key="http_to_https",
            title="HTTP -> HTTPS",
            what_checked="Редирект с http:// на https://",
            status=status_http,
            expected="Постоянный редирект 301/308 на HTTPS",
            actual=f"{_chain_summary(trace_http)} | Final: {trace_http.get('final_url') or '-'}",
            recommendation=rec_http,
            test_url=http_url,
            trace=trace_http,
        )
    )

    # 2) WWW vs non-WWW
    _notify_progress(2, "www_consistency", "WWW vs без WWW", normalized_input)
    alt_host = ""
    if base_host.startswith("www."):
        alt_host = base_host[4:]
    elif base_host:
        alt_host = f"www.{base_host}"
    alt_netloc = _build_netloc(alt_host, base_port)
    alt_www_url = urlunparse((base_scheme, alt_netloc, "/", "", "", "")) if alt_netloc else ""
    trace_www = _trace_url(alt_www_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy) if alt_www_url else {}
    if trace_www:
        traces_for_chain.append(("www_consistency", trace_www))
    final_www_host = (urlparse(str((trace_www or {}).get("final_url") or "")).hostname or "").lower()
    www_first_code = (_trace_codes(trace_www) or [None])[0] if trace_www else None
    expected_www_host = target_host or base_host
    if not alt_www_url:
        status_www = "warning"
        rec_www = "Проверьте консистентность WWW/без WWW на основной версии домена."
        actual_www = "Не удалось сформировать альтернативный WWW-хост."
    elif trace_www.get("error"):
        status_www = "error"
        rec_www = "Проверьте доступность альтернативной WWW/без-WWW версии и настройте 301 на каноническую."
        actual_www = str(trace_www.get("error") or "request error")
    elif (
        final_www_host == expected_www_host
        and int(trace_www.get("hops") or 0) >= 1
        and www_first_code in PERMANENT_REDIRECT_STATUSES
    ):
        status_www = "passed"
        rec_www = ""
        actual_www = f"{_chain_summary(trace_www)} | Final host: {final_www_host or '-'}"
    elif final_www_host == expected_www_host:
        status_www = "warning"
        rec_www = "Используйте 301/308, чтобы все версии WWW вели на один канонический хост."
        actual_www = f"{_chain_summary(trace_www)} | Final host: {final_www_host or '-'}"
    else:
        status_www = "error"
        rec_www = "Настройте единый канонический хост (www или без www) и постоянный редирект."
        actual_www = f"{_chain_summary(trace_www)} | Final host: {final_www_host or '-'}"
    scenarios.append(
        _make_scenario(
            sid=2,
            key="www_consistency",
            title="WWW vs без WWW",
            what_checked="Консистентность www.site.com и site.com",
            status=status_www,
            expected=f"Обе версии ведут на хост: {expected_www_host or '-'}",
            actual=actual_www,
            recommendation=rec_www,
            test_url=alt_www_url or "-",
            trace=trace_www if trace_www else None,
        )
    )

    # 3) Multiple slashes
    _notify_progress(3, "multiple_slashes", "Множественные слеши", normalized_input)
    slashes_url = urlunparse((base_scheme, base_netloc, "/redirect-checker//probe//", "", "", ""))
    trace_slashes = _trace_url(slashes_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("multiple_slashes", trace_slashes))
    final_slashes_path = urlparse(str(trace_slashes.get("final_url") or "")).path or "/"
    if trace_slashes.get("error"):
        status_slashes = "warning"
        rec_slashes = "Проверьте нормализацию URL с двойными слешами и добавьте 301 на чистый путь."
    elif int(trace_slashes.get("final_status_code") or 0) in (404, 410):
        status_slashes = "warning"
        rec_slashes = "Проверьте редирект для URL с множественными слешами на существующие страницы."
    elif "//" not in final_slashes_path:
        status_slashes = "passed"
        rec_slashes = ""
    else:
        status_slashes = "warning"
        rec_slashes = "Настройте удаление множественных слешей (301 на нормализованный URL)."
    scenarios.append(
        _make_scenario(
            sid=3,
            key="multiple_slashes",
            title="Множественные слеши",
            what_checked="Удаление // в URL",
            status=status_slashes,
            expected="URL с // нормализуется до одного слеша",
            actual=f"{_chain_summary(trace_slashes)} | Final path: {final_slashes_path}",
            recommendation=rec_slashes,
            test_url=slashes_url,
            trace=trace_slashes,
        )
    )

    # 4) URL case
    _notify_progress(4, "url_case", "Регистр URL", normalized_input)
    case_url = urlunparse((base_scheme, base_netloc, "/CART", "", "", ""))
    trace_case = _trace_url(case_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("url_case", trace_case))
    final_case_path = urlparse(str(trace_case.get("final_url") or "")).path or "/"
    case_first_code = (_trace_codes(trace_case) or [None])[0]
    if not enforce_lowercase:
        if trace_case.get("error"):
            status_case = "warning"
            rec_case = "Проверка lowercase отключена политикой, но убедитесь, что URL доступен для ботов."
        else:
            status_case = "passed"
            rec_case = ""
    elif trace_case.get("error"):
        status_case = "warning"
        rec_case = "Проверьте обработку URL в верхнем регистре и добавьте 301 на lowercase-версию."
    elif int(trace_case.get("final_status_code") or 0) in (404, 410):
        status_case = "warning"
        rec_case = "Маршрут может отсутствовать; для рабочих страниц держите единый lowercase URL."
    elif int(trace_case.get("hops") or 0) >= 1 and final_case_path == final_case_path.lower() and case_first_code in PERMANENT_REDIRECT_STATUSES:
        status_case = "passed"
        rec_case = ""
    else:
        status_case = "warning"
        rec_case = "Убедитесь, что uppercase URL перенаправляются 301 на lowercase."
    scenarios.append(
        _make_scenario(
            sid=4,
            key="url_case",
            title="Регистр URL",
            what_checked="Переход /CART -> /cart",
            status=status_case,
            expected=(
                "Политика lowercase отключена (допускается любой регистр)"
                if not enforce_lowercase
                else "Uppercase URL редиректится на lowercase"
            ),
            actual=f"{_chain_summary(trace_case)} | Final path: {final_case_path}",
            recommendation=rec_case,
            test_url=case_url,
            trace=trace_case,
        )
    )

    # 5) index files
    _notify_progress(5, "index_files", "Index-файлы", normalized_input)
    index_url = urlunparse((base_scheme, base_netloc, "/index.html", "", "", ""))
    trace_index = _trace_url(index_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("index_files", trace_index))
    index_first_code = (_trace_codes(trace_index) or [None])[0]
    final_index_path = urlparse(str(trace_index.get("final_url") or "")).path or "/"
    if trace_index.get("error"):
        status_index = "error"
        rec_index = "Проверьте доступность сайта и настройте 301 /index.html -> /."
    elif int(trace_index.get("hops") or 0) >= 1 and index_first_code in PERMANENT_REDIRECT_STATUSES and final_index_path in ("/", ""):
        status_index = "passed"
        rec_index = ""
    elif int(trace_index.get("final_status_code") or 0) in (200, 404, 410):
        status_index = "warning"
        rec_index = "Рекомендуется постоянный 301/308 редирект /index.* на корневой URL."
    else:
        status_index = "error"
        rec_index = "Настройте корректную обработку index-файлов: без цепочек и с 301 редиректом."
    scenarios.append(
        _make_scenario(
            sid=5,
            key="index_files",
            title="Index-файлы",
            what_checked="Редирект /index.html -> /",
            status=status_index,
            expected="Постоянный редирект на канонический URL без index-файла",
            actual=f"{_chain_summary(trace_index)} | Final path: {final_index_path}",
            recommendation=rec_index,
            test_url=index_url,
            trace=trace_index,
        )
    )

    # 6) trailing slash
    _notify_progress(6, "trailing_slash", "Trailing slash", normalized_input)
    slash_a = urlunparse((base_scheme, base_netloc, "/page", "", "", ""))
    slash_b = urlunparse((base_scheme, base_netloc, "/page/", "", "", ""))
    trace_slash_a = _trace_url(slash_a, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    trace_slash_b = _trace_url(slash_b, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("trailing_slash_a", trace_slash_a))
    traces_for_chain.append(("trailing_slash_b", trace_slash_b))
    final_path_a = urlparse(str(trace_slash_a.get("final_url") or "")).path or "/"
    final_path_b = urlparse(str(trace_slash_b.get("final_url") or "")).path or "/"

    def _path_matches_policy(path: str, policy: str) -> bool:
        if path in ("", "/"):
            return True
        if policy == "slash":
            return path.endswith("/")
        if policy == "no-slash":
            return not path.endswith("/")
        return True

    if trace_slash_a.get("error") or trace_slash_b.get("error"):
        status_trailing = "warning"
        rec_trailing = "Проверьте единый trailing slash policy и настройте редиректы 301."
    elif int(trace_slash_a.get("final_status_code") or 0) in (404, 410) and int(trace_slash_b.get("final_status_code") or 0) in (404, 410):
        status_trailing = "warning"
        rec_trailing = "Тестовые URL не существуют; проверьте правило на реальных страницах."
    else:
        same_target = _normalize_for_compare(str(trace_slash_a.get("final_url") or "")) == _normalize_for_compare(
            str(trace_slash_b.get("final_url") or "")
        )
        policy_ok = _path_matches_policy(final_path_a, trailing_slash_policy) and _path_matches_policy(
            final_path_b, trailing_slash_policy
        )
        if (
            same_target
            and policy_ok
            and (int(trace_slash_a.get("hops") or 0) >= 1 or int(trace_slash_b.get("hops") or 0) >= 1)
        ):
            status_trailing = "passed"
            rec_trailing = ""
        elif same_target and policy_ok:
            status_trailing = "warning"
            rec_trailing = "Убедитесь, что неканоничная версия trailing slash даёт 301 на каноничную."
        else:
            status_trailing = "warning"
            if trailing_slash_policy == "slash":
                rec_trailing = "Сделайте канонической версию URL со слешем на конце (/page/)."
            elif trailing_slash_policy == "no-slash":
                rec_trailing = "Сделайте канонической версию URL без слеша на конце (/page)."
            else:
                rec_trailing = "Сделайте единообразие /page и /page/ через один канонический вариант."
    scenarios.append(
        _make_scenario(
            sid=6,
            key="trailing_slash",
            title="Trailing slash",
            what_checked="Единообразие /page и /page/",
            status=status_trailing,
            expected=(
                "Каноническая версия со слешем (/page/)"
                if trailing_slash_policy == "slash"
                else (
                    "Каноническая версия без слеша (/page)"
                    if trailing_slash_policy == "no-slash"
                    else "Обе версии сходятся к одному каноническому URL"
                )
            ),
            actual=(
                f"/page: {_chain_summary(trace_slash_a)} -> {trace_slash_a.get('final_url') or '-'} | "
                f"/page/: {_chain_summary(trace_slash_b)} -> {trace_slash_b.get('final_url') or '-'} | "
                f"policy={trailing_slash_policy}"
            ),
            recommendation=rec_trailing,
            test_url=f"{slash_a} | {slash_b}",
            response_codes=_trace_codes(trace_slash_a) + _trace_codes(trace_slash_b),
            final_url=f"{trace_slash_a.get('final_url') or '-'} | {trace_slash_b.get('final_url') or '-'}",
            hops=max(int(trace_slash_a.get("hops") or 0), int(trace_slash_b.get("hops") or 0)),
            duration_ms=_sum_trace_durations(trace_slash_a, trace_slash_b),
            details={"trace_a": trace_slash_a, "trace_b": trace_slash_b},
        )
    )

    # 7) old extensions
    _notify_progress(7, "legacy_extensions", "Старые расширения", normalized_input)
    ext_html_url = urlunparse((base_scheme, base_netloc, "/legacy-page.html", "", "", ""))
    ext_php_url = urlunparse((base_scheme, base_netloc, "/legacy-page.php", "", "", ""))
    trace_ext_html = _trace_url(ext_html_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    trace_ext_php = _trace_url(ext_php_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("legacy_html", trace_ext_html))
    traces_for_chain.append(("legacy_php", trace_ext_php))

    def _is_clean_path(test_url: str) -> bool:
        p = (urlparse(test_url).path or "").lower()
        return not (p.endswith(".html") or p.endswith(".php"))

    html_good = int(trace_ext_html.get("hops") or 0) >= 1 and (_trace_codes(trace_ext_html) or [None])[0] in PERMANENT_REDIRECT_STATUSES and _is_clean_path(
        str(trace_ext_html.get("final_url") or "")
    )
    php_good = int(trace_ext_php.get("hops") or 0) >= 1 and (_trace_codes(trace_ext_php) or [None])[0] in PERMANENT_REDIRECT_STATUSES and _is_clean_path(
        str(trace_ext_php.get("final_url") or "")
    )
    if html_good or php_good:
        status_extensions = "passed"
        rec_extensions = ""
    elif int(trace_ext_html.get("final_status_code") or 0) in (404, 410) and int(trace_ext_php.get("final_status_code") or 0) in (404, 410):
        status_extensions = "warning"
        rec_extensions = "Проверьте legacy URL с расширениями в вашем проекте и настройте 301 на чистые URL."
    else:
        status_extensions = "warning"
        rec_extensions = "Настройте 301 редиректы .html/.php URL на чистые канонические адреса."
    scenarios.append(
        _make_scenario(
            sid=7,
            key="legacy_extensions",
            title="Старые расширения",
            what_checked="Редиректы .html/.php -> clean URL",
            status=status_extensions,
            expected="Старые расширения ведут на канонический URL без расширения",
            actual=(
                f".html: {_chain_summary(trace_ext_html)} -> {trace_ext_html.get('final_url') or '-'} | "
                f".php: {_chain_summary(trace_ext_php)} -> {trace_ext_php.get('final_url') or '-'}"
            ),
            recommendation=rec_extensions,
            test_url=f"{ext_html_url} | {ext_php_url}",
            response_codes=_trace_codes(trace_ext_html) + _trace_codes(trace_ext_php),
            final_url=f"{trace_ext_html.get('final_url') or '-'} | {trace_ext_php.get('final_url') or '-'}",
            hops=max(int(trace_ext_html.get("hops") or 0), int(trace_ext_php.get("hops") or 0)),
            duration_ms=_sum_trace_durations(trace_ext_html, trace_ext_php),
            details={"trace_html": trace_ext_html, "trace_php": trace_ext_php},
        )
    )

    # 8) canonical tag
    _notify_progress(8, "canonical_tag", "Canonical тег", normalized_input)
    canonical_url = str(base_trace.get("canonical_url") or "").strip()
    canonical_source = str(base_trace.get("canonical_source") or "")
    canonical_host = (urlparse(canonical_url).hostname or "").lower() if canonical_url else ""
    expected_canonical_host = target_host or base_host
    if canonical_url and canonical_host == expected_canonical_host:
        status_canonical = "passed"
        rec_canonical = ""
    elif canonical_url:
        status_canonical = "warning"
        rec_canonical = (
            "Проверьте canonical: он должен указывать на канонический host по выбранной политике."
            if canonical_host_policy != "auto"
            else "Проверьте canonical: он должен указывать на каноническую версию этого же домена."
        )
    else:
        status_canonical = "warning"
        rec_canonical = "Добавьте <link rel=\"canonical\"> в <head> для контроля дублей."
    scenarios.append(
        _make_scenario(
            sid=8,
            key="canonical_tag",
            title="Canonical тег",
            what_checked="Наличие и корректность <link rel=\"canonical\">",
            status=status_canonical,
            expected=f"Canonical присутствует и ссылается на host: {expected_canonical_host or '-'}",
            actual=(f"canonical={canonical_url}" if canonical_url else "canonical не найден"),
            recommendation=rec_canonical,
            test_url=str(base_trace.get("final_url") or normalized_input),
            trace=base_trace,
            details={"canonical_source": canonical_source},
        )
    )

    # 9) 404 page
    _notify_progress(9, "missing_404", "404-страницы", normalized_input)
    random_404 = f"/redirect-checker-404-{uuid.uuid4().hex[:10]}"
    url_404 = urlunparse((base_scheme, base_netloc, random_404, "", "", ""))
    trace_404 = _trace_url(url_404, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("missing_404", trace_404))
    status_code_404 = int(trace_404.get("final_status_code") or 0)
    if trace_404.get("error"):
        status_404 = "error"
        rec_404 = "Проверьте доступность сайта и обработку несуществующих URL."
    elif status_code_404 == 404:
        status_404 = "passed"
        rec_404 = ""
    elif status_code_404 == 410:
        status_404 = "warning"
        rec_404 = "410 допустим, но убедитесь, что это осознанная политика."
    else:
        status_404 = "error"
        rec_404 = "Несуществующие URL должны возвращать 404 (или 410), а не 200/редирект на главную."
    scenarios.append(
        _make_scenario(
            sid=9,
            key="missing_404",
            title="404-страницы",
            what_checked="Код ответа для несуществующего URL",
            status=status_404,
            expected="HTTP 404 для несуществующей страницы",
            actual=f"{_chain_summary(trace_404)} | Final status: {status_code_404 or '-'}",
            recommendation=rec_404,
            test_url=url_404,
            trace=trace_404,
        )
    )

    # 10) redirect chains
    _notify_progress(10, "redirect_chains", "Цепочки редиректов", normalized_input)
    worst_name = ""
    worst_trace: Dict[str, Any] = {}
    worst_hops = -1
    for name, trace in traces_for_chain:
        hops_val = int(trace.get("hops") or 0)
        if hops_val > worst_hops:
            worst_hops = hops_val
            worst_name = name
            worst_trace = trace
    if worst_hops <= 1:
        status_chain = "passed"
        rec_chain = ""
    elif worst_hops == 2:
        status_chain = "warning"
        rec_chain = "Сократите цепочки редиректов до одного шага (A -> C вместо A -> B -> C)."
    else:
        status_chain = "error"
        rec_chain = "Уберите длинные цепочки 3+ редиректов и оставьте прямой 301 на финальный URL."
    scenarios.append(
        _make_scenario(
            sid=10,
            key="redirect_chains",
            title="Цепочки редиректов",
            what_checked="Наличие 2+ последовательных редиректов",
            status=status_chain,
            expected="Не более 1 редиректа до финального URL",
            actual=(
                f"Макс. хопов: {max(0, worst_hops)} "
                f"(scenario={worst_name or '-'}, chain={_chain_summary(worst_trace)})"
            ),
            recommendation=rec_chain,
            test_url=str(worst_trace.get("start_url") or base_root_url),
            trace=worst_trace,
            hops=max(0, worst_hops),
            details={"worst_scenario": worst_name},
        )
    )

    # 11) user-agent comparison
    _notify_progress(11, "user_agent_emulation", "User-Agent эмуляция", normalized_input)
    ua_rows: List[Dict[str, Any]] = []
    compare_keys = ["googlebot_desktop", "googlebot_smartphone", "yandex_bot"]
    ua_traces: Dict[str, Dict[str, Any]] = {}
    for key in compare_keys:
        trace = _trace_url(base_root_url, UA_PRESETS[key]["value"], timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
        ua_traces[key] = trace
        traces_for_chain.append((f"ua_{key}", trace))
        ua_rows.append(
            {
                "key": key,
                "label": UA_PRESETS[key]["label"],
                "status_code": trace.get("final_status_code"),
                "final_url": trace.get("final_url"),
                "hops": int(trace.get("hops") or 0),
                "error": str(trace.get("error") or ""),
            }
        )
    blocked = [
        row
        for row in ua_rows
        if row.get("error") or int(row.get("status_code") or 0) in (401, 403, 429, 503)
    ]
    final_targets = {
        _normalize_for_compare(str(row.get("final_url") or ""))
        for row in ua_rows
        if str(row.get("final_url") or "")
    }
    status_codes = {int(row.get("status_code") or 0) for row in ua_rows if row.get("status_code") is not None}
    if blocked:
        status_ua = "error"
        rec_ua = "Проверьте правила WAF/Firewall/Rate-limit: боты не должны блокироваться по User-Agent."
    elif len(final_targets) <= 1 and len(status_codes) <= 1:
        status_ua = "passed"
        rec_ua = ""
    else:
        status_ua = "warning"
        rec_ua = "Сведите ответы разных User-Agent к единому каноническому поведению (URL и коды)."
    scenarios.append(
        _make_scenario(
            sid=11,
            key="user_agent_emulation",
            title="User-Agent эмуляция",
            what_checked="Сравнение ответов для Googlebot Desktop/Smartphone и Yandex Bot",
            status=status_ua,
            expected="Одинаковый канонический ответ для основных ботов",
            actual=(
                ", ".join(
                    [
                        f"{row['label']}: {row.get('status_code') or '-'} -> {row.get('final_url') or '-'}"
                        for row in ua_rows
                    ]
                )
                or "-"
            ),
            recommendation=rec_ua,
            test_url=base_root_url,
            trace=ua_traces.get(selected_key),
            details={"ua_rows": ua_rows},
        )
    )

    # 12) query params canonicalization
    _notify_progress(12, "query_params_canonicalization", "Query params canonicalization", normalized_input)
    param_probe_query = "utm_source=test&gclid=abc123&page=2&sort=asc&ref=campaign"
    param_probe_url = urlunparse((base_scheme, base_netloc, "/redirect-checker-param-probe", "", param_probe_query, ""))
    trace_params = _trace_url(param_probe_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
    traces_for_chain.append(("query_params_canonicalization", trace_params))
    final_param_pairs = parse_qsl(urlparse(str(trace_params.get("final_url") or "")).query, keep_blank_values=True)
    final_param_keys = [str(key or "").strip().lower() for key, _ in final_param_pairs if str(key or "").strip()]
    final_param_set = set(final_param_keys)
    ignore_set = set(ignore_params)
    allowed_set = set(allowed_params)

    params_violations: List[str] = []
    leaked_tracking = sorted(ignore_set.intersection(final_param_set))
    if leaked_tracking:
        params_violations.append(f"tracking params not cleaned: {', '.join(leaked_tracking)}")
    if allowed_set:
        disallowed = sorted([key for key in final_param_set if key not in allowed_set])
        if disallowed:
            params_violations.append(f"disallowed params in final URL: {', '.join(disallowed)}")

    if trace_params.get("error"):
        status_params = "warning"
        rec_params = "Проверьте обработку query-параметров и канонизацию URL после редиректов."
    elif params_violations:
        status_params = "warning"
        rec_params = (
            "Удаляйте tracking-параметры (utm, gclid и др.) и фиксируйте whitelist допустимых query params."
        )
    else:
        status_params = "passed"
        rec_params = ""
    scenarios.append(
        _make_scenario(
            sid=12,
            key="query_params_canonicalization",
            title="Query params canonicalization",
            what_checked="Очистка tracking params и контроль whitelist query-параметров",
            status=status_params,
            expected=(
                f"Игнорируемые params удаляются ({', '.join(ignore_params[:5])}{'...' if len(ignore_params) > 5 else ''}); "
                + (
                    f"разрешены только: {', '.join(allowed_params)}"
                    if allowed_params
                    else "дополнительный whitelist не задан"
                )
            ),
            actual=(
                f"{_chain_summary(trace_params)} | Final keys: "
                f"{', '.join(sorted(final_param_set)) if final_param_set else '-'}"
            ),
            recommendation=rec_params,
            test_url=param_probe_url,
            trace=trace_params,
            details={"violations": params_violations},
        )
    )

    # 13) required query params preserved
    _notify_progress(13, "required_query_params", "Preserve required params", normalized_input)
    if required_params:
        required_probe_query = "&".join([f"{name}=1" for name in required_params])
        required_probe_url = urlunparse(
            (base_scheme, base_netloc, "/redirect-checker-required-probe", "", required_probe_query, "")
        )
        trace_required = _trace_url(required_probe_url, ua_value, timeout=timeout, max_hops=max_hops, use_proxy=use_proxy)
        traces_for_chain.append(("required_query_params", trace_required))
        required_final_keys = {
            str(key or "").strip().lower()
            for key, _ in parse_qsl(urlparse(str(trace_required.get("final_url") or "")).query, keep_blank_values=True)
            if str(key or "").strip()
        }
        missing_required = [name for name in required_params if name not in required_final_keys]
        if trace_required.get("error"):
            status_required = "warning"
            rec_required = "Проверьте редиректы для URL с обязательными параметрами: параметры не должны теряться."
        elif missing_required:
            status_required = "error"
            rec_required = "Сохраните обязательные query-параметры после редиректа (например page/lang/sort)."
        else:
            status_required = "passed"
            rec_required = ""
        actual_required = (
            f"{_chain_summary(trace_required)} | Final required keys: "
            f"{', '.join(sorted(required_final_keys)) if required_final_keys else '-'}"
        )
        required_test_url = required_probe_url
    else:
        trace_required = {}
        missing_required = []
        status_required = "passed"
        rec_required = ""
        actual_required = "Список обязательных query params не задан."
        required_test_url = "-"

    scenarios.append(
        _make_scenario(
            sid=13,
            key="required_query_params",
            title="Preserve required params",
            what_checked="Сохранение обязательных query-параметров после редиректов",
            status=status_required,
            expected=(
                f"Сохраняются параметры: {', '.join(required_params)}"
                if required_params
                else "Проверка выполняется при задании required_query_params"
            ),
            actual=actual_required,
            recommendation=rec_required,
            test_url=required_test_url,
            trace=trace_required if trace_required else None,
            details={"missing": missing_required},
        )
    )

    # 14) canonical should match final URL
    _notify_progress(14, "canonical_matches_final", "Canonical vs Final URL", normalized_input)
    final_base_url = str(base_trace.get("final_url") or normalized_input)
    canonical_norm = _normalize_for_compare(canonical_url) if canonical_url else ""
    final_norm = _normalize_for_compare(final_base_url)
    if not canonical_url:
        status_canonical_match = "warning"
        rec_canonical_match = "Добавьте canonical и синхронизируйте его с финальным URL после редиректов."
    elif canonical_norm == final_norm:
        status_canonical_match = "passed"
        rec_canonical_match = ""
    else:
        status_canonical_match = "warning"
        rec_canonical_match = "Приведите canonical к финальному URL страницы (scheme/host/path)."
    scenarios.append(
        _make_scenario(
            sid=14,
            key="canonical_matches_final",
            title="Canonical vs Final URL",
            what_checked="Совпадение canonical-тега с финальным URL после редиректов",
            status=status_canonical_match,
            expected="Canonical и финальный URL совпадают",
            actual=f"canonical={canonical_url or '-'} | final={final_base_url or '-'}",
            recommendation=rec_canonical_match,
            test_url=final_base_url or normalized_input,
            trace=base_trace,
        )
    )

    # 15) mixed redirect types in one chain
    _notify_progress(15, "mixed_redirect_types", "Mixed redirect types", normalized_input)
    mixed_chains: List[str] = []
    for chain_name, trace_item in traces_for_chain:
        chain_codes = [code for code in _trace_codes(trace_item) if code in REDIRECT_STATUSES]
        if not chain_codes:
            continue
        has_permanent = any(code in PERMANENT_REDIRECT_STATUSES for code in chain_codes)
        has_temporary = any(code in REDIRECT_STATUSES and code not in PERMANENT_REDIRECT_STATUSES for code in chain_codes)
        if has_permanent and has_temporary:
            mixed_chains.append(f"{chain_name}: {' -> '.join(str(code) for code in chain_codes)}")

    if mixed_chains:
        status_mixed = "warning"
        rec_mixed = "Избегайте смешивания 301 и 302/307 в одной цепочке: оставьте единый тип редиректа."
        actual_mixed = "; ".join(mixed_chains[:3])
    else:
        status_mixed = "passed"
        rec_mixed = ""
        actual_mixed = "Смешанных цепочек (301 + 302/307) не обнаружено."
    scenarios.append(
        _make_scenario(
            sid=15,
            key="mixed_redirect_types",
            title="Mixed redirect types",
            what_checked="Смешивание permanent и temporary редиректов в одной цепочке",
            status=status_mixed,
            expected="В одной цепочке используется один тип редиректа",
            actual=actual_mixed,
            recommendation=rec_mixed,
            test_url=base_root_url,
            trace=base_trace,
            details={"mixed_chains": mixed_chains},
        )
    )

    # 16) cross-domain redirects
    _notify_progress(16, "cross_domain_redirect", "Cross-domain redirect control", normalized_input)
    allowed_hosts: set[str] = set()
    for candidate_host in [base_host, target_host, alt_host]:
        allowed_hosts.update(_expand_host_aliases(candidate_host))

    cross_domain_hits: List[Dict[str, Any]] = []
    traces_by_name = {name: trace for name, trace in traces_for_chain}
    for chain_name, trace_item in traces_for_chain:
        final_host = (urlparse(str(trace_item.get("final_url") or "")).hostname or "").lower()
        if not final_host:
            continue
        if final_host in allowed_hosts:
            continue
        cross_domain_hits.append(
            {
                "scenario": chain_name,
                "final_host": final_host,
                "final_url": str(trace_item.get("final_url") or ""),
                "start_url": str(trace_item.get("start_url") or ""),
            }
        )

    if cross_domain_hits:
        other_apex = [item for item in cross_domain_hits if _apex_domain(item["final_host"]) != _apex_domain(base_host)]
        if other_apex:
            status_cross = "error"
            rec_cross = "Уберите неожиданные cross-domain редиректы на другой домен: оставьте редиректы внутри проекта."
        else:
            status_cross = "warning"
            rec_cross = "Проверьте межсубдоменные редиректы и убедитесь, что это целевая канонизация."
        actual_cross = "; ".join(
            [
                f"{item['scenario']}: {item['final_host']}"
                for item in cross_domain_hits[:4]
            ]
        )
        first_hit = cross_domain_hits[0]
        trace_cross = traces_by_name.get(str(first_hit.get("scenario") or ""), base_trace)
        cross_test_url = str(first_hit.get("start_url") or base_root_url)
    else:
        status_cross = "passed"
        rec_cross = ""
        actual_cross = "Неожиданных переходов на другой домен не обнаружено."
        trace_cross = base_trace
        cross_test_url = base_root_url
    scenarios.append(
        _make_scenario(
            sid=16,
            key="cross_domain_redirect",
            title="Cross-domain redirect control",
            what_checked="Переходы на неожиданные домены после редиректа",
            status=status_cross,
            expected="Редиректы остаются в пределах канонического домена проекта",
            actual=actual_cross,
            recommendation=rec_cross,
            test_url=cross_test_url,
            trace=trace_cross,
            details={"cross_domain_hits": cross_domain_hits},
        )
    )

    # 17) soft-404 after redirect
    _notify_progress(17, "soft_404_detection", "Soft-404 detection", normalized_input)
    final_404_url = str(trace_404.get("final_url") or "")
    base_norm = _normalize_for_compare(str(base_trace.get("final_url") or normalized_input))
    final_404_norm = _normalize_for_compare(final_404_url)
    if status_code_404 in (404, 410):
        status_soft404 = "passed"
        rec_soft404 = ""
    elif status_code_404 == 200 and final_404_norm == base_norm:
        status_soft404 = "error"
        rec_soft404 = "Уберите soft-404: несуществующие URL не должны отдавать 200 и вести на ту же страницу."
    elif status_code_404 == 200:
        status_soft404 = "warning"
        rec_soft404 = "Проверьте soft-404: несуществующие URL должны возвращать 404/410."
    else:
        status_soft404 = "warning"
        rec_soft404 = "Проверьте обработку несуществующих URL, чтобы исключить soft-404."
    scenarios.append(
        _make_scenario(
            sid=17,
            key="soft_404_detection",
            title="Soft-404 detection",
            what_checked="Проверка soft-404 после редиректа для несуществующих URL",
            status=status_soft404,
            expected="Несуществующий URL не выглядит как валидная 200-страница",
            actual=f"status={status_code_404 or '-'} | final={final_404_url or '-'}",
            recommendation=rec_soft404,
            test_url=url_404,
            trace=trace_404,
        )
    )

    # 18) JavaScript / Meta Refresh redirect detection
    _notify_progress(18, "js_redirect", "JavaScript / Meta Refresh Redirects", normalized_input)
    js_result = _check_js_redirect(normalized_input, use_proxy=use_proxy)
    js_status = "passed"
    js_recommendation = ""
    if js_result.get("status") == "skipped":
        js_status = "info"
        js_recommendation = js_result.get("reason", "Playwright not available")
        js_actual = "skipped"
    elif js_result.get("status") == "error":
        js_status = "info"
        js_recommendation = f"Could not check JS redirects: {js_result.get('error', 'unknown error')}"
        js_actual = "error"
    elif js_result.get("is_js_redirect"):
        js_status = "warning"
        js_recommendation = (
            f"JavaScript redirect detected: {normalized_input} -> {js_result['final_url']}. "
            "Use 301 redirect instead for SEO."
        )
        js_actual = f"JS redirect to {js_result.get('final_url', '-')}"
    elif js_result.get("meta_refresh"):
        js_status = "warning"
        js_recommendation = (
            f"Meta refresh redirect detected: {js_result['meta_refresh']}. "
            "Use 301 redirect instead."
        )
        js_actual = f"Meta refresh: {js_result['meta_refresh']}"
    elif js_result.get("js_redirect_code_detected"):
        js_status = "info"
        js_recommendation = "JavaScript redirect code found in source but not triggered during page load."
        js_actual = "JS redirect code present but not triggered"
    else:
        js_actual = "No client-side redirects detected"

    scenarios.append(
        _make_scenario(
            sid=18,
            key="js_redirect",
            title="JavaScript / Meta Refresh Redirects",
            what_checked="Client-side redirect detection via Playwright rendering",
            status=js_status,
            expected="No client-side redirects (use 301 instead)",
            actual=js_actual,
            recommendation=js_recommendation,
            test_url=normalized_input,
            duration_ms=js_result.get("elapsed_ms", 0),
            details={
                "js_result": js_result,
            },
        )
    )

    passed = sum(1 for item in scenarios if item.get("status") == "passed")
    warnings = sum(1 for item in scenarios if item.get("status") == "warning")
    errors = sum(1 for item in scenarios if item.get("status") == "error")
    quality_score = max(0, min(100, 100 - warnings * 7 - errors * 15))
    quality_grade = _status_to_grade(quality_score)

    recommendations: List[str] = []
    seen_recommendations: set[str] = set()
    for item in scenarios:
        rec = str(item.get("recommendation") or "").strip()
        if not rec:
            continue
        if item.get("status") == "passed":
            continue
        if rec in seen_recommendations:
            continue
        seen_recommendations.add(rec)
        recommendations.append(rec)

    duration_ms = int((time.perf_counter() - started) * 1000)
    return {
        "task_type": "redirect_checker",
        "url": normalized_input,
        "results": {
            "checked_url": normalized_input,
            "selected_user_agent": {
                "key": selected_key,
                "label": selected_ua["label"],
                "value": selected_ua["value"],
            },
            "applied_policy": {
                "canonical_host_policy": canonical_host_policy,
                "trailing_slash_policy": trailing_slash_policy,
                "enforce_lowercase": enforce_lowercase,
                "allowed_query_params": allowed_params,
                "required_query_params": required_params,
                "ignore_query_params": ignore_params,
                "target_host": target_host,
            },
            "summary": {
                "total_scenarios": len(scenarios),
                "passed": passed,
                "warnings": warnings,
                "errors": errors,
                "quality_score": quality_score,
                "quality_grade": quality_grade,
                "duration_ms": duration_ms,
            },
            "checks_version": "v2",
            "scenarios": scenarios,
            "recommendations": recommendations,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    }
