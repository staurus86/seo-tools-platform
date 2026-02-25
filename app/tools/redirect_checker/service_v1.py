"""Redirect checker service (11 scenarios)."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

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


def _trace_url(url: str, user_agent: str, timeout: int = 12, max_hops: int = 10) -> Dict[str, Any]:
    session = requests.Session()
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
        try:
            response = session.get(current, allow_redirects=False, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            error_text = str(exc)
            break

        final_response = response
        location_raw = str(response.headers.get("Location") or "").strip()
        location_abs = urljoin(current, location_raw) if location_raw else ""
        chain.append(
            {
                "url": current,
                "status_code": int(response.status_code),
                "location": location_abs or location_raw,
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
        "chain": trace.get("chain") or [],
        "error": str(trace.get("error") or ""),
        "details": details or {},
    }


def run_redirect_checker(
    *,
    url: str,
    user_agent_key: str = "googlebot_desktop",
    timeout: int = 12,
    max_hops: int = 10,
) -> Dict[str, Any]:
    started = time.perf_counter()

    normalized_input = _normalize_http_input(url)
    if not normalized_input:
        raise ValueError("Введите корректный URL сайта (http/https или домен).")

    selected_key = user_agent_key if user_agent_key in UA_PRESETS else "googlebot_desktop"
    selected_ua = UA_PRESETS[selected_key]
    ua_value = selected_ua["value"]

    base_trace = _trace_url(normalized_input, ua_value, timeout=timeout, max_hops=max_hops)
    canonical_from_base = str(base_trace.get("final_url") or normalized_input)
    parsed_canonical = urlparse(canonical_from_base)

    base_host = (parsed_canonical.hostname or urlparse(normalized_input).hostname or "").lower()
    base_port = parsed_canonical.port
    base_scheme = parsed_canonical.scheme if parsed_canonical.scheme in ("http", "https") else "https"
    base_netloc = _build_netloc(base_host, base_port)
    base_root_url = urlunparse((base_scheme, base_netloc, "/", "", "", ""))

    scenarios: List[Dict[str, Any]] = []
    traces_for_chain: List[Tuple[str, Dict[str, Any]]] = [("base", base_trace)]

    # 1) HTTP -> HTTPS
    http_url = urlunparse(("http", base_netloc, "/", "", "", ""))
    trace_http = _trace_url(http_url, ua_value, timeout=timeout, max_hops=max_hops)
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
    alt_host = ""
    if base_host.startswith("www."):
        alt_host = base_host[4:]
    elif base_host:
        alt_host = f"www.{base_host}"
    alt_netloc = _build_netloc(alt_host, base_port)
    alt_www_url = urlunparse((base_scheme, alt_netloc, "/", "", "", "")) if alt_netloc else ""
    trace_www = _trace_url(alt_www_url, ua_value, timeout=timeout, max_hops=max_hops) if alt_www_url else {}
    if trace_www:
        traces_for_chain.append(("www_consistency", trace_www))
    final_www_host = (urlparse(str((trace_www or {}).get("final_url") or "")).hostname or "").lower()
    www_first_code = (_trace_codes(trace_www) or [None])[0] if trace_www else None
    if not alt_www_url:
        status_www = "warning"
        rec_www = "Проверьте консистентность WWW/без WWW на основной версии домена."
        actual_www = "Не удалось сформировать альтернативный WWW-хост."
    elif trace_www.get("error"):
        status_www = "error"
        rec_www = "Проверьте доступность альтернативной WWW/без-WWW версии и настройте 301 на каноническую."
        actual_www = str(trace_www.get("error") or "request error")
    elif final_www_host == base_host and int(trace_www.get("hops") or 0) >= 1 and www_first_code in PERMANENT_REDIRECT_STATUSES:
        status_www = "passed"
        rec_www = ""
        actual_www = f"{_chain_summary(trace_www)} | Final host: {final_www_host or '-'}"
    elif final_www_host == base_host:
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
            expected="Обе версии ведут на один канонический хост",
            actual=actual_www,
            recommendation=rec_www,
            test_url=alt_www_url or "-",
            trace=trace_www if trace_www else None,
        )
    )

    # 3) Multiple slashes
    slashes_url = urlunparse((base_scheme, base_netloc, "/redirect-checker//probe//", "", "", ""))
    trace_slashes = _trace_url(slashes_url, ua_value, timeout=timeout, max_hops=max_hops)
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
    case_url = urlunparse((base_scheme, base_netloc, "/CART", "", "", ""))
    trace_case = _trace_url(case_url, ua_value, timeout=timeout, max_hops=max_hops)
    traces_for_chain.append(("url_case", trace_case))
    final_case_path = urlparse(str(trace_case.get("final_url") or "")).path or "/"
    case_first_code = (_trace_codes(trace_case) or [None])[0]
    if trace_case.get("error"):
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
            expected="Uppercase URL редиректится на lowercase",
            actual=f"{_chain_summary(trace_case)} | Final path: {final_case_path}",
            recommendation=rec_case,
            test_url=case_url,
            trace=trace_case,
        )
    )

    # 5) index files
    index_url = urlunparse((base_scheme, base_netloc, "/index.html", "", "", ""))
    trace_index = _trace_url(index_url, ua_value, timeout=timeout, max_hops=max_hops)
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
    slash_a = urlunparse((base_scheme, base_netloc, "/page", "", "", ""))
    slash_b = urlunparse((base_scheme, base_netloc, "/page/", "", "", ""))
    trace_slash_a = _trace_url(slash_a, ua_value, timeout=timeout, max_hops=max_hops)
    trace_slash_b = _trace_url(slash_b, ua_value, timeout=timeout, max_hops=max_hops)
    traces_for_chain.append(("trailing_slash_a", trace_slash_a))
    traces_for_chain.append(("trailing_slash_b", trace_slash_b))
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
        if same_target and (int(trace_slash_a.get("hops") or 0) >= 1 or int(trace_slash_b.get("hops") or 0) >= 1):
            status_trailing = "passed"
            rec_trailing = ""
        elif same_target:
            status_trailing = "warning"
            rec_trailing = "Убедитесь, что неканоничная версия trailing slash даёт 301 на каноничную."
        else:
            status_trailing = "warning"
            rec_trailing = "Сделайте единообразие /page и /page/ через один канонический вариант."
    scenarios.append(
        _make_scenario(
            sid=6,
            key="trailing_slash",
            title="Trailing slash",
            what_checked="Единообразие /page и /page/",
            status=status_trailing,
            expected="Обе версии сходятся к одному каноническому URL",
            actual=(
                f"/page: {_chain_summary(trace_slash_a)} -> {trace_slash_a.get('final_url') or '-'} | "
                f"/page/: {_chain_summary(trace_slash_b)} -> {trace_slash_b.get('final_url') or '-'}"
            ),
            recommendation=rec_trailing,
            test_url=f"{slash_a} | {slash_b}",
            response_codes=_trace_codes(trace_slash_a) + _trace_codes(trace_slash_b),
            final_url=f"{trace_slash_a.get('final_url') or '-'} | {trace_slash_b.get('final_url') or '-'}",
            hops=max(int(trace_slash_a.get("hops") or 0), int(trace_slash_b.get("hops") or 0)),
            details={"trace_a": trace_slash_a, "trace_b": trace_slash_b},
        )
    )

    # 7) old extensions
    ext_html_url = urlunparse((base_scheme, base_netloc, "/legacy-page.html", "", "", ""))
    ext_php_url = urlunparse((base_scheme, base_netloc, "/legacy-page.php", "", "", ""))
    trace_ext_html = _trace_url(ext_html_url, ua_value, timeout=timeout, max_hops=max_hops)
    trace_ext_php = _trace_url(ext_php_url, ua_value, timeout=timeout, max_hops=max_hops)
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
            details={"trace_html": trace_ext_html, "trace_php": trace_ext_php},
        )
    )

    # 8) canonical tag
    canonical_url = str(base_trace.get("canonical_url") or "").strip()
    canonical_source = str(base_trace.get("canonical_source") or "")
    canonical_host = (urlparse(canonical_url).hostname or "").lower() if canonical_url else ""
    if canonical_url and canonical_host == base_host:
        status_canonical = "passed"
        rec_canonical = ""
    elif canonical_url:
        status_canonical = "warning"
        rec_canonical = "Проверьте canonical: он должен указывать на каноническую версию этого же домена."
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
            expected="Canonical присутствует и ссылается на канонический URL",
            actual=(f"canonical={canonical_url}" if canonical_url else "canonical не найден"),
            recommendation=rec_canonical,
            test_url=str(base_trace.get("final_url") or normalized_input),
            trace=base_trace,
            details={"canonical_source": canonical_source},
        )
    )

    # 9) 404 page
    random_404 = f"/redirect-checker-404-{uuid.uuid4().hex[:10]}"
    url_404 = urlunparse((base_scheme, base_netloc, random_404, "", "", ""))
    trace_404 = _trace_url(url_404, ua_value, timeout=timeout, max_hops=max_hops)
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
    ua_rows: List[Dict[str, Any]] = []
    compare_keys = ["googlebot_desktop", "googlebot_smartphone", "yandex_bot"]
    ua_traces: Dict[str, Dict[str, Any]] = {}
    for key in compare_keys:
        trace = _trace_url(base_root_url, UA_PRESETS[key]["value"], timeout=timeout, max_hops=max_hops)
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
            "summary": {
                "total_scenarios": len(scenarios),
                "passed": passed,
                "warnings": warnings,
                "errors": errors,
                "quality_score": quality_score,
                "quality_grade": quality_grade,
                "duration_ms": duration_ms,
            },
            "scenarios": scenarios,
            "recommendations": recommendations,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    }
