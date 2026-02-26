"""
SEO Tools API Routes - Full integration with original scripts
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, UploadFile, File, Form
from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import re
import json
import time
import random
import math
import requests
from urllib.parse import urljoin, urlparse

from app.validators import URLModel, normalize_http_input as _normalize_http_input

router = APIRouter(prefix="/api", tags=["SEO Tools"])

# ─── sub-routers (extracted modules) ──────────────────────────────────────
from app.api.routers import exports as _exports_mod        # noqa: E402
from app.api.routers import tasks as _tasks_mod            # noqa: E402
from app.api.routers import redirect as _redirect_mod      # noqa: E402
from app.api.routers import site_pro as _site_pro_mod      # noqa: E402
from app.api.routers import onpage as _onpage_mod          # noqa: E402
from app.api.routers import clusterizer as _clusterizer_mod  # noqa: E402

router.include_router(_exports_mod.router)
router.include_router(_tasks_mod.router)
router.include_router(_redirect_mod.router)
router.include_router(_site_pro_mod.router)
router.include_router(_onpage_mod.router)
router.include_router(_clusterizer_mod.router)
from app.api.routers import render as _render_mod           # noqa: E402
from app.api.routers import mobile as _mobile_mod           # noqa: E402
from app.api.routers import link_profile as _link_profile_mod  # noqa: E402

router.include_router(_render_mod.router)
router.include_router(_mobile_mod.router)
router.include_router(_link_profile_mod.router)

# ─── task storage — shared utilities ──────────────────────────────────────
from app.api.routers._task_store import (  # noqa: E402
    get_redis_client,
    get_task_result,
    _save_task_payload,
    create_task_result,
    create_task_pending,
    update_task_state,
    append_task_artifact,
    delete_task_result,
    task_results_memory,
)

# ============ ORIGINAl ROBOTS AUDIT LOGIC ============
EXPECTED_BOTS = ["googlebot", "yandex", "bingbot"]

SENSITIVE_PATHS = [
    "/admin", "/wp-admin", "/administrator", "/cgi-bin", "/config",
    "/database", "/backup", "/logs", "/phpmyadmin", "/.git", "/.env",
    "/wp-config", "/include", "/tmp", "/temp", "/private", "/secret",
    "/uploads",
]

RECOMMENDATIONS = [
    "Группируйте правила по User-agent для лучшей читаемости",
    "Блокируйте служебные папки: /admin, /tmp, /backup, /.git",
    "Не блокируйте CSS и JS файлы - это мешает сканированию",
    "Используйте Crawl-delay для больших сайтов",
    "Всегда указывайте Sitemap с полным URL",
    "Удаляйте дублирующиеся правила",
    "Избегайте Disallow: / если не хотите полностью заблокировать сайт",
    "Используйте Allow для создания исключений из Disallow",
    "Проверяйте файл в Google Search Console",
    "Учитывайте различия интерпретации директив разными поисковиками",
]

# Google ignores Crawl-delay; keep default recommendations aligned with current search guidance.
RECOMMENDATIONS = [item for item in RECOMMENDATIONS if "Crawl-delay" not in item]

UNSUPPORTED_ROBOTS_DIRECTIVES = {
    "noindex",
    "nofollow",
    "index",
    "noarchive",
    "nosnippet",
    "unavailable_after",
}


class Rule:
    def __init__(self, user_agent: str, path: str, line: int):
        self.user_agent = user_agent
        self.path = path
        self.line = line


class Group:
    def __init__(self):
        self.user_agents = []
        self.disallow = []
        self.allow = []


class ParseResult:
    def __init__(self):
        self.groups = []
        self.sitemaps = []
        self.crawl_delays = {}
        self.clean_params = []
        self.hosts = []
        self.raw_lines = []
        self.syntax_errors = []
        self.warnings = []
        self.unsupported_directives = []
        # For compatibility
        self.all_disallow = []
        self.all_allow = []


from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict
import re


def fetch_robots(url: str, timeout: int = 20) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """Fetch robots.txt and return (content, status_code, error)"""
    try:
        robots_url = url.rstrip('/') + '/robots.txt'
        resp = requests.get(robots_url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        return resp.text, resp.status_code, None
    except requests.exceptions.Timeout:
        return None, None, "Timeout"
    except requests.exceptions.ConnectionError:
        return None, None, "Connection Error"
    except Exception as e:
        return None, None, str(e)


def parse_robots(text: str) -> ParseResult:
    """Parse robots.txt content - FULL original implementation"""
    lines = text.splitlines()
    result = ParseResult()
    result.raw_lines = lines
    
    current_group = None
    
    for idx, raw in enumerate(lines, start=1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            err = f"Строка {idx}: Неверный синтаксис - отсутствует ':'"
            result.syntax_errors.append({"line": idx, "error": err, "content": raw})
            result.warnings.append(err)
            continue
        
        key, value = stripped.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        
        if key == "user-agent":
            if current_group is None or current_group.user_agents:
                current_group = Group()
                result.groups.append(current_group)
            current_group.user_agents.append(value)
            
        elif key == "disallow":
            if not current_group or not current_group.user_agents:
                err = f"Строка {idx}: Disallow без предшествующего User-agent"
                result.syntax_errors.append({"line": idx, "error": err, "content": raw})
                result.warnings.append(err)
                continue
            if value and not value.startswith("/"):
                result.warnings.append(f"Строка {idx}: Путь '{value}' должен начинаться с '/'")
            for ua in current_group.user_agents:
                rule = Rule(ua, value, idx)
                current_group.disallow.append(rule)
                result.all_disallow.append(rule)
                
        elif key == "allow":
            if not current_group or not current_group.user_agents:
                err = f"Строка {idx}: Allow без предшествующего User-agent"
                result.syntax_errors.append({"line": idx, "error": err, "content": raw})
                result.warnings.append(err)
                continue
            if value and not value.startswith("/"):
                result.warnings.append(f"Строка {idx}: Путь '{value}' должен начинаться с '/'")
            for ua in current_group.user_agents:
                rule = Rule(ua, value, idx)
                current_group.allow.append(rule)
                result.all_allow.append(rule)
                
        elif key == "sitemap":
            if value and not value.startswith("http://") and not value.startswith("https://"):
                result.warnings.append(f"Строка {idx}: Sitemap должен содержать полный URL")
            result.sitemaps.append(value)
            
        elif key == "crawl-delay":
            if not current_group or not current_group.user_agents:
                err = f"Строка {idx}: Crawl-delay без предшествующего User-agent"
                result.syntax_errors.append({"line": idx, "error": err, "content": raw})
                result.warnings.append(err)
                continue
            try:
                delay = float(value)
                if delay < 0:
                    raise ValueError("negative")
                for ua in current_group.user_agents:
                    result.crawl_delays[ua] = delay
            except Exception:
                err = f"Строка {idx}: Некорректный Crawl-delay: '{value}'"
                result.syntax_errors.append({"line": idx, "error": err, "content": raw})
                result.warnings.append(err)
                continue
                
        elif key == "clean-param":
            result.clean_params.append(value)
            
        elif key == "host":
            result.hosts.append(value)

        elif key in UNSUPPORTED_ROBOTS_DIRECTIVES:
            result.unsupported_directives.append({
                "line": idx,
                "directive": key,
                "value": value,
            })
            result.warnings.append(
                f"Line {idx}: '{key}' in robots.txt is not supported by Google; use meta robots or X-Robots-Tag."
            )

        else:
            result.warnings.append(f"Строка {idx}: Неизвестная директива '{key}' - будет проигнорирована")
    
    return result


def find_duplicates(all_rules: List[Rule], label: str) -> List[str]:
    """Find duplicate rules"""
    warnings = []
    by_value: Dict[str, List[int]] = defaultdict(list)
    for rule in all_rules:
        key = f"{rule.user_agent.lower()}|{rule.path}"
        by_value[key].append(rule.line)
    for key, line_nos in by_value.items():
        if len(line_nos) > 1:
            ua, path = key.split("|", 1)
            lines = ", ".join(str(n) for n in line_nos)
            warnings.append(f"Дублирующееся правило {label} для {ua}: {path} (строки: {lines})")
    return warnings


def dedupe_keep_order(items: List[str]) -> List[str]:
    """Remove duplicates preserving original order."""
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def build_param_merge_recommendations(result: ParseResult) -> List[str]:
    """Yandex Clean-param checks and recommendations."""
    recs: List[str] = []
    clean_params = [str(v or "").strip() for v in result.clean_params if str(v or "").strip()]
    if not clean_params:
        query_disallow = 0
        for group in result.groups:
            for rule in (group.disallow or []):
                path = str(rule.path or "")
                if "?" in path or "=" in path:
                    query_disallow += 1
        if query_disallow >= 3:
            recs.append(
                "Detected many parameterized Disallow rules. For Yandex crawling optimization, "
                "consider using 'Clean-param' for non-content query params."
            )
        return recs

    auto_ignored = {
        "ysclid", "yrclid", "utm_source", "utm_medium", "utm_campaign",
        "utm_term", "utm_content", "yclid", "gclid", "fbclid",
    }

    has_yandex_group = any(
        "yandex" in str(ua or "").lower()
        for group in result.groups
        for ua in (group.user_agents or [])
    )
    if not has_yandex_group:
        recs.append(
            "Clean-param is Yandex-specific. Consider a dedicated 'User-agent: Yandex' section for these rules."
        )

    seen_rules: Dict[str, int] = defaultdict(int)
    path_to_params: Dict[str, List[str]] = defaultdict(list)

    for raw in clean_params:
        seen_rules[raw] += 1
        if len(raw) > 500:
            recs.append(
                f"Clean-param rule exceeds 500 chars and may be ignored by Yandex: '{raw[:80]}...'"
            )

        parts = raw.split(None, 1)
        params_part = parts[0].strip() if parts else ""
        path_part = parts[1].strip() if len(parts) > 1 else ""

        params = [p.strip() for p in params_part.split("&") if p.strip()]
        if not params:
            recs.append(f"Invalid Clean-param syntax: '{raw}'. Expected 'param1&param2 [path]'.")
            continue

        invalid_params = [
            p for p in params
            if ("?" in p or "=" in p or "/" in p or " " in p or "&" in p)
        ]
        if invalid_params:
            recs.append(
                f"Invalid parameter token(s) in Clean-param '{raw}': {', '.join(invalid_params)}."
            )

        if path_part:
            if not path_part.startswith("/"):
                recs.append(
                    f"Clean-param path should start with '/': '{raw}'."
                )
            if "?" in path_part or "&" in path_part:
                recs.append(
                    f"Clean-param path should be URL prefix only (no query): '{raw}'."
                )

        lower_set = {p.lower() for p in params}
        if lower_set and lower_set.issubset(auto_ignored):
            recs.append(
                f"Rule '{raw}' targets mostly tracking params that Yandex can often ignore automatically. "
                "Keep it only if duplicate URLs are still reported in Webmaster."
            )

        path_key = path_part or "*"
        path_to_params[path_key].extend(params)

    for raw, cnt in seen_rules.items():
        if cnt > 1:
            recs.append(f"Duplicate Clean-param rule repeated {cnt} times: '{raw}'.")

    for path_key, params in path_to_params.items():
        uniq = dedupe_keep_order([p for p in params if p])
        if len(uniq) >= 3:
            merged = "&".join(uniq[:8])
            if path_key == "*":
                recs.append(
                    "Potential optimization (Yandex-only, requires validation): "
                    f"if ALL these params never change page content, you may merge into one global rule: "
                    f"'Clean-param: {merged}'. Otherwise keep path-specific rules."
                )
            else:
                recs.append(
                    "Potential optimization (Yandex-only, requires validation): "
                    f"for path '{path_key}', if these params do not affect document content, "
                    f"you may merge into: 'Clean-param: {merged} {path_key}'."
                )

    return dedupe_keep_order(recs)




def _normalize_rule_path(path: str) -> str:
    value = (path or "").strip()
    if value.endswith("*") and len(value) > 1:
        value = value[:-1]
    return value


def analyze_group_and_rule_conflicts(result: ParseResult) -> Dict[str, Any]:
    warnings: List[str] = []
    details: List[Dict[str, Any]] = []
    groups_by_ua: Dict[str, int] = defaultdict(int)
    rules_by_ua: Dict[str, Dict[str, set]] = defaultdict(lambda: {"allow": set(), "disallow": set()})

    for group in result.groups:
        for ua in (group.user_agents or []):
            ua_l = (ua or "").strip().lower()
            if not ua_l:
                continue
            groups_by_ua[ua_l] += 1
            for rule in (group.allow or []):
                rules_by_ua[ua_l]["allow"].add(_normalize_rule_path(rule.path))
            for rule in (group.disallow or []):
                rules_by_ua[ua_l]["disallow"].add(_normalize_rule_path(rule.path))

    for ua, count in groups_by_ua.items():
        if count > 1:
            warnings.append(
                f"User-agent '{ua}' appears in multiple groups ({count}). "
                "Merge into one group to avoid ambiguous interpretation."
            )
            details.append({"type": "ua_fragmented_groups", "user_agent": ua, "groups": count})

    for ua, packs in rules_by_ua.items():
        conflicted = sorted((packs["allow"] & packs["disallow"]) - {""})
        for path in conflicted:
            warnings.append(
                f"Conflicting directives for '{ua}': both Allow and Disallow for '{path}'. "
                "This can lead to crawler-specific behavior differences."
            )
            details.append({"type": "allow_disallow_same_path", "user_agent": ua, "path": path})

    return {"warnings": warnings, "details": details}


def analyze_longest_match_behaviour(result: ParseResult) -> Dict[str, Any]:
    notes: List[str] = []
    details: List[Dict[str, Any]] = []
    rules_by_ua: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: {"allow": [], "disallow": []})

    for group in result.groups:
        for ua in (group.user_agents or []):
            ua_l = (ua or "").strip().lower()
            if not ua_l:
                continue
            for rule in (group.allow or []):
                path = _normalize_rule_path(rule.path)
                if path:
                    rules_by_ua[ua_l]["allow"].append(path)
            for rule in (group.disallow or []):
                path = _normalize_rule_path(rule.path)
                if path:
                    rules_by_ua[ua_l]["disallow"].append(path)

    for ua, packs in rules_by_ua.items():
        for allow_path in packs["allow"]:
            for disallow_path in packs["disallow"]:
                if disallow_path.startswith(allow_path) and len(disallow_path) > len(allow_path):
                    notes.append(
                        f"Longest-match note for '{ua}': Allow '{allow_path}' is broader than "
                        f"Disallow '{disallow_path}'. Deeper URLs may remain blocked."
                    )
                    details.append(
                        {
                            "user_agent": ua,
                            "allow_path": allow_path,
                            "disallow_path": disallow_path,
                            "type": "allow_broader_than_disallow",
                        }
                    )
        if len(notes) >= 30:
            break

    return {"notes": dedupe_keep_order(notes)[:30], "details": details[:100]}


def validate_host_directives(hosts: List[str]) -> Dict[str, Any]:
    warnings: List[str] = []
    normalized_hosts = [str(h or "").strip() for h in hosts if str(h or "").strip()]
    uniq = dedupe_keep_order(normalized_hosts)
    host_re = re.compile(r"^[a-z0-9.-]+(?::\d+)?$", re.I)

    if len(uniq) > 1:
        warnings.append(
            f"Multiple Host directives found ({len(uniq)}). "
            "Yandex expects a single canonical host value."
        )

    for host in uniq:
        if host.startswith(("http://", "https://")) or "/" in host:
            warnings.append(
                f"Host directive '{host}' looks invalid. Use host only (no scheme/path), e.g. 'example.com'."
            )
        elif not host_re.fullmatch(host):
            warnings.append(
                f"Host directive '{host}' has non-standard format for Yandex."
            )

    return {"warnings": warnings, "hosts": uniq}

def validate_sitemaps(sitemaps: List[str], timeout: int = 4, max_checks: int = 5) -> List[Dict[str, Any]]:
    """Validate sitemap URLs declared in robots.txt."""
    checks: List[Dict[str, Any]] = []
    unique_sitemaps = dedupe_keep_order([s for s in sitemaps if isinstance(s, str) and s.strip()])
    for index, sm in enumerate(unique_sitemaps):
        sm = sm.strip()
        if index >= max_checks:
            checks.append({
                "url": sm,
                "ok": None,
                "status_code": None,
                "content_type": None,
                "error": "Skipped (limit reached)"
            })
            continue
        try:
            parsed = urlparse(sm)
            if parsed.scheme not in ("http", "https"):
                checks.append({
                    "url": sm,
                    "ok": False,
                    "status_code": None,
                    "content_type": None,
                    "error": "Invalid sitemap URL scheme"
                })
                continue
            resp = requests.get(sm, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            content_type = (resp.headers.get("Content-Type") or "").lower()
            looks_xml = any(token in content_type for token in ["xml", "text/plain"]) or resp.text.lstrip().startswith("<?xml")
            ok = resp.status_code == 200 and looks_xml
            checks.append({
                "url": sm,
                "ok": ok,
                "status_code": resp.status_code,
                "content_type": content_type,
                "error": None if ok else "Sitemap not accessible or not XML"
            })
        except Exception as e:
            checks.append({
                "url": sm,
                "ok": False,
                "status_code": None,
                "content_type": None,
                "error": str(e)
            })
    return checks


def build_quality_metrics(
    issues: List[str],
    warnings: List[str],
    syntax_errors: List[Dict[str, Any]],
    missing_bots: List[str],
    sitemap_checks: List[Dict[str, Any]],
    full_block: bool,
    blocked_ext: List[str]
) -> Dict[str, Any]:
    """Build score, grade, production readiness and top fixes."""
    score = 100
    critical_count = len(issues)
    warning_count = len(warnings)
    syntax_count = len(syntax_errors)

    if full_block:
        score -= 70
    if blocked_ext:
        score -= 20
    score -= min(20, syntax_count * 2)
    score -= min(22, warning_count)
    if missing_bots:
        score -= min(8, len(missing_bots) * 2)

    has_sitemap = any(check.get("ok") for check in sitemap_checks) if sitemap_checks else False
    if sitemap_checks and not has_sitemap:
        score -= 10
    if not sitemap_checks:
        score -= 8

    score = max(0, min(100, score))
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 60:
        grade = "D"
    else:
        grade = "F"

    top_fixes: List[Dict[str, str]] = []
    if full_block:
        top_fixes.append({
            "priority": "critical",
            "title": "Уберите глобальную блокировку сайта",
            "why": "Disallow: / для * блокирует индексацию всего сайта.",
            "action": "Оставьте только точечные запреты для служебных разделов."
        })
    if blocked_ext:
        top_fixes.append({
            "priority": "high",
            "title": "Разблокируйте CSS/JS",
            "why": "Блокировка CSS/JS ухудшает рендеринг для поисковых роботов.",
            "action": "Удалите правила, блокирующие .css и .js."
        })
    if missing_bots:
        top_fixes.append({
            "priority": "medium",
            "title": "Добавьте группы для ключевых ботов",
            "why": "Явные правила для поисковых ботов делают поведение предсказуемым.",
            "action": f"Добавьте User-agent группы для: {', '.join(missing_bots)}."
        })
    if sitemap_checks and not has_sitemap:
        top_fixes.append({
            "priority": "high",
            "title": "Исправьте sitemap URL",
            "why": "Sitemap недоступен или возвращает некорректный контент.",
            "action": "Проверьте URL sitemap в robots.txt и доступность по HTTP 200."
        })
    if syntax_count:
        top_fixes.append({
            "priority": "high",
            "title": "Исправьте синтаксис robots.txt",
            "why": "Синтаксические ошибки могут приводить к игнорированию правил.",
            "action": "Исправьте строки с ошибками в блоке syntax_errors."
        })

    production_ready = score >= 75 and not full_block and not blocked_ext
    return {
        "quality_score": score,
        "quality_grade": grade,
        "production_ready": production_ready,
        "top_fixes": top_fixes[:5],
        "severity_counts": {
            "critical": critical_count,
            "warning": warning_count,
            "info": 0
        }
    }


def build_issues_and_warnings(result: ParseResult) -> Dict[str, Any]:
    """Build issues, warnings, and recommendations - FULL original implementation"""
    issues: List[str] = []
    warnings: List[str] = list(result.warnings)
    
    all_disallow: List[Rule] = []
    all_allow: List[Rule] = []
    for group in result.groups:
        all_disallow.extend(group.disallow)
        all_allow.extend(group.allow)
    
    warnings.extend(find_duplicates(all_disallow, "Disallow"))
    warnings.extend(find_duplicates(all_allow, "Allow"))

    conflict_scan = analyze_group_and_rule_conflicts(result)
    warnings.extend(conflict_scan["warnings"])

    longest_match_scan = analyze_longest_match_behaviour(result)
    warnings.extend(longest_match_scan["notes"])

    host_scan = validate_host_directives(result.hosts)
    warnings.extend(host_scan["warnings"])
    if result.crawl_delays:
        warnings.append("Crawl-delay found: Google ignores this directive.")
    
    if not result.sitemaps:
        warnings.append("Не указана директива Sitemap")
    
    # Check for full site block
    full_block = any(
        rule.user_agent.lower() == "*" and rule.path.strip() == "/"
        for rule in all_disallow
    )
    if full_block:
        issues.append("КРИТИЧНО: Весь сайт заблокирован для всех роботов (Disallow: /)")
    
    # Check blocked extensions
    blocked_ext = []
    for ext in [".css", ".js"]:
        if any(ext in rule.path for rule in all_disallow):
            blocked_ext.append(ext)
    if blocked_ext:
        issues.append(
            "Заблокированы важные ресурсы: "
            + ", ".join(blocked_ext)
            + " - это мешает сканированию"
        )
    
    # Check expected bots
    present_agents = set()
    for group in result.groups:
        for ua in group.user_agents:
            present_agents.add(ua.lower())
    
    missing_bots = [
        bot for bot in EXPECTED_BOTS if not any(bot in ua for ua in present_agents)
    ]
    if missing_bots:
        warnings.append("Рекомендуется добавить правила для: " + ", ".join(missing_bots))
    
    # Check sensitive paths
    unblocked_sensitive = []
    blocked_paths = set(rule.path for rule in all_disallow)
    for path in SENSITIVE_PATHS:
        if path not in blocked_paths:
            unblocked_sensitive.append(path)
    if unblocked_sensitive:
        warnings.append(
            "Рекомендуется заблокировать: "
            + ", ".join(unblocked_sensitive[:8])
        )
    
    # Generate recommendations and quality metrics
    if result.unsupported_directives:
        warnings.append(
            "Unsupported directives found in robots.txt (e.g., noindex/nofollow). "
            "Use meta robots or X-Robots-Tag for indexation control."
        )
    param_recs = build_param_merge_recommendations(result)
    all_recommendations = dedupe_keep_order(RECOMMENDATIONS.copy() + param_recs)
    warnings = dedupe_keep_order(warnings)
    issues = dedupe_keep_order(issues)

    sitemap_checks = validate_sitemaps(result.sitemaps)
    metrics = build_quality_metrics(
        issues=issues,
        warnings=warnings,
        syntax_errors=result.syntax_errors,
        missing_bots=missing_bots,
        sitemap_checks=sitemap_checks,
        full_block=full_block,
        blocked_ext=blocked_ext
    )
    info_issues = [
        f"Обнаружено групп правил: {len(result.groups)}",
        f"Проверено sitemap URL: {len(sitemap_checks)}"
    ]
    if metrics["production_ready"]:
        info_issues.append("Robots.txt готов к продакшн-использованию.")
    else:
        info_issues.append("Требуются правки перед продакшн-использованием.")
    
    return {
        "issues": issues,
        "critical_issues": issues,
        "warnings": warnings,
        "warning_issues": warnings,
        "info_issues": info_issues,
        "recommendations": all_recommendations,
        "param_recommendations": param_recs,
        "present_agents": list(present_agents),
        "missing_bots": missing_bots,
        "sitemaps": result.sitemaps,
        "sitemap_checks": sitemap_checks,
        "crawl_delays": result.crawl_delays,
        "hosts": result.hosts,
        "host_validation": host_scan,
        "directive_conflicts": conflict_scan,
        "longest_match_analysis": longest_match_scan,
        "unsupported_directives": result.unsupported_directives,
        "syntax_errors": result.syntax_errors,
        "quality_score": metrics["quality_score"],
        "quality_grade": metrics["quality_grade"],
        "production_ready": metrics["production_ready"],
        "top_fixes": metrics["top_fixes"],
        "severity_counts": {
            "critical": metrics["severity_counts"]["critical"],
            "warning": metrics["severity_counts"]["warning"],
            "info": len(info_issues)
        },
        "quick_status": "pass" if metrics["production_ready"] else ("fail" if metrics["quality_score"] < 60 else "warn")
    }


def collect_stats(result: ParseResult) -> Dict[str, int]:
    """Collect statistics from parsed result"""
    stats = {
        "user_agents": 0,
        "disallow_rules": 0,
        "allow_rules": 0,
        "sitemaps": len(result.sitemaps),
        "crawl_delays": len(result.crawl_delays),
        "clean_params": len(result.clean_params),
        "hosts": len(result.hosts),
        "lines_count": len(result.raw_lines),
    }
    for group in result.groups:
        stats["user_agents"] += len(group.user_agents)
        stats["disallow_rules"] += len(group.disallow)
        stats["allow_rules"] += len(group.allow)
    return stats


def check_robots_full(url: str) -> Dict[str, Any]:
    """
    FULL robots.txt audit - полная интеграция оригинального скрипта
    Returns complete analysis matching original robots_audit.py
    """
    print(f"[ROBOTS] Starting full audit for: {url}")
    
    raw_text, status_code, error = fetch_robots(url)
    
    if error:
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": False,
                "status_code": None,
                "content_length": 0,
                "lines_count": 0,
                "user_agents": 0,
                "disallow_rules": 0,
                "allow_rules": 0,
                "sitemaps": [],
                "issues": [f"Ошибка загрузки: {error}"],
                "warnings": [],
                "recommendations": RECOMMENDATIONS,
                "syntax_errors": [],
                "critical_issues": [f"Ошибка загрузки: {error}"],
                "warning_issues": [],
                "info_issues": [],
                "hosts": [],
                "sitemap_checks": [],
                "quality_score": 0,
                "quality_grade": "F",
                "production_ready": False,
                "top_fixes": [{
                    "priority": "critical",
                    "title": "Обеспечьте доступность robots.txt",
                    "why": "Файл robots.txt недоступен, анализ и управление индексацией невозможны.",
                    "action": "Проверьте DNS/SSL/доступность сайта и путь /robots.txt."
                }],
                "severity_counts": {"critical": 1, "warning": 0, "info": 0},
                "error": error,
                "can_continue": False,
                "raw_content": "",
            }
        }
    
    if status_code != 200:
        http_notes: List[str] = []
        issues: List[str] = []
        warnings: List[str] = []
        top_fixes: List[Dict[str, str]] = []
        severity_counts = {"critical": 0, "warning": 0, "info": 0}
        quality_score = 35
        quality_grade = "F"

        if status_code in (404, 410):
            warnings.append(
                f"Robots.txt returns HTTP {status_code}. Google treats this as no robots restrictions (except 429 case)."
            )
            http_notes.append("Google: 4xx (except 429) is processed like missing robots.txt.")
            http_notes.append("Yandex: robots rules are unavailable for reading until file is restored.")
            top_fixes.append({
                "priority": "medium",
                "title": "Create /robots.txt",
                "why": "Search bots cannot read explicit crawl/indexing rules from your domain.",
                "action": "Publish /robots.txt and include at least User-agent and Sitemap directives."
            })
            severity_counts["warning"] = 1
            quality_score = 55
            quality_grade = "D"
        elif status_code == 429:
            issues.append("Robots.txt returns HTTP 429 (Too Many Requests). Bots may postpone crawling.")
            warnings.append("429 for robots.txt can delay crawling and make rules temporarily unavailable.")
            http_notes.append("Google: 429 is not treated as normal 4xx missing-file behavior.")
            top_fixes.append({
                "priority": "high",
                "title": "Stabilize robots.txt availability",
                "why": "Rate limiting blocks crawler access to robots directives.",
                "action": "Allow reliable access to /robots.txt without aggressive rate limits."
            })
            severity_counts["critical"] = 1
            severity_counts["warning"] = 1
            quality_score = 25
            quality_grade = "F"
        elif status_code in (401, 403):
            issues.append(f"Robots.txt returns HTTP {status_code}. Access to rules is restricted.")
            warnings.append("Bots may apply fallback behavior when robots.txt cannot be read.")
            http_notes.append("Google: 4xx (except 429) is generally treated as no robots file.")
            http_notes.append("Yandex: inaccessible robots.txt can affect predictable crawl control.")
            top_fixes.append({
                "priority": "high",
                "title": "Open access to /robots.txt",
                "why": "Crawler cannot read crawl policy due to authorization/forbidden response.",
                "action": "Return HTTP 200 for public /robots.txt and remove auth blocks."
            })
            severity_counts["critical"] = 1
            severity_counts["warning"] = 1
            quality_score = 30
            quality_grade = "F"
        elif 500 <= status_code < 600:
            issues.append(f"Robots.txt returns server error HTTP {status_code}.")
            warnings.append("Server errors on robots.txt can pause or destabilize crawler behavior.")
            http_notes.append("Google: on 5xx, crawling may pause; cached robots may be reused for a limited period.")
            http_notes.append("Yandex: unavailable robots.txt reduces crawl predictability until recovered.")
            top_fixes.append({
                "priority": "critical",
                "title": "Fix server errors on /robots.txt",
                "why": "Search bots cannot reliably fetch robots directives.",
                "action": "Return stable HTTP 200 and monitor uptime for /robots.txt."
            })
            severity_counts["critical"] = 1
            severity_counts["warning"] = 1
            quality_score = 20
            quality_grade = "F"
        elif 300 <= status_code < 400:
            warnings.append(f"Robots.txt returns redirect HTTP {status_code}.")
            http_notes.append("Keep redirects short and stable; long redirect chains may break robots fetch.")
            top_fixes.append({
                "priority": "medium",
                "title": "Serve robots.txt directly",
                "why": "Redirect chains can prevent some crawlers from reaching final robots content.",
                "action": "Return HTTP 200 on canonical /robots.txt URL."
            })
            severity_counts["warning"] = 1
            quality_score = 45
            quality_grade = "E"
        else:
            issues.append(f"Robots.txt is unavailable with HTTP {status_code}.")
            top_fixes.append({
                "priority": "high",
                "title": "Restore robots.txt availability",
                "why": "Search bots cannot consume expected crawl rules.",
                "action": "Ensure /robots.txt returns HTTP 200 with valid directives."
            })
            severity_counts["critical"] = 1
            quality_score = 30
            quality_grade = "F"

        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": False,
                "status_code": status_code,
                "content_length": len(raw_text) if raw_text else 0,
                "lines_count": len(raw_text.splitlines()) if raw_text else 0,
                "user_agents": 0,
                "disallow_rules": 0,
                "allow_rules": 0,
                "sitemaps": [],
                "issues": issues,
                "warnings": warnings,
                "recommendations": RECOMMENDATIONS,
                "syntax_errors": [],
                "critical_issues": issues,
                "warning_issues": warnings,
                "info_issues": http_notes,
                "hosts": [],
                "sitemap_checks": [],
                "quality_score": quality_score,
                "quality_grade": quality_grade,
                "production_ready": False,
                "top_fixes": top_fixes,
                "severity_counts": severity_counts,
                "error": None,
                "can_continue": True,
                "raw_content": raw_text or "",
                "http_status_analysis": {
                    "status_code": status_code,
                    "notes": http_notes
                }
            }
        }

    # Full parsing and analysis
    result = parse_robots(raw_text)
    stats = collect_stats(result)
    analysis = build_issues_and_warnings(result)
    if len(raw_text.encode("utf-8")) > 512000:
        analysis["warnings"] = dedupe_keep_order(
            analysis["warnings"] + ["robots.txt is larger than 500 KiB; Google ignores content after this limit."]
        )
        analysis["warning_issues"] = analysis["warnings"]
    
    # Build detailed response
    response = {
        "task_type": "robots_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "robots_txt_found": True,
            "status_code": status_code,
            "content_length": len(raw_text),
            "lines_count": stats["lines_count"],
            "user_agents": stats["user_agents"],
            "disallow_rules": stats["disallow_rules"],
            "allow_rules": stats["allow_rules"],
            "sitemaps": analysis["sitemaps"],
            "issues": analysis["issues"],
            "critical_issues": analysis.get("critical_issues", analysis["issues"]),
            "warnings": analysis["warnings"],
            "warning_issues": analysis.get("warning_issues", analysis["warnings"]),
            "info_issues": analysis.get("info_issues", []),
            "recommendations": analysis["recommendations"],
            "syntax_errors": analysis["syntax_errors"],
            "hosts": analysis["hosts"],
            "crawl_delays": analysis["crawl_delays"],
            "clean_params": result.clean_params,
            "param_recommendations": analysis.get("param_recommendations", []),
            "present_agents": analysis["present_agents"],
            "missing_bots": analysis.get("missing_bots", []),
            "sitemap_checks": analysis.get("sitemap_checks", []),
            "quality_score": analysis.get("quality_score", 0),
            "quality_grade": analysis.get("quality_grade", "F"),
            "production_ready": analysis.get("production_ready", False),
            "top_fixes": analysis.get("top_fixes", []),
            "severity_counts": analysis.get("severity_counts", {"critical": len(analysis["issues"]), "warning": len(analysis["warnings"]), "info": 0}),
            "quick_status": analysis.get("quick_status", "warn"),
            "machine_summary": {
                "user_agents_count": stats["user_agents"],
                "disallow_count": stats["disallow_rules"],
                "allow_count": stats["allow_rules"],
                "sitemap_count": len(analysis.get("sitemaps", [])),
                "critical_count": len(analysis.get("critical_issues", analysis["issues"])),
                "warning_count": len(analysis.get("warning_issues", analysis["warnings"])),
                "score": analysis.get("quality_score", 0),
                "grade": analysis.get("quality_grade", "F"),
                "production_ready": analysis.get("production_ready", False)
            },
            "error": None,
            "can_continue": True,
            "raw_content": raw_text,
            # Detailed groups for UI
            "groups_detail": [
                {
                    "user_agents": group.user_agents,
                    "disallow": [{"path": r.path, "line": r.line} for r in group.disallow],
                    "allow": [{"path": r.path, "line": r.line} for r in group.allow],
                }
                for group in result.groups
            ],
        }
    }
    
    print(f"[ROBOTS] Audit completed: {stats['user_agents']} UAs, {stats['disallow_rules']} disallow rules")
    
    return response


def check_robots_simple(url: str) -> Dict[str, Any]:
    """Simplified robots.txt analysis (legacy fallback)."""
    import requests
    
    robots_url = url.rstrip('/') + '/robots.txt'
    
    try:
        response = requests.get(robots_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        raw_text = response.text
        
        result = parse_robots(raw_text)
        analysis = build_issues_and_warnings(result)
        
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": response.status_code == 200,
                "status_code": response.status_code,
                "content_length": len(raw_text),
                "lines_count": len(raw_text.splitlines()),
                "user_agents": len(analysis["present_agents"]),
                "disallow_rules": len(result.all_disallow),
                "allow_rules": len(result.all_allow),
                "sitemaps": analysis["sitemaps"],
                "issues": analysis["issues"],
                "critical_issues": analysis.get("critical_issues", analysis["issues"]),
                "warnings": analysis["warnings"],
                "warning_issues": analysis.get("warning_issues", analysis["warnings"]),
                "info_issues": analysis.get("info_issues", []),
                "recommendations": analysis["recommendations"],
                "syntax_errors": analysis["syntax_errors"],
                "hosts": analysis["hosts"],
                "sitemap_checks": analysis.get("sitemap_checks", []),
                "quality_score": analysis.get("quality_score", 0),
                "quality_grade": analysis.get("quality_grade", "F"),
                "production_ready": analysis.get("production_ready", False),
                "top_fixes": analysis.get("top_fixes", []),
                "severity_counts": analysis.get("severity_counts", {"critical": len(analysis["issues"]), "warning": len(analysis["warnings"]), "info": 0}),
                "quick_status": analysis.get("quick_status", "warn"),
                "raw_content": raw_text,
            }
        }
    except Exception as e:
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e),
                "robots_txt_found": False,
                "quality_score": 0,
                "quality_grade": "F",
                "production_ready": False,
                "critical_issues": [str(e)],
                "warning_issues": [],
                "info_issues": [],
                "top_fixes": [],
                "severity_counts": {"critical": 1, "warning": 0, "info": 0}
            }
        }


def check_sitemap_full(url: Union[str, List[str]]) -> Dict[str, Any]:
    """Full sitemap validation with sitemap index traversal and URL export."""
    import xml.etree.ElementTree as ET
    from app.config import settings

    def local_name(tag: str) -> str:
        if not tag:
            return ""
        return tag.split("}", 1)[1] if "}" in tag else tag

    def find_child_text(node: ET.Element, child_name: str) -> str:
        for child in list(node):
            if local_name(child.tag).lower() == child_name.lower():
                return (child.text or "").strip()
        return ""

    def find_children(node: ET.Element, child_name: str) -> List[ET.Element]:
        out: List[ET.Element] = []
        for child in list(node):
            if local_name(child.tag).lower() == child_name.lower():
                out.append(child)
        return out

    def is_http_url(value: str) -> bool:
        try:
            v = str(value or "").strip()
            # Guard against broken concatenated values like "...xmlhttps://...".
            if not v or any(ch in v for ch in [" ", "\n", "\r", "\t"]):
                return False
            if (v.count("http://") + v.count("https://")) > 1:
                return False
            p = urlparse(v)
            return p.scheme in ("http", "https") and bool(p.netloc)
        except Exception:
            return False

    def is_valid_lastmod(value: str) -> bool:
        if not value:
            return True
        date_only = re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
        dt_utc = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value)
        dt_tz = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\+|-)\d{2}:\d{2}", value)
        dt_frac_utc = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z", value)
        dt_frac_tz = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(?:\+|-)\d{2}:\d{2}", value)
        return bool(date_only or dt_utc or dt_tz or dt_frac_utc or dt_frac_tz)

    def parse_lastmod_dt(value: str) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                return dt.astimezone().replace(tzinfo=None)
            return dt
        except Exception:
            try:
                return datetime.strptime(raw, "%Y-%m-%d")
            except Exception:
                return None

    def is_valid_hreflang_code(value: str) -> bool:
        v = str(value or "").strip().lower()
        if not v:
            return False
        if v == "x-default":
            return True
        return bool(re.fullmatch(r"[a-z]{2,3}(?:-[a-z0-9]{2,8})*", v))

    def sample_spread(items: List[str], size: int) -> List[str]:
        if size <= 0 or not items:
            return []
        if len(items) <= size:
            return items
        if size == 1:
            return [items[0]]
        step = (len(items) - 1) / float(size - 1)
        picks = sorted({int(round(i * step)) for i in range(size)})
        return [items[idx] for idx in picks if 0 <= idx < len(items)]

    def build_issue(severity: str, code: str, title: str, details: str, action: str, owner: str = "SEO") -> Dict[str, Any]:
        return {
            "severity": severity,
            "code": code,
            "title": title,
            "details": details,
            "action": action,
            "owner": owner,
        }

    max_sitemaps = max(10, min(2000, int(getattr(settings, "SITEMAP_MAX_FILES", 500) or 500)))
    max_export_urls = max(1000, int(getattr(settings, "SITEMAP_MAX_EXPORT_URLS", 100000) or 100000))
    export_chunk_size = 25000
    max_urls_preview_per_sitemap = 2000
    max_file_size = 52428800
    max_urls_per_sitemap = 50000
    stale_days = max(30, min(3650, int(getattr(settings, "SITEMAP_STALE_DAYS", 180) or 180)))
    live_check_sample_size = max(10, min(20, int(getattr(settings, "SITEMAP_LIVE_CHECK_SAMPLE", 15) or 15)))
    live_check_timeout = max(2, min(15, int(getattr(settings, "SITEMAP_LIVE_CHECK_TIMEOUT", 6) or 6)))
    root_urls: List[str]
    if isinstance(url, list):
        root_urls = [str(u).strip() for u in url if str(u).strip()]
    else:
        root_urls = [str(url).strip()] if str(url).strip() else []
    root_urls = list(dict.fromkeys(root_urls))
    primary_root_url = root_urls[0] if root_urls else ""
    queue: List[str] = list(root_urls)
    visited: set = set()
    sitemap_files: List[Dict[str, Any]] = []
    all_urls: List[str] = []
    seen_urls: set = set()
    url_first_seen_in: Dict[str, str] = {}
    duplicate_urls_count = 0
    duplicate_details: List[Dict[str, str]] = []
    duplicate_details_truncated = False
    max_duplicate_details = 500
    invalid_urls_count = 0
    invalid_lastmod_count = 0
    invalid_changefreq_count = 0
    invalid_priority_count = 0
    # Freshness metrics
    lastmod_present_count = 0
    lastmod_missing_count = 0
    lastmod_future_count = 0
    stale_lastmod_count = 0
    uniform_lastmod_files = 0
    # Hreflang metrics
    hreflang_links_count = 0
    hreflang_urls_count = 0
    hreflang_invalid_code_count = 0
    hreflang_invalid_href_count = 0
    hreflang_duplicate_lang_count = 0
    hreflang_has_x_default = False
    # Media extensions metrics
    image_tags_count = 0
    image_missing_loc_count = 0
    video_tags_count = 0
    video_missing_required_count = 0
    news_tags_count = 0
    news_missing_required_count = 0
    # Structure metrics
    repeated_child_refs = 0
    self_child_refs = 0
    max_depth_seen = 0
    warnings: List[str] = []
    errors: List[str] = []
    tool_notes: List[str] = []
    allowed_changefreq = {"always", "hourly", "daily", "weekly", "monthly", "yearly", "never"}
    root_status_code = None
    now_utc = datetime.utcnow()

    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        while queue and len(visited) < max_sitemaps:
            sitemap_url = queue.pop(0).strip()
            if not sitemap_url or sitemap_url in visited:
                continue
            visited.add(sitemap_url)
            parsed_depth = max(0, str(urlparse(sitemap_url).path or "").count("/") - 1)
            max_depth_seen = max(max_depth_seen, parsed_depth)

            file_report: Dict[str, Any] = {
                "sitemap_url": sitemap_url,
                "ok": False,
                "status_code": None,
                "type": "unknown",
                "size_bytes": 0,
                "urls_count": 0,
                "duplicate_count": 0,
                "duplicate_urls": [],
                "urls_omitted": 0,
                "errors": [],
                "warnings": [],
                "tool_notes": [],
                "urls": [],
            }

            try:
                response = session.get(sitemap_url, timeout=20, allow_redirects=True)
                file_report["status_code"] = response.status_code
                file_report["size_bytes"] = len(response.content or b"")
                if root_status_code is None:
                    root_status_code = response.status_code

                if response.status_code != 200:
                    file_report["errors"].append(f"HTTP {response.status_code}")
                    sitemap_files.append(file_report)
                    continue

                if file_report["size_bytes"] > max_file_size:
                    file_report["warnings"].append("Размер файла превышает 50 МиБ.")

                try:
                    root = ET.fromstring(response.content)
                except ET.ParseError as parse_error:
                    file_report["errors"].append(f"Ошибка парсинга XML: {parse_error}")
                    sitemap_files.append(file_report)
                    continue

                root_tag = local_name(root.tag).lower()
                file_report["type"] = root_tag

                if root_tag == "sitemapindex":
                    child_count = 0
                    for sm_node in root.iter():
                        if local_name(sm_node.tag).lower() != "sitemap":
                            continue
                        loc = find_child_text(sm_node, "loc")
                        if not loc:
                            file_report["warnings"].append("В sitemap-индексе найден элемент без <loc>.")
                            continue
                        if not is_http_url(loc):
                            file_report["warnings"].append(f"Некорректный URL дочернего sitemap: {loc}")
                            continue
                        if loc == sitemap_url:
                            self_child_refs += 1
                            file_report["warnings"].append(f"Самоссылка в sitemap-индексе: {loc}")
                            continue
                        child_count += 1
                        if loc in visited or loc in queue:
                            repeated_child_refs += 1
                            file_report["warnings"].append(f"Дочерний sitemap указан несколько раз: {loc}")
                            continue
                        if (len(visited) + len(queue) < max_sitemaps):
                            queue.append(loc)
                    if child_count == 0:
                        file_report["warnings"].append("Sitemap-индекс не содержит дочерних sitemap.")
                    file_report["ok"] = len(file_report["errors"]) == 0

                elif root_tag == "urlset":
                    file_urls: List[str] = []
                    file_duplicate_urls: List[str] = []
                    file_duplicate_occurrences = 0
                    file_lastmods: List[str] = []
                    file_invalid_lastmod_count = 0
                    file_invalid_changefreq_count = 0
                    file_invalid_priority_count = 0
                    file_future_lastmod_count = 0
                    file_stale_lastmod_count = 0
                    file_invalid_lastmod_examples: List[str] = []
                    file_future_lastmod_examples: List[str] = []
                    file_stale_lastmod_examples: List[str] = []
                    file_lastmod_url_samples: Dict[str, List[str]] = {}
                    for url_node in root.iter():
                        if local_name(url_node.tag).lower() != "url":
                            continue
                        loc = find_child_text(url_node, "loc")
                        if not loc:
                            file_report["warnings"].append("В urlset найден элемент без <loc>.")
                            continue
                        if not is_http_url(loc):
                            invalid_urls_count += 1
                            file_report["warnings"].append(f"Некорректный URL в <loc>: {loc}")
                            continue

                        lastmod = find_child_text(url_node, "lastmod")
                        if lastmod:
                            parsed_lastmod = parse_lastmod_dt(lastmod)
                            if not is_valid_lastmod(lastmod) or parsed_lastmod is None:
                                invalid_lastmod_count += 1
                                file_invalid_lastmod_count += 1
                                if len(file_invalid_lastmod_examples) < 5:
                                    file_invalid_lastmod_examples.append(loc)
                            else:
                                lastmod_present_count += 1
                                lastmod_iso_date = parsed_lastmod.date().isoformat()
                                file_lastmods.append(lastmod_iso_date)
                                bucket = file_lastmod_url_samples.setdefault(lastmod_iso_date, [])
                                if len(bucket) < 3:
                                    bucket.append(loc)
                                if parsed_lastmod > now_utc:
                                    lastmod_future_count += 1
                                    file_future_lastmod_count += 1
                                    if len(file_future_lastmod_examples) < 5:
                                        file_future_lastmod_examples.append(loc)
                                if (now_utc - parsed_lastmod).days > stale_days:
                                    stale_lastmod_count += 1
                                    file_stale_lastmod_count += 1
                                    if len(file_stale_lastmod_examples) < 5:
                                        file_stale_lastmod_examples.append(loc)
                        else:
                            lastmod_missing_count += 1

                        changefreq = find_child_text(url_node, "changefreq").lower()
                        if changefreq and changefreq not in allowed_changefreq:
                            invalid_changefreq_count += 1
                            file_invalid_changefreq_count += 1

                        priority_raw = find_child_text(url_node, "priority")
                        if priority_raw:
                            try:
                                priority_value = float(priority_raw)
                                if priority_value < 0 or priority_value > 1:
                                    invalid_priority_count += 1
                                    file_invalid_priority_count += 1
                            except Exception:
                                invalid_priority_count += 1
                                file_invalid_priority_count += 1

                        # Minimal hreflang validation in sitemap (only when present)
                        local_hreflang_seen = set()
                        local_hreflang_count = 0
                        for child in list(url_node):
                            if local_name(child.tag).lower() != "link":
                                continue
                            rel = str(child.attrib.get("rel", "")).strip().lower()
                            href = str(child.attrib.get("href", "")).strip()
                            hreflang = str(child.attrib.get("hreflang", "")).strip().lower()
                            if rel != "alternate" or not (href or hreflang):
                                continue
                            hreflang_links_count += 1
                            local_hreflang_count += 1
                            if hreflang == "x-default":
                                hreflang_has_x_default = True
                            if not is_valid_hreflang_code(hreflang):
                                hreflang_invalid_code_count += 1
                            if not href or not is_http_url(href):
                                hreflang_invalid_href_count += 1
                            if hreflang in local_hreflang_seen:
                                hreflang_duplicate_lang_count += 1
                            local_hreflang_seen.add(hreflang)
                        if local_hreflang_count > 0:
                            hreflang_urls_count += 1

                        # Media extensions (minimal validation)
                        image_nodes = find_children(url_node, "image")
                        image_tags_count += len(image_nodes)
                        for image_node in image_nodes:
                            image_loc = find_child_text(image_node, "loc")
                            if not image_loc or not is_http_url(image_loc):
                                image_missing_loc_count += 1

                        video_nodes = find_children(url_node, "video")
                        video_tags_count += len(video_nodes)
                        for video_node in video_nodes:
                            has_thumb = bool(find_child_text(video_node, "thumbnail_loc"))
                            has_title = bool(find_child_text(video_node, "title"))
                            has_desc = bool(find_child_text(video_node, "description"))
                            has_content = bool(find_child_text(video_node, "content_loc") or find_child_text(video_node, "player_loc"))
                            if not (has_thumb and has_title and has_desc and has_content):
                                video_missing_required_count += 1

                        news_nodes = find_children(url_node, "news")
                        news_tags_count += len(news_nodes)
                        for news_node in news_nodes:
                            if not find_child_text(news_node, "publication_date") or not find_child_text(news_node, "title"):
                                news_missing_required_count += 1

                        file_urls.append(loc)
                        if loc in seen_urls:
                            duplicate_urls_count += 1
                            file_duplicate_occurrences += 1
                            file_duplicate_urls.append(loc)
                            first_sitemap = url_first_seen_in.get(loc, "")
                            if len(duplicate_details) < max_duplicate_details:
                                duplicate_details.append({
                                    "url": loc,
                                    "first_sitemap": first_sitemap,
                                    "duplicate_sitemap": sitemap_url
                                })
                            else:
                                duplicate_details_truncated = True
                        else:
                            seen_urls.add(loc)
                            url_first_seen_in[loc] = sitemap_url
                            if len(all_urls) < max_export_urls:
                                all_urls.append(loc)

                    file_report["urls_count"] = len(file_urls)
                    file_report["urls"] = file_urls[:max_urls_preview_per_sitemap]
                    file_report["urls_omitted"] = max(0, len(file_urls) - max_urls_preview_per_sitemap)
                    file_report["duplicate_count"] = file_duplicate_occurrences
                    file_report["duplicate_urls"] = dedupe_keep_order(file_duplicate_urls)[:200]
                    if len(file_urls) > max_urls_per_sitemap:
                        file_report["warnings"].append("В одном sitemap-файле более 50 000 URL.")
                    if file_report["urls_omitted"] > 0:
                        file_report["tool_notes"].append(
                            f"Превью URL ограничено для UI/API: скрыто {file_report['urls_omitted']} URL; полный подсчет и валидация выполнены."
                        )
                    if file_invalid_lastmod_count > 0:
                        file_report["warnings"].append(
                            f"Некорректные значения <lastmod>: {file_invalid_lastmod_count}. Примеры: {' | '.join(file_invalid_lastmod_examples[:5])}"
                        )
                    if file_invalid_changefreq_count > 0:
                        file_report["warnings"].append(f"Некорректные значения <changefreq>: {file_invalid_changefreq_count}.")
                    if file_invalid_priority_count > 0:
                        file_report["warnings"].append(f"Некорректные значения <priority>: {file_invalid_priority_count}.")
                    if file_future_lastmod_count > 0:
                        file_report["warnings"].append(
                            f"Будущие значения <lastmod>: {file_future_lastmod_count}. Примеры: {' | '.join(file_future_lastmod_examples[:5])}"
                        )
                    if file_stale_lastmod_count > 0:
                        file_report["warnings"].append(
                            f"Устаревшие значения <lastmod> (> {stale_days} дней): {file_stale_lastmod_count}. Примеры: {' | '.join(file_stale_lastmod_examples[:5])}"
                        )
                    if len(file_lastmods) >= 20:
                        histogram: Dict[str, int] = {}
                        for d in file_lastmods:
                            histogram[d] = histogram.get(d, 0) + 1
                        dominant = max(histogram.values()) if histogram else 0
                        if dominant / max(1, len(file_lastmods)) >= 0.9:
                            uniform_lastmod_files += 1
                            dominant_value = max(histogram, key=histogram.get) if histogram else ""
                            dominant_ratio = round((dominant / max(1, len(file_lastmods))) * 100, 2)
                            dominant_examples = file_lastmod_url_samples.get(dominant_value, [])[:3]
                            file_report["lastmod_uniformity"] = {
                                "dominant_date": dominant_value,
                                "dominant_count": dominant,
                                "total_with_lastmod": len(file_lastmods),
                                "dominant_ratio_pct": dominant_ratio,
                                "sample_urls": dominant_examples,
                            }
                            file_report["warnings"].append(
                                f"Подозрительно однотипные lastmod: {dominant}/{len(file_lastmods)} ({dominant_ratio}%) = {dominant_value}. Примеры: {' | '.join(dominant_examples)}"
                            )
                    file_report["ok"] = len(file_report["errors"]) == 0

                else:
                    file_report["errors"].append(f"Неподдерживаемый корневой XML-тег: {root_tag}")

                sitemap_files.append(file_report)

            except Exception as fetch_error:
                file_report["errors"].append(str(fetch_error))
                sitemap_files.append(file_report)

        if queue:
            tool_notes.append(f"Достигнут лимит обхода sitemap: {max_sitemaps} файлов (осталось в очереди: {len(queue)}).")

        errors.extend([f"{item['sitemap_url']}: {err}" for item in sitemap_files for err in item.get("errors", [])])
        warnings.extend([f"{item['sitemap_url']}: {warn}" for item in sitemap_files for warn in item.get("warnings", [])])

        if duplicate_details_truncated:
            tool_notes.append(f"Список дублей сокращен до {max_duplicate_details} записей.")

        valid_files = sum(1 for item in sitemap_files if item.get("ok"))
        total_urls_discovered = sum(item.get("urls_count", 0) for item in sitemap_files if item.get("type") == "urlset")
        urls_export_truncated = total_urls_discovered > len(all_urls)
        export_parts_count = (len(all_urls) + export_chunk_size - 1) // export_chunk_size if all_urls else 0

        # Lightweight live check (sampled 10..20 URLs only)
        live_indexability_checks: List[Dict[str, Any]] = []
        live_non_indexable_count = 0
        live_check_errors_count = 0
        sampled_urls = random.sample(list(seen_urls), min(live_check_sample_size, len(seen_urls))) if seen_urls else []
        canonical_checked_count = 0
        canonical_missing_count = 0
        canonical_invalid_count = 0
        canonical_non_self_count = 0
        if sampled_urls:
            live_session = requests.Session()
            live_session.headers.update({"User-Agent": "Mozilla/5.0"})
            for sample_url in sampled_urls:
                item = {
                    "url": sample_url,
                    "status_code": None,
                    "indexable": None,
                    "reasons": [],
                    "response_ms": None,
                    "canonical_status": "n/a",
                    "canonical_url": "",
                }
                started = time.time()
                try:
                    live_response = live_session.get(sample_url, timeout=live_check_timeout, allow_redirects=True)
                    item["status_code"] = live_response.status_code
                    item["response_ms"] = int((time.time() - started) * 1000)
                    reasons: List[str] = []
                    if live_response.status_code >= 400:
                        reasons.append(f"HTTP {live_response.status_code}")
                    x_robots = str(live_response.headers.get("X-Robots-Tag", "") or "")
                    if x_robots and re.search(r"\b(noindex|none)\b", x_robots, flags=re.IGNORECASE):
                        reasons.append(f"X-Robots-Tag: {x_robots}")
                    content_type = str(live_response.headers.get("Content-Type", "") or "").lower()
                    if "html" in content_type:
                        try:
                            body = live_response.text[:200000]
                        except Exception:
                            body = ""
                        if body:
                            m = re.search(r'<meta[^>]+name=["\']robots["\'][^>]*content=["\']([^"\']+)["\']', body, flags=re.IGNORECASE)
                            if m and re.search(r"\b(noindex|none)\b", m.group(1), flags=re.IGNORECASE):
                                reasons.append(f"meta robots: {m.group(1)}")
                            canonical_checked_count += 1
                            c = re.search(r'<link[^>]+rel=["\'][^"\']*\bcanonical\b[^"\']*["\'][^>]*href=["\']([^"\']+)["\']', body, flags=re.IGNORECASE)
                            if not c:
                                c = re.search(r'<link[^>]+href=["\']([^"\']+)["\'][^>]*rel=["\'][^"\']*\bcanonical\b[^"\']*["\']', body, flags=re.IGNORECASE)
                            if not c:
                                item["canonical_status"] = "missing"
                                canonical_missing_count += 1
                            else:
                                canonical_raw = str(c.group(1) or "").strip()
                                canonical_abs = urljoin(sample_url, canonical_raw)
                                item["canonical_url"] = canonical_abs
                                if not is_http_url(canonical_abs):
                                    item["canonical_status"] = "invalid"
                                    canonical_invalid_count += 1
                                    reasons.append("canonical: некорректный URL")
                                else:
                                    norm_src = sample_url.rstrip("/")
                                    norm_can = canonical_abs.rstrip("/")
                                    if norm_src == norm_can:
                                        item["canonical_status"] = "self"
                                    else:
                                        item["canonical_status"] = "other"
                                        canonical_non_self_count += 1
                                        reasons.append("canonical указывает на другой URL")
                    item["reasons"] = reasons
                    item["indexable"] = (200 <= int(live_response.status_code) < 300) and len(reasons) == 0
                    if item["indexable"] is False:
                        live_non_indexable_count += 1
                except Exception as live_err:
                    item["indexable"] = False
                    item["reasons"] = [str(live_err)]
                    item["response_ms"] = int((time.time() - started) * 1000)
                    live_non_indexable_count += 1
                    live_check_errors_count += 1
                live_indexability_checks.append(item)

        if canonical_checked_count > 0 and (canonical_missing_count + canonical_invalid_count + canonical_non_self_count) > 0:
            warnings.append(
                "Проверка canonical на случайной выборке: "
                f"отсутствует={canonical_missing_count}, некорректный={canonical_invalid_count}, не self-canonical={canonical_non_self_count} "
                f"(выборка={canonical_checked_count})."
            )

        recommendations: List[str] = []
        highlights: List[str] = []
        quality_score = 100

        if len(sitemap_files) > 0 and len(errors) == 0 and len(warnings) == 0:
            highlights.append("Структура sitemap валидна, парсинг выполнен без ошибок.")
        if total_urls_discovered > 0:
            highlights.append(f"Обнаружено URL: {total_urls_discovered}. Уникальных URL: {len(seen_urls)}.")
        if duplicate_urls_count == 0 and total_urls_discovered > 0:
            highlights.append("Дубли URL между просканированными sitemap-файлами не обнаружены.")
        if hreflang_links_count > 0:
            highlights.append(f"В sitemap обнаружены hreflang-ссылки: {hreflang_links_count}.")
        if image_tags_count + video_tags_count + news_tags_count > 0:
            highlights.append(f"Обнаружены media-расширения (изображения/видео/новости): {image_tags_count}/{video_tags_count}/{news_tags_count}.")
        if live_indexability_checks:
            highlights.append(f"Проверена live-выборка индексируемости: {len(live_indexability_checks)} URL, неиндексируемых: {live_non_indexable_count}.")

        if invalid_urls_count > 0:
            recommendations.append("Исправьте некорректные <loc> и оставьте только абсолютные HTTP/HTTPS URL.")
            quality_score -= min(25, invalid_urls_count)
        if invalid_lastmod_count > 0:
            recommendations.append("Приведите <lastmod> к формату W3C (YYYY-MM-DD или полный ISO-8601).")
            quality_score -= min(20, invalid_lastmod_count)
        if stale_lastmod_count > 0:
            recommendations.append(
                f"Обновите устаревшие URL (старше {stale_days} дней по <lastmod>) и поддерживайте корректные сигналы обновления."
            )
            quality_score -= min(10, stale_lastmod_count)
        if lastmod_future_count > 0:
            recommendations.append("Исправьте будущие даты в <lastmod>; поисковые системы могут считать такой сигнал недостоверным.")
            quality_score -= min(10, lastmod_future_count)
        if invalid_changefreq_count > 0:
            recommendations.append("Используйте только допустимые значения <changefreq> (always/hourly/daily/weekly/monthly/yearly/never).")
            quality_score -= min(10, invalid_changefreq_count)
        if invalid_priority_count > 0:
            recommendations.append("Используйте значения <priority> только в диапазоне 0.0..1.0.")
            quality_score -= min(10, invalid_priority_count)
        if duplicate_urls_count > 0:
            recommendations.append("Удалите дубли URL между sitemap-файлами.")
            quality_score -= min(20, duplicate_urls_count)
        if self_child_refs > 0 or repeated_child_refs > 0:
            recommendations.append("Исправьте структуру sitemap-индекса (уберите самоссылки и повторяющиеся ссылки на дочерние sitemap).")
            quality_score -= min(10, self_child_refs + repeated_child_refs)
        if queue:
            quality_score -= 10
        if urls_export_truncated:
            tool_notes.append(f"Превью экспорта ограничено до {max_export_urls} URL; для полного списка используйте экспорт частями.")
        if total_urls_discovered > max_urls_per_sitemap:
            recommendations.append("Как минимум один sitemap превышает 50 000 URL; разделите его на несколько файлов.")
            quality_score -= 10
        if any((item.get("size_bytes", 0) or 0) > max_file_size for item in sitemap_files):
            recommendations.append("Как минимум один sitemap-файл превышает 50 МиБ; разделите или сожмите sitemap-файлы.")
            quality_score -= 10
        if hreflang_links_count > 0 and (hreflang_invalid_code_count + hreflang_invalid_href_count + hreflang_duplicate_lang_count) > 0:
            recommendations.append("Исправьте hreflang в sitemap (валидный код, абсолютный href, без дублирования языков в рамках URL).")
            quality_score -= min(10, hreflang_invalid_code_count + hreflang_invalid_href_count + hreflang_duplicate_lang_count)
        if image_tags_count > 0 and image_missing_loc_count > 0:
            recommendations.append("Убедитесь, что каждый <image:image> содержит валидный <image:loc> URL.")
            quality_score -= min(8, image_missing_loc_count)
        if video_tags_count > 0 and video_missing_required_count > 0:
            recommendations.append("Заполните обязательные поля video sitemap (thumbnail_loc, title, description, content_loc/player_loc).")
            quality_score -= min(8, video_missing_required_count)
        if news_tags_count > 0 and news_missing_required_count > 0:
            recommendations.append("Заполните обязательные поля news sitemap (publication_date и title).")
            quality_score -= min(8, news_missing_required_count)
        if live_non_indexable_count > 0:
            recommendations.append("Проверьте неиндексируемые URL из live-выборки (HTTP-ошибки или noindex).")
            quality_score -= min(15, live_non_indexable_count)
        if canonical_checked_count > 0 and (canonical_missing_count + canonical_invalid_count + canonical_non_self_count) > 0:
            recommendations.append("Проверьте canonical в выборке URL (отсутствует/некорректный/не self canonical).")
            quality_score -= min(8, canonical_missing_count + canonical_invalid_count + canonical_non_self_count)

        if not recommendations:
            recommendations.append("Критических проблем sitemap не обнаружено. Поддерживайте текущую структуру и отслеживайте состояние в инструментах вебмастеров.")

        issues: List[Dict[str, Any]] = []
        if invalid_urls_count > 0:
            issues.append(build_issue(
                "critical",
                "invalid_loc_urls",
                "Некорректные URL в <loc>",
                f"Найдено некорректных sitemap URL: {invalid_urls_count}.",
                "Исправьте некорректные <loc> и оставьте только абсолютные HTTP/HTTPS URL.",
                "SEO/Dev",
            ))
        if duplicate_urls_count > 0:
            issues.append(build_issue(
                "warning",
                "duplicate_urls",
                "Дубли URL между sitemap-файлами",
                f"Найдено повторов URL: {duplicate_urls_count}.",
                "Уберите дубли URL во всех sitemap-файлах.",
                "SEO",
            ))
        if self_child_refs > 0 or repeated_child_refs > 0:
            issues.append(build_issue(
                "warning",
                "sitemap_index_structure",
                "Проблемы структуры sitemap-индекса",
                f"Самоссылки: {self_child_refs}, повторные ссылки: {repeated_child_refs}.",
                "Уберите самоссылки и повторные ссылки на дочерние sitemap.",
                "Dev",
            ))
        if invalid_lastmod_count + stale_lastmod_count + lastmod_future_count > 0:
            issues.append(build_issue(
                "warning",
                "lastmod_quality",
                "Проблемы актуальности lastmod",
                f"Некорректных: {invalid_lastmod_count}, устаревших: {stale_lastmod_count}, будущих: {lastmod_future_count}.",
                "Нормализуйте формат lastmod и поддерживайте реалистичные и актуальные даты.",
                "SEO/Content",
            ))
        if hreflang_links_count > 0 and (hreflang_invalid_code_count + hreflang_invalid_href_count + hreflang_duplicate_lang_count) > 0:
            issues.append(build_issue(
                "warning",
                "hreflang_sitemap_issues",
                "Проблемы hreflang в sitemap",
                f"Некорректные коды: {hreflang_invalid_code_count}, некорректные href: {hreflang_invalid_href_count}, дубли языков: {hreflang_duplicate_lang_count}.",
                "Исправьте hreflang и href в alternate-ссылках sitemap.",
                "SEO",
            ))
        if (image_tags_count > 0 and image_missing_loc_count > 0) or (video_tags_count > 0 and video_missing_required_count > 0) or (news_tags_count > 0 and news_missing_required_count > 0):
            issues.append(build_issue(
                "warning",
                "media_extension_issues",
                "Проблемы расширений media/news sitemap",
                f"image: без loc={image_missing_loc_count}, video: без обязательных полей={video_missing_required_count}, news: без обязательных полей={news_missing_required_count}.",
                "Заполните обязательные поля в расширениях image/video/news sitemap.",
                "SEO/Dev",
            ))
        if live_non_indexable_count > 0:
            issues.append(build_issue(
                "critical",
                "live_non_indexable_sample",
                "В live-выборке есть неиндексируемые URL",
                f"{live_non_indexable_count} из {len(live_indexability_checks)} URL в выборке выглядят неиндексируемыми.",
                "Исправьте noindex/HTTP-проблемы на страницах из выборки и запустите проверку повторно.",
                "SEO/Dev",
            ))
        if canonical_checked_count > 0 and (canonical_missing_count + canonical_invalid_count + canonical_non_self_count) > 0:
            issues.append(build_issue(
                "warning",
                "canonical_sample_issues",
                "Проблемы canonical в случайной выборке",
                f"Выборка={canonical_checked_count}, отсутствует={canonical_missing_count}, некорректный={canonical_invalid_count}, не self={canonical_non_self_count}.",
                "Исправьте отсутствующие/некорректные canonical и проверьте цели canonical для URL из выборки.",
                "SEO/Dev",
            ))

        severity_counts = {
            "critical": sum(1 for it in issues if it.get("severity") == "critical"),
            "warning": sum(1 for it in issues if it.get("severity") == "warning"),
            "info": sum(1 for it in issues if it.get("severity") == "info"),
        }
        severity_weight = {"critical": 0, "warning": 1, "info": 2}
        issues_sorted = sorted(issues, key=lambda x: severity_weight.get(str(x.get("severity")), 9))
        top_fixes = dedupe_keep_order([it.get("action", "") for it in issues_sorted if it.get("action")])[:10]
        action_plan: List[Dict[str, Any]] = []
        for issue in issues_sorted[:12]:
            sev = str(issue.get("severity", "warning"))
            action_plan.append({
                "priority": "P0" if sev == "critical" else ("P1" if sev == "warning" else "P2"),
                "owner": issue.get("owner", "SEO"),
                "issue": issue.get("title", ""),
                "action": issue.get("action", ""),
                "sla": "24h" if sev == "critical" else ("3d" if sev == "warning" else "7d"),
            })
        if not action_plan and recommendations:
            action_plan.append({
                "priority": "P2",
                "owner": "SEO",
                "issue": "Критические блокеры отсутствуют",
                "action": recommendations[0],
                "sla": "7d",
            })

        quality_score = max(0, min(100, quality_score))
        if quality_score >= 90:
            quality_grade = "A"
        elif quality_score >= 80:
            quality_grade = "B"
        elif quality_score >= 70:
            quality_grade = "C"
        elif quality_score >= 60:
            quality_grade = "D"
        else:
            quality_grade = "F"

        return {
            "task_type": "sitemap_validate",
            "url": primary_root_url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "root_sitemaps": root_urls,
                "valid": len(errors) == 0 and len(sitemap_files) > 0,
                "status_code": root_status_code,
                "urls_count": total_urls_discovered,
                "unique_urls_count": len(seen_urls),
                "duplicate_urls_count": duplicate_urls_count,
                "duplicate_details": duplicate_details,
                "duplicate_details_truncated": duplicate_details_truncated,
                "sitemaps_scanned": len(sitemap_files),
                "sitemaps_valid": valid_files,
                "errors": dedupe_keep_order(errors),
                "warnings": dedupe_keep_order(warnings),
                "tool_notes": dedupe_keep_order(tool_notes),
                "recommendations": recommendations,
                "highlights": dedupe_keep_order(highlights),
                "quality_score": quality_score,
                "quality_grade": quality_grade,
                "sitemap_files": sitemap_files,
                "export_urls": all_urls,
                "urls_export_truncated": urls_export_truncated,
                "max_export_urls": max_export_urls,
                "export_chunk_size": export_chunk_size,
                "export_parts_count": export_parts_count,
                "scan_limit_files": max_sitemaps,
                "scan_limit_reached": bool(queue),
                "scan_queue_remaining": len(queue),
                "invalid_lastmod_count": invalid_lastmod_count,
                "invalid_changefreq_count": invalid_changefreq_count,
                "invalid_priority_count": invalid_priority_count,
                "invalid_urls_count": invalid_urls_count,
                "size": sum(item.get("size_bytes", 0) for item in sitemap_files),
                "max_depth_seen": max_depth_seen,
                "self_child_refs": self_child_refs,
                "repeated_child_refs": repeated_child_refs,
                "freshness": {
                    "lastmod_present_count": lastmod_present_count,
                    "lastmod_missing_count": lastmod_missing_count,
                    "lastmod_future_count": lastmod_future_count,
                    "stale_lastmod_count": stale_lastmod_count,
                    "uniform_lastmod_files": uniform_lastmod_files,
                    "stale_threshold_days": stale_days,
                },
                "hreflang": {
                    "detected": hreflang_links_count > 0,
                    "links_count": hreflang_links_count,
                    "urls_count": hreflang_urls_count,
                    "invalid_code_count": hreflang_invalid_code_count,
                    "invalid_href_count": hreflang_invalid_href_count,
                    "duplicate_lang_count": hreflang_duplicate_lang_count,
                    "has_x_default": hreflang_has_x_default,
                },
                "media_extensions": {
                    "image_tags_count": image_tags_count,
                    "image_missing_loc_count": image_missing_loc_count,
                    "video_tags_count": video_tags_count,
                    "video_missing_required_count": video_missing_required_count,
                    "news_tags_count": news_tags_count,
                    "news_missing_required_count": news_missing_required_count,
                },
                "live_indexability_checks": live_indexability_checks,
                "live_check_sample_size": len(live_indexability_checks),
                "live_non_indexable_count": live_non_indexable_count,
                "live_check_errors_count": live_check_errors_count,
                "canonical_sample": {
                    "sample_size": canonical_checked_count,
                    "missing_count": canonical_missing_count,
                    "invalid_count": canonical_invalid_count,
                    "non_self_count": canonical_non_self_count,
                },
                "issues": issues_sorted,
                "severity_counts": severity_counts,
                "top_fixes": top_fixes,
                "action_plan": action_plan,
            }
        }
    except Exception as e:
        return {
            "task_type": "sitemap_validate",
            "url": primary_root_url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "valid": False,
                "error": str(e),
                "urls_count": 0,
                "export_urls": [],
                "sitemap_files": [],
            }
        }


def check_bots_full(
    url: str,
    selected_bots: Optional[List[str]] = None,
    bot_groups: Optional[List[str]] = None,
    retry_profile: str = "standard",
    criticality_profile: str = "balanced",
    sla_profile: str = "standard",
    baseline_enabled: bool = True,
    ai_block_expected: bool = False,
    batch_mode: bool = False,
    batch_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Bot accessibility check with feature-flagged v2 engine."""
    from app.config import settings

    engine = (getattr(settings, "BOT_CHECK_ENGINE", "legacy") or "legacy").lower()
    has_custom_selection = bool(selected_bots or bot_groups)
    if has_custom_selection:
        engine = "v2"
    if engine == "v2":
        try:
            from app.tools.bots.service_v2 import BotAccessibilityServiceV2

            checker = BotAccessibilityServiceV2(
                timeout=getattr(settings, "BOT_CHECK_TIMEOUT", 15),
                max_workers=getattr(settings, "BOT_CHECK_MAX_WORKERS", 10),
                retry_profile=retry_profile,
                criticality_profile=criticality_profile,
                sla_profile=sla_profile,
                baseline_enabled=baseline_enabled,
                ai_block_expected=ai_block_expected,
            )
            if batch_mode:
                return checker.run_batch(batch_urls or [], selected_bots=selected_bots, bot_groups=bot_groups)
            return checker.run(url, selected_bots=selected_bots, bot_groups=bot_groups)
        except Exception as e:
            print(f"[API] bot v2 failed, fallback to legacy: {e}")
            legacy = _check_bots_legacy(url)
            legacy_results = legacy.get("results", {})
            legacy_results["engine"] = "legacy-fallback"
            legacy_results["engine_error"] = str(e)
            legacy_results["selected_bots_ignored"] = selected_bots or []
            legacy_results["selected_groups_ignored"] = bot_groups or []
            return legacy

    return _check_bots_legacy(url)


def _check_bots_legacy(url: str) -> Dict[str, Any]:
    import requests

    bots = [
        ("Googlebot", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"),
        ("YandexBot", "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"),
        ("Bingbot", "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"),
        ("DuckDuckBot", "DuckDuckBot/1.0; (+https://duckduckgo.com/duckbot)"),
        ("GPTBot", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.0; +https://openai.com/gptbot)"),
    ]
    
    results = {}
    for bot_name, user_agent in bots:
        try:
            resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=10)
            results[bot_name] = {
                "status": resp.status_code,
                "accessible": resp.status_code == 200,
                "response_time": resp.elapsed.total_seconds() if hasattr(resp, 'elapsed') else None
            }
        except Exception as e:
            results[bot_name] = {"error": str(e), "accessible": False}
    
    return {
        "task_type": "bot_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "engine": "legacy",
            "bots_checked": [b[0] for b in bots],
            "bot_results": results,
            "summary": {
                "total": len(bots),
                "accessible": sum(1 for r in results.values() if r.get("accessible")),
            }
        }
    }


# ============ REQUEST MODELS ============
class RobotsCheckRequest(URLModel):
    url: str

class SitemapValidateRequest(URLModel):
    url: str

class BotCheckRequest(URLModel):
    url: str
    selected_bots: Optional[List[str]] = None
    bot_groups: Optional[List[str]] = None
    retry_profile: Optional[str] = "standard"
    criticality_profile: Optional[str] = "balanced"
    sla_profile: Optional[str] = "standard"
    baseline_enabled: bool = True
    ai_block_expected: bool = False
    scan_mode: Optional[str] = "single"
    batch_urls: Optional[List[str]] = None

    @field_validator("selected_bots", "bot_groups", mode="before")
    @classmethod
    def _normalize_list_fields(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            return [v] if v else []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return value



def _is_likely_sitemap_url(value: str) -> bool:
    parsed = urlparse(value)
    path = (parsed.path or "").lower()
    if not path:
        return False
    if path.endswith(".xml") or "sitemap" in path:
        return True
    return False


def _looks_like_sitemap_xml(payload: str) -> bool:
    text = str(payload or "").lstrip("\ufeff \n\r\t").lower()
    return text.startswith("<?xml") or "<urlset" in text or "<sitemapindex" in text


def _discover_sitemap_urls(site_url: str, timeout: int = 12) -> tuple[List[str], Optional[str]]:
    """Discover sitemap URLs for a site. Returns (sitemap_urls, source)."""
    candidate_root = _normalize_http_input(site_url)
    if not candidate_root:
        return [], None

    parsed_root = urlparse(candidate_root)
    root = f"{parsed_root.scheme}://{parsed_root.netloc}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SEO-Tools/1.0)"}

    def _candidate_score(sitemap_url: str) -> int:
        path = (urlparse(sitemap_url).path or "").lower().strip("/")
        filename = path.split("/")[-1] if path else ""
        score = 0
        # Prefer common "main" sitemap/index files.
        if filename in ("sitemap.xml", "sitemap_index.xml", "sitemap-index.xml", "wp-sitemap.xml"):
            score += 120
        if "index" in filename:
            score += 40
        if filename.startswith("sitemap"):
            score += 20
        # De-prioritize vertical/topic sitemaps as default entry point.
        if re.search(r"(news|image|video|blog|post|tag|category|product|forum|help|article|media)", filename):
            score -= 60
        # Slightly prefer shallower paths.
        score -= path.count("/")
        return score

    with requests.Session() as session:
        # 1) robots.txt sitemap declarations (priority)
        try:
            robots_resp = session.get(urljoin(root, "/robots.txt"), timeout=timeout, allow_redirects=True, headers=headers)
            if robots_resp.status_code == 200:
                robots_candidates: List[str] = []
                for line in (robots_resp.text or "").splitlines():
                    if not re.match(r"^\s*sitemap\s*:", line, flags=re.IGNORECASE):
                        continue
                    raw_loc = line.split(":", 1)[1].strip() if ":" in line else ""
                    if not raw_loc:
                        continue
                    loc = urljoin(root + "/", raw_loc)
                    normalized_loc = _normalize_http_input(loc)
                    if not normalized_loc:
                        continue
                    try:
                        sm_resp = session.get(normalized_loc, timeout=timeout, allow_redirects=True, headers=headers)
                        if sm_resp.status_code == 200 and _looks_like_sitemap_xml(sm_resp.text[:10000]):
                            robots_candidates.append(normalized_loc)
                    except Exception:
                        continue
                if robots_candidates:
                    unique_candidates = list(dict.fromkeys(robots_candidates))
                    unique_candidates.sort(key=lambda u: (_candidate_score(u), -len(u)), reverse=True)
                    return unique_candidates, "robots.txt"
        except Exception:
            pass

        # 2) Common fallback sitemap paths
        common_paths = (
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap-index.xml",
            "/sitemaps.xml",
            "/sitemaps/sitemap.xml",
            "/wp-sitemap.xml",
        )
        for path in common_paths:
            loc = urljoin(root, path)
            try:
                sm_resp = session.get(loc, timeout=timeout, allow_redirects=True, headers=headers)
                if sm_resp.status_code == 200 and _looks_like_sitemap_xml(sm_resp.text[:10000]):
                    return [loc], "common_path"
            except Exception:
                continue

    return [], None

    @field_validator("batch_urls", mode="before")
    @classmethod
    def _normalize_batch_urls(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            parts = [x.strip() for x in re.split(r"[\r\n,;]+", value) if x.strip()]
            return parts
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return value


# ============ API ENDPOINTS ============

@router.post("/tasks/robots-check")
async def create_robots_check(data: RobotsCheckRequest):
    """Full robots.txt analysis"""
    url = data.url
    
    print(f"[API] Full robots.txt analysis for: {url}")
    
    result = check_robots_full(url)
    task_id = f"robots-{datetime.now().timestamp()}"
    create_task_result(task_id, "robots_check", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Robots.txt analysis completed"
    }


# ExportRequest re-exported for backward compatibility (used in main.py)
from app.api.routers.exports import ExportRequest  # noqa: E402

@router.post("/tasks/sitemap-validate")
async def create_sitemap_validate(data: SitemapValidateRequest):
    """Full sitemap validation"""
    raw_input = str(data.url or "").strip()
    normalized_input = _normalize_http_input(raw_input)
    if not normalized_input:
        raise HTTPException(status_code=422, detail="Введите корректный домен или URL sitemap.")

    if _is_likely_sitemap_url(normalized_input):
        target_sitemap_urls = [normalized_input]
        discovery_source = "direct_input"
    else:
        discovered_urls, source = _discover_sitemap_urls(normalized_input)
        if not discovered_urls:
            raise HTTPException(
                status_code=422,
                detail="Мы не нашли sitemap автоматически. Введите полный URL sitemap (например, https://example.com/sitemap.xml)."
            )
        target_sitemap_urls = discovered_urls
        discovery_source = source or "auto_discovery"

    print(
        f"[API] Полная валидация sitemap для input={normalized_input}, "
        f"sitemaps={len(target_sitemap_urls)}, source={discovery_source}"
    )

    result = check_sitemap_full(target_sitemap_urls)
    if isinstance(result, dict):
        result["input_url"] = normalized_input
        result["resolved_sitemap_url"] = target_sitemap_urls[0] if target_sitemap_urls else ""
        result["resolved_sitemap_urls"] = target_sitemap_urls
        result["sitemap_discovery_source"] = discovery_source
    task_id = f"sitemap-{datetime.now().timestamp()}"
    create_task_result(
        task_id,
        "sitemap_validate",
        target_sitemap_urls[0] if target_sitemap_urls else normalized_input,
        result
    )
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Валидация sitemap завершена"
    }


@router.post("/tasks/bot-check")
async def create_bot_check(data: BotCheckRequest):
    """Full bot accessibility check"""
    url = data.url
    
    print(f"[API] Full bot check for: {url}")
    
    result = check_bots_full(
        url,
        selected_bots=data.selected_bots,
        bot_groups=data.bot_groups,
        retry_profile=(data.retry_profile or "standard"),
        criticality_profile=(data.criticality_profile or "balanced"),
        sla_profile=(data.sla_profile or "standard"),
        baseline_enabled=bool(data.baseline_enabled),
        ai_block_expected=bool(data.ai_block_expected),
        batch_mode=(str(data.scan_mode or "single").lower() == "batch"),
        batch_urls=(data.batch_urls or []),
    )
    task_id = f"bots-{datetime.now().timestamp()}"
    create_task_result(task_id, "bot_check", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Bot check completed"
    }

# ============ ADDITIONAL TOOLS ============

def check_site_full(input_url: str, max_pages: int = 20) -> Dict[str, Any]:
    """
    Full site analysis - максимально приближено к оригинальному seopro.py
    Краулинг, анализ контента, технологии, ссылки, редиректы
    """
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin, urlparse
    from collections import defaultdict, Counter
    import re
    import time
    
    url = input_url  # Use input_url as url for compatibility
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    })
    session.verify = False
    
    parsed_base = urlparse(url)
    base_domain = parsed_base.netloc.lower()
    base_normalized = base_domain[4:] if base_domain.startswith('www.') else base_domain
    
    visited = set()
    to_visit = [(url, 0)]
    all_pages_data = []
    broken_links = []
    external_links = []
    internal_links = []
    all_urls = set()
    redirects = []
    sitemap_urls = set()
    
    # Для подсчёта внутренних ссылок (упрощённый page authority)
    internal_links_count = defaultdict(int)
    
    def is_internal(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            netloc = parsed.netloc.lower()
            netloc_norm = netloc[4:] if netloc.startswith('www.') else netloc
            return netloc_norm == base_normalized or netloc_norm.endswith('.' + base_normalized)
        except:
            return False
    
    def normalize_url(url: str, drop_query: bool = True) -> str:
        if not url:
            return ''
        try:
            parsed = urlparse(url)
            scheme = (parsed.scheme or '').lower()
            netloc = parsed.netloc or ''
            if netloc.endswith(':80'):
                netloc = netloc[:-3]
            if netloc.endswith(':443'):
                netloc = netloc[:-4]
            path = parsed.path or '/'
            if path != '/':
                path = path.rstrip('/')
            query = '' if drop_query else parsed.query
            return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else '')
        except:
            return url.strip()
    
    def is_valid_page(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            path = parsed.path.lower()
            exts = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico', '.pdf', 
                    '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.mp3', '.mp4', '.css', 
                    '.js', '.json', '.xml', '.rss', '.woff', '.woff2', '.ttf', '.eot', '.otf']
            for ext in exts:
                if path.endswith(ext):
                    return False
            admin_paths = ['/admin', '/wp-admin', '/administrator', '/manage', '/login', 
                          '/logout', '/signup', '/register', '/api/', '/static/', '/media/', 
                          '/uploads/', '/cdn/', '/captcha', '/recaptcha']
            for admin in admin_paths:
                if admin in path:
                    return False
            return True
        except:
            return False
    
    def looks_blocked(response) -> bool:
        try:
            if response is None:
                return True
            if response.status_code in (403, 429, 503):
                return True
            body = (response.text or '').lower()
            blocked_words = ['captcha', 'access denied', 'forbidden', 'cloudflare', 'ddos-guard', 
                           'ваш доступ заблокирован', 'доступ запрещён']
            if any(x in body for x in blocked_words):
                return True
            if len(body.strip()) < 800:
                return True
        except:
            return True
        return False
    
    def extract_tech(html_text: str) -> list:
        tech = []
        text = html_text.lower()
        if 'react' in text and 'data-react' not in text:
            tech.append('React')
        if 'vue' in text or 'vue.js' in text:
            tech.append('Vue.js')
        if 'angular' in text:
            tech.append('Angular')
        if 'jquery' in text:
            tech.append('jQuery')
        if 'bootstrap' in text:
            tech.append('Bootstrap')
        if 'wordpress' in text:
            tech.append('WordPress')
        if 'bitrix' in text or '1c-bitrix' in text:
            tech.append('1C-Bitrix')
        if 'django' in text:
            tech.append('Django')
        if 'flask' in text:
            tech.append('Flask')
        if 'laravel' in text:
            tech.append('Laravel')
        if 'node.js' in text or 'nodejs' in text:
            tech.append('Node.js')
        if 'php' in text and 'php' not in tech:
            tech.append('PHP')
        if 'asp.net' in text or 'asp.net' in text.lower():
            tech.append('ASP.NET')
        if 'gatsby' in text:
            tech.append('Gatsby')
        if 'next.js' in text or 'nextjs' in text:
            tech.append('Next.js')
        if 'webpack' in text:
            tech.append('Webpack')
        if 'gulp' in text:
            tech.append('Gulp')
        if 'grunt' in text:
            tech.append('Grunt')
        if 'google tag manager' in text or 'gtm.js' in html_text:
            tech.append('Google Tag Manager')
        if 'yandex.metrika' in text or 'яндекс.метрика' in text:
            tech.append('Yandex.Metrika')
        if 'google analytics' in text:
            tech.append('Google Analytics')
        return list(set(tech))
    
    def analyze_page(page_url: str, depth: int, html: str, response) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        
        title = soup.find('title')
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        canonical = soup.find('link', rel='canonical')
        og_title = soup.find('meta', property='og:title')
        og_desc = soup.find('meta', property='og:description')
        twitter_card = soup.find('meta', attrs={'name': 'twitter:card'})
        
        h1_tags = soup.find_all('h1')
        h2_tags = soup.find_all('h2')
        h3_tags = soup.find_all('h3')
        
        images = soup.find_all('img')
        images_without_alt = [img for img in images if not img.get('alt') or not img.get('alt').strip()]
        images_empty_alt = [img for img in images if img.get('alt', '').strip() == '']
        
        links = soup.find_all('a')
        page_links = []
        dofollow = 0
        nofollow = 0
        
        for link in links:
            href = link.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript'):
                rel = link.get('rel', [])
                if 'nofollow' in rel:
                    nofollow += 1
                else:
                    dofollow += 1
                page_links.append(href)
        
        scripts = soup.find_all('script')
        styles = soup.find_all('style')
        iframes = soup.find_all('iframe')
        
        body = soup.find('body')
        text_content = body.get_text(strip=True) if body else ''
        word_count = len(text_content.split())
        char_count = len(text_content)
        
        has_json_ld = len(soup.find_all('script', {'type': 'application/ld+json'}))
        has_microdata = len(soup.find_all(attrs={'itemscope': True}))
        has_rdfa = len(soup.find_all(attrs={'typeof': True}))
        has_hreflang = len(soup.find_all('link', {'rel': 'alternate', 'hreflang': True}))
        
        breadcrumbs = soup.find_all(class_=re.compile(r'breadcrumb', re.I))
        has_breadcrumbs = 1 if breadcrumbs else 0
        
        page_issues = []
        
        if not title:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует тег Title', 'url': page_url})
        else:
            if len(title.text) < 30:
                page_issues.append({'type': 'warning', 'issue': f'Title слишком короткий ({len(title.text)} символов)', 'url': page_url})
            elif len(title.text) > 70:
                page_issues.append({'type': 'warning', 'issue': f'Title слишком длинный ({len(title.text)} символов)', 'url': page_url})
        
        if not meta_desc:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует Meta Description', 'url': page_url})
        elif len(meta_desc.get('content', '')) < 120:
            page_issues.append({'type': 'warning', 'issue': 'Meta Description слишком короткий', 'url': page_url})
        
        if len(h1_tags) == 0:
            page_issues.append({'type': 'warning', 'issue': 'Отсутствует тег H1', 'url': page_url})
        elif len(h1_tags) > 1:
            page_issues.append({'type': 'warning', 'issue': f'Много тегов H1 ({len(h1_tags)})', 'url': page_url})
        
        if len(images_without_alt) > 0:
            page_issues.append({'type': 'warning', 'issue': f'{len(images_without_alt)} изображений без alt-текста', 'url': page_url})
        
        if not has_json_ld and not has_microdata:
            page_issues.append({'type': 'info', 'issue': 'Отсутствует Schema.org разметка', 'url': page_url})
        
        if not og_title or not og_desc:
            page_issues.append({'type': 'info', 'issue': 'Отсутствуют Open Graph теги', 'url': page_url})
        
        if not twitter_card:
            page_issues.append({'type': 'info', 'issue': 'Отсутствует Twitter Cards', 'url': page_url})
        
        if has_breadcrumbs == 0:
            page_issues.append({'type': 'info', 'issue': 'Отсутствуют хлебные крошки', 'url': page_url})
        
        return {
            'url': page_url,
            'depth': depth,
            'status': 'success' if response and response.status_code < 400 else 'error',
            'status_code': response.status_code if response else None,
            'title': title.text if title else None,
            'title_length': len(title.text) if title else 0,
            'meta_description': meta_desc.get('content') if meta_desc else None,
            'meta_keywords': meta_keywords.get('content') if meta_keywords else None,
            'meta_robots': meta_robots.get('content') if meta_robots else None,
            'canonical': canonical.get('href') if canonical else None,
            'og_tags': bool(og_title and og_desc),
            'twitter_card': bool(twitter_card),
            'h1_count': len(h1_tags),
            'h2_count': len(h2_tags),
            'h3_count': len(h3_tags),
            'images_total': len(images),
            'images_without_alt': len(images_without_alt),
            'images_empty_alt': len(images_empty_alt),
            'links_total': len(links),
            'links_found': page_links,
            'dofollow': dofollow,
            'nofollow': nofollow,
            'scripts_count': len(scripts),
            'styles_count': len(styles),
            'iframes_count': len(iframes),
            'word_count': word_count,
            'char_count': char_count,
            'has_json_ld': has_json_ld,
            'has_microdata': has_microdata,
            'has_rdfa': has_rdfa,
            'has_hreflang': has_hreflang,
            'has_breadcrumbs': has_breadcrumbs,
            'tech_stack': extract_tech(html),
            'issues': page_issues,
            'load_time': response.elapsed.total_seconds() if response and hasattr(response, 'elapsed') else None
        }
    
    def load_sitemap() -> bool:
        """Загрузка sitemap.xml из robots.txt или стандартных путей"""
        try:
            robots_url = urljoin(url, '/robots.txt')
            try:
                response = session.get(robots_url, timeout=10)
                if response.status_code == 200:
                    for line in response.text.split('\n'):
                        line = line.strip()
                        if line.lower().startswith('sitemap:'):
                            sitemap_url = line.split(':', 1)[1].strip()
                            try:
                                sm_response = session.get(sitemap_url, timeout=15)
                                if sm_response.status_code == 200:
                                    soup = BeautifulSoup(sm_response.content, 'xml')
                                    for url_tag in soup.find_all('url'):
                                        loc = url_tag.find('loc')
                                        if loc and loc.text:
                                            sitemap_urls.add(loc.text.strip())
                            except:
                                pass
            except:
                pass
        except:
            pass
        
        standard_paths = ['/sitemap.xml', '/sitemap_index.xml', '/sitemaps/sitemap.xml', 
                          '/wp-sitemap.xml', '/sitemap/sitemap.xml']
        for path in standard_paths:
            sitemap_url = urljoin(url, path)
            try:
                response = session.get(sitemap_url, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'xml')
                    for url_tag in soup.find_all('url'):
                        loc = url_tag.find('loc')
                        if loc and loc.text:
                            sitemap_urls.add(loc.text.strip())
                    return True
            except:
                continue
        return False
    
    # Загружаем sitemap
    load_sitemap()
    
    start_time = time.time()
    pages_crawled = 0
    
    try:
        while to_visit and pages_crawled < max_pages:
            current_url, depth = to_visit.pop(0)
            
            if current_url in visited:
                continue
            if not is_valid_page(current_url):
                continue
            
            visited.add(current_url)
            all_urls.add(current_url)
            
            try:
                response = session.get(current_url, timeout=10, allow_redirects=True)
                
                if response.history:
                    for r in response.history:
                        redirects.append({
                            'from': normalize_url(r.url),
                            'to': normalize_url(response.url),
                            'status': r.status_code
                        })
                    current_url = response.url
                
                if response.status_code >= 400:
                    broken_links.append({
                        'url': normalize_url(current_url),
                        'status': response.status_code,
                        'error': response.reason if hasattr(response, 'reason') else 'Error'
                    })
                    pages_crawled += 1
                    continue
                
                if looks_blocked(response):
                    pages_crawled += 1
                    continue
                
                page_data = analyze_page(current_url, depth, response.text, response)
                all_pages_data.append(page_data)
                
                if depth < 2:
                    for link in page_data.get('links_found', [])[:30]:
                        if link:
                            full_link = urljoin(current_url, link)
                            norm_link = normalize_url(full_link)
                            if is_internal(full_link) and is_valid_page(full_link) and norm_link not in visited:
                                to_visit.append((full_link, depth + 1))
                                all_urls.add(full_link)
                                internal_links.append(norm_link)
                            elif not is_internal(full_link):
                                external_links.append({'url': norm_link, 'text': '', 'follow': 'dofollow'})
                
                pages_crawled += 1
                
            except requests.exceptions.Timeout:
                broken_links.append({'url': normalize_url(current_url), 'status': 'timeout', 'error': 'Connection timeout'})
                pages_crawled += 1
            except requests.exceptions.ConnectionError:
                broken_links.append({'url': normalize_url(current_url), 'status': 'connection_error', 'error': 'Connection error'})
                pages_crawled += 1
            except Exception as e:
                broken_links.append({'url': normalize_url(current_url), 'status': 'error', 'error': str(e)[:100]})
                pages_crawled += 1
        
        elapsed_time = time.time() - start_time
        
        # Агрегация результатов
        all_tech = []
        for page in all_pages_data:
            all_tech.extend(page.get('tech_stack', []))
        tech_counts = Counter(all_tech)
        top_technologies = [{'tech': t, 'count': c} for t, c in tech_counts.most_common(15)]
        
        all_issues = []
        for page in all_pages_data:
            all_issues.extend(page.get('issues', []))
        
        critical_issues = [i for i in all_issues if i.get('type') == 'critical']
        warning_issues = [i for i in all_issues if i.get('type') == 'warning']
        info_issues = [i for i in all_issues if i.get('type') == 'info']
        
        total_images = sum(p.get('images_total', 0) for p in all_pages_data)
        total_images_without_alt = sum(p.get('images_without_alt', 0) for p in all_pages_data)
        total_links = sum(p.get('links_total', 0) for p in all_pages_data)
        
        pages_with_title = len([p for p in all_pages_data if p.get('title')])
        pages_with_desc = len([p for p in all_pages_data if p.get('meta_description')])
        pages_with_h1 = len([p for p in all_pages_data if p.get('h1_count', 0) > 0])
        pages_with_schema = len([p for p in all_pages_data if p.get('has_json_ld') or p.get('has_microdata')])
        pages_with_og = len([p for p in all_pages_data if p.get('og_tags')])
        pages_with_breadcrumbs = len([p for p in all_pages_data if p.get('has_breadcrumbs')])
        
        avg_word_count = sum(p.get('word_count', 0) for p in all_pages_data) / max(1, len(all_pages_data))
        avg_load_time = sum(p.get('load_time', 0) for p in all_pages_data if p.get('load_time')) / max(1, len([p for p in all_pages_data if p.get('load_time')]))
        
        # SEO Score расчёт
        seo_score = 100
        seo_score -= min(30, len(critical_issues) * 10)
        seo_score -= min(20, len(warning_issues) * 3)
        seo_score -= min(10, total_images_without_alt * 0.5)
        seo_score -= min(10, len(broken_links) * 2)
        seo_score -= min(5, len(redirects) * 1)
        seo_score -= min(5, len(info_issues) * 0.5)
        seo_score = max(0, min(100, seo_score))
        
        # Категоризация по страницам
        good_pages = len([p for p in all_pages_data if len([i for i in p.get('issues', []) if i.get('type') in ['critical', 'warning']]) == 0])
        average_pages = len([p for p in all_pages_data if len([i for i in p.get('issues', []) if i.get('type') == 'critical']) == 0 and len([i for i in p.get('issues', []) if i.get('type') == 'warning']) <= 2])
        bad_pages = len(all_pages_data) - good_pages - average_pages
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "summary": {
                    "pages_crawled": pages_crawled,
                    "pages_total_found": len(all_urls),
                    "crawl_time_seconds": round(elapsed_time, 2),
                    "sitemap_urls_found": len(sitemap_urls),
                    "internal_links_count": len(set(internal_links)),
                    "external_links_count": len(set([e.get('url') for e in external_links])),
                    "broken_links_count": len(broken_links),
                    "redirects_count": len(redirects),
                    "seo_score": seo_score,
                    "critical_issues": len(critical_issues),
                    "warning_issues": len(warning_issues),
                    "info_issues": len(info_issues),
                    "good_pages": good_pages,
                    "average_pages": average_pages,
                    "bad_pages": max(0, bad_pages)
                },
                "content_analysis": {
                    "total_images": total_images,
                    "images_without_alt": total_images_without_alt,
                    "total_links": total_links,
                    "average_word_count": round(avg_word_count, 0),
                    "average_load_time": round(avg_load_time, 3),
                    "pages_with_title": pages_with_title,
                    "pages_with_meta_desc": pages_with_desc,
                    "pages_with_h1": pages_with_h1,
                    "pages_with_schema_org": pages_with_schema,
                    "pages_with_og_tags": pages_with_og,
                    "pages_with_breadcrumbs": pages_with_breadcrumbs
                },
                "technology_stack": top_technologies,
                "pages_detail": all_pages_data[:30],
                "broken_links": broken_links[:30],
                "redirects": redirects[:20],
                "sitemap_urls": list(sitemap_urls)[:50],
                "all_issues": all_issues[:100],
                "critical_issues": critical_issues[:20],
                "warning_issues": warning_issues[:50],
                "info_issues": info_issues[:30],
                "external_links_sample": external_links[:30],
                "recommendations": generate_recommendations_full(url, critical_issues, warning_issues, info_issues, tech_counts, seo_score, pages_crawled)
            }
        }
    except Exception as e:
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e)
            }
        }
    
    def analyze_page(page_url: str, depth: int, html: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        
        title = soup.find('title')
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        canonical = soup.find('link', rel='canonical')
        h1_tags = soup.find_all('h1')
        h2_tags = soup.find_all('h2')
        
        images = soup.find_all('img')
        images_without_alt = [img for img in images if not img.get('alt') or not img.get('alt').strip()]
        images_with_empty_alt = [img for img in images if img.get('alt', '').strip() == '']
        
        links = soup.find_all('a')
        page_links = []
        for link in links:
            href = link.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript'):
                page_links.append(href)
        
        scripts = soup.find_all('script')
        styles = soup.find_all('style')
        iframes = soup.find_all('iframe')
        
        body = soup.find('body')
        text_content = body.get_text(strip=True) if body else ''
        word_count = len(text_content.split())
        
        has_og_tags = bool(soup.find('meta', property='og:title'))
        has_twitter_cards = bool(soup.find('meta', property='twitter:card'))
        has_schema = bool(soup.find('script', type='application/ld+json'))
        
        page_issues = []
        if not title:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует тег title', 'url': page_url})
        if title and len(title.text) < 30:
            page_issues.append({'type': 'warning', 'issue': 'Слишком короткий title (< 30 символов)', 'url': page_url})
        if title and len(title.text) > 70:
            page_issues.append({'type': 'warning', 'issue': 'Слишком длинный title (> 70 символов)', 'url': page_url})
        if not meta_desc:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует meta description', 'url': page_url})
        if meta_desc and len(meta_desc.get('content', '')) < 120:
            page_issues.append({'type': 'warning', 'issue': 'Слишком короткий meta description', 'url': page_url})
        if len(h1_tags) == 0:
            page_issues.append({'type': 'warning', 'issue': 'Не найден H1', 'url': page_url})
        elif len(h1_tags) > 1:
            page_issues.append({'type': 'warning', 'issue': f'Несколько H1 ({len(h1_tags)})', 'url': page_url})
        if len(images_without_alt) > 0:
            page_issues.append({'type': 'warning', 'issue': f'Изображений без alt: {len(images_without_alt)}', 'url': page_url})
        
        return {
            'url': page_url,
            'depth': depth,
            'status': 'success',
            'title': title.text if title else None,
            'title_length': len(title.text) if title else 0,
            'meta_description': meta_desc.get('content') if meta_desc else None,
            'meta_keywords': meta_keywords.get('content') if meta_keywords else None,
            'meta_robots': meta_robots.get('content') if meta_robots else None,
            'canonical': canonical.get('href') if canonical else None,
            'h1_count': len(h1_tags),
            'h2_count': len(h2_tags),
            'images_total': len(images),
            'images_without_alt': len(images_without_alt),
            'images_empty_alt': len(images_with_empty_alt),
            'links_total': len(links),
            'links_found': page_links,
            'scripts_count': len(scripts),
            'styles_count': len(styles),
            'iframes_count': len(iframes),
            'word_count': word_count,
            'has_og_tags': has_og_tags,
            'has_twitter_cards': has_twitter_cards,
            'has_schema_org': has_schema,
            'issues': page_issues,
            'tech_stack': extract_tech(html)
        }
    
    start_time = time.time()
    pages_crawled = 0
    
    try:
        while to_visit and pages_crawled < max_pages:
            current_url, depth = to_visit.pop(0)
            
            if current_url in visited:
                continue
            if not is_valid_page(current_url):
                continue
            
            visited.add(current_url)
            all_urls.add(current_url)
            
            try:
                response = session.get(current_url, timeout=10, allow_redirects=True)
                
                if response.history:
                    for r in response.history:
                        redirects.append({
                            'from': r.url,
                            'to': response.url,
                            'status': r.status_code
                        })
                    current_url = response.url
                
                if response.status_code >= 400:
                    broken_links.append({
                        'url': current_url,
                        'status': response.status_code
                    })
                    pages_crawled += 1
                    continue
                
                page_data = analyze_page(current_url, depth, response.text)
                all_pages_data.append(page_data)
                internal_links.extend([l for l in page_data['links_found'] if is_internal(l)])
                
                if depth < 2:
                    for link in page_data['links_found'][:50]:
                        if link not in visited and len(all_urls) < max_pages * 2:
                            full_link = urljoin(current_url, link)
                            if is_internal(full_link) and is_valid_page(full_link):
                                to_visit.append((full_link, depth + 1))
                                all_urls.add(full_link)
                
                external_links.extend([l for l in page_data['links_found'] if not is_internal(l)])
                
                pages_crawled += 1
                
            except requests.exceptions.Timeout:
                broken_links.append({'url': current_url, 'status': 'timeout'})
                pages_crawled += 1
            except requests.exceptions.ConnectionError:
                broken_links.append({'url': current_url, 'status': 'connection_error'})
                pages_crawled += 1
            except Exception as e:
                broken_links.append({'url': current_url, 'status': str(e)})
                pages_crawled += 1
        
        elapsed_time = time.time() - start_time
        
        all_tech = []
        for page in all_pages_data:
            all_tech.extend(page.get('tech_stack', []))
        tech_counts = Counter(all_tech)
        top_technologies = [{'tech': t, 'count': c} for t, c in tech_counts.most_common(10)]
        
        all_issues = []
        for page in all_pages_data:
            all_issues.extend(page.get('issues', []))
        
        critical_issues = [i for i in all_issues if i.get('type') == 'critical']
        warning_issues = [i for i in all_issues if i.get('type') == 'warning']
        
        total_images = sum(p.get('images_total', 0) for p in all_pages_data)
        total_images_without_alt = sum(p.get('images_without_alt', 0) for p in all_pages_data)
        total_links = sum(p.get('links_total', 0) for p in all_pages_data)
        
        pages_with_title = len([p for p in all_pages_data if p.get('title')])
        pages_with_desc = len([p for p in all_pages_data if p.get('meta_description')])
        pages_with_h1 = len([p for p in all_pages_data if p.get('h1_count', 0) > 0])
        pages_with_schema = len([p for p in all_pages_data if p.get('has_schema_org')])
        
        avg_word_count = sum(p.get('word_count', 0) for p in all_pages_data) / max(1, len(all_pages_data))
        
        redirect_count = len(redirects)
        broken_count = len(broken_links)
        
        seo_score = 100
        seo_score -= min(30, len(critical_issues) * 10)
        seo_score -= min(20, len(warning_issues) * 3)
        seo_score -= min(20, total_images_without_alt * 0.5)
        seo_score -= min(10, redirect_count * 2)
        seo_score -= min(10, broken_count * 2)
        seo_score = max(0, seo_score)
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "summary": {
                    "pages_crawled": pages_crawled,
                    "pages_total_found": len(all_urls),
                    "crawl_time_seconds": round(elapsed_time, 2),
                    "internal_links_count": len(set(internal_links)),
                    "external_links_count": len(set(external_links)),
                    "broken_links_count": broken_count,
                    "redirects_count": redirect_count,
                    "seo_score": seo_score,
                    "critical_issues": len(critical_issues),
                    "warning_issues": len(warning_issues)
                },
                "content_analysis": {
                    "total_images": total_images,
                    "images_without_alt": total_images_without_alt,
                    "total_links": total_links,
                    "average_word_count": round(avg_word_count, 0),
                    "pages_with_title": pages_with_title,
                    "pages_with_meta_desc": pages_with_desc,
                    "pages_with_h1": pages_with_h1,
                    "pages_with_schema_org": pages_with_schema
                },
                "technology_stack": top_technologies,
                "pages_detail": all_pages_data[:50],
                "broken_links": broken_links[:50],
                "redirects": redirects[:20],
                "all_issues": all_issues[:100],
                "critical_issues": critical_issues[:20],
                "warning_issues": warning_issues[:50],
                "recommendations": generate_recommendations(critical_issues, warning_issues, tech_counts, seo_score)
            }
        }
    except Exception as e:
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e)
            }
        }


def generate_recommendations_full(site_url: str, critical_issues: list, warning_issues: list, info_issues: list, tech_counts: dict, score: int, pages_crawled: int) -> list:
    """Генерация рекомендаций на основе анализа - приближено к оригинальному скрипту"""
    recommendations = []
    
    if score < 30:
        recommendations.append({"priority": "critical", "text": "КРИТИЧЕСКИ НИЗКИЙ SEO SCORE! Требуется немедленный аудит и исправления всех критических проблем."})
    elif score < 50:
        recommendations.append({"priority": "high", "text": "Низкая SEO-оценка. Необходимо исправить критические проблемы в первую очередь."})
    elif score < 70:
        recommendations.append({"priority": "medium", "text": "Средняя SEO-оценка. Есть возможности для улучшения."})
    
    # Критические проблемы
    if any('title' in i.get('issue', '').lower() for i in critical_issues):
        count = len([i for i in critical_issues if 'title' in i.get('issue', '').lower()])
        recommendations.append({"priority": "high", "text": f"Добавьте Title теги на {count} страниц (оптимальная длина 60-70 символов)"})
    
    if any('description' in i.get('issue', '').lower() for i in critical_issues):
        count = len([i for i in critical_issues if 'description' in i.get('issue', '').lower()])
        recommendations.append({"priority": "high", "text": f"Добавьте Meta Description на {count} страниц (оптимальная длина 120-160 символов)"})
    
    if any('h1' in i.get('issue', '').lower() for i in warning_issues):
        count = len([i for i in warning_issues if 'h1' in i.get('issue', '').lower()])
        recommendations.append({"priority": "medium", "text": f"Исправьте теги H1 на {count} страниц (должен быть 1 тег H1 на страницу)"})
    
    # Изображения
    img_issues = [i for i in warning_issues if 'image' in i.get('issue', '').lower() or 'alt' in i.get('issue', '').lower()]
    if img_issues:
        total_img = sum(1 for i in img_issues)
        recommendations.append({"priority": "medium", "text": f"Добавьте alt-текст к {total_img} изображениям для улучшения SEO и доступности"})
    
    # Технологии
    if 'WordPress' in tech_counts:
        recommendations.append({"priority": "medium", "text": "Для WordPress установите SEO-плагин: Yoast SEO или Rank Math"})
    
    if 'jQuery' in tech_counts and 'React' not in tech_counts and 'Vue.js' not in tech_counts:
        recommendations.append({"priority": "low", "text": "Сайт использует jQuery. Рассмотрите переход на современный JS-фреймворк"})
    
    # Schema.org
    schema_issues = [i for i in info_issues if 'schema' in i.get('issue', '').lower()]
    if schema_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте Schema.org разметку (Organization, Breadcrumb, Product, Article) для улучшения сниппетов в поиске"})
    
    # Open Graph
    og_issues = [i for i in info_issues if 'open graph' in i.get('issue', '').lower()]
    if og_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте Open Graph теги (og:title, og:description, og:image) для улучшения шаринга в соцсетях"})
    
    # Twitter Cards
    twitter_issues = [i for i in info_issues if 'twitter' in i.get('issue', '').lower()]
    if twitter_issues:
        recommendations.append({"priority": "low", "text": "Добавьте Twitter Cards мета-теги для красивого отображения ссылок в Twitter"})
    
    # Хлебные крошки
    breadcrumb_issues = [i for i in info_issues if 'хлеб' in i.get('issue', '').lower() or 'breadcrumb' in i.get('issue', '').lower()]
    if breadcrumb_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте хлебные крошки на страницы для улучшения навигации и SEO"})
    
    # Аналитика
    has_analytics = any('Analytics' in t or 'Metrika' in t for t in tech_counts.keys())
    if not has_analytics:
        recommendations.append({"priority": "medium", "text": "Подключите системы аналитики: Google Analytics и/или Yandex.Metrika для отслеживания посещаемости"})
    
    # Скорость
    recommendations.append({"priority": "high", "text": "Оптимизируйте скорость загрузки страниц (сжатие изображений, минификация CSS/JS, кэширование)"})
    
    # Битые ссылки
    broken_count = len([i for i in critical_issues if 'broken' in i.get('issue', '').lower() or any(x in i.get('issue', '').lower() for x in ['404', 'ошибка', 'error'])])
    if broken_count > 0:
        recommendations.append({"priority": "high", "text": f"Найдено {broken_count} битых ссылок. Проверьте и исправьте или удалите их"})
    
    # Канонические URL
    canon_issues = [i for i in warning_issues if 'canonical' in i.get('issue', '').lower()]
    if canon_issues:
        recommendations.append({"priority": "medium", "text": "Настройте канонические URL для всех страниц во избежание дублирования контента"})
    
    # Мобильная адаптация
    mobile_issues = [i for i in critical_issues if 'mobile' in i.get('issue', '').lower() or 'viewport' in i.get('issue', '').lower()]
    if mobile_issues:
        recommendations.append({"priority": "high", "text": "Адаптируйте сайт для мобильных устройств (Viewport, Responsive Design)"})
    
    # HTTPS
    if not site_url.startswith('https://'):
        recommendations.append({"priority": "high", "text": "Перенесите сайт на HTTPS для безопасности и SEO"})
    
    return recommendations



def check_core_web_vitals(url: str, strategy: str = "desktop") -> Dict[str, Any]:
    from app.tools.core_web_vitals import run_core_web_vitals

    return run_core_web_vitals(url=url, strategy=strategy)


class SiteAnalyzeRequest(URLModel):
    url: str
    max_pages: int = 20



class CoreWebVitalsRequest(URLModel):
    url: Optional[str] = ""
    strategy: Optional[str] = "desktop"
    scan_mode: Optional[str] = "single"
    batch_urls: Optional[List[str]] = None
    competitor_mode: bool = False

    @field_validator("strategy", mode="before")
    @classmethod
    def _normalize_strategy(cls, value):
        token = str(value or "desktop").strip().lower()
        return token if token in {"mobile", "desktop"} else "desktop"

    @field_validator("scan_mode", mode="before")
    @classmethod
    def _normalize_scan_mode(cls, value):
        token = str(value or "single").strip().lower()
        return token if token in {"single", "batch"} else "single"

    @field_validator("batch_urls", mode="before")
    @classmethod
    def _normalize_batch_urls(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            return [x.strip() for x in re.split(r"[\r\n,;]+", value) if x.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return []

    @field_validator("competitor_mode", mode="before")
    @classmethod
    def _normalize_competitor_mode(cls, value):
        if isinstance(value, bool):
            return value
        token = str(value or "").strip().lower()
        return token in {"1", "true", "yes", "on"}


def _build_core_web_vitals_batch_result(
    *,
    strategy: str,
    source: str,
    sites: List[Dict[str, Any]],
) -> Dict[str, Any]:
    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _median(values: List[float]) -> Optional[float]:
        arr = [float(x) for x in values if x is not None]
        if not arr:
            return None
        arr.sort()
        n = len(arr)
        mid = n // 2
        if n % 2 == 1:
            return arr[mid]
        return (arr[mid - 1] + arr[mid]) / 2.0

    score_values: List[float] = []
    metric_lcp_values: List[float] = []
    metric_inp_values: List[float] = []
    metric_cls_values: List[float] = []
    category_acc: Dict[str, List[float]] = {
        "performance": [],
        "accessibility": [],
        "best_practices": [],
        "seo": [],
    }
    status_counts = {"good": 0, "needs_improvement": 0, "poor": 0, "unknown": 0}
    top_recommendations: Dict[str, int] = {}
    common_opportunities: Dict[str, Dict[str, Any]] = {}
    plan_counts: Dict[str, int] = {}
    risk_counts = {"low": 0, "medium": 0, "high": 0}
    failed_urls: List[Dict[str, str]] = []
    successful_urls = 0

    for site in sites:
        if str(site.get("status") or "").lower() != "success":
            failed_urls.append(
                {
                    "url": str(site.get("url") or ""),
                    "error": str(site.get("error") or "Scan failed"),
                }
            )
            continue

        successful_urls += 1
        summary = site.get("summary") or {}
        status = str(summary.get("core_web_vitals_status") or "unknown").strip().lower()
        if status not in status_counts:
            status = "unknown"
        status_counts[status] += 1

        score_raw = summary.get("performance_score")
        try:
            if score_raw is not None:
                score_values.append(float(score_raw))
        except Exception:
            pass

        metrics = site.get("metrics") or {}
        lcp = _as_float(((metrics.get("lcp") or {}).get("field_value_ms")))
        if lcp is None:
            lcp = _as_float(((metrics.get("lcp") or {}).get("lab_value_ms")))
        inp = _as_float(((metrics.get("inp") or {}).get("field_value_ms")))
        if inp is None:
            inp = _as_float(((metrics.get("inp") or {}).get("lab_value_ms")))
        cls = _as_float(((metrics.get("cls") or {}).get("field_value")))
        if cls is None:
            cls = _as_float(((metrics.get("cls") or {}).get("lab_value")))
        if lcp is not None:
            metric_lcp_values.append(lcp)
        if inp is not None:
            metric_inp_values.append(inp)
        if cls is not None:
            metric_cls_values.append(cls)

        categories = site.get("categories") or {}
        for key in category_acc.keys():
            cat_val = _as_float(categories.get(key))
            if cat_val is not None:
                category_acc[key].append(cat_val)

        analysis = site.get("analysis") or {}
        risk_level = str(analysis.get("risk_level") or "").lower()
        if risk_level in risk_counts:
            risk_counts[risk_level] += 1

        for rec in (site.get("recommendations") or []):
            text = str(rec or "").strip()
            if not text:
                continue
            top_recommendations[text] = int(top_recommendations.get(text, 0) or 0) + 1

        for plan_item in (site.get("action_plan") or []):
            if not isinstance(plan_item, dict):
                continue
            title = str(plan_item.get("action") or "").strip()
            if not title:
                continue
            plan_counts[title] = int(plan_counts.get(title, 0) or 0) + 1

        for opp in (site.get("opportunities") or []):
            if not isinstance(opp, dict):
                continue
            opp_id = str(opp.get("id") or "")
            opp_title = str(opp.get("title") or opp_id or "").strip()
            if not opp_title:
                continue
            key = opp_id or opp_title
            bucket = common_opportunities.setdefault(
                key,
                {
                    "id": opp_id,
                    "title": opp_title,
                    "count": 0,
                    "critical_count": 0,
                    "high_count": 0,
                    "total_savings_ms": 0.0,
                    "total_savings_bytes": 0.0,
                    "group": str(opp.get("group") or ""),
                },
            )
            bucket["count"] = int(bucket.get("count") or 0) + 1
            priority = str(opp.get("priority") or "").lower()
            if priority == "critical":
                bucket["critical_count"] = int(bucket.get("critical_count") or 0) + 1
            if priority == "high":
                bucket["high_count"] = int(bucket.get("high_count") or 0) + 1
            ms = _as_float(opp.get("savings_ms"))
            b = _as_float(opp.get("savings_bytes"))
            if ms is not None:
                bucket["total_savings_ms"] = float(bucket.get("total_savings_ms") or 0.0) + ms
            if b is not None:
                bucket["total_savings_bytes"] = float(bucket.get("total_savings_bytes") or 0.0) + b

    if status_counts["poor"] > 0:
        batch_status = "poor"
    elif status_counts["needs_improvement"] > 0:
        batch_status = "needs_improvement"
    elif successful_urls > 0 and status_counts["good"] == successful_urls:
        batch_status = "good"
    else:
        batch_status = "unknown"

    recommendations = []
    for text, count in sorted(top_recommendations.items(), key=lambda item: (-item[1], item[0].lower()))[:8]:
        if count > 1:
            recommendations.append(f"{text} (повторяется на {count} URL)")
        else:
            recommendations.append(text)

    avg_score = round(sum(score_values) / len(score_values), 1) if score_values else None
    median_score = round(_median(score_values), 1) if score_values else None
    min_score = round(min(score_values), 1) if score_values else None
    max_score = round(max(score_values), 1) if score_values else None
    total_urls = len(sites)
    failed_count = len(failed_urls)
    common_opportunities_rows: List[Dict[str, Any]] = []
    for value in common_opportunities.values():
        ms_total = float(value.get("total_savings_ms") or 0.0)
        bytes_total = float(value.get("total_savings_bytes") or 0.0)
        common_opportunities_rows.append(
            {
                "id": value.get("id") or "",
                "title": value.get("title") or "",
                "group": value.get("group") or "",
                "count": int(value.get("count") or 0),
                "critical_count": int(value.get("critical_count") or 0),
                "high_count": int(value.get("high_count") or 0),
                "total_savings_ms": round(ms_total, 1),
                "total_savings_kib": round(bytes_total / 1024.0, 1),
            }
        )
    common_opportunities_rows.sort(
        key=lambda item: (
            -int(item.get("count") or 0),
            -int(item.get("critical_count") or 0),
            -float(item.get("total_savings_ms") or 0.0),
            -float(item.get("total_savings_kib") or 0.0),
        )
    )
    common_opportunities_rows = common_opportunities_rows[:12]

    priority_urls = []
    for site in sites:
        if str(site.get("status") or "").lower() != "success":
            priority_urls.append(
                {
                    "url": str(site.get("url") or ""),
                    "status": "error",
                    "score": None,
                    "reason": str(site.get("error") or "scan error"),
                }
            )
            continue
        site_summary = site.get("summary") or {}
        site_score = _as_float(site_summary.get("performance_score"))
        cwv = str(site_summary.get("core_web_vitals_status") or "unknown").lower()
        top_issue = ""
        opps = site.get("opportunities") or []
        if isinstance(opps, list) and opps:
            top_issue = str((opps[0] or {}).get("title") or "")
        reason = top_issue or f"CWV: {cwv}"
        priority_urls.append(
            {
                "url": str(site.get("url") or ""),
                "status": cwv,
                "score": site_score,
                "reason": reason,
            }
        )
    priority_urls.sort(
        key=lambda item: (
            0 if str(item.get("status") or "") == "error" else 1,
            0 if str(item.get("status") or "") == "poor" else 1 if str(item.get("status") or "") == "needs_improvement" else 2,
            float(item.get("score") if item.get("score") is not None else 101.0),
        )
    )
    priority_urls = priority_urls[:8]

    batch_action_plan = []
    for action, count in sorted(plan_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:8]:
        batch_action_plan.append(
            {
                "action": action,
                "affected_urls": count,
                "priority": "P1" if count >= max(2, math.ceil(total_urls * 0.5)) else "P2",
            }
        )

    return {
        "mode": "batch",
        "strategy": strategy,
        "source": source,
        "summary": {
            "total_urls": total_urls,
            "successful_urls": successful_urls,
            "failed_urls": failed_count,
            "average_performance_score": avg_score,
            "median_performance_score": median_score,
            "min_performance_score": min_score,
            "max_performance_score": max_score,
            "core_web_vitals_status": batch_status,
            "status_counts": status_counts,
            "metrics_average": {
                "lcp_ms": round(sum(metric_lcp_values) / len(metric_lcp_values), 1) if metric_lcp_values else None,
                "inp_ms": round(sum(metric_inp_values) / len(metric_inp_values), 1) if metric_inp_values else None,
                "cls": round(sum(metric_cls_values) / len(metric_cls_values), 3) if metric_cls_values else None,
            },
            "categories_average": {
                "performance": round(sum(category_acc["performance"]) / len(category_acc["performance"]), 1)
                if category_acc["performance"]
                else None,
                "accessibility": round(sum(category_acc["accessibility"]) / len(category_acc["accessibility"]), 1)
                if category_acc["accessibility"]
                else None,
                "best_practices": round(sum(category_acc["best_practices"]) / len(category_acc["best_practices"]), 1)
                if category_acc["best_practices"]
                else None,
                "seo": round(sum(category_acc["seo"]) / len(category_acc["seo"]), 1) if category_acc["seo"] else None,
            },
            "risk_counts": risk_counts,
        },
        "sites": sites,
        "failed_urls": failed_urls,
        "recommendations": recommendations,
        "common_opportunities": common_opportunities_rows,
        "priority_urls": priority_urls,
        "action_plan": batch_action_plan,
        "checked_at": datetime.utcnow().isoformat(),
    }


def _build_core_web_vitals_competitor_result(
    *,
    strategy: str,
    source: str,
    sites: List[Dict[str, Any]],
) -> Dict[str, Any]:
    base = _build_core_web_vitals_batch_result(strategy=strategy, source=source, sites=sites)

    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _median(values: List[float]) -> Optional[float]:
        arr = [float(x) for x in values if x is not None]
        if not arr:
            return None
        arr.sort()
        n = len(arr)
        mid = n // 2
        if n % 2 == 1:
            return arr[mid]
        return (arr[mid - 1] + arr[mid]) / 2.0

    def _site_is_success(site: Dict[str, Any]) -> bool:
        return str(site.get("status") or "").strip().lower() == "success"

    def _extract_metric(site: Dict[str, Any], metric_key: str, field_key: str, lab_key: str) -> Optional[float]:
        metrics = site.get("metrics") or {}
        payload = metrics.get(metric_key) or {}
        value = _as_float(payload.get(field_key))
        if value is None:
            value = _as_float(payload.get(lab_key))
        return value

    def _extract_site_snapshot(site: Dict[str, Any]) -> Dict[str, Any]:
        summary = site.get("summary") or {}
        opportunities = site.get("opportunities") or []
        recommendations = site.get("recommendations") or []
        top_focus = ""
        if isinstance(opportunities, list) and opportunities and isinstance(opportunities[0], dict):
            top_focus = str((opportunities[0] or {}).get("title") or "")
        if not top_focus and isinstance(recommendations, list) and recommendations:
            top_focus = str(recommendations[0] or "")
        return {
            "url": str(site.get("url") or ""),
            "status": str(site.get("status") or "error").lower(),
            "score": _as_float(summary.get("performance_score")),
            "cwv_status": str(summary.get("core_web_vitals_status") or "unknown").lower(),
            "lcp_ms": _extract_metric(site, "lcp", "field_value_ms", "lab_value_ms"),
            "inp_ms": _extract_metric(site, "inp", "field_value_ms", "lab_value_ms"),
            "cls": _extract_metric(site, "cls", "field_value", "lab_value"),
            "risk_level": str((site.get("analysis") or {}).get("risk_level") or "unknown").lower(),
            "top_focus": top_focus,
            "error": str(site.get("error") or ""),
        }

    def _rank_key(item: Dict[str, Any]) -> Tuple[int, float, float, float, float]:
        status_rank = {
            "good": 0,
            "needs_improvement": 1,
            "poor": 2,
            "unknown": 3,
        }.get(str(item.get("cwv_status") or "unknown"), 3)
        score = float(item.get("score") if item.get("score") is not None else -1.0)
        lcp = float(item.get("lcp_ms") if item.get("lcp_ms") is not None else 1e12)
        inp = float(item.get("inp_ms") if item.get("inp_ms") is not None else 1e12)
        cls = float(item.get("cls") if item.get("cls") is not None else 1e12)
        return (status_rank, -score, lcp, inp, cls)

    snapshots = [_extract_site_snapshot(site) for site in sites]
    primary = snapshots[0] if snapshots else {
        "url": "",
        "status": "error",
        "score": None,
        "cwv_status": "unknown",
        "lcp_ms": None,
        "inp_ms": None,
        "cls": None,
        "risk_level": "unknown",
        "top_focus": "",
        "error": "missing primary site",
    }
    competitors = snapshots[1:] if len(snapshots) > 1 else []

    successful = [item for item in snapshots if item.get("status") == "success"]
    successful_competitors = [item for item in competitors if item.get("status") == "success"]
    failed_competitors = [item for item in competitors if item.get("status") != "success"]

    ranked_success = sorted(successful, key=_rank_key)
    leader = ranked_success[0] if ranked_success else None
    primary_rank: Optional[int] = None
    for idx, item in enumerate(ranked_success, start=1):
        if str(item.get("url") or "") == str(primary.get("url") or ""):
            primary_rank = idx
            break

    peer_pool = successful_competitors if successful_competitors else [item for item in successful if item.get("url") != primary.get("url")]
    peer_scores = [item.get("score") for item in peer_pool if item.get("score") is not None]
    peer_lcp = [item.get("lcp_ms") for item in peer_pool if item.get("lcp_ms") is not None]
    peer_inp = [item.get("inp_ms") for item in peer_pool if item.get("inp_ms") is not None]
    peer_cls = [item.get("cls") for item in peer_pool if item.get("cls") is not None]

    benchmark = {
        "primary_url": primary.get("url"),
        "primary_rank": primary_rank,
        "total_ranked": len(ranked_success),
        "market_leader_url": (leader or {}).get("url"),
        "market_leader_score": round(float((leader or {}).get("score")), 1) if leader and leader.get("score") is not None else None,
        "competitor_median_score": round(_median(peer_scores), 1) if peer_scores else None,
        "competitor_median_lcp_ms": round(_median(peer_lcp), 1) if peer_lcp else None,
        "competitor_median_inp_ms": round(_median(peer_inp), 1) if peer_inp else None,
        "competitor_median_cls": round(_median(peer_cls), 3) if peer_cls else None,
    }

    primary_score = _as_float(primary.get("score"))
    primary_lcp = _as_float(primary.get("lcp_ms"))
    primary_inp = _as_float(primary.get("inp_ms"))
    primary_cls = _as_float(primary.get("cls"))

    comparison_rows: List[Dict[str, Any]] = []
    for item in competitors:
        score = _as_float(item.get("score"))
        lcp = _as_float(item.get("lcp_ms"))
        inp = _as_float(item.get("inp_ms"))
        cls = _as_float(item.get("cls"))
        comparison_rows.append(
            {
                "url": item.get("url"),
                "status": item.get("status"),
                "cwv_status": item.get("cwv_status"),
                "score": round(score, 1) if score is not None else None,
                "score_delta_vs_primary": round(score - primary_score, 1) if score is not None and primary_score is not None else None,
                "lcp_ms": round(lcp, 1) if lcp is not None else None,
                "lcp_delta_ms_vs_primary": round(lcp - primary_lcp, 1) if lcp is not None and primary_lcp is not None else None,
                "inp_ms": round(inp, 1) if inp is not None else None,
                "inp_delta_ms_vs_primary": round(inp - primary_inp, 1) if inp is not None and primary_inp is not None else None,
                "cls": round(cls, 3) if cls is not None else None,
                "cls_delta_vs_primary": round(cls - primary_cls, 3) if cls is not None and primary_cls is not None else None,
                "top_focus": item.get("top_focus") or "",
                "risk_level": item.get("risk_level") or "unknown",
                "error": item.get("error") or "",
            }
        )

    gaps_for_primary: List[str] = []
    strengths_of_primary: List[str] = []

    median_score = _as_float(benchmark.get("competitor_median_score"))
    median_lcp = _as_float(benchmark.get("competitor_median_lcp_ms"))
    median_inp = _as_float(benchmark.get("competitor_median_inp_ms"))
    median_cls = _as_float(benchmark.get("competitor_median_cls"))

    if primary.get("status") != "success":
        gaps_for_primary.append("Primary URL завершился с ошибкой сканирования; сравнение с рынком неполное.")
    else:
        if median_score is not None and primary_score is not None:
            delta = round(primary_score - median_score, 1)
            if delta < 0:
                gaps_for_primary.append(f"Performance score ниже медианы конкурентов на {abs(delta)} п.")
            elif delta > 0:
                strengths_of_primary.append(f"Performance score выше медианы конкурентов на {delta} п.")
        if median_lcp is not None and primary_lcp is not None:
            delta = round(primary_lcp - median_lcp, 1)
            if delta > 0:
                gaps_for_primary.append(f"LCP медленнее медианы конкурентов на {delta} ms.")
            elif delta < 0:
                strengths_of_primary.append(f"LCP быстрее медианы конкурентов на {abs(delta)} ms.")
        if median_inp is not None and primary_inp is not None:
            delta = round(primary_inp - median_inp, 1)
            if delta > 0:
                gaps_for_primary.append(f"INP хуже медианы конкурентов на {delta} ms.")
            elif delta < 0:
                strengths_of_primary.append(f"INP лучше медианы конкурентов на {abs(delta)} ms.")
        if median_cls is not None and primary_cls is not None:
            delta = round(primary_cls - median_cls, 3)
            if delta > 0:
                gaps_for_primary.append(f"CLS выше (хуже) медианы конкурентов на {delta}.")
            elif delta < 0:
                strengths_of_primary.append(f"CLS ниже (лучше) медианы конкурентов на {abs(delta)}.")
        if str(primary.get("cwv_status") or "unknown") != "good" and any(
            str(item.get("cwv_status") or "unknown") == "good" for item in successful_competitors
        ):
            gaps_for_primary.append("Primary URL не в статусе GOOD по CWV, при этом у конкурентов есть GOOD-результаты.")

    competitor_common: Dict[str, Dict[str, Any]] = {}
    for original_site in sites[1:]:
        if not _site_is_success(original_site):
            continue
        for opp in (original_site.get("opportunities") or []):
            if not isinstance(opp, dict):
                continue
            title = str(opp.get("title") or opp.get("id") or "").strip()
            if not title:
                continue
            key = str(opp.get("id") or title)
            bucket = competitor_common.setdefault(
                key,
                {
                    "id": str(opp.get("id") or ""),
                    "title": title,
                    "group": str(opp.get("group") or ""),
                    "count": 0,
                },
            )
            bucket["count"] = int(bucket.get("count") or 0) + 1
    competitor_common_rows = list(competitor_common.values())
    competitor_common_rows.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("title") or "")))
    competitor_common_rows = competitor_common_rows[:8]

    primary_plan: List[Dict[str, Any]] = []
    if sites and _site_is_success(sites[0]):
        plan_raw = sites[0].get("action_plan") or []
        if isinstance(plan_raw, list):
            primary_plan = [item for item in plan_raw if isinstance(item, dict)][:12]

    recommendations: List[str] = []
    recommendations.extend(gaps_for_primary[:5])
    recommendations.extend(str(item) for item in strengths_of_primary[:3])
    for item in competitor_common_rows[:3]:
        recommendations.append(
            f"У конкурентов часто встречается: {item.get('title')} ({item.get('count')} URL)."
        )
    if not recommendations:
        recommendations.extend(base.get("recommendations") or [])
    recommendations = recommendations[:12]

    summary = dict(base.get("summary") or {})
    summary.update(
        {
            "analysis_profile": "competitor",
            "primary_url": primary.get("url"),
            "primary_status": primary.get("status"),
            "primary_score": round(primary_score, 1) if primary_score is not None else None,
            "primary_cwv_status": primary.get("cwv_status"),
            "primary_rank": f"{primary_rank}/{len(ranked_success)}" if primary_rank is not None and ranked_success else None,
            "market_leader_url": benchmark.get("market_leader_url"),
            "market_leader_score": benchmark.get("market_leader_score"),
            "competitors_total": max(0, len(sites) - 1),
            "competitors_success": len(successful_competitors),
            "competitors_failed": len(failed_competitors),
        }
    )

    primary_payload: Dict[str, Any] = {}
    if sites:
        primary_raw = sites[0]
        primary_payload = {
            "url": str(primary_raw.get("url") or ""),
            "status": str(primary_raw.get("status") or "error"),
            "summary": primary_raw.get("summary") or {},
            "metrics": primary_raw.get("metrics") or {},
            "categories": primary_raw.get("categories") or {},
            "diagnostics": primary_raw.get("diagnostics") or {},
            "analysis": primary_raw.get("analysis") or {},
            "opportunities": primary_raw.get("opportunities") or [],
            "recommendations": primary_raw.get("recommendations") or [],
            "action_plan": primary_raw.get("action_plan") or [],
            "error": str(primary_raw.get("error") or ""),
        }

    return {
        "mode": "competitor",
        "strategy": strategy,
        "source": source,
        "summary": summary,
        "primary": primary_payload,
        "competitors": sites[1:] if len(sites) > 1 else [],
        "comparison_rows": comparison_rows,
        "benchmark": benchmark,
        "gaps_for_primary": gaps_for_primary,
        "strengths_of_primary": strengths_of_primary,
        "common_opportunities": competitor_common_rows,
        "sites": sites,
        "failed_urls": base.get("failed_urls") or [],
        "recommendations": recommendations,
        "action_plan": primary_plan if primary_plan else (base.get("action_plan") or []),
        "checked_at": datetime.utcnow().isoformat(),
    }


@router.post("/tasks/site-analyze")
async def create_site_analyze(data: SiteAnalyzeRequest):
    """Full site analysis with multi-page crawling"""
    url = data.url
    max_pages = min(data.max_pages, 50)
    
    print(f"[API] Full site analysis for: {url} (max_pages={max_pages})")
    
    result = check_site_full(url, max_pages)
    task_id = f"site-{datetime.now().timestamp()}"
    create_task_result(task_id, "site_analyze", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Site analysis completed"
    }






@router.post("/tasks/core-web-vitals")
async def create_core_web_vitals(data: CoreWebVitalsRequest, background_tasks: BackgroundTasks):
    """Run Core Web Vitals scan via PageSpeed Insights API (single, batch, competitor compare)."""
    strategy = str(data.strategy or "desktop").strip().lower()
    if strategy not in {"mobile", "desktop"}:
        strategy = "desktop"
    scan_mode = str(data.scan_mode or "single").strip().lower()
    if scan_mode not in {"single", "batch"}:
        scan_mode = "single"
    competitor_mode = bool(getattr(data, "competitor_mode", False))
    if competitor_mode:
        scan_mode = "batch"

    max_batch_urls = 10
    raw_batch_urls = [str(item or "").strip() for item in (data.batch_urls or []) if str(item or "").strip()]
    if scan_mode == "batch" and not raw_batch_urls and str(data.url or "").strip():
        raw_batch_urls = [str(data.url).strip()]

    if scan_mode == "batch":
        if not raw_batch_urls:
            raise HTTPException(status_code=422, detail="Добавьте хотя бы один URL для batch Core Web Vitals сканирования.")
        if len(raw_batch_urls) > max_batch_urls:
            raise HTTPException(status_code=422, detail=f"Лимит batch Core Web Vitals: максимум {max_batch_urls} URL.")

        normalized_urls: List[str] = []
        seen = set()
        invalid_urls: List[str] = []
        for raw_value in raw_batch_urls:
            normalized = _normalize_http_input(raw_value)
            if not normalized:
                invalid_urls.append(raw_value)
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            normalized_urls.append(normalized)

        if invalid_urls:
            preview = ", ".join(invalid_urls[:3])
            raise HTTPException(
                status_code=422,
                detail=f"Некорректные URL в batch-списке: {preview}",
            )
        if not normalized_urls:
            raise HTTPException(status_code=422, detail="Не удалось подготовить список URL для batch сканирования.")
        if len(normalized_urls) > max_batch_urls:
            raise HTTPException(status_code=422, detail=f"Лимит batch Core Web Vitals: максимум {max_batch_urls} URL.")
        if competitor_mode and len(normalized_urls) < 2:
            raise HTTPException(
                status_code=422,
                detail="Для режима анализа конкурентов укажите минимум 2 URL: первый — ваш сайт, далее конкуренты.",
            )

        task_id = f"cwv-{datetime.now().timestamp()}"
        create_task_pending(task_id, "core_web_vitals", normalized_urls[0], status_message="Задача поставлена в очередь")
        print(
            f"[API] Core Web Vitals batch queued: urls={len(normalized_urls)}, "
            f"strategy={strategy}, competitor_mode={competitor_mode}, task_id={task_id}"
        )

        def _run_core_web_vitals_batch_task() -> None:
            total = len(normalized_urls)
            sites: List[Dict[str, Any]] = []
            source = "pagespeed_insights_api"
            try:
                update_task_state(
                    task_id,
                    status="RUNNING",
                    progress=5,
                    status_message=(
                        "Подготовка конкурентного Core Web Vitals сравнения"
                        if competitor_mode
                        else "Подготовка batch Core Web Vitals сканирования"
                    ),
                    progress_meta={
                        "processed_pages": 0,
                        "total_pages": total,
                        "queue_size": total,
                        "current_url": normalized_urls[0] if normalized_urls else "",
                        "competitor_mode": competitor_mode,
                    },
                )

                for index, target_url in enumerate(normalized_urls, start=1):
                    before_progress = 5 + int(((index - 1) / max(1, total)) * 85)
                    update_task_state(
                        task_id,
                        status="RUNNING",
                        progress=min(95, max(5, before_progress)),
                        status_message=f"Core Web Vitals: {index}/{total}",
                        progress_meta={
                            "processed_pages": index - 1,
                            "total_pages": total,
                            "queue_size": max(0, total - index + 1),
                            "current_url": target_url,
                        },
                    )

                    try:
                        scan_result = check_core_web_vitals(target_url, strategy=strategy)
                        payload = scan_result.get("results", {}) if isinstance(scan_result, dict) else {}
                        summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
                        metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
                        categories = payload.get("categories", {}) if isinstance(payload, dict) else {}
                        diagnostics = payload.get("diagnostics", {}) if isinstance(payload, dict) else {}
                        analysis = payload.get("analysis", {}) if isinstance(payload, dict) else {}
                        opportunities = payload.get("opportunities", []) if isinstance(payload, dict) else []
                        recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
                        action_plan = payload.get("action_plan", []) if isinstance(payload, dict) else []
                        source = str(payload.get("source") or source)
                        sites.append(
                            {
                                "url": str(scan_result.get("url") or target_url),
                                "status": "success",
                                "summary": summary,
                                "metrics": metrics,
                                "categories": categories,
                                "diagnostics": diagnostics,
                                "analysis": analysis,
                                "opportunities": opportunities[:8] if isinstance(opportunities, list) else [],
                                "recommendations": recommendations if isinstance(recommendations, list) else [],
                                "action_plan": action_plan[:8] if isinstance(action_plan, list) else [],
                                "checked_at": payload.get("checked_at"),
                            }
                        )
                    except Exception as exc:
                        sites.append(
                            {
                                "url": target_url,
                                "status": "error",
                                "error": str(exc),
                            }
                        )

                    after_progress = 5 + int((index / max(1, total)) * 85)
                    update_task_state(
                        task_id,
                        status="RUNNING",
                        progress=min(95, max(5, after_progress)),
                        status_message=f"Core Web Vitals: {index}/{total} завершено",
                        progress_meta={
                            "processed_pages": index,
                            "total_pages": total,
                            "queue_size": max(0, total - index),
                            "current_url": target_url,
                        },
                    )

                batch_payload = _build_core_web_vitals_batch_result(
                    strategy=strategy,
                    source=source,
                    sites=sites,
                )
                if competitor_mode:
                    batch_payload = _build_core_web_vitals_competitor_result(
                        strategy=strategy,
                        source=source,
                        sites=sites,
                    )
                failed_count = int((batch_payload.get("summary") or {}).get("failed_urls") or 0)
                success_count = int((batch_payload.get("summary") or {}).get("successful_urls") or 0)
                status_message = (
                    f"Batch Core Web Vitals завершен: успех {success_count}, ошибки {failed_count}"
                    if failed_count > 0
                    else f"Batch Core Web Vitals завершен: проверено {success_count} URL"
                )
                if competitor_mode:
                    status_message = (
                        f"Конкурентный анализ CWV завершен: успех {success_count}, ошибки {failed_count}"
                        if failed_count > 0
                        else f"Конкурентный анализ CWV завершен: сравнение по {success_count} URL"
                    )
                result_payload = {
                    "task_type": "core_web_vitals",
                    "url": normalized_urls[0],
                    "results": batch_payload,
                }
                update_task_state(
                    task_id,
                    status="SUCCESS",
                    progress=100,
                    status_message=status_message,
                    progress_meta={
                        "processed_pages": total,
                        "total_pages": total,
                        "queue_size": 0,
                        "current_url": normalized_urls[-1] if normalized_urls else "",
                        "competitor_mode": competitor_mode,
                    },
                    result=result_payload,
                    error=None,
                )
            except Exception as exc:
                update_task_state(
                    task_id,
                    status="FAILURE",
                    progress=100,
                    status_message="Ошибка batch Core Web Vitals сканирования",
                    error=str(exc),
                )

        background_tasks.add_task(_run_core_web_vitals_batch_task)
        return {
            "task_id": task_id,
            "status": "PENDING",
            "message": "Core Web Vitals competitor scan queued" if competitor_mode else "Core Web Vitals batch scan queued",
        }

    url = _normalize_http_input(data.url or "")
    if not url:
        raise HTTPException(status_code=422, detail="Введите корректный URL сайта (домен или http/https URL).")

    task_id = f"cwv-{datetime.now().timestamp()}"
    create_task_pending(task_id, "core_web_vitals", url, status_message="Задача поставлена в очередь")
    print(f"[API] Core Web Vitals single queued for: {url}, strategy={strategy}, task_id={task_id}")

    def _run_core_web_vitals_single_task() -> None:
        try:
            update_task_state(
                task_id,
                status="RUNNING",
                progress=10,
                status_message="Запуск Core Web Vitals сканирования",
                progress_meta={
                    "processed_pages": 0,
                    "total_pages": 1,
                    "queue_size": 1,
                    "current_url": url,
                },
            )
            result = check_core_web_vitals(url, strategy=strategy)
            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Core Web Vitals scan completed",
                progress_meta={
                    "processed_pages": 1,
                    "total_pages": 1,
                    "queue_size": 0,
                    "current_url": url,
                },
                result=result,
                error=None,
            )
        except ValueError as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Ошибка Core Web Vitals сканирования",
                error=str(exc),
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Ошибка Core Web Vitals сканирования",
                error=f"Core Web Vitals scan failed: {exc}",
            )

    background_tasks.add_task(_run_core_web_vitals_single_task)
    return {
        "task_id": task_id,
        "status": "PENDING",
        "message": "Core Web Vitals scan queued",
    }


