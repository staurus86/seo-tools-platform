"""Unified Full SEO Audit — run all tools on a single URL."""
import time
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable


def run_unified_audit(
    *,
    url: str,
    use_proxy: bool = False,
    skip_tools: Optional[list] = None,
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Run all SEO tools on a single URL and produce a combined report."""
    started = time.perf_counter()
    results = {}
    errors = {}
    skip_set = set(s.lower().strip() for s in (skip_tools or []))
    all_tools = [
        ("robots", "Robots.txt Audit"),
        ("sitemap", "Sitemap Validation"),
        ("onpage", "OnPage Audit"),
        ("render", "Render Audit"),
        ("mobile", "Mobile Audit"),
        ("bot_check", "Bot Accessibility"),
        ("redirect", "Redirect Checker"),
        ("cwv", "Core Web Vitals"),
    ]
    tools_order = [(k, n) for k, n in all_tools if k not in skip_set]

    total = len(tools_order)

    for i, (tool_key, tool_name) in enumerate(tools_order):
        pct = int((i / total) * 100) if total > 0 else 100
        if progress_callback:
            progress_callback(progress=pct, status_message=f"[{i+1}/{total}] {tool_name}...")

        try:
            result = _run_tool(tool_key, url, use_proxy)
            results[tool_key] = result
        except Exception as e:
            errors[tool_key] = str(e)  # string, not dict — for clean JS rendering

    duration_ms = int((time.perf_counter() - started) * 1000)

    # Build combined scores (only for tools that ran successfully)
    scores = _extract_scores(results)

    # Generate developer task specs (ТЗ)
    dev_tasks = _generate_dev_tasks(results, scores, url)

    # Overall grade — only count tools that actually ran
    overall_score = _calculate_overall_score(scores)

    return {
        "task_type": "unified_audit",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "duration_ms": duration_ms,
        "tools_run": len(results),
        "tools_failed": len(errors),
        "overall_score": overall_score,
        "overall_grade": _score_to_grade(overall_score),
        "scores": scores,
        "results": results,
        "errors": errors,
        "dev_tasks": dev_tasks,
        "skipped_tools": list(skip_set),
    }


def _run_tool(tool_key: str, url: str, use_proxy: bool) -> dict:
    if tool_key == "robots":
        import asyncio
        from app.api.routers.robots import check_robots_full_async
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(check_robots_full_async(url, use_proxy=use_proxy))
        finally:
            loop.close()

    elif tool_key == "sitemap":
        import asyncio
        from app.api.routers.robots import check_sitemap_full_async
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(check_sitemap_full_async(url, use_proxy=use_proxy))
        finally:
            loop.close()

    elif tool_key == "onpage":
        from app.tools.onpage import OnPageAuditServiceV1
        svc = OnPageAuditServiceV1()
        return svc.run(url=url, use_proxy=use_proxy)

    elif tool_key == "render":
        from app.tools.render.service_v2 import RenderAuditServiceV2
        svc = RenderAuditServiceV2(use_proxy=use_proxy)
        return svc.run(url=url, task_id=f"unified-render-{time.time()}")

    elif tool_key == "mobile":
        from app.tools.mobile.service_v2 import MobileCheckServiceV2
        svc = MobileCheckServiceV2(use_proxy=use_proxy)
        return svc.run(url=url, task_id=f"unified-mobile-{time.time()}", mode="quick")

    elif tool_key == "bot_check":
        from app.tools.bots.service_v2 import BotAccessibilityServiceV2
        svc = BotAccessibilityServiceV2(use_proxy=use_proxy)
        return svc.run(url)

    elif tool_key == "redirect":
        from app.tools.redirect_checker import run_redirect_checker
        return run_redirect_checker(url=url, use_proxy=use_proxy)

    elif tool_key == "cwv":
        from app.tools.core_web_vitals import run_core_web_vitals
        return run_core_web_vitals(url=url, combined=True, use_proxy=use_proxy)

    raise ValueError(f"Unknown tool: {tool_key}")


def _deep_get(d: dict, *keys, default=0):
    """Safely traverse nested dicts."""
    current = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, {})
        else:
            return default
    return current if current != {} else default


def _extract_scores(results: dict) -> dict:
    scores = {}

    # OnPage score — may be at result.score or result.results.score
    if "onpage" in results:
        onpage = results["onpage"]
        onpage_score = onpage.get("score") or _deep_get(onpage, "results", "score") or _deep_get(onpage, "results", "scores", "onpage_score") or 0
        scores["onpage"] = round(float(onpage_score), 1) if onpage_score else 0
    else:
        scores["onpage"] = None

    # Render score — look in multiple places.
    # If the tool ran successfully but score resolves to 0, check for meaningful
    # data (seo_checks, raw_snapshot).  A successful render with no JS issues
    # means the page is essentially identical → score ~100.
    if "render" in results:
        render = results["render"]
        render_score = (
            _deep_get(render, "results", "summary", "score") or
            _deep_get(render, "summary", "score") or
            _deep_get(render, "comparison", "score") or
            _deep_get(render, "results", "comparison", "score") or
            _deep_get(render, "results", "score") or
            render.get("score") or 0
        )
        render_score = round(float(render_score), 1) if render_score else 0
        if render_score == 0:
            # Render ran but score is 0 — check if there's real data indicating
            # the tool completed (no JS rendering issues ≈ perfect score).
            render_r = render.get("results", render)
            has_data = (
                render_r.get("seo_checks")
                or render_r.get("raw_snapshot")
                or render_r.get("rendered_snapshot")
                or render_r.get("comparison")
            )
            if has_data:
                render_score = 100.0
        scores["render"] = render_score
    else:
        scores["render"] = None

    # Mobile friendly
    if "mobile" in results:
        mobile = results["mobile"]
        mobile_friendly = mobile.get("mobile_friendly") or _deep_get(mobile, "results", "mobile_friendly")
        scores["mobile_friendly"] = 100 if mobile_friendly else 50
    else:
        scores["mobile_friendly"] = None

    # Bot accessibility
    if "bot_check" in results:
        bot = results["bot_check"]
        bot_summary = bot.get("summary") or _deep_get(bot, "results", "summary") or {}
        accessible = int(bot_summary.get("accessible", 0) or 0)
        non_indexable = int(bot_summary.get("non_indexable", 0) or 0)
        total_bots = accessible + non_indexable
        scores["bot_accessibility"] = round(accessible / max(1, total_bots) * 100) if total_bots > 0 else 0
    else:
        scores["bot_accessibility"] = None

    # Redirect grade — prefer quality_score from the tool itself, fall back to
    # passed/total ratio.  The redirect checker returns results.summary with
    # quality_score (0-100), quality_grade, passed, warnings, errors, total_scenarios.
    if "redirect" in results:
        redirect = results["redirect"]
        redirect_summary = redirect.get("summary") or _deep_get(redirect, "results", "summary") or {}
        quality_score = redirect_summary.get("quality_score")
        if quality_score is not None:
            scores["redirect"] = round(float(quality_score), 1)
        else:
            total_sc = int(redirect_summary.get("total_scenarios", 0) or 0)
            passed = int(redirect_summary.get("passed", 0) or 0)
            scores["redirect"] = round(passed / max(1, total_sc) * 100) if total_sc > 0 else 0
    else:
        scores["redirect"] = None

    # CWV performance — handle combined and single modes.
    # Combined: cwv = {"results": {"combined": True, "mobile": {...}, "desktop": {...}}}
    # Single:   cwv = {"results": {"summary": {"performance_score": N}, "categories": {...}}}
    if "cwv" in results:
        cwv = results["cwv"]
        cwv_r = cwv.get("results", cwv)  # unwrap results wrapper
        if cwv_r.get("combined"):
            mobile_perf = (
                _deep_get(cwv_r, "mobile", "summary", "performance_score") or
                _deep_get(cwv_r, "mobile", "categories", "performance") or 0
            )
            desktop_perf = (
                _deep_get(cwv_r, "desktop", "summary", "performance_score") or
                _deep_get(cwv_r, "desktop", "categories", "performance") or 0
            )
            scores["cwv_mobile"] = round(float(mobile_perf), 1)
            scores["cwv_desktop"] = round(float(desktop_perf), 1)
            scores["cwv_avg"] = round((float(mobile_perf) + float(desktop_perf)) / 2, 1)
        else:
            perf = (
                _deep_get(cwv_r, "summary", "performance_score") or
                _deep_get(cwv_r, "categories", "performance") or 0
            )
            scores["cwv_avg"] = round(float(perf), 1)
    else:
        scores["cwv_mobile"] = None
        scores["cwv_desktop"] = None
        scores["cwv_avg"] = None

    # Robots
    if "robots" in results:
        robots = results["robots"]
        robots_found = robots.get("robots_txt_found") or _deep_get(robots, "results", "robots_txt_found")
        scores["robots_ok"] = 100 if robots_found else 0
    else:
        scores["robots_ok"] = None

    return scores


def _calculate_overall_score(scores: dict) -> float:
    weights = {
        "onpage": 0.20,
        "render": 0.10,
        "mobile_friendly": 0.15,
        "bot_accessibility": 0.10,
        "redirect": 0.10,
        "cwv_avg": 0.25,
        "robots_ok": 0.10,
    }
    total = 0
    weight_sum = 0
    for key, weight in weights.items():
        val = scores.get(key)
        if val is None:
            continue  # tool failed or was skipped — don't penalize
        if isinstance(val, (int, float)) and val > 0:
            total += float(val) * weight
            weight_sum += weight
    # If some tools didn't run/failed, normalize by actual weight sum
    return round(total / max(0.01, weight_sum), 1)


def _score_to_grade(score: float) -> str:
    if score >= 90:
        return "A+"
    if score >= 80:
        return "A"
    if score >= 70:
        return "B"
    if score >= 60:
        return "C"
    if score >= 50:
        return "D"
    return "F"


def _generate_dev_tasks(results: dict, scores: dict, url: str) -> list:
    """Generate developer task specifications (ТЗ) from audit results."""
    tasks = []

    # --- OnPage issues ---
    onpage = results.get("onpage", {})
    onpage_r = onpage.get("results", onpage)
    for issue in (onpage_r.get("issues") or []):
        severity = issue.get("severity", "info")
        priority = "P1" if severity == "critical" else "P2" if severity == "warning" else "P3"
        tasks.append({
            "priority": priority,
            "category": "SEO / Content",
            "source_tool": "OnPage Audit",
            "title": issue.get("title", ""),
            "description": issue.get("details", ""),
            "owner": "SEO",
        })

    # --- Render issues ---
    render = results.get("render", {})
    render_r = render.get("results", render)
    seo_checks = render_r.get("seo_checks", {})
    if isinstance(seo_checks, dict):
        seo_checks = seo_checks.get("items", [])
    for check in (seo_checks or []):
        if check.get("status") in ("fail", "warn"):
            tasks.append({
                "priority": "P1" if check.get("severity") == "critical" else "P2",
                "category": "Frontend / Rendering",
                "source_tool": "Render Audit",
                "title": check.get("label", ""),
                "description": check.get("details", ""),
                "fix": check.get("fix", ""),
                "owner": "Frontend",
            })

    # --- Mobile issues (deduplicate portrait/landscape duplicates by title) ---
    mobile = results.get("mobile", {})
    mobile_r = mobile.get("results", mobile)
    seen_mobile_titles = set()
    for issue in (mobile_r.get("issues") or []):
        title = issue.get("title", "")
        if title in seen_mobile_titles:
            continue
        seen_mobile_titles.add(title)
        tasks.append({
            "priority": "P1" if issue.get("severity") == "critical" else "P2",
            "category": "Frontend / Mobile",
            "source_tool": "Mobile Audit",
            "title": title,
            "description": issue.get("details", ""),
            "owner": "Frontend",
        })

    # --- Redirect issues ---
    redirect = results.get("redirect", {})
    redirect_r = redirect.get("results", redirect)
    for scenario in (redirect_r.get("scenarios") or []):
        if scenario.get("status") in ("failed", "warning"):
            tasks.append({
                "priority": "P1" if scenario.get("status") == "failed" else "P2",
                "category": "DevOps / Redirects",
                "source_tool": "Redirect Checker",
                "title": scenario.get("title", ""),
                "description": scenario.get("recommendation", ""),
                "owner": "DevOps",
            })

    # --- CWV opportunities ---
    cwv = results.get("cwv", {})
    cwv_data = cwv.get("mobile", cwv) if cwv.get("combined") else cwv
    for opp in (cwv_data.get("opportunities") or [])[:10]:
        if opp.get("priority") in ("critical", "high"):
            tasks.append({
                "priority": "P1" if opp.get("priority") == "critical" else "P2",
                "category": "Performance / CWV",
                "source_tool": "Core Web Vitals",
                "title": opp.get("title", ""),
                "description": (opp.get("description") or "")[:300],
                "savings": f"{opp.get('savings_ms', 0)}ms" if opp.get("savings_ms") else "",
                "owner": opp.get("group", "Frontend"),
            })

    # --- Bot blocker issues ---
    bot = results.get("bot_check", {})
    bot_r = bot.get("results", bot)
    for blocker in (bot_r.get("priority_blockers") or []):
        tasks.append({
            "priority": "P0",
            "category": "DevOps / Access",
            "source_tool": "Bot Checker",
            "title": blocker.get("title", ""),
            "description": f"Affects {blocker.get('affected_bots', 0)} bots",
            "owner": "DevOps",
        })

    # Sort by priority
    priority_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    tasks.sort(key=lambda t: priority_order.get(t.get("priority", "P3"), 99))

    return tasks
