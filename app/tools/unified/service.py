"""Unified Full SEO Audit — run all tools on a single URL."""
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Callable


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
            errors[tool_key] = {"error": str(e), "traceback": traceback.format_exc()[-500:]}

    duration_ms = int((time.perf_counter() - started) * 1000)

    # Build combined scores
    scores = _extract_scores(results)

    # Generate developer task specs (ТЗ)
    dev_tasks = _generate_dev_tasks(results, scores, url)

    # Overall grade
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
        import time as _t
        svc = RenderAuditServiceV2(use_proxy=use_proxy)
        return svc.run(url=url, task_id=f"unified-render-{_t.time()}")

    elif tool_key == "mobile":
        from app.tools.mobile.service_v2 import MobileCheckServiceV2
        import time as _t
        svc = MobileCheckServiceV2(use_proxy=use_proxy, mode="quick")
        return svc.run(url=url, task_id=f"unified-mobile-{_t.time()}")

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


def _extract_scores(results: dict) -> dict:
    scores = {}

    # OnPage score
    onpage = results.get("onpage", {})
    onpage_r = onpage.get("results", onpage)
    scores["onpage"] = onpage_r.get("score", onpage_r.get("scores", {}).get("onpage_score", 0))

    # Render score
    render = results.get("render", {})
    render_r = render.get("results", render)
    comp = render_r.get("comparison", {})
    scores["render"] = comp.get("score", 0) if isinstance(comp, dict) else 0

    # Mobile friendly
    mobile = results.get("mobile", {})
    mobile_r = mobile.get("results", mobile)
    scores["mobile_friendly"] = 100 if mobile_r.get("mobile_friendly", False) else 50

    # Bot accessibility
    bot = results.get("bot_check", {})
    bot_r = bot.get("results", bot)
    bot_summary = bot_r.get("summary", {})
    total_bots = bot_summary.get("accessible", 0) + bot_summary.get("non_indexable", 0)
    scores["bot_accessibility"] = round(bot_summary.get("accessible", 0) / max(1, total_bots) * 100)

    # Redirect grade
    redirect = results.get("redirect", {})
    redirect_r = redirect.get("results", redirect)
    redirect_summary = redirect_r.get("summary", {})
    total_sc = redirect_summary.get("total_scenarios", 23)
    passed = redirect_summary.get("passed", 0)
    scores["redirect"] = round(passed / max(1, total_sc) * 100)

    # CWV performance
    cwv = results.get("cwv", {})
    if cwv.get("combined"):
        mobile_perf = cwv.get("mobile", {}).get("categories_scores", {}).get("performance", 0)
        desktop_perf = cwv.get("desktop", {}).get("categories_scores", {}).get("performance", 0)
        scores["cwv_mobile"] = mobile_perf
        scores["cwv_desktop"] = desktop_perf
        scores["cwv_avg"] = round((mobile_perf + desktop_perf) / 2)
    else:
        scores["cwv_avg"] = cwv.get("categories_scores", {}).get("performance", 0)

    # Robots
    robots = results.get("robots", {})
    robots_r = robots.get("results", robots)
    scores["robots_ok"] = 100 if robots_r.get("robots_txt_found", False) else 0

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
        val = scores.get(key, 0)
        if isinstance(val, (int, float)):
            total += val * weight
            weight_sum += weight
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
    """Generate developer task specifications from audit results."""
    tasks = []

    # --- OnPage issues ---
    onpage = results.get("onpage", {})
    onpage_r = onpage.get("results", onpage)
    for issue in onpage_r.get("issues", []):
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
    for check in render_r.get("seo_checks", {}).get("items", []):
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

    # --- Mobile issues ---
    mobile = results.get("mobile", {})
    mobile_r = mobile.get("results", mobile)
    for issue in mobile_r.get("issues", []):
        tasks.append({
            "priority": "P1" if issue.get("severity") == "critical" else "P2",
            "category": "Frontend / Mobile",
            "source_tool": "Mobile Audit",
            "title": issue.get("title", ""),
            "description": issue.get("details", ""),
            "owner": "Frontend",
        })

    # --- Redirect issues ---
    redirect = results.get("redirect", {})
    redirect_r = redirect.get("results", redirect)
    for scenario in redirect_r.get("scenarios", []):
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
    for opp in cwv_data.get("opportunities", [])[:10]:
        if opp.get("priority") in ("critical", "high"):
            tasks.append({
                "priority": "P1" if opp.get("priority") == "critical" else "P2",
                "category": "Performance / CWV",
                "source_tool": "Core Web Vitals",
                "title": opp.get("title", ""),
                "description": opp.get("description", "")[:300],
                "savings": f"{opp.get('savings_ms', 0)}ms" if opp.get("savings_ms") else "",
                "owner": opp.get("group", "Frontend"),
            })

    # --- Bot blocker issues ---
    bot = results.get("bot_check", {})
    bot_r = bot.get("results", bot)
    for blocker in bot_r.get("priority_blockers", []):
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
