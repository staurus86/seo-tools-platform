"""Scoring model for LLM Crawler Simulation (MVP v1)."""
from __future__ import annotations

from typing import Any, Dict, List


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _contains_token(value: str, token: str) -> bool:
    return token.lower() in str(value or "").lower()


def compute_score(
    *,
    nojs: Dict[str, Any],
    rendered: Dict[str, Any] | None,
    diff: Dict[str, Any],
    policies: Dict[str, Any],
) -> Dict[str, Any]:
    top_issues: List[str] = []

    # Access (0-35)
    access = 0.0
    status_code = int((nojs.get("status_code") or 0))
    if 200 <= status_code < 400:
        access = 35.0
    elif 400 <= status_code < 500:
        if status_code in {401, 403, 429}:
            access = 5.0
        elif status_code == 404:
            access = 15.0
        else:
            access = 10.0
        top_issues.append(f"HTTP {status_code}: low accessibility for crawlers")
    elif status_code >= 500:
        access = 0.0
        top_issues.append(f"HTTP {status_code}: server errors reduce crawlability")

    challenge = (nojs.get("challenge") or {})
    if bool(challenge.get("is_challenge")):
        access -= 15
        top_issues.append("Challenge/WAF detected: can block AI/search crawling")

    meta = (nojs.get("meta") or {})
    meta_robots = str(meta.get("meta_robots") or "").lower()
    x_robots = str(meta.get("x_robots_tag") or "").lower()
    if _contains_token(meta_robots, "noindex") or _contains_token(x_robots, "noindex"):
        access -= 25
        top_issues.append("noindex signal found (meta/X-Robots-Tag)")
    if _contains_token(meta_robots, "nosnippet") or _contains_token(x_robots, "nosnippet"):
        access -= 10
        top_issues.append("nosnippet signal can limit LLM extractability")

    profiles = ((policies.get("robots") or {}).get("profiles") or {})
    ai_profile = profiles.get("ai-bot") or {}
    if ai_profile and not bool(ai_profile.get("allowed", True)):
        access -= 8
        top_issues.append("robots.txt disallow for ai-bot profile")
    access = _clamp(access, 0.0, 35.0)

    # Content (0-30)
    content = 0.0
    rendered_text_len = int((((rendered or {}).get("content") or {}).get("main_text_length") or 0))
    nojs_text_len = int((((nojs or {}).get("content") or {}).get("main_text_length") or 0))
    baseline_text_len = rendered_text_len if rendered_text_len > 0 else nojs_text_len
    if baseline_text_len < 300:
        content += 5
        top_issues.append("Low extracted text volume (<300 chars)")
    elif baseline_text_len <= 1200:
        content += 15
    else:
        content += 25

    text_coverage = diff.get("textCoverage")
    try:
        coverage_val = float(text_coverage) if text_coverage is not None else None
    except Exception:
        coverage_val = None
    if coverage_val is not None:
        if coverage_val > 0.7:
            content += 5
        elif coverage_val >= 0.3:
            content += 2
        else:
            top_issues.append("Low No-JS text coverage vs rendered (<0.3)")
    content = _clamp(content, 0.0, 30.0)

    # Structure (0-20)
    structure = 0.0
    source = rendered if rendered else nojs
    headings = (source.get("headings") or {})
    h1 = int(headings.get("h1") or 0)
    h2 = int(headings.get("h2") or 0)
    h3 = int(headings.get("h3") or 0)
    if h1 > 0:
        structure += 6
    else:
        top_issues.append("Missing H1")
    structure += min(8.0, (min(h2, 4) * 1.5) + (min(h3, 4) * 0.5))
    struct = (source.get("structure") or {})
    if int(struct.get("lists_count") or 0) > 0:
        structure += 3
    if int(struct.get("tables_count") or 0) > 0:
        structure += 3
    if int(struct.get("lists_count") or 0) == 0 and int(struct.get("tables_count") or 0) == 0:
        top_issues.append("No list/table structures detected")
    structure = _clamp(structure, 0.0, 20.0)

    # Signals (0-30)
    signals_score = 0.0
    schema = (source.get("schema") or {})
    schema_types = schema.get("jsonld_types") or []
    schema_bonus = min(10.0, float(len(schema_types) * 2))
    signals_score += schema_bonus
    signals = (source.get("signals") or {})
    if bool(signals.get("author_present")):
        signals_score += 4
    if bool(signals.get("date_present")):
        signals_score += 3
    if not bool(signals.get("author_present")) and not bool(signals.get("date_present")):
        top_issues.append("No author/date trust signals detected")
    # Core Web Vitals signals (from policies["cwv"] if present)
    cwv = (policies.get("cwv") or {}).get("summary") or {}
    lcp = cwv.get("lcp_ms")
    cls = cwv.get("cls")
    inp = cwv.get("inp_ms")
    if lcp is not None:
        lcp_val = float(lcp)
        if lcp_val <= 2500:
            signals_score += 5
        elif lcp_val <= 4000:
            signals_score += 2
        else:
            top_issues.append(f"High LCP ({int(lcp_val)} ms)")
    if cls is not None:
        cls_val = float(cls)
        if cls_val <= 0.1:
            signals_score += 3
        elif cls_val <= 0.25:
            signals_score += 1
        else:
            top_issues.append(f"High CLS ({cls_val:.2f})")
    if inp is not None:
        inp_val = float(inp)
        if inp_val <= 200:
            signals_score += 2
        elif inp_val <= 500:
            signals_score += 1
        else:
            top_issues.append(f"High INP ({int(inp_val)} ms)")
    signals_score = _clamp(signals_score, 0.0, 30.0)

    total = int(round(_clamp(access + content + structure + signals_score, 0.0, 100.0)))
    top_issues = list(dict.fromkeys(top_issues))[:10]

    return {
        "total": total,
        "breakdown": {
            "access": round(access, 2),
            "content": round(content, 2),
            "structure": round(structure, 2),
            "signals": round(signals_score, 2),
        },
        "top_issues": top_issues,
    }
