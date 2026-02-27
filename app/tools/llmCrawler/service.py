"""Core execution pipeline for LLM Crawler Simulation."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse
from collections import Counter

import requests
import re
import math

from app.config import settings
from app.tools.http_text import decode_response_text

from .extraction import build_snapshot
from .patterns import DIRECTIVE_RESTRICTIVE_TOKENS
from .policies import evaluate_profile_access, parse_robots_rules
from .scoring import compute_score
from .security import assert_safe_url, normalize_http_url, safe_redirect_target


REDIRECT_STATUSES = {301, 302, 303, 307, 308}
UA_NOJS = "Mozilla/5.0 (compatible; LLMCrawlerNoJS/1.0; +https://example.com/bot)"
UA_RENDER = "Mozilla/5.0 (compatible; LLMCrawlerRendered/1.0; +https://example.com/bot)"
MAX_BROWSER_AGE_SEC = 300
BOT_USER_AGENTS = {
    "gptbot": "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)",
    "googlebot": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
}
STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "your", "you", "are", "was", "were", "into",
    "about", "have", "has", "will", "can", "not", "but", "our", "their", "also", "http", "https", "www",
    "как", "что", "это", "для", "или", "если", "без", "при", "под", "над", "быть", "есть", "так", "как",
    "они", "она", "его", "ее", "это", "the", "page", "home", "menu", "navigation", "read", "more",
}


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


def _fetch_profile_snapshot(
    *,
    profile: str,
    url: str,
    timeout_ms: int,
    max_redirect_hops: int,
    max_html_bytes: int,
    show_headers: bool,
) -> Optional[Dict[str, Any]]:
    user_agent = BOT_USER_AGENTS.get(profile)
    if not user_agent:
        return None
    payload = _fetch_http(
        url=url,
        user_agent=user_agent,
        timeout_ms=timeout_ms,
        max_redirect_hops=max_redirect_hops,
        max_html_bytes=max_html_bytes,
    )
    return build_snapshot(
        html=str(payload.get("body_text") or ""),
        final_url=str(payload.get("final_url") or url),
        status_code=payload.get("status_code"),
        headers=payload.get("headers") or {},
        timing_ms=int(payload.get("timing_ms") or 0),
        redirect_chain=list(payload.get("redirect_chain") or []),
        show_headers=show_headers,
        content_type=str(payload.get("content_type") or ""),
        size_bytes=int(payload.get("size_bytes") or 0),
        truncated=bool(payload.get("truncated")),
    )


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


def _tokens(text: str) -> List[str]:
    words = re.findall(r"[A-Za-zА-Яа-я0-9]{3,}", str(text or "").lower())
    return [w for w in words if w not in STOPWORDS]


def _top_keywords(text: str, limit: int = 6) -> List[str]:
    toks = _tokens(text)
    if not toks:
        return []
    freq = Counter(toks)
    return [w for w, _ in freq.most_common(limit)]


def _tfidf_cosine(question: str, text: str, corpus: List[str]) -> float:
    q_tokens = _tokens(question)
    t_tokens = _tokens(text)
    if not q_tokens or not t_tokens:
        return 0.0
    docs = [set(_tokens(doc)) for doc in corpus if doc]
    if not docs:
        docs = [set(t_tokens)]
    n = len(docs)

    def idf(token: str) -> float:
        df = sum(1 for d in docs if token in d)
        return math.log((n + 1) / (df + 1)) + 1.0

    tf_q = Counter(q_tokens)
    tf_t = Counter(t_tokens)
    keys = set(tf_q.keys()) | set(tf_t.keys())
    if not keys:
        return 0.0
    dot = 0.0
    nq = 0.0
    nt = 0.0
    for k in keys:
        w = idf(k)
        vq = tf_q.get(k, 0) * w
        vt = tf_t.get(k, 0) * w
        dot += vq * vt
        nq += vq * vq
        nt += vt * vt
    if nq <= 0 or nt <= 0:
        return 0.0
    return float(dot / math.sqrt(nq * nt))


def _chunk_entity_density(text: str) -> float:
    words = re.findall(r"[A-Za-zА-Яа-я0-9]+", str(text or ""))
    if not words:
        return 0.0
    entities = re.findall(r"\b[A-ZА-Я][A-Za-zА-Яа-я0-9]{2,}\b", str(text or ""))
    return min(1.0, len(entities) / max(1, len(words)))


def _chunk_content_ratio(text: str) -> float:
    raw = str(text or "")
    if not raw:
        return 0.0
    alpha = len(re.findall(r"[A-Za-zА-Яа-я0-9]", raw))
    return min(1.0, alpha / max(1, len(raw)))


def _rank_chunks_for_question(snapshot: Dict[str, Any], question: str, limit: int = 3) -> List[Dict[str, Any]]:
    chunks = ((snapshot.get("content") or {}).get("chunks") or [])
    if not chunks:
        return []
    corpus = [str(c.get("text") or "") for c in chunks]
    ranked: List[Dict[str, Any]] = []
    for c in chunks:
        text = str(c.get("text") or "")
        rel = _tfidf_cosine(question, text, corpus)
        ent = _chunk_entity_density(text)
        ratio = _chunk_content_ratio(text)
        score = (rel * 0.65) + (ent * 0.2) + (ratio * 0.15)
        ranked.append(
            {
                "idx": int(c.get("idx") or 0),
                "score": round(score, 4),
                "relevance": round(rel, 4),
                "entity_density": round(ent, 4),
                "content_ratio": round(ratio, 4),
                "preview": text[:240],
                "text": text,
            }
        )
    ranked.sort(key=lambda x: x.get("score", 0), reverse=True)
    return ranked[: max(1, int(limit))]


def _sentences(text: str) -> List[str]:
    return [s.strip() for s in re.split(r"[.!?]+", str(text or "")) if s.strip()]


def _build_extractive_preview(chunks: List[Dict[str, Any]], bullets: int = 3) -> List[str]:
    lines: List[str] = []
    for item in chunks:
        sents = _sentences(item.get("text") or "")
        for s in sents[:2]:
            clean = s.strip()
            if len(clean) < 40:
                continue
            if clean in lines:
                continue
            lines.append(clean)
            if len(lines) >= bullets:
                return lines
    return lines


def _module_status(evaluated: bool, reason: str = "", score: Optional[float] = None, factors: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "status": "evaluated" if evaluated else "not_evaluated",
        "reason": reason or ("ok" if evaluated else "not_evaluated"),
        "score": score if evaluated else None,
        "factors": factors or [],
    }


def _content_clarity(snapshot: Dict[str, Any], entity_graph: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    content = snapshot.get("content") or {}
    headings = snapshot.get("headings") or {}
    text_len = int(content.get("main_text_length") or 0)
    if text_len < 120:
        return _module_status(False, "insufficient_content", None, ["Extracted text is too short for reliable clarity scoring"])

    h1_count = int(headings.get("h1") or 0)
    h2_count = int(headings.get("h2") or 0)
    heading_integrity = 100.0 if h1_count >= 1 else 45.0
    heading_integrity = min(100.0, heading_integrity + (min(6, h2_count) * 6))

    main_preview = str(content.get("main_text_preview") or "")
    words = _tokens(main_preview)
    unique_ratio = (len(set(words)) / max(1, len(words))) if words else 0.0
    entity_count = 0
    if entity_graph:
        entity_count = sum(len(v or []) for v in entity_graph.values())
    if entity_count == 0:
        entity_count = len(re.findall(r"\b[A-ZА-Я][A-Za-zА-Яа-я0-9]{2,}\b", main_preview))
    entity_density = min(1.0, entity_count / max(1, len(words)))

    readability = float(content.get("readability_score") or 0.0)
    readability_norm = max(0.0, min(1.0, readability / 100.0))
    boilerplate = float(content.get("boilerplate_ratio") or 0.0)
    boilerplate_score = max(0.0, min(1.0, 1.0 - boilerplate))

    score = (
        (heading_integrity * 0.28)
        + (max(0.0, min(1.0, unique_ratio * 2.0)) * 100 * 0.22)
        + (entity_density * 100 * 0.2)
        + (readability_norm * 100 * 0.2)
        + (boilerplate_score * 100 * 0.1)
    )
    factors = [
        f"Heading integrity: {round(heading_integrity, 2)}",
        f"Unique word ratio: {round(unique_ratio, 3)}",
        f"Entity density: {round(entity_density, 3)}",
        f"Readability normalized: {round(readability_norm, 3)}",
        f"Boilerplate ratio: {round(boilerplate, 3)}",
    ]
    return _module_status(True, "ok", round(max(0.0, min(100.0, score)), 2), factors)


def _detect_topic(snapshot: Dict[str, Any], llm_sim: Dict[str, Any] | None) -> Dict[str, Any]:
    meta = snapshot.get("meta") or {}
    headings = snapshot.get("headings") or {}
    content = snapshot.get("content") or {}

    llm_topic = str((llm_sim or {}).get("summary") or "").strip()
    title = str(meta.get("title") or "").strip()
    h1_texts = headings.get("h1_texts") or []
    h1_text = str(h1_texts[0] if h1_texts else "").strip()
    text = " ".join(
        [
            str(content.get("main_text_preview") or ""),
            str(content.get("readability_text") or ""),
            str(content.get("trafilatura_text") or ""),
            str(meta.get("description") or ""),
        ]
    )

    fallback_used = False
    topic = llm_topic[:220]
    if not topic:
        fallback_used = True
        if h1_text:
            topic = h1_text[:220]
        elif title:
            topic = title[:220]
        else:
            keywords = _top_keywords(text, limit=5)
            topic = " / ".join(keywords) if keywords else "Topic not detected"

    words = _tokens(text)
    key = _tokens(topic)
    density = 0.0
    if key and words:
        density = sum(1 for w in words if w in key) / max(1, len(words))
    confidence = 35.0
    if title:
        confidence += 18
    if h1_text:
        confidence += 22
    if int(content.get("main_text_length") or 0) >= 800:
        confidence += 12
    confidence += min(13.0, density * 100)
    confidence = max(5.0, min(100.0, confidence))
    return {
        "topic": topic,
        "confidence": round(confidence, 2),
        "topic_fallback_used": bool(fallback_used),
        "keyword_density": round(density, 4),
    }


def _metrics_bytes(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    http = snapshot.get("http") or {}
    content = snapshot.get("content") or {}
    html_bytes = int(http.get("size_bytes") or 0)
    text_bytes = int(content.get("main_text_length") or 0)
    ratio = round((text_bytes / max(1, html_bytes)), 4) if html_bytes else None
    return {
        "html_bytes": html_bytes,
        "text_bytes": text_bytes,
        "text_html_ratio": ratio,
        "main_content_ratio": round(float(content.get("main_content_ratio") or 0.0), 4),
        "boilerplate_ratio": round(float(content.get("boilerplate_ratio") or 0.0), 4),
        "formula": "text_html_ratio = text_bytes / html_bytes",
    }


def _snippet_library() -> Dict[str, str]:
    return {
        "jsonld_organization": """<script type=\"application/ld+json\">{\"@context\":\"https://schema.org\",\"@type\":\"Organization\",\"name\":\"Your Company\",\"url\":\"https://example.com\",\"logo\":\"https://example.com/logo.png\"}</script>""",
        "jsonld_article": """<script type=\"application/ld+json\">{\"@context\":\"https://schema.org\",\"@type\":\"Article\",\"headline\":\"Article title\",\"author\":{\"@type\":\"Person\",\"name\":\"Author Name\"},\"datePublished\":\"2026-01-01\"}</script>""",
        "jsonld_product": """<script type=\"application/ld+json\">{\"@context\":\"https://schema.org\",\"@type\":\"Product\",\"name\":\"Product name\",\"brand\":{\"@type\":\"Brand\",\"name\":\"Brand\"}}</script>""",
        "jsonld_breadcrumb": """<script type=\"application/ld+json\">{\"@context\":\"https://schema.org\",\"@type\":\"BreadcrumbList\",\"itemListElement\":[{\"@type\":\"ListItem\",\"position\":1,\"name\":\"Home\",\"item\":\"https://example.com\"}]}</script>""",
        "author_block_html": """<section class=\"author-box\"><p>By <strong>Author Name</strong></p><time datetime=\"2026-01-01\">January 1, 2026</time></section>""",
        "robots_ai_allow": """User-agent: GPTBot\nAllow: /\n\nUser-agent: Google-Extended\nAllow: /\n""",
    }


def _ai_directive_audit(snapshot: Dict[str, Any], policies: Dict[str, Any]) -> Dict[str, Any]:
    meta = snapshot.get("meta") or {}
    robots_profiles = ((policies.get("robots") or {}).get("profiles") or {})
    meta_blob = f"{meta.get('meta_robots', '')} {meta.get('x_robots_tag', '')}".lower()
    restricted = [tok for tok in DIRECTIVE_RESTRICTIVE_TOKENS if tok in meta_blob]
    mapping = {
        "gptbot": "gptbot",
        "google_extended": "google-extended",
        "ccbot": "ccbot",
        "claudebot": "claudebot",
        "perplexitybot": "perplexitybot",
    }
    profiles: Dict[str, Any] = {}
    blocked = 0
    restricted_count = 0
    allowed = 0
    for out_name, profile_key in mapping.items():
        p = robots_profiles.get(profile_key) or {}
        robots_allowed = bool(p.get("allowed", True))
        if not robots_allowed:
            status = "blocked"
            blocked += 1
        elif restricted:
            status = "restricted"
            restricted_count += 1
        else:
            status = "allowed"
            allowed += 1
        profiles[out_name] = {
            "robots_allowed": robots_allowed,
            "status": status,
            "reason": p.get("reason") or ("meta/x-robots restrictive tokens" if restricted else "ok"),
        }
    return {
        "profiles": profiles,
        "meta_restrictive_tokens": restricted,
        "summary": {"allowed": allowed, "restricted": restricted_count, "blocked": blocked},
    }


def _detection_issues(
    snapshot: Dict[str, Any],
    ai_blocks: Dict[str, Any],
    llm_sim: Dict[str, Any] | None,
    ingestion: Dict[str, Any] | None,
    eeat: Dict[str, Any] | None,
) -> List[str]:
    issues: List[str] = []
    content = snapshot.get("content") or {}
    if int(content.get("main_text_length") or 0) < 250:
        issues.append("Insufficient extracted text (<250 chars)")
    if float(content.get("main_content_ratio") or 0) < 0.2:
        issues.append("Very low main content ratio (<20%)")
    if float(ai_blocks.get("coverage_percent") or 0) < 35:
        issues.append("Low AI block detection coverage (<35%)")
    if not (llm_sim or {}).get("enabled"):
        issues.append("LLM simulation not executed")
    if ingestion and str(ingestion.get("status")) == "not_evaluated":
        issues.append(f"Ingestion not evaluated: {ingestion.get('reason')}")
    if eeat and str(eeat.get("status")) == "not_evaluated":
        issues.append(f"EEAT not evaluated: {eeat.get('reason')}")
    return list(dict.fromkeys(issues))[:10]


def _build_improvement_library(
    snapshot: Dict[str, Any],
    ai_blocks: Dict[str, Any],
    directives: Dict[str, Any],
    detection_issues: List[str],
    snippets: Dict[str, str],
) -> Dict[str, Any]:
    catalog = [
        {
            "id": "add_org_schema",
            "title": "Add Organization schema",
            "why": "Improves entity grounding and citation confidence",
            "snippet_key": "jsonld_organization",
        },
        {
            "id": "add_article_schema",
            "title": "Add Article schema with author/date",
            "why": "Improves trust and factual extraction",
            "snippet_key": "jsonld_article",
        },
        {
            "id": "author_block",
            "title": "Add visible author/date block",
            "why": "Improves EEAT and attribution",
            "snippet_key": "author_block_html",
        },
        {
            "id": "robots_ai_allow",
            "title": "Allow AI crawlers in robots",
            "why": "Prevents bot access gaps",
            "snippet_key": "robots_ai_allow",
        },
        {
            "id": "faq_schema",
            "title": "Add FAQ section with FAQPage schema",
            "why": "Improves answerability for AI assistants",
            "snippet_key": "jsonld_article",
        },
    ]
    missing_critical = [str(x) for x in (ai_blocks.get("missing_critical") or [])]
    schema = snapshot.get("schema") or {}
    signals = snapshot.get("signals") or {}
    needs: List[Dict[str, Any]] = []

    def add_need(item_id: str, reason: str) -> None:
        item = next((x for x in catalog if x["id"] == item_id), None)
        if not item:
            return
        need = dict(item)
        need["reason"] = reason
        key = need.get("snippet_key")
        if key and snippets.get(key):
            need["snippet"] = snippets[key]
        needs.append(need)

    if not schema.get("jsonld_types"):
        add_need("add_org_schema", "No JSON-LD types detected")
    if not signals.get("author_present") or not signals.get("date_present"):
        add_need("author_block", "Author/date signals missing")
    if any("Author block" in m for m in missing_critical):
        add_need("add_article_schema", "Critical author block not detected")
    if any("FAQ block" in m for m in missing_critical):
        add_need("faq_schema", "FAQ content block not detected")
    blocked = int(((directives.get("summary") or {}).get("blocked") or 0))
    if blocked > 0:
        add_need("robots_ai_allow", f"{blocked} AI bot profiles are blocked by robots")

    return {
        "catalog": catalog,
        "missing": needs[:12],
        "detection_issues": detection_issues[:10],
        "library_version": "std-1.0",
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
    profile_set = {str(p).lower().strip() for p in profiles if str(p).strip()}
    run_cloaking_requested = bool(options.get("runCloaking", False))
    quality_mode = True

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
    gpt_snapshot: Optional[Dict[str, Any]] = None
    gbot_snapshot: Optional[Dict[str, Any]] = None
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

    cloaking_result: Dict[str, Any] = _cloaking_not_executed("not_requested", can_run=True)
    cloaking_enabled = bool(getattr(settings, "LLM_CRAWLER_CLOAKING_ENABLED", False))
    should_try_cloaking = bool(run_cloaking_requested or {"gptbot", "google-extended"}.issubset(profile_set))
    if not cloaking_enabled:
        cloaking_result = _cloaking_not_executed("feature_disabled", can_run=False)
    elif not render_js or not rendered_snapshot:
        cloaking_result = _cloaking_not_executed("render_required", can_run=True)
    elif should_try_cloaking:
        try:
            target_for_bots = str(rendered_snapshot.get("final_url") or nojs_snapshot.get("final_url") or normalized_url)
            gpt_snapshot = _fetch_profile_snapshot(
                profile="gptbot",
                url=target_for_bots,
                timeout_ms=timeout_ms,
                max_redirect_hops=max_redirect_hops,
                max_html_bytes=max_html_bytes,
                show_headers=show_headers,
            )
            gbot_snapshot = _fetch_profile_snapshot(
                profile="googlebot",
                url=target_for_bots,
                timeout_ms=timeout_ms,
                max_redirect_hops=max_redirect_hops,
                max_html_bytes=max_html_bytes,
                show_headers=show_headers,
            )
            if gpt_snapshot and gbot_snapshot:
                cloaking_result = _cloaking_analysis(rendered_snapshot, gpt_snapshot, gbot_snapshot)
            else:
                cloaking_result = _cloaking_not_executed("bot_snapshots_missing", can_run=True)
        except Exception as exc:
            cloaking_result = _cloaking_not_executed(f"fetch_failed: {exc}", can_run=True)
    else:
        cloaking_result = _cloaking_not_executed("profiles_missing_for_cloaking", can_run=True)

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
    citation_prob = _compute_citation_probability(nojs_snapshot, score)
    entity_graph = _build_entity_graph(nojs_snapshot) if bool(getattr(settings, "LLM_CRAWLER_ENTITY_GRAPH_ENABLED", False)) else None
    if entity_graph:
        nojs_snapshot["entity_graph"] = entity_graph
    eeat = (
        _compute_eeat(nojs_snapshot, score)
        if bool(getattr(settings, "LLM_CRAWLER_EEAT_ENABLED", False))
        else _module_status(False, "feature_disabled", None, ["Enable LLM_CRAWLER_EEAT_ENABLED"])
    )
    vector_score = _vector_quality_score(nojs_snapshot, entity_graph) if bool(getattr(settings, "LLM_CRAWLER_VECTOR_SCORE_ENABLED", False)) else None
    ingestion = (
        _llm_ingestion(nojs_snapshot, diff)
        if bool(getattr(settings, "LLM_SIMULATION_ENABLED", False))
        else _module_status(False, "feature_disabled", None, ["Enable LLM_SIMULATION_ENABLED"])
    )
    discoverability = _crawler_path_sim(nojs_snapshot)
    ai_understanding = _ai_understanding(nojs_snapshot, llm_sim)
    trust_signal_score = _trust_score(nojs_snapshot)
    content_loss_percent = _content_loss(diff, nojs_snapshot)
    citation_breakdown = _citation_breakdown(nojs_snapshot)
    projected_score = _projected_score(score, citation_breakdown)
    projected_waterfall = _projected_score_waterfall(score, citation_breakdown, trust_signal_score)
    answer_preview = _ai_answer_preview(nojs_snapshot, llm_sim)
    metrics_bytes = _metrics_bytes(nojs_snapshot)
    if rendered_snapshot:
        rendered_metrics = _metrics_bytes(rendered_snapshot)
        metrics_bytes["rendered_html_bytes"] = rendered_metrics.get("html_bytes")
        metrics_bytes["rendered_text_bytes"] = rendered_metrics.get("text_bytes")
        metrics_bytes["rendered_text_html_ratio"] = rendered_metrics.get("text_html_ratio")
    ai_blocks = nojs_snapshot.get("ai_blocks") or {}
    ai_directives = _ai_directive_audit(nojs_snapshot, policies)
    snippet_library = _snippet_library()
    detection_issues = _detection_issues(nojs_snapshot, ai_blocks, llm_sim, ingestion, eeat)
    improvement_library = _build_improvement_library(
        nojs_snapshot,
        ai_blocks,
        ai_directives,
        detection_issues,
        snippet_library,
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
        "ai_understanding_score": ai_understanding.get("score"),
        "ai_understanding": ai_understanding,
        "topic_fallback_used": bool(ai_understanding.get("topic_fallback_used")),
        "trust_signal_score": trust_signal_score,
        "content_loss_percent": content_loss_percent,
        "citation_breakdown": citation_breakdown,
        "projected_score_after_fixes": projected_score,
        "projected_score_waterfall": projected_waterfall,
        "ai_answer_preview": answer_preview,
        "preview_mode": answer_preview.get("preview_mode"),
        "chunk_ranking_debug": answer_preview.get("chunk_ranking_debug") or ((llm_sim or {}).get("chunk_ranking_debug") if isinstance(llm_sim, dict) else []),
        "metrics_bytes": metrics_bytes,
        "snippet_library": snippet_library,
        "ai_blocks": ai_blocks,
        "ai_directives": ai_directives,
        "improvement_library": improvement_library,
        "detection_issues": detection_issues,
        "ui_wow_enabled": quality_mode,
        "engine": "llm_crawler_mvp_v1",
    }


def _build_recommendations(nojs: Dict[str, Any], rendered: Optional[Dict[str, Any]], policies: Dict[str, Any], score: Dict[str, Any]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    snippets = _snippet_library()
    meta = (nojs.get("meta") or {})
    social = (nojs.get("social") or {})
    resources = (nojs.get("resources") or {})
    schema = (nojs.get("schema") or {})
    signals = (nojs.get("signals") or {})
    links = (nojs.get("links") or {})
    content = (nojs.get("content") or {})
    challenge = (nojs.get("challenge") or {})
    ai_blocks = (nojs.get("ai_blocks") or {})
    missing_blocks = [str(x) for x in (ai_blocks.get("missing_critical") or [])]

    def add_rec(priority: str, area: str, title: str, expected_lift: str, evidence: List[str], source: List[str], snippet_key: str | None = None) -> None:
        rec: Dict[str, Any] = {
            "priority": priority,
            "area": area,
            "title": title,
            "expected_lift": expected_lift,
            "evidence": evidence[:3],
            "source": source[:3],
        }
        if snippet_key and snippets.get(snippet_key):
            rec["snippet_key"] = snippet_key
            rec["snippet"] = snippets[snippet_key]
        recs.append(rec)

    if "noindex" in str(meta.get("meta_robots") or "").lower():
        add_rec(
            "P0",
            "crawlability",
            "Уберите noindex для страниц, которые должны индексироваться ботами/LLM",
            "+10..15",
            [f"meta robots: {meta.get('meta_robots')}", f"x-robots-tag: {meta.get('x_robots_tag', '')}".strip()],
            ["head", "http_headers"],
            "robots_ai_allow",
        )
    if challenge.get("is_challenge"):
        add_rec(
            "P0",
            "access",
            "WAF/челлендж блокирует ботов — ослабьте правила для известных AI-ботов",
            "+8..14",
            [f"Challenge reasons: {', '.join(challenge.get('reasons') or []) or '-'}"],
            ["network", "body"],
        )
    if resources.get("cookie_wall"):
        add_rec(
            "P0",
            "access",
            "Cookie/consent wall перекрывает контент — добавьте бот-байпас или серверный рендер",
            "+7..12",
            ["Cookie/consent markers detected in HTML body"],
            ["body"],
        )
    if resources.get("paywall") or resources.get("login_wall"):
        add_rec(
            "P0",
            "access",
            "Paywall/Login wall скрывает текст — предусмотрите публичный пререндер или открытый виджет",
            "+7..12",
            [f"paywall={resources.get('paywall')}", f"login_wall={resources.get('login_wall')}"],
            ["body"],
        )
    if resources.get("csp_strict"):
        add_rec(
            "P1",
            "access",
            "Слишком строгий CSP (script-src 'none') может ломать JS — ослабьте для нужных скриптов",
            "+4..7",
            ["CSP policy indicates blocked script execution"],
            ["http_headers"],
        )
    if int(resources.get("mixed_content_count") or 0) > 0:
        add_rec(
            "P1",
            "resources",
            "Исправьте mixed content (http ресурсы на https странице)",
            "+3..5",
            [f"Mixed content count: {resources.get('mixed_content_count')}"],
            ["network", "body"],
        )
    if not schema.get("jsonld_types"):
        add_rec(
            "P1",
            "schema",
            "Добавьте JSON-LD (Organization/Article/Product) для доверия и извлечения",
            "+8..12",
            ["No JSON-LD types found on page"],
            ["head", "body"],
            "jsonld_organization",
        )
    if float(schema.get("coverage_score") or 0) < 50:
        add_rec(
            "P1",
            "schema",
            "Увеличьте покрытие schema.org (Organization/Person/Article/Product)",
            "+6..10",
            [f"Schema coverage score: {schema.get('coverage_score', 0)}%"],
            ["head", "body"],
            "jsonld_article",
        )
    if not signals.get("author_present") or not signals.get("date_present"):
        add_rec(
            "P1",
            "trust",
            "Укажите автора/дату публикации — повышает понятность и доверие",
            "+5..8",
            [f"author_present={signals.get('author_present')}", f"date_present={signals.get('date_present')}"],
            ["head", "body"],
            "author_block_html",
        )
    if not social.get("og_present") or not social.get("twitter_present"):
        add_rec(
            "P2",
            "social",
            "Добавьте OpenGraph/Twitter метатеги для консистентных сниппетов и LLM-карточек",
            "+2..4",
            [f"OG present={social.get('og_present')}", f"Twitter present={social.get('twitter_present')}"],
            ["head"],
        )
    if int(links.get("js_only_count") or 0) > 0:
        add_rec(
            "P1",
            "links",
            "Избегайте JS-only ссылок — используйте href для навигации ботов",
            "+4..7",
            [f"JS-only links count: {links.get('js_only_count', 0)}"],
            ["body"],
        )
    if float(links.get("anchor_quality_score") or 0) < 50:
        add_rec(
            "P2",
            "links",
            "Улучшите анкоры ссылок — больше смысловых текстов вместо 'здесь/читать'",
            "+2..4",
            [f"Anchor quality score: {links.get('anchor_quality_score', 0)}"],
            ["body"],
        )
    if int(content.get("main_text_length") or 0) < 500:
        add_rec(
            "P2",
            "content",
            "Увеличьте основной текст/контент — сейчас он слишком короткий для извлечения",
            "+3..6",
            [f"main_text_length={content.get('main_text_length', 0)}"],
            ["body"],
        )
    if float(score.get("js_dependency_score", 0)) > 70:
        add_rec(
            "P1",
            "js_dependency",
            "Высокая зависимость от JS — обеспечьте SSR или пререндер",
            "+5..9",
            [f"js_dependency_score={score.get('js_dependency_score', 0)}"],
            ["network", "render"],
        )
    if any("Author block" in x for x in missing_blocks):
        add_rec(
            "P1",
            "trust",
            "Критичный блок автора не найден в DOM/тексте",
            "+5..8",
            [f"Missing critical blocks: {', '.join(missing_blocks[:4])}"],
            ["body", "schema"],
            "author_block_html",
        )
    if any("Contact info" in x for x in missing_blocks):
        add_rec(
            "P2",
            "trust",
            "Добавьте явный блок контактов (email/tel/address)",
            "+3..5",
            [f"Missing critical blocks: {', '.join(missing_blocks[:4])}"],
            ["body"],
        )
    return recs[:10]


def _run_llm_simulation(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    content = snapshot.get("content") or {}
    ranked = _rank_chunks_for_question(snapshot, "What is this page about?", limit=3)
    ranked_text = " ".join([str(x.get("text") or "") for x in ranked])
    text = ranked_text or content.get("main_text_preview") or content.get("readability_text") or ""
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
        "chunk_ranking_debug": [{"idx": r.get("idx"), "score": r.get("score")} for r in ranked],
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
    content = snapshot.get("content") or {}
    if int(content.get("main_text_length") or 0) < 120:
        return _module_status(False, "insufficient_content", None, ["Not enough extracted text for EEAT evaluation"])
    total = 50
    factors: List[str] = []
    if signals.get("author_present"):
        total += 10
        factors.append("Author signal present")
    else:
        factors.append("Author signal missing")
    if signals.get("date_present"):
        total += 5
        factors.append("Publish date present")
    else:
        factors.append("Publish date missing")
    if schema.get("coverage_score", 0) >= 50:
        total += 10
        factors.append("Schema coverage >= 50%")
    if schema.get("coverage_score", 0) >= 75:
        total += 5
        factors.append("Schema coverage >= 75%")
    if meta.get("canonical"):
        total += 3
        factors.append("Canonical URL present")
    if signals.get("author_samples"):
        total += 2
        factors.append("Author samples in metadata")
    total = max(0, min(100, total))
    payload = _module_status(True, "ok", float(total), factors)
    payload["score"] = float(total)
    return payload


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
    if chunks_count == 0:
        payload = _module_status(False, "no_chunks_available", None, ["Chunking produced no usable chunks"])
        payload.update(
            {
                "chunks_count": 0,
                "avg_chunk_quality": None,
                "lost_content_percent": None,
                "ingestion_risk": "unknown",
            }
        )
        return payload
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
    payload = _module_status(
        True,
        "ok",
        float(avg_quality),
        [
            f"Average chunk length: {round(avg_len, 2)}",
            f"Readability score: {round(readability, 2)}",
            f"Lost content percent: {round(lost * 100, 2)}",
        ],
    )
    payload.update(
        {
        "chunks_count": chunks_count,
        "avg_chunk_quality": avg_quality,
        "lost_content_percent": round(lost * 100, 2),
        "ingestion_risk": ingestion_risk,
        }
    )
    return payload


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


def _ai_understanding(snapshot: Dict[str, Any], llm_sim: Dict[str, Any] | None) -> Dict[str, Any]:
    content = snapshot.get("content") or {}
    signals = snapshot.get("signals") or {}
    entity_graph = snapshot.get("entity_graph") or {}
    topic_info = _detect_topic(snapshot, llm_sim)
    topic = topic_info.get("topic") or ""
    entities = (llm_sim or {}).get("entities") or []
    if not entities:
        entities = (
            (entity_graph.get("organizations") or [])
            + (entity_graph.get("persons") or [])
            + (entity_graph.get("products") or [])
        )[:10]
    clarity = _content_clarity(snapshot, entity_graph)
    score = 50
    if topic:
        score += 20
    if signals.get("author_present"):
        score += 5
    if len(entities) > 3:
        score += 10
    clarity_score = clarity.get("score")
    readability = float(content.get("readability_score") or 0)
    if clarity_score is not None:
        score += min(15, float(clarity_score) / 6)
    else:
        score += min(10, readability / 6)
    confidence = float(topic_info.get("confidence") or 0)
    score = (score * 0.7) + (confidence * 0.3)
    return {
        "score": max(0, min(100, round(score, 2))),
        "topic": topic[:200],
        "topic_confidence": round(confidence, 2),
        "topic_fallback_used": bool(topic_info.get("topic_fallback_used")),
        "entities": entities[:10],
        "content_clarity": clarity.get("score"),
        "content_clarity_status": clarity.get("status"),
        "content_clarity_reason": clarity.get("reason"),
        "content_clarity_factors": clarity.get("factors") or [],
        "intent": "informational",
    }


def _trust_score(snapshot: Dict[str, Any]) -> float:
    signals = snapshot.get("signals") or {}
    schema = snapshot.get("schema") or {}
    trust = 0
    if signals.get("author_present"):
        trust += 25
    if signals.get("date_present"):
        trust += 15
    if schema.get("coverage_score", 0) >= 50:
        trust += 30
    return float(max(0, min(100, trust)))


def _content_loss(diff: Dict[str, Any], snapshot: Dict[str, Any]) -> float:
    text_coverage = diff.get("textCoverage")
    if text_coverage is None:
        return 0.0
    try:
        loss = max(0.0, 1 - float(text_coverage))
    except Exception:
        loss = 0.0
    return round(loss * 100, 2)


def _citation_breakdown(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    schema = snapshot.get("schema") or {}
    signals = snapshot.get("signals") or {}
    content = snapshot.get("content") or {}
    return {
        "schema": 100 if schema.get("coverage_score", 0) >= 75 else 50 if schema.get("coverage_score", 0) > 0 else 0,
        "author": 100 if signals.get("author_present") else 0,
        "content_clarity": min(100, float(content.get("readability_score") or 0)),
        "bot_accessibility": 100,
        "structure": min(100, (snapshot.get("headings") or {}).get("h2", 0) * 10),
    }


def _projected_score(score: Dict[str, Any], citation_breakdown: Dict[str, Any]) -> float:
    base = float(score.get("total", 0))
    gain = 0
    if citation_breakdown.get("schema", 0) < 50:
        gain += 12
    if citation_breakdown.get("author", 0) == 0:
        gain += 8
    return float(max(base, min(100, base + gain)))


def _projected_score_waterfall(score: Dict[str, Any], citation_breakdown: Dict[str, Any], trust_signal_score: float) -> Dict[str, Any]:
    base = float(score.get("total", 0))
    steps: List[Dict[str, Any]] = []
    running = base
    if citation_breakdown.get("schema", 0) < 50:
        delta = 12.0
        running += delta
        steps.append({"label": "Schema coverage", "delta": delta, "value": round(min(100, running), 2)})
    if citation_breakdown.get("author", 0) == 0:
        delta = 8.0
        running += delta
        steps.append({"label": "Author/date trust", "delta": delta, "value": round(min(100, running), 2)})
    if trust_signal_score < 50:
        delta = 5.0
        running += delta
        steps.append({"label": "Trust signals", "delta": delta, "value": round(min(100, running), 2)})
    target = max(base, min(100, running))
    return {"baseline": round(base, 2), "target": round(target, 2), "steps": steps}


def _ai_answer_preview(snapshot: Dict[str, Any], llm_sim: Dict[str, Any] | None) -> Dict[str, Any]:
    question = "What is this page about?"
    ranked = _rank_chunks_for_question(snapshot, question, limit=3)
    bullets = _build_extractive_preview(ranked, bullets=3)
    mode = "llm" if bool((llm_sim or {}).get("enabled")) else "extractive"
    answer = ""
    if ranked:
        # Use only top-ranked chunks for grounded preview.
        ranked_text = " ".join([str(x.get("text") or "") for x in ranked[:2]])
        answer = " ".join(_sentences(ranked_text)[:2]).strip()
    if not answer:
        answer = (snapshot.get("content") or {}).get("main_text_preview", "")[:280]
    confidence = (llm_sim or {}).get("scores", {}).get("answer_quality_score", None)
    if confidence is None and ranked:
        confidence = round(min(100, (sum(float(x.get("score") or 0) for x in ranked) / max(1, len(ranked))) * 100), 2)
    return {
        "question": question,
        "answer": answer or "Not enough content",
        "confidence": confidence,
        "preview_mode": mode,
        "bullets": bullets,
        "chunk_ranking_debug": [
            {"idx": int(x.get("idx") or 0), "score": x.get("score"), "relevance": x.get("relevance")}
            for x in ranked
        ],
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
        "status": "executed",
        "similarity_scores": {
            "browser_vs_gptbot": sim_bg,
            "browser_vs_googlebot": sim_bb,
        },
        "length_delta": round(len_delta, 3),
        "missing_sections": missing[:20],
        "evidence": [
            f"Cosine(browser,gptbot)={sim_bg}",
            f"Cosine(browser,googlebot)={sim_bb}",
            f"Length delta={round(len_delta, 3)}",
        ],
    }


def _cloaking_not_executed(reason: str, can_run: bool = True) -> Dict[str, Any]:
    return {
        "status": "not_executed",
        "reason": reason,
        "risk": "unknown",
        "similarity_scores": {},
        "missing_sections": [],
        "can_run": can_run,
        "evidence": [],
    }
