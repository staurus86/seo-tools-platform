"""HTML extraction utilities for LLM crawler snapshots."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from .patterns import detect_ai_blocks
from .segmentation import segment_content
try:  # optional dependency
    import trafilatura  # type: ignore
except Exception:  # pragma: no cover - optional at runtime
    trafilatura = None
try:  # optional dependency
    from readability import Document  # type: ignore
except Exception:  # pragma: no cover
    Document = None
try:  # optional dependency
    import extruct  # type: ignore
except Exception:  # pragma: no cover
    extruct = None


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _flesch_reading_ease(text: str) -> float:
    raw = _safe_text(text)
    if not raw:
        return 0.0
    words = re.findall(r"[A-Za-zА-Яа-я0-9]+", raw)
    if not words:
        return 0.0
    sentences = [x for x in re.split(r"[.!?]+", raw) if x.strip()]
    sentence_count = max(1, len(sentences))
    word_count = max(1, len(words))
    syllables = 0
    for word in words:
        parts = re.findall(r"[aeiouyаеёиоуыэюя]+", word.lower())
        syllables += max(1, len(parts))
    score = 206.835 - (1.015 * (word_count / sentence_count)) - (84.6 * (syllables / word_count))
    return round(max(0.0, min(100.0, score)), 2)


def _extract_jsonld_types(soup: BeautifulSoup) -> List[str]:
    found: Set[str] = set()

    def collect_types(payload: Any) -> None:
        if isinstance(payload, list):
            for item in payload:
                collect_types(item)
            return
        if isinstance(payload, dict):
            raw_type = payload.get("@type")
            if isinstance(raw_type, str) and raw_type.strip():
                found.add(raw_type.strip())
            elif isinstance(raw_type, list):
                for item in raw_type:
                    if isinstance(item, str) and item.strip():
                        found.add(item.strip())
            for value in payload.values():
                collect_types(value)

    for script in soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.I)}):
        payload = _safe_text(script.string or script.text)
        if not payload:
            continue
        try:
            parsed = json.loads(payload)
            collect_types(parsed)
        except Exception:
            continue
    return sorted(found)


def _extract_main_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.extract()
    node = soup.find("main") or soup.find("article") or soup.body or soup
    return _safe_text(" ".join(node.stripped_strings))


def _extract_links(soup: BeautifulSoup, base_url: str, limit: int = 20) -> Dict[str, Any]:
    top_links: List[Dict[str, str]] = []
    all_urls: List[str] = []
    seen = set()
    js_only = 0
    meaningful = 0
    for link in soup.find_all("a", href=True):
        href = _safe_text(link.get("href"))
        if not href:
            continue
        if href.lower().startswith("javascript:"):
            js_only += 1
            continue
        abs_href = _safe_text(urljoin(base_url, href))
        if not abs_href:
            continue
        all_urls.append(abs_href)
        if abs_href in seen:
            continue
        seen.add(abs_href)
        anchor = _safe_text(" ".join(link.stripped_strings))
        if len(anchor) >= 4 and anchor.lower() not in {"here", "link", "click", "читать", "подробнее"}:
            meaningful += 1
        top_links.append({"anchor": anchor[:200], "url": abs_href[:1000]})
        if len(top_links) >= limit:
            break
    # anchors without href but onclick
    for link in soup.find_all("a", href=False):
        if link.get("onclick"):
            js_only += 1
            if len(_safe_text(" ".join(link.stripped_strings))) >= 4:
                meaningful += 1
    anchor_quality = round((meaningful / max(1, len(top_links))) * 100, 2) if top_links else 0.0
    return {
        "count": len(all_urls),
        "unique_count": len(set(all_urls)),
        "top": top_links,
        "all_urls": list(dict.fromkeys(all_urls)),
        "js_only_count": js_only,
        "anchor_quality_score": anchor_quality,
    }


def _extract_author_date_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    author_candidates = []
    for tag in soup.find_all("meta"):
        name = _safe_text(tag.get("name")).lower()
        prop = _safe_text(tag.get("property")).lower()
        if name == "author" or prop in {"article:author", "og:author"}:
            author_candidates.append(_safe_text(tag.get("content")))

    date_candidates = []
    for tag in soup.find_all("meta"):
        name = _safe_text(tag.get("name")).lower()
        prop = _safe_text(tag.get("property")).lower()
        if name in {"date", "pubdate", "publish_date"} or prop in {"article:published_time", "article:modified_time"}:
            date_candidates.append(_safe_text(tag.get("content")))
    for tag in soup.find_all("time"):
        dt = _safe_text(tag.get("datetime"))
        if dt:
            date_candidates.append(dt)

    return {
        "author_present": bool([x for x in author_candidates if x]),
        "author_samples": [x for x in author_candidates if x][:5],
        "date_present": bool([x for x in date_candidates if x]),
        "date_samples": [x for x in date_candidates if x][:5],
    }


def _extract_social_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    og_tags = []
    tw_tags = []
    for tag in soup.find_all("meta"):
        prop = _safe_text(tag.get("property")).lower()
        name = _safe_text(tag.get("name")).lower()
        if prop.startswith("og:"):
            og_tags.append(prop)
        if name.startswith("twitter:") or prop.startswith("twitter:"):
            tw_tags.append(name or prop)
    return {
        "og_present": bool(og_tags),
        "og_count": len(og_tags),
        "twitter_present": bool(tw_tags),
        "twitter_count": len(tw_tags),
    }


def detect_challenge(status_code: int | None, headers: Dict[str, Any], html: str) -> Dict[str, Any]:
    reasons: List[str] = []
    h = {str(k).lower(): _safe_text(v).lower() for k, v in (headers or {}).items()}
    body = _safe_text(html).lower()
    if int(status_code or 0) in {401, 403, 429, 503}:
        reasons.append(f"status_{int(status_code)}")
    if "cf-ray" in h or "x-sucuri-id" in h or "x-ddos" in h:
        reasons.append("waf_headers")
    challenge_markers = [
        "captcha",
        "attention required",
        "cloudflare",
        "access denied",
        "verify you are human",
    ]
    if any(token in body for token in challenge_markers):
        reasons.append("challenge_body")
    return {"is_challenge": bool(reasons), "reasons": reasons}


def detect_resource_barriers(final_url: str, headers: Dict[str, Any], html: str, soup: BeautifulSoup) -> Dict[str, Any]:
    h = {str(k).lower(): _safe_text(v).lower() for k, v in (headers or {}).items()}
    url_scheme = "https" if str(final_url).lower().startswith("https") else "http"
    body = _safe_text(html).lower()

    cookie_wall = any(token in body for token in ["cookie consent", "accept cookies", "gdpr", "we value your privacy"])
    paywall = any(token in body for token in ["paywall", "subscribe to continue", "subscription required", "digital subscription"])
    login_wall = any(token in body for token in ["please sign in", "log in to continue", "login to read"])

    csp_header = h.get("content-security-policy", "")
    csp_strict = "script-src 'none'" in csp_header or "default-src 'none'" in csp_header

    mixed_content = 0
    if url_scheme == "https":
        for tag in soup.find_all(src=True):
            if str(tag.get("src") or "").lower().startswith("http://"):
                mixed_content += 1
        for tag in soup.find_all("link", href=True):
            if str(tag.get("href") or "").lower().startswith("http://"):
                mixed_content += 1

    return {
        "cookie_wall": cookie_wall,
        "paywall": paywall,
        "login_wall": login_wall,
        "csp_strict": csp_strict,
        "mixed_content_count": mixed_content,
    }


def build_snapshot(
    *,
    html: str,
    final_url: str,
    status_code: int | None,
    headers: Dict[str, Any],
    timing_ms: int,
    redirect_chain: List[Dict[str, Any]],
    show_headers: bool,
    content_type: str,
    size_bytes: int,
    truncated: bool,
) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    title_tag = soup.find("title")
    title = _safe_text(title_tag.get_text(" ", strip=True) if title_tag else "")
    meta_description = ""
    desc_tag = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    if desc_tag:
        meta_description = _safe_text(desc_tag.get("content"))
    meta_robots = ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"robots", re.I)})
    if meta_tag:
        meta_robots = _safe_text(meta_tag.get("content"))
    canonical = ""
    canonical_tag = soup.find("link", attrs={"rel": re.compile(r"canonical", re.I)})
    if canonical_tag:
        canonical = _safe_text(canonical_tag.get("href"))
        if canonical:
            canonical = urljoin(final_url, canonical)

    hreflang: List[Dict[str, str]] = []
    for tag in soup.find_all("link", attrs={"rel": re.compile(r"alternate", re.I), "hreflang": True}):
        href = _safe_text(tag.get("href"))
        lang = _safe_text(tag.get("hreflang"))
        if not href:
            continue
        hreflang.append({"lang": lang, "href": urljoin(final_url, href)})
        if len(hreflang) >= 50:
            break

    h1_tags = soup.find_all("h1")
    h2_tags = soup.find_all("h2")
    h3_tags = soup.find_all("h3")
    headings = {
        "h1": len(h1_tags),
        "h2": len(h2_tags),
        "h3": len(h3_tags),
        "h1_texts": [_safe_text(tag.get_text(" ", strip=True))[:240] for tag in h1_tags[:5]],
        "h2_texts": [_safe_text(tag.get_text(" ", strip=True))[:240] for tag in h2_tags[:8]],
    }

    raw_main_text = _extract_main_text(soup)
    full_text = _safe_text(" ".join(soup.stripped_strings))
    links = _extract_links(soup, final_url, limit=20)
    segmentation = segment_content(
        soup=soup,
        rendered_text=full_text,
        extracted_text=raw_main_text,
        links=links,
        headings=headings,
    )
    main_text = _safe_text(segmentation.get("main_text") or raw_main_text)
    main_content_ratio = len(main_text) / max(1, len(full_text))
    boilerplate_ratio = max(0.0, 1.0 - main_content_ratio)

    # Reader-mode variants
    readability_text = ""
    trafilatura_text = ""
    if Document:
        try:
            readability_text = _safe_text(Document(html).summary())[:5000]
        except Exception:
            readability_text = ""
    if trafilatura:
        try:
            trafilatura_text = _safe_text(trafilatura.extract(html, url=final_url) or "")[:5000]
        except Exception:
            trafilatura_text = ""

    # Chunking (simple fixed-size by characters)
    chunks: List[Dict[str, Any]] = []
    chunk_src = main_text or readability_text or trafilatura_text
    if chunk_src:
        step = 1200
        overlap = 100
        start = 0
        idx = 1
        while start < len(chunk_src) and len(chunks) < 10:
            end = min(len(chunk_src), start + step)
            chunk_text = chunk_src[start:end]
            chunks.append({"idx": idx, "text": chunk_text})
            idx += 1
            start = end - overlap

    words = re.findall(r"[A-Za-zА-Яа-я0-9]+", main_text)
    schema_types = _extract_jsonld_types(soup)
    microdata_types: List[str] = []
    rdfa_types: List[str] = []
    if extruct:
        try:
            data = extruct.extract(html, base_url=final_url, syntaxes=["microdata", "rdfa"], errors="log")
            microdata_items = data.get("microdata") or []
            rdfa_items = data.get("rdfa") or []
            for item in microdata_items:
                t = item.get("type") or item.get("@type")
                if isinstance(t, list):
                    microdata_types.extend([_safe_text(x) for x in t if _safe_text(x)])
                elif isinstance(t, str):
                    microdata_types.append(_safe_text(t))
            for item in rdfa_items:
                t = item.get("type") or item.get("@type")
                if isinstance(t, list):
                    rdfa_types.extend([_safe_text(x) for x in t if _safe_text(x)])
                elif isinstance(t, str):
                    rdfa_types.append(_safe_text(t))
        except Exception:
            pass
    required_schema = {"Organization", "Person", "Article", "Product"}
    coverage_found = len((set(schema_types) | set(microdata_types) | set(rdfa_types)) & required_schema)
    coverage_score = round((coverage_found / max(1, len(required_schema))) * 100, 2)
    x_robots_tag = _safe_text((headers or {}).get("X-Robots-Tag") or (headers or {}).get("x-robots-tag"))
    signals = _extract_author_date_signals(soup)
    social = _extract_social_meta(soup)
    challenge = detect_challenge(status_code, headers, html)
    resources = detect_resource_barriers(final_url, headers, html, soup)
    ai_blocks = detect_ai_blocks(
        soup=soup,
        main_text=main_text,
        full_text=full_text,
        schema_types=schema_types,
    )

    snapshot: Dict[str, Any] = {
        "final_url": final_url,
        "status_code": int(status_code) if status_code is not None else None,
        "redirect_chain": redirect_chain,
        "meta": {
            "title": title,
            "description": meta_description,
            "meta_robots": meta_robots,
            "canonical": canonical,
            "hreflang": hreflang,
            "x_robots_tag": x_robots_tag,
        },
        "http": {
            "content_type": content_type,
            "size_bytes": int(size_bytes or 0),
            "timing_ms": int(timing_ms or 0),
            "truncated": bool(truncated),
        },
        "content": {
            "main_text_length": len(main_text),
            "raw_main_text_length": len(raw_main_text),
            "word_count": len(words),
            "readability_score": _flesch_reading_ease(main_text),
            "main_text_preview": main_text[:2000],
            "readability_text": readability_text,
            "trafilatura_text": trafilatura_text,
            "main_content_ratio": round(main_content_ratio, 4),
            "boilerplate_ratio": round(boilerplate_ratio, 4),
            "noise_breakdown": segmentation.get("noise_breakdown") or {},
            "main_content_confidence": segmentation.get("main_content_confidence") or {},
            "main_source": "segmented_main" if main_text != raw_main_text else "raw_main",
            "chunks": chunks,
        },
        "headings": headings,
        "links": {
            "count": links["count"],
            "unique_count": links["unique_count"],
            "top": links["top"],
            "all_urls": links["all_urls"][:1000],
            "js_only_count": links.get("js_only_count", 0),
            "anchor_quality_score": links.get("anchor_quality_score", 0.0),
        },
        "schema": {
            "jsonld_types": schema_types,
            "microdata_types": microdata_types[:50],
            "rdfa_types": rdfa_types[:50],
            "coverage_score": coverage_score,
            "count": len(schema_types),
        },
        "social": social,
        "structure": {
            "lists_count": len(soup.find_all(["ul", "ol"])),
            "tables_count": len(soup.find_all("table")),
        },
        "resources": resources,
        "signals": signals,
        "challenge": challenge,
        "ai_blocks": ai_blocks,
        "segmentation": {
            "content_segments": (segmentation.get("content_segments") or [])[:60],
            "noise_breakdown": segmentation.get("noise_breakdown") or {},
            "main_content_confidence": segmentation.get("main_content_confidence") or {},
            "segment_version": segmentation.get("segment_version") or "seg-v1",
        },
    }

    if show_headers:
        safe_headers = {str(k): _safe_text(v)[:500] for k, v in (headers or {}).items()}
        snapshot["http"]["headers"] = safe_headers

    return snapshot
