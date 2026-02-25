"""HTML extraction utilities for LLM crawler snapshots."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup


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
    for link in soup.find_all("a", href=True):
        href = _safe_text(link.get("href"))
        if not href:
            continue
        abs_href = _safe_text(urljoin(base_url, href))
        if not abs_href:
            continue
        all_urls.append(abs_href)
        if abs_href in seen:
            continue
        seen.add(abs_href)
        anchor = _safe_text(" ".join(link.stripped_strings))
        top_links.append({"anchor": anchor[:200], "url": abs_href[:1000]})
        if len(top_links) >= limit:
            break
    return {
        "count": len(all_urls),
        "unique_count": len(set(all_urls)),
        "top": top_links,
        "all_urls": list(dict.fromkeys(all_urls)),
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

    headings = {
        "h1": len(soup.find_all("h1")),
        "h2": len(soup.find_all("h2")),
        "h3": len(soup.find_all("h3")),
    }

    main_text = _extract_main_text(soup)
    words = re.findall(r"[A-Za-zА-Яа-я0-9]+", main_text)
    links = _extract_links(soup, final_url, limit=20)
    schema_types = _extract_jsonld_types(soup)
    x_robots_tag = _safe_text((headers or {}).get("X-Robots-Tag") or (headers or {}).get("x-robots-tag"))
    signals = _extract_author_date_signals(soup)
    challenge = detect_challenge(status_code, headers, html)

    snapshot: Dict[str, Any] = {
        "final_url": final_url,
        "status_code": int(status_code) if status_code is not None else None,
        "redirect_chain": redirect_chain,
        "meta": {
            "title": title,
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
            "word_count": len(words),
            "readability_score": _flesch_reading_ease(main_text),
            "main_text_preview": main_text[:2000],
        },
        "headings": headings,
        "links": {
            "count": links["count"],
            "unique_count": links["unique_count"],
            "top": links["top"],
            "all_urls": links["all_urls"][:1000],
        },
        "schema": {
            "jsonld_types": schema_types,
            "count": len(schema_types),
        },
        "structure": {
            "lists_count": len(soup.find_all(["ul", "ol"])),
            "tables_count": len(soup.find_all("table")),
        },
        "signals": signals,
        "challenge": challenge,
    }

    if show_headers:
        safe_headers = {str(k): _safe_text(v)[:500] for k, v in (headers or {}).items()}
        snapshot["http"]["headers"] = safe_headers

    return snapshot
