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


def _normalize_schema_type(value: Any) -> str:
    raw = _safe_text(value)
    if not raw:
        return ""
    # Normalize URL-like schema types: https://schema.org/Organization -> Organization
    cleaned = re.split(r"[#?/]", raw)[-1] if ("://" in raw or "/" in raw or "#" in raw) else raw
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    if ":" in cleaned and cleaned.lower().startswith("schema:"):
        cleaned = cleaned.split(":", 1)[1]
    # Keep stable formatting for known CamelCase schema names.
    return cleaned


SCHEMA_ENTITY_GROUPS: Dict[str, Set[str]] = {
    "organizations": {
        "organization",
        "localbusiness",
        "corporation",
        "ngo",
        "brand",
        "educationalorganization",
        "governmentorganization",
        "medicalorganization",
        "newsmediaorganization",
    },
    "persons": {"person", "author"},
    "products": {
        "product",
        "service",
        "softwareapplication",
        "offer",
        "aggregateoffer",
        "individualproduct",
    },
    "locations": {"place", "city", "country", "state", "postaladdress"},
}


def _schema_types_from_value(value: Any) -> List[str]:
    raw: List[str] = []
    if isinstance(value, str):
        raw = [value]
    elif isinstance(value, list):
        raw = [str(x) for x in value if _safe_text(x)]
    elif value is not None:
        raw = [str(value)]
    out: List[str] = []
    for item in raw:
        norm = _normalize_schema_type(item)
        if norm:
            out.append(norm)
    return out


def _entity_names_from_node(node: Dict[str, Any]) -> List[str]:
    keys = ("name", "legalName", "alternateName", "headline", "title")
    found: List[str] = []
    for key in keys:
        val = node.get(key)
        if isinstance(val, str):
            txt = _safe_text(val)
            if txt:
                found.append(txt)
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    txt = _safe_text(item)
                    if txt:
                        found.append(txt)
        elif isinstance(val, dict):
            txt = _safe_text(val.get("name"))
            if txt:
                found.append(txt)
    given = _safe_text(node.get("givenName"))
    family = _safe_text(node.get("familyName"))
    if given and family:
        found.append(f"{given} {family}")
    elif given:
        found.append(given)
    unique: List[str] = []
    seen: Set[str] = set()
    for item in found:
        cleaned = re.sub(r"\s+", " ", item).strip()
        if len(cleaned) < 2 or len(cleaned) > 140:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(cleaned)
    return unique[:10]


def _schema_entity_buckets(types: List[str]) -> Set[str]:
    buckets: Set[str] = set()
    lowered = {str(x).strip().lower() for x in (types or []) if str(x).strip()}
    for bucket, allowed in SCHEMA_ENTITY_GROUPS.items():
        if lowered & allowed:
            buckets.add(bucket)
    return buckets


def _add_schema_entity(entities: Dict[str, Set[str]], bucket: str, value: str) -> None:
    txt = re.sub(r"\s+", " ", _safe_text(value)).strip()
    if len(txt) < 2 or len(txt) > 140:
        return
    entities.setdefault(bucket, set()).add(txt)


def _collect_schema_data_from_node(
    payload: Any,
    *,
    types_found: Set[str],
    entities: Dict[str, Set[str]],
) -> None:
    if isinstance(payload, list):
        for item in payload:
            _collect_schema_data_from_node(item, types_found=types_found, entities=entities)
        return
    if isinstance(payload, dict):
        raw_types = (
            _schema_types_from_value(payload.get("@type"))
            + _schema_types_from_value(payload.get("type"))
            + _schema_types_from_value(payload.get("itemtype"))
        )
        for item in raw_types:
            types_found.add(item)
        buckets = _schema_entity_buckets(raw_types)
        names = _entity_names_from_node(payload)
        if buckets and names:
            for bucket in buckets:
                for name in names:
                    _add_schema_entity(entities, bucket, name)
        # microdata/extruct often wraps values under properties.
        props = payload.get("properties")
        if isinstance(props, dict):
            _collect_schema_data_from_node(props, types_found=types_found, entities=entities)
        for value in payload.values():
            _collect_schema_data_from_node(value, types_found=types_found, entities=entities)


def _empty_schema_entities() -> Dict[str, Set[str]]:
    return {"organizations": set(), "persons": set(), "products": set(), "locations": set()}


def _merge_schema_entities(target: Dict[str, Set[str]], source: Dict[str, Any]) -> None:
    for bucket in ("organizations", "persons", "products", "locations"):
        for value in (source.get(bucket) or []):
            _add_schema_entity(target, bucket, str(value))


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


def _extract_jsonld_schema_data(soup: BeautifulSoup) -> Dict[str, Any]:
    found: Set[str] = set()
    entities = _empty_schema_entities()

    for script in soup.find_all("script"):
        script_type = _safe_text(script.get("type")).lower()
        payload = _safe_text(script.string or script.text)
        if not payload:
            continue
        candidate = ("ld+json" in script_type) or ("@context" in payload and "schema.org" in payload.lower())
        if not candidate:
            continue
        try:
            parsed = json.loads(payload)
            _collect_schema_data_from_node(parsed, types_found=found, entities=entities)
        except Exception:
            # Fallback for slightly invalid JSON-LD payloads.
            for match in re.finditer(r'"@type"\s*:\s*"(.*?)"', payload, flags=re.I):
                norm = _normalize_schema_type(match.group(1))
                if norm:
                    found.add(norm)
            for match in re.finditer(r"'@type'\s*:\s*'(.*?)'", payload, flags=re.I):
                norm = _normalize_schema_type(match.group(1))
                if norm:
                    found.add(norm)
            for match in re.finditer(r'"@type"\s*:\s*\[(.*?)\]', payload, flags=re.I | re.S):
                part = match.group(1)
                for val in re.findall(r'"(.*?)"', part):
                    norm = _normalize_schema_type(val)
                    if norm:
                        found.add(norm)
            for match in re.finditer(r'"name"\s*:\s*"(.*?)"', payload, flags=re.I):
                name = _safe_text(match.group(1))
                if name and len(name) <= 140:
                    _add_schema_entity(entities, "organizations", name)
    return {
        "types": sorted(found),
        "entities": {k: sorted(v)[:30] for k, v in entities.items()},
    }


def _extract_jsonld_types(soup: BeautifulSoup) -> List[str]:
    return list((_extract_jsonld_schema_data(soup) or {}).get("types") or [])


def _extract_microdata_types_fallback(soup: BeautifulSoup) -> List[str]:
    found: Set[str] = set()
    for tag in soup.find_all(attrs={"itemtype": True}):
        itemtype = _safe_text(tag.get("itemtype"))
        if not itemtype:
            continue
        for part in re.split(r"\s+", itemtype):
            norm = _normalize_schema_type(part)
            if norm:
                found.add(norm)
    return sorted(found)


def _extract_rdfa_types_fallback(soup: BeautifulSoup) -> List[str]:
    found: Set[str] = set()
    for tag in soup.find_all(attrs={"typeof": True}):
        raw = _safe_text(tag.get("typeof"))
        if not raw:
            continue
        for part in re.split(r"\s+", raw):
            norm = _normalize_schema_type(part)
            if norm:
                found.add(norm)
    return sorted(found)


def _extract_dom_schema_entities(soup: BeautifulSoup) -> Dict[str, List[str]]:
    entities = _empty_schema_entities()
    # Microdata itemscope blocks.
    for tag in soup.find_all(attrs={"itemtype": True}):
        itemtype = _safe_text(tag.get("itemtype"))
        if not itemtype:
            continue
        types = []
        for part in re.split(r"\s+", itemtype):
            norm = _normalize_schema_type(part)
            if norm:
                types.append(norm)
        buckets = _schema_entity_buckets(types)
        if not buckets:
            continue
        names: List[str] = []
        for node in tag.find_all(attrs={"itemprop": True}):
            prop = _safe_text(node.get("itemprop")).lower()
            if prop not in {"name", "legalname", "alternatename", "headline", "title"}:
                continue
            candidate = _safe_text(node.get("content") or " ".join(node.stripped_strings))
            if candidate:
                names.append(candidate)
        if not names:
            fallback = _safe_text(tag.get("content") or " ".join(tag.stripped_strings))
            if fallback:
                names.append(fallback)
        for bucket in buckets:
            for name in names[:4]:
                _add_schema_entity(entities, bucket, name)

    # RDFa typeof blocks.
    for tag in soup.find_all(attrs={"typeof": True}):
        raw = _safe_text(tag.get("typeof"))
        if not raw:
            continue
        types = []
        for part in re.split(r"\s+", raw):
            norm = _normalize_schema_type(part)
            if norm:
                types.append(norm)
        buckets = _schema_entity_buckets(types)
        if not buckets:
            continue
        names: List[str] = []
        for node in tag.find_all(attrs={"property": True}):
            prop = _safe_text(node.get("property")).lower()
            if prop not in {"name", "schema:name", "headline", "title"}:
                continue
            candidate = _safe_text(node.get("content") or " ".join(node.stripped_strings))
            if candidate:
                names.append(candidate)
        if not names:
            fallback = _safe_text(tag.get("content") or " ".join(tag.stripped_strings))
            if fallback:
                names.append(fallback)
        for bucket in buckets:
            for name in names[:4]:
                _add_schema_entity(entities, bucket, name)

    return {k: sorted(v)[:30] for k, v in entities.items()}


def _extract_main_text(soup: BeautifulSoup) -> str:
    # Work on a copy: callers rely on original soup for schema/DOM parsing.
    clone = BeautifulSoup(str(soup), "html.parser")
    for tag in clone(["script", "style", "noscript", "template"]):
        tag.extract()
    node = clone.find("main") or clone.find("article") or clone.body or clone
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


def _detect_author_info(soup: BeautifulSoup, text: str) -> bool:
    raw = (text or "").lower()
    if soup.find(attrs={"rel": re.compile("author", re.I)}):
        return True
    if soup.find(attrs={"itemprop": re.compile("author", re.I)}):
        return True
    if soup.find(attrs={"class": re.compile(r"author|byline|editor|reviewed|эксперт|автор", re.I)}):
        return True
    author_tokens = (
        "author",
        "written by",
        "editor",
        "reviewed by",
        "fact checked",
        "автор",
        "редактор",
        "проверено",
        "эксперт",
        "материал подготовил",
    )
    return any(token in raw for token in author_tokens)


def _detect_contact_info(soup: BeautifulSoup, text: str) -> bool:
    raw = (text or "").lower()
    if soup.find("a", href=re.compile(r"^(mailto:|tel:)", re.I)):
        return True
    if soup.find(attrs={"itemprop": re.compile(r"address|telephone|email|contactpoint", re.I)}):
        return True
    contact_tokens = (
        "contact",
        "contacts",
        "support",
        "customer service",
        "hotline",
        "address",
        "email",
        "e-mail",
        "контакты",
        "связаться",
        "поддержка",
        "горячая линия",
        "адрес",
        "почта",
    )
    return any(token in raw for token in contact_tokens)


def _detect_legal_docs(text: str) -> bool:
    raw = (text or "").lower()
    legal_tokens = (
        "privacy policy",
        "terms of use",
        "terms and conditions",
        "gdpr",
        "ccpa",
        "cookie policy",
        "refund policy",
        "disclaimer",
        "privacy",
        "terms",
        "cookies",
        "политика конфиденциальности",
        "политика обработки персональных данных",
        "условия использования",
        "пользовательское соглашение",
        "оферта",
        "куки",
    )
    return any(token in raw for token in legal_tokens)


def _detect_reviews(soup: BeautifulSoup, text: str) -> bool:
    raw = (text or "").lower()
    if soup.find(attrs={"itemprop": re.compile("review|rating", re.I)}):
        return True
    if soup.find(attrs={"class": re.compile(r"review|rating|testimonial|отзыв", re.I)}):
        return True
    review_tokens = (
        "review",
        "rating",
        "testimonial",
        "stars",
        "score",
        "customer stories",
        "отзыв",
        "отзывы",
        "рейтинг",
        "оценка",
    )
    return any(token in raw for token in review_tokens)


def _detect_trust_badges(text: str) -> bool:
    raw = (text or "").lower()
    badge_tokens = (
        "secure",
        "verified",
        "ssl",
        "tls",
        "https",
        "guarantee",
        "trusted",
        "certified",
        "official partner",
        "money-back",
        "warranty",
        "iso",
        "pci dss",
        "безопасно",
        "защищено",
        "проверено",
        "гарантия",
        "сертификат",
        "лицензия",
    )
    return any(token in raw for token in badge_tokens)


def _extract_author_date_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    text = _safe_text(" ".join(soup.stripped_strings))[:80000]
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

    author_present = bool([x for x in author_candidates if x]) or _detect_author_info(soup, text)
    organization_present = bool(
        soup.find(attrs={"itemprop": re.compile(r"organization|publisher|brand", re.I)})
        or soup.find(attrs={"class": re.compile(r"organization|company|publisher|brand", re.I)})
        or soup.find("meta", attrs={"property": re.compile(r"og:site_name", re.I)})
    )

    return {
        "author_present": author_present,
        "author_samples": [x for x in author_candidates if x][:5],
        "date_present": bool([x for x in date_candidates if x]),
        "date_samples": [x for x in date_candidates if x][:5],
        "has_contact_info": _detect_contact_info(soup, text),
        "has_legal_docs": _detect_legal_docs(text),
        "has_reviews": _detect_reviews(soup, text),
        "trust_badges": _detect_trust_badges(text),
        "organization_present": organization_present,
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
    site_name = ""
    site_name_tag = soup.find("meta", attrs={"property": re.compile(r"og:site_name", re.I)})
    if site_name_tag:
        site_name = _safe_text(site_name_tag.get("content"))
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

    segmentation = segment_content(
        soup=soup,
        rendered_text=full_text,
        extracted_text=raw_main_text,
        links=links,
        headings=headings,
        readability_text=readability_text,
        trafilatura_text=trafilatura_text,
    )
    content_extraction = segmentation.get("content_extraction") or {}
    main_text = _safe_text(segmentation.get("main_text") or raw_main_text)
    main_content_ratio = float(segmentation.get("main_text_ratio") or segmentation.get("main_ratio") or 0.0)
    if main_content_ratio <= 0:
        main_content_ratio = len(main_text) / max(1, len(full_text))
    boilerplate_ratio = float(segmentation.get("boilerplate_ratio") or max(0.0, 1.0 - main_content_ratio))

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
    jsonld_data = _extract_jsonld_schema_data(soup)
    schema_types = list(jsonld_data.get("types") or [])
    schema_entities: Dict[str, Set[str]] = _empty_schema_entities()
    _merge_schema_entities(schema_entities, jsonld_data.get("entities") or {})

    microdata_types: List[str] = []
    rdfa_types: List[str] = []
    microdata_types.extend(_extract_microdata_types_fallback(soup))
    rdfa_types.extend(_extract_rdfa_types_fallback(soup))
    _merge_schema_entities(schema_entities, _extract_dom_schema_entities(soup))
    if extruct:
        try:
            data = extruct.extract(html, base_url=final_url, syntaxes=["microdata", "rdfa"], errors="log")
            microdata_items = data.get("microdata") or []
            rdfa_items = data.get("rdfa") or []
            for item in microdata_items:
                local_types: Set[str] = set()
                local_entities = _empty_schema_entities()
                _collect_schema_data_from_node(item, types_found=local_types, entities=local_entities)
                microdata_types.extend(sorted(local_types))
                _merge_schema_entities(schema_entities, local_entities)
            for item in rdfa_items:
                local_types = set()
                local_entities = _empty_schema_entities()
                _collect_schema_data_from_node(item, types_found=local_types, entities=local_entities)
                rdfa_types.extend(sorted(local_types))
                _merge_schema_entities(schema_entities, local_entities)
        except Exception:
            pass
    # Unique and clean.
    schema_types = sorted({x for x in schema_types if _safe_text(x)})
    microdata_types = sorted({x for x in microdata_types if _safe_text(x)})
    rdfa_types = sorted({x for x in rdfa_types if _safe_text(x)})
    total_types = set(schema_types) | set(microdata_types) | set(rdfa_types)
    schema_entity_payload = {k: sorted(v)[:30] for k, v in schema_entities.items()}
    lower_total = {str(x).strip().lower() for x in total_types}
    required_schema_l = {"organization"}
    if lower_total & {"article", "newsarticle", "blogposting", "techarticle", "analysisnewsarticle", "liveblogposting"}:
        required_schema_l.update({"article", "person"})
    if lower_total & {"product", "offer", "aggregateoffer", "individualproduct"}:
        required_schema_l.add("product")
    if lower_total & {"faqpage", "qapage", "question"}:
        required_schema_l.add("faqpage")
    if lower_total & {"review", "aggregaterating", "rating"}:
        required_schema_l.add("review")
    if lower_total & {"itemlist", "collectionpage"}:
        required_schema_l.add("itemlist")
    if lower_total & {"event", "sportsevent", "musicevent"}:
        required_schema_l.add("event")
    if len(required_schema_l) == 1:
        required_schema_l.add("article")
    coverage_found = len(lower_total & required_schema_l)
    coverage_score = round((coverage_found / max(1, len(required_schema_l))) * 100, 2)
    x_robots_tag = _safe_text((headers or {}).get("X-Robots-Tag") or (headers or {}).get("x-robots-tag"))
    signals = _extract_author_date_signals(soup)
    if not bool(signals.get("organization_present")):
        lower_types = {str(x).lower() for x in total_types}
        signals["organization_present"] = bool({"organization", "localbusiness"} & lower_types)
    social = _extract_social_meta(soup)
    challenge = detect_challenge(status_code, headers, html)
    resources = detect_resource_barriers(final_url, headers, html, soup)
    ai_schema_types = sorted(total_types)
    ai_blocks = detect_ai_blocks(
        soup=soup,
        main_text=main_text,
        full_text=full_text,
        schema_types=ai_schema_types,
    )

    snapshot: Dict[str, Any] = {
        "final_url": final_url,
        "status_code": int(status_code) if status_code is not None else None,
        "redirect_chain": redirect_chain,
        "meta": {
            "title": title,
            "description": meta_description,
            "site_name": site_name,
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
            "main_content_analysis": segmentation.get("main_content_analysis") or {},
            "content_extraction": content_extraction,
            "navigation_detection": segmentation.get("navigation_detection") or {},
            "ads_detection": segmentation.get("ads_detection") or {},
            "utility_detection": segmentation.get("utility_detection") or {},
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
            "entities": schema_entity_payload,
            "coverage_score": coverage_score,
            "coverage_required_types": sorted(required_schema_l),
            "count": len(total_types),
            "jsonld_count": len(schema_types),
            "microdata_count": len(microdata_types),
            "rdfa_count": len(rdfa_types),
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
            "main_ratio": segmentation.get("main_ratio"),
            "main_text_ratio": segmentation.get("main_text_ratio"),
            "nav_ratio": segmentation.get("nav_ratio"),
            "utility_ratio": segmentation.get("utility_ratio"),
            "supporting_ratio": segmentation.get("supporting_ratio"),
            "boilerplate_ratio": segmentation.get("boilerplate_ratio"),
            "segmentation_confidence": segmentation.get("segmentation_confidence"),
            "confidence": segmentation.get("confidence"),
            "extractor_agreement": segmentation.get("extractor_agreement"),
            "main_selectors": segmentation.get("main_selectors") or [],
            "content_extraction": content_extraction,
            "navigation_detection": segmentation.get("navigation_detection") or {},
            "ads_detection": segmentation.get("ads_detection") or {},
            "utility_detection": segmentation.get("utility_detection") or {},
            "main_content_analysis": segmentation.get("main_content_analysis") or {},
            "segment_version": segmentation.get("segment_version") or "seg-fusion-v2",
        },
    }

    if show_headers:
        safe_headers = {str(k): _safe_text(v)[:500] for k, v in (headers or {}).items()}
        snapshot["http"]["headers"] = safe_headers

    return snapshot
