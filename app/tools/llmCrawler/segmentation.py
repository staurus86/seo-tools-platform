"""Fusion segmentation engine for noisy pages (feeds/catalog/nav-heavy layouts)."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Tuple
from collections import Counter


_WORD_RE = re.compile(r"[A-Za-zА-Яа-я0-9]+")
_EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}", flags=re.I)
_PHONE_RE = re.compile(r"\+?\d[\d\-\s\(\)]{7,}")

_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "your",
    "about",
    "как",
    "что",
    "это",
    "для",
    "или",
    "если",
    "страница",
    "menu",
    "home",
}

# Strict ad signals only (no false positives by legal/company words).
_AD_TOKENS = (
    "adsbygoogle",
    "doubleclick",
    "yandex_rtb",
    "googletag",
    "adriver",
    "mgid",
    "taboola",
)
_AD_DOMAIN_RE = re.compile(
    r"(doubleclick\.net|googlesyndication\.com|googletagmanager\.com|yandex\.(ru|net)/ads|adriver\.ru|mgid\.com|taboola\.com)",
    flags=re.I,
)

_LIVE_RE = re.compile(r"(\b\d+\s*:\s*\d+\b|\blive\b|qualifier|schedule|match|score|fixture)", flags=re.I)
_NAV_HINT_RE = re.compile(
    r"(catalog|menu|nav|navigation|sidebar|megamenu|mega-menu|section-list|header|footer|breadcrumbs)",
    flags=re.I,
)
_CTA_RE = re.compile(
    r"(buy now|get started|book demo|request quote|contact us|sign up|try free|subscribe|learn more)",
    flags=re.I,
)
_UTILITY_HINT_RE = re.compile(
    r"(contact|contacts|support|phone|email|address|privacy|terms|cookie|login|signup|register|cta)",
    flags=re.I,
)
_REVIEW_RE = re.compile(r"(review|rating|testimonial|case study|обзор|рейтинг|отзыв)", flags=re.I)
_FAQ_RE = re.compile(r"(faq|frequently asked|q&a|question|вопрос|ответ)", flags=re.I)
_PRODUCT_RE = re.compile(r"(product|sku|price|pricing|buy|cart|checkout|товар|цена|купить)", flags=re.I)
_CATEGORY_RE = re.compile(r"(category|catalog|collection|tag|категор|раздел)", flags=re.I)
_LEGAL_RE = re.compile(r"(privacy|terms|policy|legal|gdpr|cookies|оферт|политик|условия)", flags=re.I)
_HEADER_HINT_RE = re.compile(r"(header|topbar|navbar|site-header)", flags=re.I)
_SIDEBAR_HINT_RE = re.compile(r"(sidebar|side-nav|aside|filter|facets?)", flags=re.I)
_NOISE_RE = re.compile(r"(related posts|you may also like|recommended|similar items|share this|поделиться)", flags=re.I)
_LINKISH_HINTS = {
    "home",
    "menu",
    "catalog",
    "category",
    "news",
    "contact",
    "about",
    "privacy",
    "terms",
    "login",
    "signup",
    "register",
}

_TAG_PRIORITY = {
    "main": 1.0,
    "article": 1.0,
    "section.content": 1.0,
    "div.content": 1.0,
    "div.article": 1.0,
    "section": 0.7,
    "div.text": 0.7,
    "div.description": 0.7,
    "header": 0.2,
    "footer": 0.2,
    "nav": 0.15,
    "aside": 0.15,
    "menu": 0.1,
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _words(text: str) -> List[str]:
    return _WORD_RE.findall(str(text or "").lower())


def _word_count(text: str) -> int:
    return len(_words(text))


def _token_set(text: str) -> set[str]:
    return {w for w in _words(text) if len(w) >= 3 and w not in _STOPWORDS}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return float(len(a & b) / max(1, len(a | b)))


def _semantic_density(text: str) -> float:
    words = _words(text)
    if not words:
        return 0.0
    informative = [w for w in words if len(w) >= 4 and w not in _STOPWORDS]
    return round(len(set(informative)) / max(1, len(words)), 4)


def _text_density(text: str) -> float:
    raw = str(text or "")
    return round(len(_words(raw)) / max(1, len(raw)), 4)


def _unique_ratio(text: str) -> float:
    ws = _words(text)
    if not ws:
        return 0.0
    return round(len(set(ws)) / max(1, len(ws)), 4)


def _extractor_agreement(texts: List[str]) -> float:
    cleaned = [t for t in texts if _word_count(t) >= 40]
    if len(cleaned) < 2:
        return 0.0
    sets = [_token_set(x) for x in cleaned]
    sims: List[float] = []
    for i in range(len(sets)):
        for j in range(i + 1, len(sets)):
            sims.append(_jaccard(sets[i], sets[j]))
    if not sims:
        return 0.0
    return round(sum(sims) / len(sims), 4)


def _heading_tokens(headings: Dict[str, Any] | None, soup: Any) -> set[str]:
    raw: List[str] = []
    h = headings or {}
    for item in (h.get("h1_texts") or []):
        raw.append(str(item))
    for item in (h.get("h2_texts") or []):
        raw.append(str(item))
    if not raw:
        for tag in (soup.find_all("h1") or [])[:4]:
            raw.append(" ".join(tag.stripped_strings))
        for tag in (soup.find_all("h2") or [])[:8]:
            raw.append(" ".join(tag.stripped_strings))
    return {w for w in _words(" ".join(raw)) if len(w) >= 3 and w not in _STOPWORDS}


def _heading_overlap(text: str, heading_terms: set[str]) -> float:
    if not heading_terms:
        return 0.0
    text_terms = _token_set(text)
    if not text_terms:
        return 0.0
    return round(len(text_terms & heading_terms) / max(1, len(heading_terms)), 4)


def _text_linkish_ratio(text: str) -> float:
    ws = _words(text)
    if not ws:
        return 1.0
    hint_hits = sum(1 for w in ws if w in _LINKISH_HINTS)
    url_hits = len(re.findall(r"(https?://|www\.|/[a-z0-9_\-]{2,})", str(text or "").lower()))
    ratio = (hint_hits + (url_hits * 2)) / max(1, len(ws))
    return min(1.0, max(0.0, ratio * 3.0))


def _extractor_quality(text: str, heading_terms: set[str]) -> float:
    words = _word_count(text)
    if words == 0:
        return 0.0
    text_len_norm = min(1.0, words / 900.0)
    semantic = min(1.0, max(0.0, _semantic_density(text)))
    overlap = min(1.0, max(0.0, _heading_overlap(text, heading_terms)))
    low_link_density = 1.0 - _text_linkish_ratio(text)
    uniq = min(1.0, max(0.0, _unique_ratio(text)))
    score = (
        (0.30 * text_len_norm)
        + (0.25 * semantic)
        + (0.20 * overlap)
        + (0.15 * low_link_density)
        + (0.10 * uniq)
    )
    return round(min(1.0, max(0.0, score)), 4)


def _heading_based_text(soup: Any) -> str:
    parts: List[str] = []
    for node in soup.find_all(["h1", "h2", "h3", "p", "li"]):
        text = _norm(" ".join(getattr(node, "stripped_strings", [])))
        if not text:
            continue
        if str(getattr(node, "name", "")).lower() in {"h1", "h2", "h3"}:
            parts.append(text)
            continue
        if len(text) >= 60:
            parts.append(text)
        if len(parts) >= 80:
            break
    return _norm(" ".join(parts))


def _landmark_text(soup: Any) -> str:
    parts: List[str] = []
    selectors = [
        "main",
        "article",
        "[role='main']",
        "section[role='main']",
        "section.content",
        "div.content",
        "div.article",
    ]
    for node in soup.select(", ".join(selectors)):
        text = _norm(" ".join(getattr(node, "stripped_strings", [])))
        if _word_count(text) < 30:
            continue
        parts.append(text)
        if len(parts) >= 12:
            break
    return _norm(" ".join(parts))


def _dom_depth(node: Any) -> int:
    depth = 0
    cur = node
    while getattr(cur, "parent", None) is not None and depth < 64:
        depth += 1
        cur = cur.parent
    return depth


def _selector_key(node: Any) -> str:
    tag = str(getattr(node, "name", "") or "").lower()
    classes = [str(x).lower() for x in (getattr(node, "get", lambda *_: [])("class", []) or [])]
    if tag == "section" and "content" in classes:
        return "section.content"
    if tag == "div" and "content" in classes:
        return "div.content"
    if tag == "div" and "article" in classes:
        return "div.article"
    if tag == "div" and "text" in classes:
        return "div.text"
    if tag == "div" and "description" in classes:
        return "div.description"
    return tag


def _priority(selector: str) -> float:
    return float(_TAG_PRIORITY.get(selector, 0.55))


def _strict_ads_detection(soup: Any) -> Dict[str, Any]:
    hits: List[str] = []
    for script in soup.find_all("script"):
        src = str(script.get("src") or "")
        body = str(script.string or script.text or "")
        blob = f"{src} {body}".lower()
        for token in _AD_TOKENS:
            if token in blob:
                hits.append(token)
        if src and _AD_DOMAIN_RE.search(src):
            hits.append("ad_domain_script")
    for frame in soup.find_all("iframe", src=True):
        src = str(frame.get("src") or "")
        if _AD_DOMAIN_RE.search(src):
            hits.append("ad_domain_iframe")
    uniq = list(dict.fromkeys(hits))
    detected = bool(uniq)
    confidence = 0.94 if not detected else min(0.99, 0.7 + (len(uniq) * 0.07))
    return {
        "ads_detected": detected,
        "confidence": round(float(confidence), 4),
        "evidence": uniq[:10],
    }


def _classify_block(
    *,
    node: Any,
    text: str,
    link_count: int,
    ads_detected: bool,
) -> Tuple[str, List[str], float, bool]:
    reasons: List[str] = []
    tag = str(getattr(node, "name", "") or "").lower()
    attrs = " ".join(
        [
            str(getattr(node, "get", lambda *_: "")("id", "") or ""),
            " ".join([str(x) for x in (getattr(node, "get", lambda *_: [])("class", []) or [])]),
            tag,
        ]
    ).lower()
    words = _word_count(text)
    text_density = _text_density(text)
    link_density = float(link_count / max(1, words))
    mega_hint = bool(_NAV_HINT_RE.search(attrs))

    if ads_detected and (
        any(tok in attrs for tok in ("ad", "banner", "promo", "sponsor"))
        or any(tok in text.lower() for tok in _AD_TOKENS)
    ):
        reasons.append("strict ad tech markers")
        return "ads", reasons, 0.92, mega_hint

    if _LIVE_RE.search(text) and (link_density >= 0.12 or words <= 140):
        reasons.append("live/score markers")
        return "live_scores", reasons, 0.84, mega_hint

    if _HEADER_HINT_RE.search(attrs) and link_density > 0.2:
        reasons.append("header/navigation hints")
        return "header", reasons, 0.8, mega_hint
    if _SIDEBAR_HINT_RE.search(attrs) and link_density > 0.18:
        reasons.append("sidebar hints")
        return "sidebar", reasons, 0.8, mega_hint

    if tag in {"footer"}:
        reasons.append("footer tag")
        return "footer", reasons, 0.86, mega_hint
    if tag in {"nav", "menu", "header", "aside"}:
        reasons.append("navigation tag")
        return "nav", reasons, 0.85, mega_hint

    # Generic mega-menu style nav detection.
    if mega_hint and link_density >= 0.5 and text_density < 0.2:
        reasons.append("mega menu link density")
        return "nav", reasons, 0.93, True

    if link_density > 0.58 and text_density < 0.22:
        reasons.append("high link density")
        return "nav", reasons, 0.82, mega_hint

    if _CATEGORY_RE.search(attrs) and link_density >= 0.2:
        reasons.append("category/catalog hints")
        return "category", reasons, 0.78, mega_hint

    if _REVIEW_RE.search(attrs) or _REVIEW_RE.search(text):
        reasons.append("review/testimonial markers")
        return "reviews", reasons, 0.76, mega_hint
    if _FAQ_RE.search(attrs) or _FAQ_RE.search(text):
        reasons.append("faq markers")
        return "faq", reasons, 0.78, mega_hint
    if _PRODUCT_RE.search(attrs) or (_PRODUCT_RE.search(text) and words >= 20):
        reasons.append("product/commerce markers")
        return "product", reasons, 0.77, mega_hint
    if _LEGAL_RE.search(attrs) or _LEGAL_RE.search(text):
        reasons.append("legal/policy markers")
        return "legal", reasons, 0.8, mega_hint
    if _CTA_RE.search(text):
        reasons.append("cta markers")
        return "cta", reasons, 0.75, mega_hint
    if _NOISE_RE.search(text):
        reasons.append("generic recommendation/noise block")
        return "noise", reasons, 0.72, mega_hint

    utility_match = bool(_UTILITY_HINT_RE.search(attrs) or _UTILITY_HINT_RE.search(text))
    has_contact = bool(_EMAIL_RE.search(text) or _PHONE_RE.search(text))
    has_cta = bool(_CTA_RE.search(text))
    if utility_match or has_contact or has_cta:
        reasons.append("utility/contact/cta markers")
        return "utility", reasons, 0.8, mega_hint

    if words < 18:
        reasons.append("short block")
        return "utility", reasons, 0.64, mega_hint

    reasons.append("content candidate")
    return "main", reasons, 0.7, mega_hint


def segment_content(
    *,
    soup: Any,
    rendered_text: str,
    extracted_text: str,
    links: Dict[str, Any] | None = None,
    headings: Dict[str, Any] | None = None,
    readability_text: str | None = None,
    trafilatura_text: str | None = None,
    justext_text: str | None = None,
    fusion_enabled: bool = True,
    max_segments: int = 120,
) -> Dict[str, Any]:
    """Segment page content into main/noise classes with extractor fusion."""
    selectors = [
        "main",
        "article",
        "section",
        "div",
        "nav",
        "header",
        "footer",
        "aside",
        "menu",
    ]
    candidates = soup.select(", ".join(selectors))
    ads_info = _strict_ads_detection(soup)
    segments: List[Dict[str, Any]] = []
    seen_hashes: set[str] = set()
    main_parts: List[str] = []
    main_selectors: Counter = Counter()

    nav_links = 0
    total_links = 0
    mega_menu_detected = False
    utility_blocks = 0
    utility_chars = 0
    supporting_parts: List[str] = []
    repetition_counter: Counter = Counter()
    heading_terms = _heading_tokens(headings, soup)
    total_candidates = max(1, len(candidates))

    for idx, node in enumerate(candidates):
        if len(segments) >= max_segments:
            break
        raw = _norm(" ".join(getattr(node, "stripped_strings", [])))
        if len(raw) < 35:
            continue
        sig = hashlib.sha1(raw[:1500].encode("utf-8", errors="ignore")).hexdigest()
        if sig in seen_hashes:
            continue
        seen_hashes.add(sig)

        selector = _selector_key(node)
        priority = _priority(selector)
        link_count = len(node.find_all("a", href=True))
        words = _word_count(raw)
        text_density = _text_density(raw)
        link_density = round(float(link_count / max(1, words)), 4)
        unique_score = _unique_ratio(raw)
        length_norm = min(1.0, words / 220.0)
        depth = _dom_depth(node)
        depth_weight = max(0.2, 1.0 - min(0.7, abs(depth - 8) / 14.0))
        pos_ratio = idx / max(1, total_candidates - 1)
        position_weight = max(0.15, 1.0 - abs(pos_ratio - 0.45) * 1.4)
        schema_hint = bool(getattr(node, "get", lambda *_: "")("itemtype") or getattr(node, "get", lambda *_: "")("typeof"))
        schema_weight = 1.0 if schema_hint else 0.55
        rep_key = " ".join(_words(raw)[:16])
        repetition_counter[rep_key] += 1
        repetition_score = min(1.0, (repetition_counter[rep_key] - 1) / 4.0)
        block_score = (
            (length_norm * 0.30)
            + ((1.0 - min(1.0, link_density)) * 0.22)
            + (priority * 0.17)
            + (position_weight * 0.08)
            + (unique_score * 0.08)
            + (depth_weight * 0.08)
            + (schema_weight * 0.07)
        )
        block_score = max(0.0, min(1.0, block_score))
        total_links += link_count

        seg_type, reasons, conf, mega_hint = _classify_block(
            node=node,
            text=raw,
            link_count=link_count,
            ads_detected=bool(ads_info.get("ads_detected")),
        )
        if mega_hint and seg_type == "nav":
            mega_menu_detected = True
        if seg_type == "nav":
            nav_links += link_count
        if seg_type == "utility":
            utility_blocks += 1
            utility_chars += len(raw)

        seg_class = "supporting"
        if seg_type in {"nav", "footer", "header", "sidebar", "category"}:
            seg_class = "navigation"
        elif seg_type in {"ads", "live_scores", "noise"}:
            seg_class = "boilerplate"
        elif seg_type in {"utility", "cta", "legal"}:
            seg_class = "utility"
        elif seg_type in {"main", "reviews", "faq", "product"}:
            if block_score >= 0.62 and words >= 70:
                seg_class = "main"
            else:
                seg_class = "supporting"

        if seg_class == "main" and priority >= 0.55:
            main_parts.append(raw)
            main_selectors[selector] += 1
        elif seg_class == "supporting":
            supporting_parts.append(raw)

        segments.append(
            {
                "id": len(segments) + 1,
                "type": seg_type,
                "segment_class": seg_class,
                "selector": selector,
                "tag": str(getattr(node, "name", "") or ""),
                "chars": len(raw),
                "words": words,
                "links": link_count,
                "link_density": link_density,
                "text_density": text_density,
                "priority": round(priority, 3),
                "block_score": round(block_score, 4),
                "position_weight": round(position_weight, 4),
                "unique_score": round(unique_score, 4),
                "repetition_score": round(repetition_score, 4),
                "depth": depth,
                "depth_weight": round(depth_weight, 4),
                "schema_hint": bool(schema_hint),
                "confidence": round(conf, 3),
                "reasons": reasons[:3],
                "text": raw[:500],
            }
        )

    dom_main_text = _norm(" ".join(main_parts))
    dom_supporting_text = _norm(" ".join(supporting_parts))
    heading_based = _heading_based_text(soup)
    landmark_text = _landmark_text(soup)
    extractor_texts = [
        str(extracted_text or ""),
        str(readability_text or ""),
        str(trafilatura_text or ""),
        str(justext_text or ""),
        dom_main_text,
        heading_based,
        landmark_text,
    ]
    agreement = _extractor_agreement(extractor_texts)

    extractor_map = {
        "dom_density": dom_main_text,
        "heading_based": heading_based,
        "readability": str(readability_text or ""),
        "trafilatura": str(trafilatura_text or ""),
    }
    if fusion_enabled:
        extractor_map["semantic_landmark"] = landmark_text
        extractor_map["justext"] = str(justext_text or "")
    extractor_scores = {
        k: _extractor_quality(v, heading_terms)
        for k, v in extractor_map.items()
    }
    primary_extractor = "dom_density"
    if extractor_scores:
        primary_extractor = max(extractor_scores.keys(), key=lambda key: extractor_scores[key])

    fallback_candidates = sorted(
        [_norm(x) for x in extractor_texts if _word_count(x) >= 60],
        key=lambda t: _word_count(t),
        reverse=True,
    )
    main_text = dom_main_text or (fallback_candidates[0] if fallback_candidates else _norm(str(extracted_text or "")))
    if _word_count(main_text) < 80 and _word_count(dom_supporting_text) >= 80:
        main_text = _norm(f"{main_text} {dom_supporting_text}")
    if _word_count(main_text) < 60 and fallback_candidates:
        main_text = fallback_candidates[0]
    if primary_extractor in extractor_map and _word_count(str(extractor_map.get(primary_extractor) or "")) >= 70:
        chosen = _norm(str(extractor_map.get(primary_extractor) or ""))
        if _word_count(chosen) >= _word_count(main_text):
            main_text = chosen
    # Weighted fusion: when extractors disagree, merge top candidates to preserve semantic coverage.
    weighted_candidates = [
        (name, _norm(text), float(extractor_scores.get(name) or 0.0))
        for name, text in extractor_map.items()
        if _word_count(text) >= 55
    ]
    weighted_candidates.sort(key=lambda x: x[2], reverse=True)
    if fusion_enabled and agreement < 0.42 and len(weighted_candidates) >= 2:
        merged_parts: List[str] = []
        seen = set()
        for name, text, score in weighted_candidates[:3]:
            if score < 0.2:
                continue
            for sent in re.split(r"(?<=[.!?])\s+", text):
                clean = _norm(sent)
                if _word_count(clean) < 8:
                    continue
                key = clean.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged_parts.append(clean)
                if len(merged_parts) >= 24:
                    break
            if len(merged_parts) >= 24:
                break
        merged_text = _norm(" ".join(merged_parts))
        if _word_count(merged_text) >= _word_count(main_text):
            main_text = merged_text
            primary_extractor = "weighted_merge"
    main_text = main_text[:140000]

    counts = {"main": 0, "ads": 0, "live_scores": 0, "nav": 0, "footer": 0, "utility": 0}
    class_counts = {"main": 0, "supporting": 0, "navigation": 0, "utility": 0, "boilerplate": 0}
    total_chars = 0
    for seg in segments:
        seg_type = str(seg.get("type") or "utility")
        chars = int(seg.get("chars") or 0)
        counts[seg_type] = counts.get(seg_type, 0) + chars
        seg_class = str(seg.get("segment_class") or "utility")
        class_counts[seg_class] = class_counts.get(seg_class, 0) + chars
        total_chars += chars
    total_chars = max(1, total_chars)

    main_ratio = round(float(class_counts.get("main", 0) / total_chars), 4)
    nav_ratio = round(float(class_counts.get("navigation", 0) / total_chars), 4)
    utility_ratio = round(float(class_counts.get("utility", 0) / total_chars), 4)
    supporting_ratio = round(float(class_counts.get("supporting", 0) / total_chars), 4)
    main_text_ratio = round(float(len(main_text) / max(1, len(str(rendered_text or "")))), 4)
    boilerplate_ratio = round(max(0.0, 1.0 - main_ratio), 4)
    breakdown = {
        "main_pct": round((counts.get("main", 0) / total_chars) * 100, 2),
        "ads_pct": round((counts.get("ads", 0) / total_chars) * 100, 2),
        "live_pct": round((counts.get("live_scores", 0) / total_chars) * 100, 2),
        "nav_pct": round(((counts.get("nav", 0) + counts.get("footer", 0)) / total_chars) * 100, 2),
        "utility_pct": round((counts.get("utility", 0) / total_chars) * 100, 2),
    }

    text_density_main = _text_density(main_text)
    semantic_density = _semantic_density(main_text)
    conf = (
        (agreement * 0.45)
        + (min(1.0, text_density_main / 0.24) * 0.25)
        + (min(1.0, semantic_density * 2.2) * 0.2)
        + (min(1.0, main_text_ratio / 0.6) * 0.1)
    )
    conf = max(0.0, min(1.0, conf))
    reasons: List[str] = []
    if agreement < 0.35:
        reasons.append("Low agreement between extractors")
    if breakdown["nav_pct"] >= 45:
        reasons.append("Navigation density is high")
    if breakdown["main_pct"] < 25:
        reasons.append("Main content share is low")
    if _word_count(main_text) < 80:
        reasons.append("Main text is short after filtering")
    if not reasons:
        reasons.append("Stable extraction consensus")

    level = "high" if conf >= 0.75 else "medium" if conf >= 0.5 else "low"
    nav_ratio = round(float(nav_links / max(1, total_links)), 4) if total_links else 0.0

    extraction_confidence = round(min(1.0, max(0.0, (agreement * 0.45) + (max(extractor_scores.values() or [0.0]) * 0.55))), 4)
    if extraction_confidence >= 0.78:
        confidence_reason = "Strong agreement and dense semantic overlap across extractors."
    elif extraction_confidence >= 0.55:
        confidence_reason = "Moderate extractor agreement; fused primary content selected."
    else:
        confidence_reason = "Low extractor agreement; fallback merge used to preserve meaning."
    if not fusion_enabled:
        confidence_reason = "Legacy extractor pipeline (fusion disabled by feature flag)."

    segment_tree = [
        {
            "id": int(seg.get("id") or 0),
            "type": seg.get("type"),
            "segment_class": seg.get("segment_class"),
            "selector": seg.get("selector"),
            "depth": int(seg.get("depth") or 0),
            "block_score": seg.get("block_score"),
            "confidence": seg.get("confidence"),
        }
        for seg in segments[:80]
    ]
    main_nodes = [int(seg.get("id") or 0) for seg in segments if str(seg.get("segment_class") or "") == "main"][:80]
    noise_nodes = [
        int(seg.get("id") or 0)
        for seg in segments
        if str(seg.get("segment_class") or "") in {"navigation", "utility", "boilerplate"}
    ][:120]

    return {
        "content_segments": segments[:60],
        "main_text": main_text,
        "content_extraction": {
            "primary_extractor": primary_extractor,
            "extractor_scores": extractor_scores,
            "extraction_confidence": extraction_confidence,
            "extractor_agreement_score": agreement,
            "confidence_reason": confidence_reason,
            "coverage_percent": round(main_text_ratio * 100.0, 2),
            "available_extractors": len([x for x in extractor_map.values() if _word_count(str(x or "")) >= 30]),
        },
        "noise_breakdown": breakdown,
        "main_content_confidence": {
            "level": level,
            "score": round(conf, 4),
            "reasons": reasons[:5],
        },
        "main_ratio": main_ratio,
        "main_text_ratio": main_text_ratio,
        "nav_ratio": nav_ratio,
        "utility_ratio": utility_ratio,
        "supporting_ratio": supporting_ratio,
        "boilerplate_ratio": boilerplate_ratio,
        "segmentation_confidence": round(conf, 4),
        "confidence": round(conf, 4),
        "extractor_agreement": agreement,
        "main_selectors": [k for k, _ in main_selectors.most_common(6)],
        "segment_tree": segment_tree,
        "main_content_nodes": main_nodes,
        "noise_nodes": noise_nodes,
        "navigation_detection": {
            "mega_menu_detected": bool(mega_menu_detected),
            "nav_link_ratio": nav_ratio,
        },
        "ads_detection": ads_info,
        "utility_detection": {
            "utility_blocks": int(utility_blocks),
            "utility_ratio": round(float(utility_chars / total_chars), 4),
        },
        "main_content_analysis": {
            "confidence": round(conf, 4),
            "semantic_density": round(semantic_density, 4),
            "text_density": round(text_density_main, 4),
        },
        "segment_version": "seg-fusion-v3" if fusion_enabled else "seg-fusion-v2",
    }
