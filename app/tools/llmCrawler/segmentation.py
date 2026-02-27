"""Heuristic content segmentation for noisy pages (feeds/live/ads/navigation)."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List


_ADS_RE = re.compile(
    r"(реклама\s*18\+|рекламодатель|инн|ооо|erid|\bpromo\b|\badvert\b|\bad\b)",
    flags=re.I,
)
_LIVE_RE = re.compile(
    r"(\b\d+\s*:\s*\d+\b|\blive\b|продолжается|pgl|qualifier|матч|тайм|сет)",
    flags=re.I,
)
_UTILITY_RE = re.compile(
    r"(login|sign in|register|subscription|newsletter|search|filter|sort|подписк|вход|регистрац|поиск|фильтр)",
    flags=re.I,
)
_NAV_HINT_RE = re.compile(r"(menu|nav|navigation|header|sidebar|breadcrumbs|footer|подвал|меню|навигац)", flags=re.I)
_FOOTER_HINT_RE = re.compile(r"(footer|copyright|all rights reserved|политика|terms|условия|копирайт)", flags=re.I)
_WORD_RE = re.compile(r"[A-Za-zА-Яа-я0-9]+")


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _word_count(text: str) -> int:
    return len(_WORD_RE.findall(text))


def _short_token_ratio(text: str) -> float:
    words = _WORD_RE.findall(text.lower())
    if not words:
        return 0.0
    short = [w for w in words if len(w) <= 3]
    return float(len(short) / max(1, len(words)))


def _segment_type(node: Any, text: str, link_count: int) -> tuple[str, List[str], float]:
    reasons: List[str] = []
    tag_name = str(getattr(node, "name", "") or "").lower()
    attrs = " ".join(
        [
            " ".join(getattr(node, "get", lambda *_: [])("class", []) or []),
            str(getattr(node, "get", lambda *_: "")("id", "") or ""),
            tag_name,
        ]
    ).strip()
    words = _word_count(text)
    link_density = float(link_count / max(1, words))
    short_ratio = _short_token_ratio(text)
    score_hits = len(re.findall(r"\b\d+\s*:\s*\d+\b", text))

    if _ADS_RE.search(text) or _ADS_RE.search(attrs):
        reasons.append("ad markers")
        return "ads", reasons, 0.9
    if _LIVE_RE.search(text) or (_LIVE_RE.search(attrs) and words < 500) or score_hits >= 2:
        reasons.append("live score markers")
        if score_hits >= 2:
            reasons.append("repeated score patterns")
        return "live_scores", reasons, 0.88
    if tag_name == "footer" or _FOOTER_HINT_RE.search(attrs):
        reasons.append("footer tag/hints")
        return "footer", reasons, 0.85
    if (
        tag_name in {"nav", "header"}
        or _NAV_HINT_RE.search(attrs)
        or (link_density > 0.28 and words < 260)
        or (short_ratio > 0.55 and link_density > 0.18)
    ):
        reasons.append("navigation density")
        return "nav", reasons, 0.8
    if _UTILITY_RE.search(text) or _UTILITY_RE.search(attrs):
        reasons.append("utility markers")
        return "utility", reasons, 0.72
    return "main", reasons, 0.6


def segment_content(
    *,
    soup: Any,
    rendered_text: str,
    extracted_text: str,
    links: Dict[str, Any] | None = None,
    headings: Dict[str, Any] | None = None,
    max_segments: int = 90,
) -> Dict[str, Any]:
    """Segment page content into meaningful/noise classes using deterministic heuristics."""
    candidates = soup.select("main, article, section, nav, footer, aside, header, table, div")
    segments: List[Dict[str, Any]] = []
    seen_hashes: set[str] = set()
    main_text_parts: List[str] = []

    for node in candidates:
        if len(segments) >= max_segments:
            break
        raw = _norm(" ".join(getattr(node, "stripped_strings", [])))
        if len(raw) < 35:
            continue
        key = hashlib.sha1(raw[:1200].encode("utf-8", errors="ignore")).hexdigest()
        if key in seen_hashes:
            continue
        seen_hashes.add(key)

        link_count = len(node.find_all("a", href=True))
        seg_type, reasons, confidence = _segment_type(node, raw, link_count)
        words = _word_count(raw)
        link_density = round(float(link_count / max(1, words)), 4)
        if seg_type == "main":
            main_text_parts.append(raw)
        segments.append(
            {
                "id": len(segments) + 1,
                "type": seg_type,
                "tag": str(getattr(node, "name", "") or ""),
                "chars": len(raw),
                "words": words,
                "links": link_count,
                "link_density": link_density,
                "confidence": round(confidence, 3),
                "reasons": reasons[:3],
                "text": raw[:500],
            }
        )

    # Fallback: if segmentation did not collect meaningful content, keep extracted body.
    main_text = _norm(" ".join(main_text_parts))[:120000]
    if len(main_text) < 200:
        main_text = _norm(str(extracted_text or ""))[:120000]

    counts = {"main": 0, "ads": 0, "live_scores": 0, "nav": 0, "footer": 0, "utility": 0}
    total_chars = 0
    for seg in segments:
        seg_type = str(seg.get("type") or "utility")
        chars = int(seg.get("chars") or 0)
        total_chars += chars
        counts[seg_type] = counts.get(seg_type, 0) + chars
    total_chars = max(1, total_chars)

    breakdown = {
        "main_pct": round((counts.get("main", 0) / total_chars) * 100, 2),
        "ads_pct": round((counts.get("ads", 0) / total_chars) * 100, 2),
        "live_pct": round((counts.get("live_scores", 0) / total_chars) * 100, 2),
        "nav_pct": round(((counts.get("nav", 0) + counts.get("footer", 0)) / total_chars) * 100, 2),
        "utility_pct": round((counts.get("utility", 0) / total_chars) * 100, 2),
    }

    reasons: List[str] = []
    level = "medium"
    if breakdown["main_pct"] >= 65 and (breakdown["ads_pct"] + breakdown["live_pct"] + breakdown["nav_pct"]) <= 35:
        level = "high"
        reasons.append("Main segments dominate page text")
    elif breakdown["main_pct"] < 40:
        level = "low"
        reasons.append("Main segments are weak compared to feed/navigation noise")
    if breakdown["ads_pct"] >= 20:
        reasons.append("Heavy ad markers detected")
    if breakdown["live_pct"] >= 18:
        reasons.append("Live-score stream patterns detected")
    if breakdown["nav_pct"] >= 35:
        reasons.append("Navigation/footer density is high")
    if len(main_text) < 300:
        level = "low"
        reasons.append("Main text after segmentation is too short")
    if not reasons:
        reasons.append("Balanced content segmentation")

    return {
        "content_segments": segments[:60],
        "main_text": main_text,
        "noise_breakdown": breakdown,
        "main_content_confidence": {"level": level, "reasons": reasons[:5]},
        "segment_version": "seg-v1",
    }
