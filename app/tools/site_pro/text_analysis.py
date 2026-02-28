"""Text analysis utilities for Site Audit Pro."""
from __future__ import annotations

import re
from collections import Counter
from typing import Dict, List

from .constants import FILLER_WORDS, STOP_WORDS, TOXIC_WORDS, TOKEN_RE, TOKEN_LONG_RE


def _tokenize(text: str) -> List[str]:
    tokens = TOKEN_RE.findall((text or "").lower())
    return [t for t in tokens if t not in STOP_WORDS]


def _tokenize_long(text: str, min_len: int = 4) -> List[str]:
    return [
        token
        for token in TOKEN_LONG_RE.findall((text or "").lower())
        if len(token) >= min_len
    ]


def _readability_score(text: str) -> float:
    # Lightweight readability heuristic: shorter sentences and moderate word length score higher.
    raw = (text or "").strip()
    if not raw:
        return 0.0
    sentences = [s for s in re.split(r"[.!?]+", raw) if s.strip()]
    words = TOKEN_LONG_RE.findall(raw)
    if not words:
        return 0.0
    avg_sentence_len = len(words) / max(1, len(sentences))
    avg_word_len = sum(len(w) for w in words) / max(1, len(words))
    score = 100.0 - max(0.0, (avg_sentence_len - 14.0) * 2.2) - max(0.0, (avg_word_len - 5.5) * 8.0)
    return round(max(0.0, min(100.0, score)), 1)


def _calc_toxicity(tokens: List[str]) -> float:
    if not tokens:
        return 0.0
    toxic = sum(1 for t in tokens if t in TOXIC_WORDS)
    return round((toxic / max(1, len(tokens))) * 100.0, 2)


def _calc_filler_ratio(text: str) -> float:
    raw = TOKEN_LONG_RE.findall((text or "").lower())
    if not raw:
        return 0.0
    filler = sum(1 for t in raw if t in FILLER_WORDS)
    return round(filler / len(raw), 4)


def _avg_sentence_length(text: str) -> float:
    raw = (text or "").strip()
    if not raw:
        return 0.0
    words = TOKEN_LONG_RE.findall(raw)
    sentences = [s for s in re.split(r"[.!?]+", raw) if s.strip()]
    if not words:
        return 0.0
    return round(len(words) / max(1, len(sentences)), 2)


def _avg_word_length(text: str) -> float:
    words = TOKEN_LONG_RE.findall((text or ""))
    if not words:
        return 0.0
    return round(sum(len(w) for w in words) / len(words), 2)


def _complex_words_percent(text: str) -> float:
    words = TOKEN_LONG_RE.findall((text or ""))
    if not words:
        return 0.0
    complex_words = [w for w in words if len(w) >= 8]
    return round((len(complex_words) / len(words)) * 100.0, 2)


def _extract_top_keywords(tokens: List[str], top_n: int = 10) -> List[str]:
    if not tokens:
        return []
    tf = Counter(tokens)
    return [t for t, _ in tf.most_common(top_n)]


def _keyword_density_profile(tokens: List[str], top_n: int = 10) -> Dict[str, float]:
    if not tokens:
        return {}
    total = len(tokens)
    tf = Counter(tokens)
    profile: Dict[str, float] = {}
    for term, count in tf.most_common(top_n):
        profile[term] = round((count / total) * 100.0, 3)
    return profile


def _keyword_stuffing_score(text: str) -> float:
    words = (text or "").lower().split()
    if len(words) < 50:
        return 0.0
    filtered = [word for word in words if word.isalpha() and len(word) > 3 and word not in STOP_WORDS]
    if not filtered:
        return 0.0
    max_percentage = 0.0
    for _, count in Counter(filtered).most_common(5):
        pct = (count / len(filtered)) * 100.0
        if pct > 3.0:
            max_percentage = max(max_percentage, pct)
    return round(max_percentage, 2)
