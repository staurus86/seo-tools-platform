"""Adapter bridge for future seopro.py migration."""
from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import re
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup

from .schema import (
    NormalizedSiteAuditPayload,
    SiteAuditProIssue,
    NormalizedSiteAuditRow,
    SiteAuditProSummary,
)


class SiteAuditProAdapter:
    """
    Transitional adapter.
    Current behavior returns a deterministic normalized skeleton so API/UI wiring
    can be shipped before full seopro function-level porting.
    """

    TOKEN_RE = re.compile(r"[a-zA-Z\u0400-\u04FF0-9]{3,}")
    TOKEN_LONG_RE = re.compile(r"\b[a-zA-Z\u0400-\u04FF0-9]+\b")
    STOP_WORDS = {
        "the", "and", "for", "that", "this", "with", "from", "your", "you", "are", "was", "were",
        "about", "into", "http", "https", "www", "com", "site", "page", "seo",
        "\u043a\u0430\u043a", "\u044d\u0442\u043e", "\u0434\u043b\u044f", "\u0447\u0442\u043e", "\u0438\u043b\u0438", "\u043f\u0440\u0438",
    }
    WEAK_ANCHORS = {
        "click here", "here", "read more", "more", "link",
        "\u043f\u043e\u0434\u0440\u043e\u0431\u043d\u0435\u0435", "\u0442\u0443\u0442",
    }
    FILLER_WORDS = {
        "very", "really", "basically", "actually", "simply", "just", "literally", "maybe", "perhaps",
        "\u043f\u0440\u043e\u0441\u0442\u043e", "\u043e\u0447\u0435\u043d\u044c", "\u043a\u0430\u043a \u0431\u044b",
        "\u043d\u0430\u0432\u0435\u0440\u043d\u043e\u0435", "\u0432\u043e\u0437\u043c\u043e\u0436\u043d\u043e",
    }
    TOXIC_WORDS = {
        "hate", "stupid", "idiot", "trash", "scam",
        "\u043d\u0435\u043d\u0430\u0432\u0438\u0436\u0443", "\u0442\u0443\u043f\u043e\u0439", "\u043c\u0443\u0441\u043e\u0440",
    }
    BOILERPLATE_PATTERNS = (
        r"\u00a9",
        r"privacy",
        r"terms",
        r"contacts?",
        r"subscribe",
        r"read also",
        r"\u00a9\s*\d{4}",
        r"\u043f\u043e\u043b\u0438\u0442\u0438\u043a\u0430",
        r"\u0443\u0441\u043b\u043e\u0432\u0438\u044f",
        r"\u043a\u043e\u043d\u0442\u0430\u043a\u0442\u044b",
        r"\u043f\u043e\u0434\u043f\u0438\u0441\u0430\u0442\u044c\u0441\u044f",
    )
    AI_TECH_MARKERS = (
        "ai",
        "chatgpt",
        "generated",
        "llm",
        "neural",
    )
    # Additional modern LLM-style phrases (RU/EN) frequently seen in generated text.
    AI_LLM_STYLE_MARKERS = (
        "as an ai language model",
        "as an ai assistant",
        "i can't browse the internet",
        "i hope this helps",
        "feel free to ask",
        "it is important to note that",
        "it's important to note that",
        "it is worth noting that",
        "in today's fast-paced world",
        "in this comprehensive guide",
        "let's dive into",
        "delve into",
        "overall, it can be said that",
        "when it comes to",
        "one of the key aspects",
        "to sum up",
        "in summary",
        "step-by-step guide",
        "here are some",
        "here's a concise",
        "unlock the potential of",
        "state-of-the-art",
        "robust and scalable",
        "seamlessly",
        "tailored to your needs",
        "\u0432 \u044d\u0442\u043e\u0439 \u0441\u0442\u0430\u0442\u044c\u0435 \u043c\u044b \u0440\u0430\u0441\u0441\u043c\u043e\u0442\u0440\u0438\u043c",
        "\u0434\u0430\u0432\u0430\u0439\u0442\u0435 \u0440\u0430\u0437\u0431\u0435\u0440\u0435\u043c",
        "\u043d\u0438\u0436\u0435 \u043f\u0440\u0438\u0432\u0435\u0434\u0435\u043d\u044b",
        "\u0432\u043e\u0442 \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e",
        "\u0441 \u0443\u0447\u0435\u0442\u043e\u043c \u0432\u044b\u0448\u0435\u0438\u0437\u043b\u043e\u0436\u0435\u043d\u043d\u043e\u0433\u043e",
        "\u0438\u0441\u0445\u043e\u0434\u044f \u0438\u0437 \u044d\u0442\u043e\u0433\u043e",
        "\u043c\u043e\u0436\u043d\u043e \u0432\u044b\u0434\u0435\u043b\u0438\u0442\u044c \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0438\u0435",
        "\u043f\u0440\u0435\u0434\u043b\u0430\u0433\u0430\u044e \u0440\u0430\u0441\u0441\u043c\u043e\u0442\u0440\u0435\u0442\u044c",
        "\u043a\u0430\u043a \u044f\u0437\u044b\u043a\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c",
        "\u043a\u0430\u043a \u0438\u0438 \u043c\u043e\u0434\u0435\u043b\u044c",
        "\u043d\u0430\u0434\u0435\u044e\u0441\u044c, \u044d\u0442\u043e \u043f\u043e\u043c\u043e\u0436\u0435\u0442",
        "\u0435\u0441\u043b\u0438 \u0445\u043e\u0442\u0438\u0442\u0435, \u043c\u043e\u0433\u0443",
    )
    # Parity set from seopro.py detect_ai_markers (phrase-level markers).
    AI_PHRASE_MARKERS = (
        "\u043a\u0430\u043a \u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e",
        "\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u0432\u0430\u0436\u043d\u043e \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u043d\u0435 \u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u0437\u0430\u0431\u044b\u0432\u0430\u0442\u044c",
        "\u0441\u0442\u043e\u0438\u0442 \u0437\u0430\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u0432 \u0446\u0435\u043b\u043e\u043c \u043c\u043e\u0436\u043d\u043e \u0441\u043a\u0430\u0437\u0430\u0442\u044c",
        "\u043c\u043e\u0436\u043d\u043e \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c, \u0447\u0442\u043e",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u0434\u043e\u043b\u0436\u043d\u043e \u0431\u044b\u0442\u044c \u043e\u0442\u043c\u0435\u0447\u0435\u043d\u043e",
        "\u0432\u0430\u0436\u043d\u043e \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u043a\u0430\u043a \u043c\u044b \u0432\u0438\u0434\u0438\u043c",
        "\u0432 \u0441\u043e\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e\u043c \u043c\u0438\u0440\u0435",
        "\u043d\u0430 \u0441\u0435\u0433\u043e\u0434\u043d\u044f\u0448\u043d\u0438\u0439 \u0434\u0435\u043d\u044c",
        "\u0432 \u043d\u0430\u0441\u0442\u043e\u044f\u0449\u0435\u0435 \u0432\u0440\u0435\u043c\u044f",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u043d\u0443\u0436\u043d\u043e \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u043f\u043e\u0434\u0432\u043e\u0434\u044f \u0438\u0442\u043e\u0433",
        "\u0438\u0442\u0430\u043a,",
        "\u0432 \u0437\u0430\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0435",
        "\u0432\u0430\u0436\u043d\u043e\u0435 \u0437\u0430\u043c\u0435\u0447\u0430\u043d\u0438\u0435",
        "\u0441\u0442\u043e\u0438\u0442 \u043e\u0431\u0440\u0430\u0442\u0438\u0442\u044c \u0432\u043d\u0438\u043c\u0430\u043d\u0438\u0435",
        "\u0431\u0435\u0441\u0441\u043f\u043e\u0440\u043d\u043e",
        "\u043a\u0430\u043a \u043f\u0440\u0430\u0432\u0438\u043b\u043e",
        "\u0432 \u0431\u043e\u043b\u044c\u0448\u0438\u043d\u0441\u0442\u0432\u0435 \u0441\u043b\u0443\u0447\u0430\u0435\u0432",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c \u0442\u0430\u043a\u0436\u0435",
        "\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u043c\u043e\u0436\u043d\u043e \u0441\u0434\u0435\u043b\u0430\u0442\u044c \u0432\u044b\u0432\u043e\u0434",
        "\u043e\u0447\u0435\u0432\u0438\u0434\u043d\u043e, \u0447\u0442\u043e",
        "\u044d\u0442\u043e \u043e\u0431\u044a\u044f\u0441\u043d\u044f\u0435\u0442\u0441\u044f \u0442\u0435\u043c",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u0438\u043c\u0435\u0442\u044c \u0432 \u0432\u0438\u0434\u0443",
        "\u043e\u0442\u043c\u0435\u0442\u0438\u043c, \u0447\u0442\u043e",
        "\u043a\u0430\u043a \u0431\u044b\u043b\u043e \u0441\u043a\u0430\u0437\u0430\u043d\u043e",
        "\u0442\u0430\u043a\u0438\u043c \u043e\u0431\u0440\u0430\u0437\u043e\u043c",
        "\u0441\u043b\u0435\u0434\u043e\u0432\u0430\u0442\u0435\u043b\u044c\u043d\u043e",
        "\u0432 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u0435",
        "\u0432 \u043a\u043e\u043d\u0435\u0447\u043d\u043e\u043c \u0441\u0447\u0435\u0442\u0435",
        "\u0432 \u0441\u0432\u044f\u0437\u0438 \u0441 \u044d\u0442\u0438\u043c",
        "\u043d\u0430 \u043e\u0441\u043d\u043e\u0432\u0430\u043d\u0438\u0438 \u0432\u044b\u0448\u0435\u0441\u043a\u0430\u0437\u0430\u043d\u043d\u043e\u0433\u043e",
        "\u0432 \u0441\u0432\u0435\u0442\u0435 \u0432\u044b\u0448\u0435\u0441\u043a\u0430\u0437\u0430\u043d\u043d\u043e\u0433\u043e",
        "\u0440\u0435\u0437\u044e\u043c\u0438\u0440\u0443\u044f",
        "\u0432\u043a\u0440\u0430\u0442\u0446\u0435",
        "\u0432 \u0434\u0432\u0443\u0445 \u0441\u043b\u043e\u0432\u0430\u0445",
        "\u043f\u043e \u0441\u0443\u0442\u0438",
        "\u043f\u043e \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443",
        "\u0432 \u043f\u0440\u0438\u043d\u0446\u0438\u043f\u0435",
        "\u0432 \u043e\u0431\u0449\u0435\u043c \u0438 \u0446\u0435\u043b\u043e\u043c",
        "\u0432 \u0446\u0435\u043b\u043e\u043c",
        "\u0432 \u043e\u0431\u0449\u0435\u0439 \u0441\u043b\u043e\u0436\u043d\u043e\u0441\u0442\u0438",
        "\u043d\u0435 \u0441\u0435\u043a\u0440\u0435\u0442, \u0447\u0442\u043e",
        "\u043e\u0431\u0449\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e, \u0447\u0442\u043e",
        "\u0445\u043e\u0440\u043e\u0448\u043e \u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e, \u0447\u0442\u043e",
        "\u0441\u0442\u043e\u0438\u0442 \u0443\u043f\u043e\u043c\u044f\u043d\u0443\u0442\u044c",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u0443\u043f\u043e\u043c\u044f\u043d\u0443\u0442\u044c",
        "\u043d\u0435\u043b\u044c\u0437\u044f \u043d\u0435 \u0443\u043f\u043e\u043c\u044f\u043d\u0443\u0442\u044c",
        "\u043f\u0440\u0438\u043c\u0435\u0447\u0430\u0442\u0435\u043b\u044c\u043d\u043e, \u0447\u0442\u043e",
        "\u0438\u043d\u0442\u0435\u0440\u0435\u0441\u043d\u043e \u043e\u0442\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u043b\u044e\u0431\u043e\u043f\u044b\u0442\u043d\u043e, \u0447\u0442\u043e",
        "\u0432\u0430\u0436\u043d\u043e \u043f\u043e\u043d\u0438\u043c\u0430\u0442\u044c",
        "\u0441\u043b\u0435\u0434\u0443\u0435\u0442 \u043f\u043e\u043d\u0438\u043c\u0430\u0442\u044c",
        "\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e \u043f\u043e\u043d\u0438\u043c\u0430\u0442\u044c",
        "\u0441\u0442\u043e\u0438\u0442 \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u0445\u043e\u0442\u0435\u043b\u043e\u0441\u044c \u0431\u044b \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0443\u0442\u044c",
        "\u043e\u0441\u043e\u0431\u043e \u043f\u043e\u0434\u0447\u0435\u0440\u043a\u043d\u0435\u043c",
        "\u043e\u0431\u0440\u0430\u0442\u0438\u043c \u0432\u043d\u0438\u043c\u0430\u043d\u0438\u0435",
        "\u0441\u0442\u043e\u0438\u0442 \u043e\u0431\u0440\u0430\u0442\u0438\u0442\u044c \u0432\u043d\u0438\u043c\u0430\u043d\u0438\u0435",
        "\u043e\u0431\u0440\u0430\u0449\u0430\u0435\u043c \u0432\u043d\u0438\u043c\u0430\u043d\u0438\u0435",
        "\u043a\u0430\u043a \u0432\u0438\u0434\u043d\u043e",
        "\u043a\u0430\u043a \u043c\u043e\u0436\u043d\u043e \u0432\u0438\u0434\u0435\u0442\u044c",
        "\u043a\u0430\u043a \u043d\u0435\u0442\u0440\u0443\u0434\u043d\u043e \u0437\u0430\u043c\u0435\u0442\u0438\u0442\u044c",
        "\u043d\u0430 \u0441\u0430\u043c\u043e\u043c \u0434\u0435\u043b\u0435",
        "\u043f\u043e \u043f\u0440\u0430\u0432\u0434\u0435 \u0433\u043e\u0432\u043e\u0440\u044f",
        "\u0447\u0435\u0441\u0442\u043d\u043e \u0433\u043e\u0432\u043e\u0440\u044f",
        "\u0431\u0435\u0437 \u0441\u043e\u043c\u043d\u0435\u043d\u0438\u044f",
        "\u043d\u0435\u0441\u043e\u043c\u043d\u0435\u043d\u043d\u043e",
        "\u0431\u0435\u0437\u0443\u0441\u043b\u043e\u0432\u043d\u043e",
        "\u043e\u0447\u0435\u0432\u0438\u0434\u043d\u043e",
        "\u043a\u043e\u043d\u0435\u0447\u043d\u043e",
        "\u0440\u0430\u0437\u0443\u043c\u0435\u0435\u0442\u0441\u044f",
        "\u0435\u0441\u0442\u0435\u0441\u0442\u0432\u0435\u043d\u043d\u043e",
    )

    def _is_internal_url(self, candidate: str, base_host: str) -> bool:
        parsed = urlparse(candidate)
        if not parsed.scheme.startswith("http"):
            return False
        return parsed.netloc == base_host

    def _normalize_url(self, raw_url: str) -> str:
        clean, _ = urldefrag((raw_url or "").strip())
        if clean.endswith("/") and len(clean) > len(urlparse(clean).scheme) + 3:
            return clean.rstrip("/")
        return clean

    def _extract_internal_links(self, page_url: str, soup: BeautifulSoup, base_host: str) -> List[str]:
        links: List[str] = []
        for tag in soup.find_all("a", href=True):
            href = (tag.get("href") or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            candidate = self._normalize_url(urljoin(page_url, href))
            if self._is_internal_url(candidate, base_host):
                links.append(candidate)
        return links

    def _tokenize(self, text: str) -> List[str]:
        tokens = self.TOKEN_RE.findall((text or "").lower())
        return [t for t in tokens if t not in self.STOP_WORDS]

    def _tokenize_long(self, text: str, min_len: int = 4) -> List[str]:
        return [
            token
            for token in self.TOKEN_LONG_RE.findall((text or "").lower())
            if len(token) >= min_len
        ]

    def _readability_score(self, text: str) -> float:
        # Lightweight readability heuristic: shorter sentences and moderate word length score higher.
        raw = (text or "").strip()
        if not raw:
            return 0.0
        sentences = [s for s in re.split(r"[.!?]+", raw) if s.strip()]
        words = self.TOKEN_LONG_RE.findall(raw)
        if not words:
            return 0.0
        avg_sentence_len = len(words) / max(1, len(sentences))
        avg_word_len = sum(len(w) for w in words) / max(1, len(words))
        score = 100.0 - max(0.0, (avg_sentence_len - 14.0) * 2.2) - max(0.0, (avg_word_len - 5.5) * 8.0)
        return round(max(0.0, min(100.0, score)), 1)

    def _calc_toxicity(self, tokens: List[str]) -> float:
        if not tokens:
            return 0.0
        toxic = sum(1 for t in tokens if t in self.TOXIC_WORDS)
        return round((toxic / max(1, len(tokens))) * 100.0, 2)

    def _calc_filler_ratio(self, text: str) -> float:
        raw = self.TOKEN_LONG_RE.findall((text or "").lower())
        if not raw:
            return 0.0
        filler = sum(1 for t in raw if t in self.FILLER_WORDS)
        return round(filler / len(raw), 4)

    def _avg_sentence_length(self, text: str) -> float:
        raw = (text or "").strip()
        if not raw:
            return 0.0
        words = self.TOKEN_LONG_RE.findall(raw)
        sentences = [s for s in re.split(r"[.!?]+", raw) if s.strip()]
        if not words:
            return 0.0
        return round(len(words) / max(1, len(sentences)), 2)

    def _avg_word_length(self, text: str) -> float:
        words = self.TOKEN_LONG_RE.findall((text or ""))
        if not words:
            return 0.0
        return round(sum(len(w) for w in words) / len(words), 2)

    def _complex_words_percent(self, text: str) -> float:
        words = self.TOKEN_LONG_RE.findall((text or ""))
        if not words:
            return 0.0
        complex_words = [w for w in words if len(w) >= 8]
        return round((len(complex_words) / len(words)) * 100.0, 2)

    def _extract_top_keywords(self, tokens: List[str], top_n: int = 10) -> List[str]:
        if not tokens:
            return []
        tf = Counter(tokens)
        return [t for t, _ in tf.most_common(top_n)]

    def _keyword_density_profile(self, tokens: List[str], top_n: int = 10) -> Dict[str, float]:
        if not tokens:
            return {}
        total = len(tokens)
        tf = Counter(tokens)
        profile: Dict[str, float] = {}
        for term, count in tf.most_common(top_n):
            profile[term] = round((count / total) * 100.0, 3)
        return profile

    def _keyword_stuffing_score(self, text: str) -> float:
        words = (text or "").lower().split()
        if len(words) < 50:
            return 0.0
        filtered = [word for word in words if word.isalpha() and len(word) > 3 and word not in self.STOP_WORDS]
        if not filtered:
            return 0.0
        max_percentage = 0.0
        for _, count in Counter(filtered).most_common(5):
            pct = (count / len(filtered)) * 100.0
            if pct > 3.0:
                max_percentage = max(max_percentage, pct)
        return round(max_percentage, 2)

    def _heading_distribution(self, soup: BeautifulSoup) -> Dict[str, int]:
        return {f"h{i}": len(soup.find_all(f"h{i}")) for i in range(1, 7)}

    def _semantic_tags_count(self, soup: BeautifulSoup) -> int:
        return len(soup.find_all(["main", "article", "section", "aside", "nav", "header", "footer"]))

    def _content_density(self, soup: BeautifulSoup, text: str) -> float:
        text_words = len((text or "").split())
        total_words = len(soup.get_text(" ", strip=True).split())
        if total_words <= 0:
            return 0.0
        return round((text_words / total_words) * 100.0, 2)

    def _boilerplate_percent(self, text: str) -> float:
        raw = text or ""
        if not raw:
            return 0.0
        total = len(raw.split())
        if total <= 0:
            return 0.0
        matches = sum(len(re.findall(pattern, raw, flags=re.I)) for pattern in self.BOILERPLATE_PATTERNS)
        score = min(100.0, (matches * 5.0) / (total / 100.0)) if total > 0 else 0.0
        return round(score, 1)

    def _unique_percent(self, text: str) -> float:
        if not text:
            return 0.0
        words = [word.lower() for word in self.TOKEN_LONG_RE.findall(text) if len(word) > 2]
        if not words:
            return 0.0
        filtered = [word for word in words if word not in self.STOP_WORDS]
        if not filtered:
            return 0.0
        return round(min(100.0, max(0.0, (len(set(filtered)) / len(filtered)) * 100.0)), 1)


    def _detect_structured_data(self, soup: BeautifulSoup) -> Tuple[int, Dict[str, int], List[str]]:
        json_ld_tags = soup.find_all("script", attrs={"type": lambda v: str(v).lower().strip() == "application/ld+json"})
        microdata_items = soup.find_all(attrs={"itemtype": True})
        rdfa_items = soup.find_all(attrs={"typeof": True})
        detail = {
            "json_ld": len(json_ld_tags),
            "microdata": len(microdata_items),
            "rdfa": len(rdfa_items),
        }
        types: Set[str] = set()
        for item in microdata_items:
            itemtype = str(item.get("itemtype") or "").strip()
            if itemtype:
                types.add(itemtype[:120])
        return detail["json_ld"] + detail["microdata"] + detail["rdfa"], detail, sorted(types)[:15]

    def _extract_jsonld_objects(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        objects: List[Dict[str, Any]] = []
        tags = soup.find_all("script", attrs={"type": lambda v: str(v).lower().strip() == "application/ld+json"})
        for tag in tags:
            raw = (tag.string or tag.get_text() or "").strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            stack: List[Any] = [payload]
            while stack:
                current = stack.pop()
                if isinstance(current, list):
                    stack.extend(current)
                    continue
                if not isinstance(current, dict):
                    continue
                objects.append(current)
                graph = current.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)
        return objects

    @staticmethod
    def _jsonld_types(obj: Dict[str, Any]) -> Set[str]:
        raw = obj.get("@type")
        if isinstance(raw, list):
            return {str(x).strip().lower() for x in raw if str(x).strip()}
        if isinstance(raw, str):
            val = raw.strip().lower()
            return {val} if val else set()
        return set()

    def _validate_structured_common(self, soup: BeautifulSoup) -> List[str]:
        codes: List[str] = []
        objects = self._extract_jsonld_objects(soup)
        for obj in objects:
            types = self._jsonld_types(obj)
            if not types:
                continue

            if "product" in types:
                if not str(obj.get("name") or "").strip():
                    codes.append("product_missing_name")
                offers = obj.get("offers")
                offer_items = offers if isinstance(offers, list) else ([offers] if isinstance(offers, dict) else [])
                if not offer_items:
                    codes.append("product_missing_offers")
                else:
                    has_price = any(str((item or {}).get("price") or "").strip() for item in offer_items if isinstance(item, dict))
                    has_currency = any(
                        str((item or {}).get("priceCurrency") or "").strip()
                        for item in offer_items
                        if isinstance(item, dict)
                    )
                    if not has_price:
                        codes.append("product_missing_price")
                    if not has_currency:
                        codes.append("product_missing_price_currency")

            if types.intersection({"article", "newsarticle", "blogposting"}):
                if not str(obj.get("headline") or "").strip():
                    codes.append("article_missing_headline")
                if not str(obj.get("datePublished") or "").strip():
                    codes.append("article_missing_date_published")
                if not obj.get("author"):
                    codes.append("article_missing_author")

            if types.intersection({"organization", "localbusiness"}):
                if not str(obj.get("name") or "").strip():
                    codes.append("organization_missing_name")
                if not str(obj.get("url") or "").strip():
                    codes.append("organization_missing_url")

            if "breadcrumblist" in types:
                item_list = obj.get("itemListElement")
                if not item_list or (isinstance(item_list, list) and len(item_list) == 0):
                    codes.append("breadcrumb_missing_item_list")

            if "faqpage" in types:
                main_entity = obj.get("mainEntity")
                if not main_entity or (isinstance(main_entity, list) and len(main_entity) == 0):
                    codes.append("faq_missing_main_entity")

        return sorted(set(codes))

    def _simhash64(self, text: str) -> int:
        tokens = [t for t in self._tokenize_long(text, min_len=3) if t not in self.STOP_WORDS]
        if not tokens:
            return 0
        tf = Counter(tokens)
        vector = [0] * 64
        for token, weight in tf.items():
            h = int(hashlib.md5(token.encode("utf-8", errors="ignore")).hexdigest()[:16], 16)
            for i in range(64):
                if (h >> i) & 1:
                    vector[i] += weight
                else:
                    vector[i] -= weight
        value = 0
        for i, score in enumerate(vector):
            if score >= 0:
                value |= (1 << i)
        return value

    @staticmethod
    def _hamming64(a: int, b: int) -> int:
        return int((a ^ b).bit_count())

    def _detect_breadcrumbs(self, soup: BeautifulSoup) -> bool:
        if soup.find(attrs={"itemtype": re.compile("BreadcrumbList", re.I)}):
            return True
        for nav in soup.find_all("nav"):
            aria = (nav.get("aria-label") or "").lower()
            if "breadcrumb" in aria:
                return True
        return False

    def _classify_canonical(self, canonical: str, page_url: str, base_host: str) -> str:
        if not canonical:
            return "missing"
        parsed = urlparse(canonical)
        if parsed.scheme and not parsed.scheme.startswith("http"):
            return "invalid"
        if parsed.scheme.startswith("http") and parsed.netloc and parsed.netloc != base_host:
            return "external"
        normalized_canonical = self._normalize_url(urljoin(page_url, canonical))
        normalized_page = self._normalize_url(page_url)
        if normalized_canonical == normalized_page:
            return "self"
        if normalized_canonical:
            return "other"
        return "invalid"

    def _extract_hreflang_data(self, soup: BeautifulSoup, page_url: str) -> Tuple[List[str], Dict[str, str], bool]:
        langs: List[str] = []
        targets: Dict[str, str] = {}
        has_x_default = False
        for tag in soup.find_all("link", href=True):
            rel = [str(x).lower() for x in (tag.get("rel") or [])]
            if "alternate" not in rel:
                continue
            lang = str(tag.get("hreflang") or "").strip()
            if not lang:
                continue
            href = str(tag.get("href") or "").strip()
            if not href:
                continue
            lang_lower = lang.lower()
            normalized_target = self._normalize_url(urljoin(page_url, href))
            langs.append(lang_lower)
            targets[lang_lower] = normalized_target
            if lang_lower == "x-default":
                has_x_default = True
        return langs, targets, has_x_default

    def _content_freshness_days(self, last_modified: str) -> int | None:
        value = (last_modified or "").strip()
        if not value:
            return None
        try:
            dt = parsedate_to_datetime(value)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
            return max(0, int(delta.total_seconds() // 86400))
        except Exception:
            return None

    def _detect_contact_info(self, text: str) -> bool:
        raw = (text or "").lower()
        if re.search(r"(\+?\d[\d\s\-\(\)]{7,}\d)|([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})", raw):
            return True
        contact_tokens = (
            "contact", "contacts", "phone", "call us", "support", "help center",
            "контакт", "контакты", "телефон", "связаться", "обратная связь", "поддержка",
            "горячая линия", "адрес", "email", "e-mail",
        )
        return any(token in raw for token in contact_tokens)

    def _detect_legal_docs(self, text: str) -> bool:
        raw = (text or "").lower()
        legal_tokens = (
            "privacy", "privacy policy", "terms", "terms of use", "terms and conditions", "policy", "cookies", "gdpr",
            "ccpa", "refund policy", "shipping policy", "returns policy", "disclaimer", "public offer",
            "политика конфиденциальности", "политика обработки персональных данных", "условия использования",
            "пользовательское соглашение", "оферта", "публичная оферта", "согласие на обработку",
            "cookie", "куки", "возврат", "доставка", "отказ от ответственности",
        )
        return any(token in raw for token in legal_tokens)

    def _detect_author_info(self, soup: BeautifulSoup, text: str) -> bool:
        raw = (text or "").lower()
        if soup.find(attrs={"rel": re.compile("author", re.I)}):
            return True
        if soup.find(attrs={"itemprop": re.compile("author", re.I)}):
            return True
        if soup.find(attrs={"class": re.compile(r"author|byline|editor|reviewed|эксперт|автор", re.I)}):
            return True
        author_tokens = (
            "author", "written by", "editor", "reviewed by", "fact checked",
            "автор", "редактор", "проверено", "эксперт", "материал подготовил",
        )
        return any(token in raw for token in author_tokens)

    def _detect_reviews(self, soup: BeautifulSoup, text: str) -> bool:
        raw = (text or "").lower()
        if soup.find(attrs={"itemprop": re.compile("review|rating", re.I)}):
            return True
        if soup.find(attrs={"class": re.compile(r"review|rating|testimonial|otzyv|отзыв", re.I)}):
            return True
        review_tokens = (
            "review", "rating", "testimonial", "stars", "score", "customer stories",
            "отзыв", "отзывы", "рейтинг", "оценка", "нам доверяют", "кейсы клиентов",
        )
        return any(token in raw for token in review_tokens)

    def _detect_trust_badges(self, text: str) -> bool:
        raw = (text or "").lower()
        badge_tokens = (
            "secure", "verified", "ssl", "tls", "https", "guarantee", "trusted",
            "certified", "official partner", "money-back", "warranty", "iso", "pci dss",
            "безопасно", "защищено", "проверено", "гарантия", "официальный партнер",
            "сертифицировано", "сертификат", "лицензия",
        )
        return any(token in raw for token in badge_tokens)

    def _cta_text_quality(self, soup: BeautifulSoup) -> float:
        buttons = soup.find_all(["a", "button"])
        if not buttons:
            return 0.0
        good = 0
        for tag in buttons:
            txt = (tag.get_text(" ", strip=True) or "").lower()
            if any(token in txt for token in ("buy", "start", "contact", "book", "sign", "register", "learn more")):
                good += 1
        return round((good / len(buttons)) * 100.0, 1)

    def _h_hierarchy_summary(self, soup: BeautifulSoup, heading_distribution: Dict[str, int]) -> Tuple[str, List[str], Dict[str, Any]]:
        h1_count = int(heading_distribution.get("h1", 0))
        errors: List[str] = []
        heading_sequence = [int(tag.name[1]) for tag in soup.find_all(re.compile(r"^h[1-6]$", re.I))]
        heading_outline = [
            {
                "level": int(tag.name[1]),
                "text": (tag.get_text(" ", strip=True) or "")[:120],
            }
            for tag in soup.find_all(re.compile(r"^h[1-6]$", re.I))[:20]
        ]
        if h1_count == 0:
            errors.append("missing_h1")
        elif h1_count > 1:
            errors.append("multiple_h1")
        if heading_sequence and heading_sequence[0] != 1:
            errors.append("wrong_start")
        for prev, current in zip(heading_sequence, heading_sequence[1:]):
            if (current - prev) > 1:
                errors.append("heading_level_skip")
                break
        if not errors:
            status = "Good"
        elif "wrong_start" in errors:
            status = "Bad (wrong start)"
        elif "missing_h1" in errors:
            status = "Bad (missing h1)"
        elif "multiple_h1" in errors:
            status = "Bad (multiple h1)"
        elif "heading_level_skip" in errors:
            status = "Bad (level skip)"
        else:
            status = "Bad"
        details = {
            "total_headers": int(sum(heading_distribution.values())),
            "h1_count": h1_count,
            "heading_sequence_preview": heading_sequence[:20],
            "heading_outline": heading_outline,
        }
        return status, errors, details

    def _ai_marker_sample(self, text: str, markers: List[str]) -> str:
        raw = text or ""
        if not raw or not markers:
            return ""
        marker = str(markers[0]).strip()
        if not marker:
            return ""
        snippets: List[str] = []
        if re.fullmatch(r"[a-zA-Z0-9\u0400-\u04FF_-]+", marker):
            marker_pattern = rf"\b{re.escape(marker)}\b"
        else:
            marker_pattern = re.escape(marker)
        for m in re.finditer(marker_pattern, raw, flags=re.I):
            start = max(0, m.start() - 70)
            end = min(len(raw), m.end() + 70)
            snippet = re.sub(r"\s+", " ", raw[start:end].strip())
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= 2:
                break
        return " ... ".join(snippets)

    def _detect_ai_markers(self, text: str) -> Tuple[int, List[str]]:
        text_lower = (text or "").lower()
        if not text_lower:
            return 0, []
        found_markers: List[str] = []

        for phrase in self.AI_PHRASE_MARKERS:
            if phrase and phrase in text_lower:
                found_markers.append(phrase)

        for phrase in self.AI_LLM_STYLE_MARKERS:
            if phrase and phrase in text_lower:
                found_markers.append(phrase)

        for marker in self.AI_TECH_MARKERS:
            if re.search(rf"\b{re.escape(marker)}\b", text_lower):
                found_markers.append(marker)

        return len(found_markers), found_markers[:10]

    def _classify_page_type(self, url: str, structured_types: List[str], title: str, body_text: str) -> str:
        parsed = urlparse(url or "")
        path = (parsed.path or "").lower().strip("/")
        stypes = {str(x).lower() for x in (structured_types or [])}
        text = f"{(title or '').lower()} {(body_text or '').lower()}"
        if path in ("",):
            return "home"
        if "product" in stypes or any(x in path for x in ("product", "shop", "catalog", "товар")):
            return "product"
        if "article" in stypes or any(x in path for x in ("blog", "news", "article", "post", "статья", "новост")):
            return "article"
        if any(x in path for x in ("category", "catalog", "collection", "категор")):
            return "category"
        if any(x in text for x in ("privacy policy", "terms", "cookie", "политика", "условия", "согласие")):
            return "legal"
        if any(x in path for x in ("contact", "about", "company", "контакт", "о-компании")):
            return "service"
        return "other"

    def _apply_canonical_and_hreflang_checks(
        self,
        rows: List[NormalizedSiteAuditRow],
        *,
        start_url: str,
        extended_hreflang_checks: bool,
    ) -> None:
        row_by_url: Dict[str, NormalizedSiteAuditRow] = {}
        for r in rows:
            row_by_url[self._normalize_url(r.url)] = r
            if r.final_url:
                row_by_url[self._normalize_url(r.final_url)] = r

        for row in rows:
            canonical_raw = (row.canonical or "").strip()
            if canonical_raw:
                canonical_target = self._normalize_url(urljoin(row.url, canonical_raw))
                target = row_by_url.get(canonical_target)
                if target:
                    row.canonical_target_status = target.status_code
                    row.canonical_target_indexable = target.indexable
                    target_status = int(target.status_code or 0)
                    target_robots = (target.meta_robots or "").lower()
                    if target_status >= 400:
                        row.canonical_conflict = "canonical_target_4xx_5xx"
                        row.issues.append(
                            SiteAuditProIssue(
                                severity="critical",
                                code="canonical_target_error_status",
                                title="Canonical points to an error page",
                                details=f"Canonical target status: {target_status}",
                            )
                        )
                    elif 300 <= target_status < 400:
                        row.canonical_conflict = "canonical_target_redirect"
                        row.issues.append(
                            SiteAuditProIssue(
                                severity="warning",
                                code="canonical_target_redirect",
                                title="Canonical points to a redirect URL",
                                details=f"Canonical target status: {target_status}",
                            )
                        )
                    elif "noindex" in target_robots:
                        row.canonical_conflict = "canonical_target_noindex"
                        row.issues.append(
                            SiteAuditProIssue(
                                severity="warning",
                                code="canonical_target_noindex",
                                title="Canonical points to a noindex page",
                            )
                        )

            robots = (row.meta_robots or "").lower()
            if "noindex" in robots and (row.canonical_status or "").lower() in ("self", "other"):
                row.canonical_conflict = row.canonical_conflict or "noindex_with_canonical"
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="noindex_canonical_conflict",
                        title="Page has both canonical and noindex",
                    )
                )

        if not extended_hreflang_checks:
            return

        lang_re = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$|^x-default$", re.I)
        for row in rows:
            langs = list(row.hreflang_langs or [])
            targets = dict(row.hreflang_targets or {})
            if not langs:
                continue

            seen_langs: Set[str] = set()
            for lang in langs:
                if lang in seen_langs:
                    row.hreflang_issues.append(f"duplicate_lang:{lang}")
                    continue
                seen_langs.add(lang)
                if not lang_re.match(lang):
                    row.hreflang_issues.append(f"invalid_lang_code:{lang}")

            if len(langs) > 1 and not row.hreflang_has_x_default:
                row.hreflang_issues.append("missing_x_default")

            row_norm = self._normalize_url(row.final_url or row.url)
            for lang, target_url in targets.items():
                target_row = row_by_url.get(self._normalize_url(target_url))
                if not target_row:
                    row.hreflang_issues.append(f"target_not_scanned:{lang}")
                    continue
                back_targets = {
                    self._normalize_url(x)
                    for x in (target_row.hreflang_targets or {}).values()
                    if x
                }
                if row_norm not in back_targets:
                    row.hreflang_issues.append(f"missing_reciprocal:{lang}")

            for item in row.hreflang_issues[:15]:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="hreflang_extended_check",
                        title="Extended hreflang check warning",
                        details=item,
                    )
                )

    def _detect_cloaking(self, body_text: str, hidden_content: bool, hidden_nodes_count: int) -> bool:
        if not hidden_content:
            return False
        total_chars = len((body_text or "").strip())
        if total_chars <= 0:
            return False
        if hidden_nodes_count >= 8:
            return True
        return hidden_nodes_count >= 4 and total_chars < 300

    def _extract_anchor_data(
        self, page_url: str, soup: BeautifulSoup, base_host: str
    ) -> Tuple[List[str], int, int, int, int, int]:
        internal_links: List[str] = []
        weak_count = 0
        total = 0
        external = 0
        external_nofollow = 0
        external_follow = 0
        for tag in soup.find_all("a", href=True):
            href = (tag.get("href") or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True).lower())
            candidate = self._normalize_url(urljoin(page_url, href))
            parsed = urlparse(candidate)
            if not parsed.scheme.startswith("http"):
                continue
            total += 1
            if text in self.WEAK_ANCHORS:
                weak_count += 1
            if parsed.netloc == base_host:
                internal_links.append(candidate)
            else:
                external += 1
                rel_values = [r.strip().lower() for r in (tag.get("rel") or []) if isinstance(r, str)]
                if "nofollow" in rel_values:
                    external_nofollow += 1
                else:
                    external_follow += 1
        return internal_links, weak_count, total, external, external_nofollow, external_follow

    def _build_row(
        self,
        source_url: str,
        final_url: str,
        status_code: int,
        status_line: str,
        html: str,
        base_host: str,
        headers: Dict[str, Any],
        response_time_ms: int,
        redirect_count: int,
        html_size_bytes: int,
        detailed_checks: bool,
    ) -> Tuple[NormalizedSiteAuditRow, List[str], str, int, int]:
        soup = BeautifulSoup(html or "", "html.parser")
        body_text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
        title_tags = soup.find_all("title")
        title = (soup.title.string if soup.title and soup.title.string else "").strip()
        title_tags_count = len(title_tags)
        desc_tags = soup.find_all("meta", attrs={"name": lambda v: str(v).lower().strip() == "description"})
        desc_tag = desc_tags[0] if desc_tags else None
        description = (desc_tag.get("content") if desc_tag else "") or ""
        meta_description_tags_count = len(desc_tags)
        robots_tag = soup.find("meta", attrs={"name": "robots"})
        robots_tags = soup.find_all("meta", attrs={"name": lambda v: str(v).lower().strip() == "robots"})
        robots = ((robots_tag.get("content") if robots_tag else "") or "").lower()
        viewport_tag = soup.find("meta", attrs={"name": "viewport"})
        viewport = ((viewport_tag.get("content") if viewport_tag else "") or "").lower()
        charset_declared = bool(soup.find("meta", attrs={"charset": True}))
        multiple_meta_robots = len(robots_tags) > 1
        canonical_tag = soup.find("link", attrs={"rel": lambda x: x and "canonical" in str(x).lower()})
        canonical = (canonical_tag.get("href") if canonical_tag else "") or ""
        canonical_status = self._classify_canonical(canonical=canonical, page_url=final_url, base_host=base_host)
        breadcrumbs = self._detect_breadcrumbs(soup)
        schema_count = len(
            [
                tag
                for tag in soup.find_all("script")
                if ((tag.get("type") or "").lower().strip() == "application/ld+json")
            ]
        )
        structured_data_total, structured_data_detail, structured_types = self._detect_structured_data(soup)
        structured_error_codes = self._validate_structured_common(soup)
        hreflang_langs, hreflang_targets, hreflang_has_x_default = self._extract_hreflang_data(soup=soup, page_url=final_url)
        hreflang_count = len(hreflang_langs)
        dom_nodes_count = len(soup.find_all(True))
        h1_count = len(soup.find_all("h1"))
        h1_text = (soup.find("h1").get_text(" ", strip=True)[:120] if soup.find("h1") else "")
        images = soup.find_all("img")
        image_srcs = [self._normalize_url(urljoin(final_url, str(img.get("src") or "").strip())) for img in images if str(img.get("src") or "").strip()]
        image_src_counter = Counter(image_srcs)
        image_duplicate_src_count = sum(1 for _, c in image_src_counter.items() if c > 1)
        images_external_count = sum(1 for src in image_srcs if urlparse(src).netloc and urlparse(src).netloc != base_host)
        images_modern_format_count = sum(
            1
            for src in image_srcs
            if re.search(r"\.(webp|avif)(?:$|[?#])", src, flags=re.I)
        )
        images_without_alt = sum(1 for img in images if not (img.get("alt") or "").strip())
        generic_alt_count = sum(
            1
            for img in images
            if str(img.get("alt") or "").strip().lower() in {"image", "photo", "picture", "img", "\u0444\u043e\u0442\u043e", "\u043a\u0430\u0440\u0442\u0438\u043d\u043a\u0430"}
        )
        decorative_non_empty_alt_count = sum(
            1
            for img in images
            if (
                str(img.get("role") or "").strip().lower() == "presentation"
                or str(img.get("aria-hidden") or "").strip().lower() == "true"
            )
            and bool(str(img.get("alt") or "").strip())
        )
        images_no_width_height = sum(
            1 for img in images if not (str(img.get("width") or "").strip() and str(img.get("height") or "").strip())
        )
        images_no_lazy_load = sum(
            1
            for img in images
            if ((img.get("loading") or "").strip().lower() != "lazy")
        )
        lists_count = len(soup.find_all(["ul", "ol"]))
        tables_count = len(soup.find_all("table"))
        faq_count = len(soup.find_all(attrs={"itemtype": re.compile("FAQPage", re.I)}))
        cta_count = len(
            [
                tag
                for tag in soup.find_all(["a", "button"])
                if any(word in (tag.get_text(" ", strip=True).lower()) for word in ("buy", "order", "contact", "sign", "register"))
            ]
        )
        hidden_nodes = soup.select('[hidden], [style*="display:none"], [style*="visibility:hidden"]')
        hidden_content = bool(hidden_nodes)
        hidden_nodes_count = len(hidden_nodes)
        deprecated_tags = sorted({t.name for t in soup.find_all(["font", "center", "marquee", "blink"])})
        semantic_tags_count = self._semantic_tags_count(soup)
        heading_distribution = self._heading_distribution(soup)
        h_hierarchy, h_errors, h_details = self._h_hierarchy_summary(soup=soup, heading_distribution=heading_distribution)
        words = self._tokenize(body_text)
        ai_markers_count, ai_markers_list = self._detect_ai_markers(body_text)
        ai_marker_sample = self._ai_marker_sample(body_text, ai_markers_list)
        word_count_est = len(words)
        ai_markers_density_1k = round((ai_markers_count / max(1, word_count_est)) * 1000.0, 2) if word_count_est else 0.0
        filler_phrases = [w for w in self.FILLER_WORDS if re.search(rf"\\b{re.escape(w)}\\b", body_text.lower())][:20]
        unique_word_count = len(set(words))
        top_keywords = self._extract_top_keywords(words, top_n=10)
        keyword_density_profile = self._keyword_density_profile(words, top_n=10)
        keyword_stuffing_score = self._keyword_stuffing_score(body_text)
        lexical_diversity = round(unique_word_count / max(1, len(words)), 3) if words else 0.0
        readability_score = self._readability_score(body_text)
        avg_sentence_length = self._avg_sentence_length(body_text)
        avg_word_length = self._avg_word_length(body_text)
        complex_words_percent = self._complex_words_percent(body_text)
        content_density = self._content_density(soup=soup, text=body_text)
        boilerplate_percent = self._boilerplate_percent(text=body_text)
        toxicity_score = self._calc_toxicity(words)
        filler_ratio = self._calc_filler_ratio(body_text)
        page_type = self._classify_page_type(final_url, structured_types, title, body_text)
        # Guard against false positives on legal/policy pages with formal language patterns.
        ai_false_positive_guard = page_type in {"legal"} or bool(re.search(r"\b(api|sdk|json|http|ssl|tls|csp)\b", body_text.lower()))
        ai_risk_raw = (
            ai_markers_count * 4.0
            + ai_markers_density_1k * 2.0
            + float(toxicity_score) * 0.6
            + float(filler_ratio) * 35.0
        )
        if ai_false_positive_guard and ai_markers_count <= 6:
            ai_risk_raw *= 0.75
        if word_count_est < 120 and ai_markers_count <= 2:
            ai_risk_raw *= 0.8
        ai_risk_score = round(max(0.0, min(100.0, ai_risk_raw)), 1)
        if ai_risk_score >= 70:
            ai_risk_level = "high"
        elif ai_risk_score >= 40:
            ai_risk_level = "medium"
        else:
            ai_risk_level = "low"
        internal_links, weak_anchor_count, anchor_total, external_links, external_nofollow, external_follow = self._extract_anchor_data(
            final_url, soup, base_host
        )
        has_headers = bool(headers)
        content_encoding = (
            str(headers.get("Content-Encoding") or headers.get("content-encoding") or "").strip().lower()
            if has_headers
            else ""
        )
        compression_enabled = bool(content_encoding) if has_headers else None
        cache_control = (
            str(headers.get("Cache-Control") or headers.get("cache-control") or "").strip().lower()
            if has_headers
            else ""
        )
        etag = str(headers.get("ETag") or headers.get("etag") or "").strip() if has_headers else ""
        expires = str(headers.get("Expires") or headers.get("expires") or "").strip() if has_headers else ""
        cache_enabled = (("max-age" in cache_control) or bool(etag) or bool(expires)) if has_headers else None
        x_robots_tag = str(headers.get("X-Robots-Tag") or headers.get("x-robots-tag") or "").strip() if has_headers else ""
        last_modified = str(headers.get("Last-Modified") or headers.get("last-modified") or "").strip() if has_headers else ""
        content_freshness_days = self._content_freshness_days(last_modified)
        is_https = urlparse(final_url).scheme.lower() == "https"
        og_tags = len(soup.find_all("meta", attrs={"property": lambda v: str(v).lower().startswith("og:") if v else False}))
        js_count = len(soup.find_all("script"))
        external_script_tags = [tag for tag in soup.find_all("script", src=True)]
        js_assets_count = len(external_script_tags)
        css_assets_count = len(
            [
                tag
                for tag in soup.find_all("link", href=True)
                if "stylesheet" in [str(x).lower() for x in (tag.get("rel") or [])]
            ]
        )
        render_blocking_js_count = len(
            [
                tag
                for tag in soup.find_all("script", src=True)
                if (
                    not tag.get("async")
                    and not tag.get("defer")
                    and bool(tag.find_parent("head"))
                )
            ]
        )
        preload_hints_count = len(
            [
                tag
                for tag in soup.find_all("link", href=True)
                if "preload" in [str(x).lower() for x in (tag.get("rel") or [])]
            ]
        )
        js_dependence = js_count >= 8
        has_main_tag = bool(soup.find("main"))
        cloaking_detected = self._detect_cloaking(
            body_text=body_text,
            hidden_content=hidden_content,
            hidden_nodes_count=hidden_nodes_count,
        )
        has_contact_info = self._detect_contact_info(body_text)
        has_legal_docs = self._detect_legal_docs(body_text)
        has_author_info = self._detect_author_info(soup, body_text)
        has_reviews = self._detect_reviews(soup, body_text)
        trust_badges = self._detect_trust_badges(body_text)
        cta_text_quality = self._cta_text_quality(soup)
        total_links = anchor_total
        follow_links_total = 0
        nofollow_links_total = 0
        for tag in soup.find_all("a", href=True):
            rel_values = [r.strip().lower() for r in (tag.get("rel") or []) if isinstance(r, str)]
            if "nofollow" in rel_values:
                nofollow_links_total += 1
            else:
                follow_links_total += 1

        parsed_url = urlparse(final_url or source_url)
        query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        url_params_count = len(query_params)
        path_depth = len([seg for seg in (parsed_url.path or "").split("/") if seg])
        crawl_budget_risk = "low"
        if url_params_count >= 3 or path_depth >= 5:
            crawl_budget_risk = "high"
        elif url_params_count >= 1 or path_depth >= 3:
            crawl_budget_risk = "medium"

        perf_penalty = 0.0
        perf_penalty += max(0.0, (response_time_ms - 800) / 120.0)
        perf_penalty += max(0.0, ((html_size_bytes / 1024.0) - 180.0) / 12.0)
        perf_penalty += max(0.0, (dom_nodes_count - 1800) / 220.0)
        perf_penalty += render_blocking_js_count * 2.0
        perf_light_score = round(max(0.0, min(100.0, 100.0 - perf_penalty)), 1)

        csp_present = bool(str(headers.get("Content-Security-Policy") or headers.get("content-security-policy") or "").strip())
        hsts_present = bool(str(headers.get("Strict-Transport-Security") or headers.get("strict-transport-security") or "").strip())
        x_frame_options_present = bool(str(headers.get("X-Frame-Options") or headers.get("x-frame-options") or "").strip())
        referrer_policy_present = bool(str(headers.get("Referrer-Policy") or headers.get("referrer-policy") or "").strip())
        permissions_policy_present = bool(str(headers.get("Permissions-Policy") or headers.get("permissions-policy") or "").strip())
        mixed_content_count = len(re.findall(r"""(?:src|href)\s*=\s*["']http://""", html or "", flags=re.I)) if is_https else 0
        security_headers_score = round(
            (
                (20.0 if csp_present else 0.0)
                + (20.0 if hsts_present else 0.0)
                + (20.0 if x_frame_options_present else 0.0)
                + (20.0 if referrer_policy_present else 0.0)
                + (20.0 if permissions_policy_present else 0.0)
                - min(20.0, mixed_content_count * 2.0)
            ),
            1,
        )

        issues: List[SiteAuditProIssue] = []
        penalty = 0.0
        if status_code >= 400:
            issues.append(
                SiteAuditProIssue(
                    severity="critical",
                    code="http_status_error",
                    title="HTTP status indicates page error",
                    details=f"Status code: {status_code}",
                )
            )
            penalty += 60
        if not title:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="missing_title",
                    title="Title is missing",
                    details="Page has no <title>.",
                )
            )
            penalty += 20
        if detailed_checks and title_tags_count > 1:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="multiple_title_tags",
                    title="Multiple <title> tags found",
                    details=f"Count: {title_tags_count}",
                )
            )
            penalty += 6
        if not description.strip():
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="missing_meta_description",
                    title="Meta description is missing",
                    details="Page has no <meta name='description'>.",
                )
            )
            penalty += 8
        if detailed_checks and meta_description_tags_count > 1:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="multiple_meta_descriptions",
                    title="Multiple meta description tags found",
                    details=f"Count: {meta_description_tags_count}",
                )
            )
            penalty += 4
        if detailed_checks and not charset_declared:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="missing_charset_meta",
                    title="Charset meta declaration is missing",
                )
            )
            penalty += 2
        if detailed_checks and not viewport:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="missing_viewport_meta",
                    title="Viewport meta declaration is missing",
                )
            )
            penalty += 2
        if detailed_checks and multiple_meta_robots:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="multiple_meta_robots",
                    title="Multiple meta robots tags found",
                )
            )
            penalty += 3
        if "noindex" in robots:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="noindex_detected",
                    title="Page contains noindex directive",
                    details=f"meta robots: {robots}",
                )
            )
            penalty += 15
        if not canonical:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="missing_canonical",
                    title="Canonical link is missing",
                )
            )
            penalty += 5
        if len(words) < 120:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="thin_content",
                    title="Thin content detected",
                    details=f"Word count: {len(words)}",
                )
            )
            penalty += 10
        if detailed_checks and perf_light_score < 60:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="light_perf_low_score",
                    title="Low lightweight performance score",
                    details=f"Score: {perf_light_score}",
                )
            )
            penalty += 6
        if h1_count != 1:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="h1_hierarchy_issue",
                    title="H1 hierarchy issue",
                    details=f"H1 count: {h1_count}",
                )
            )
            penalty += 7
        if compression_enabled is False:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="compression_disabled",
                    title="Response compression is not detected",
                )
            )
            penalty += 4
        if cache_enabled is False:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="cache_disabled",
                    title="Cache hints are missing in response headers",
                )
            )
            penalty += 3
        if not is_https:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="non_https_url",
                    title="Page is not served over HTTPS",
                )
            )
            penalty += 12
        if detailed_checks and (images_count := len(images)):
            modern_ratio = (images_modern_format_count / max(1, images_count)) * 100.0
            if modern_ratio < 20.0:
                issues.append(
                    SiteAuditProIssue(
                        severity="info",
                        code="low_modern_image_formats",
                        title="Low usage of WebP/AVIF image formats",
                        details=f"Modern formats: {images_modern_format_count}/{images_count}",
                    )
                )
                penalty += 2
        if detailed_checks and image_duplicate_src_count > 0:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="duplicate_image_sources",
                    title="Duplicate image sources found on page",
                    details=f"Duplicate sources: {image_duplicate_src_count}",
                )
            )
            penalty += 2
        if detailed_checks and generic_alt_count > 0:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="generic_alt_texts",
                    title="Generic image alt texts found",
                    details=f"Generic alt count: {generic_alt_count}",
                )
            )
            penalty += 2
        if detailed_checks and decorative_non_empty_alt_count > 0:
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="decorative_images_with_alt",
                    title="Decorative images should have empty alt",
                    details=f"Decorative with non-empty alt: {decorative_non_empty_alt_count}",
                )
            )
            penalty += 2
        if detailed_checks and crawl_budget_risk == "high":
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="crawl_budget_risk_high",
                    title="High crawl budget risk for URL pattern",
                    details=f"params={url_params_count}, depth={path_depth}",
                )
            )
            penalty += 4
        elif detailed_checks and crawl_budget_risk == "medium":
            issues.append(
                SiteAuditProIssue(
                    severity="info",
                    code="crawl_budget_risk_medium",
                    title="Medium crawl budget risk for URL pattern",
                    details=f"params={url_params_count}, depth={path_depth}",
                )
            )
            penalty += 1
        if detailed_checks and structured_error_codes:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="structured_data_common_errors",
                    title="Common structured data errors detected",
                    details=", ".join(structured_error_codes[:8]),
                )
            )
            penalty += min(12, len(structured_error_codes) * 2)
        if detailed_checks and ai_risk_score >= 70:
            issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="ai_risk_high",
                    title="High AI-text risk signals detected",
                    details=f"risk={ai_risk_score}, density={ai_markers_density_1k}/1k",
                )
            )
            penalty += 4

        indexable = ("noindex" not in robots and status_code < 400 and canonical_status != "external")
        if status_code >= 400:
            indexability_reason = "http_error"
        elif "noindex" in robots:
            indexability_reason = "meta_noindex"
        elif "noindex" in x_robots_tag.lower():
            indexability_reason = "x_robots_noindex"
        elif canonical_status == "external":
            indexability_reason = "canonical_external"
        else:
            indexability_reason = "indexable"

        health_score = max(0.0, round(100.0 - penalty, 1))
        trust_score = round(
            min(
                100.0,
                5.0
                + (20.0 if has_contact_info else 0.0)
                + (20.0 if has_legal_docs else 0.0)
                + (25.0 if has_reviews else 0.0)
                + (30.0 if trust_badges else 0.0),
            ),
            1,
        )
        eeat_components = {
            "expertise": round(min(20.0, 6.0 + (12.0 if has_author_info else 0.0)), 1),
            "authoritativeness": round(min(30.0, 22.0 + (8.0 if has_reviews else 0.0) + (5.0 if trust_badges else 0.0)), 1),
            "trustworthiness": round(
                30.0 if (has_contact_info and has_legal_docs) else (15.0 if (has_contact_info or has_legal_docs) else 0.0),
                1,
            ),
            "experience": round(20.0 if (len(words) >= 300 and has_author_info) else 0.0, 1),
        }
        eeat_score = round(min(100.0, sum(float(v) for v in eeat_components.values())), 1)

        row = NormalizedSiteAuditRow(
            url=source_url,
            final_url=final_url,
            status_code=status_code,
            status_line=(status_line or "").strip() or None,
            response_time_ms=response_time_ms,
            response_headers_count=len(headers or {}),
            html_size_bytes=html_size_bytes,
            content_kb=round((html_size_bytes or 0) / 1024.0, 1),
            dom_nodes_count=dom_nodes_count,
            js_assets_count=js_assets_count,
            css_assets_count=css_assets_count,
            render_blocking_js_count=render_blocking_js_count,
            preload_hints_count=preload_hints_count,
            perf_light_score=perf_light_score,
            redirect_count=redirect_count,
            is_https=is_https,
            compression_enabled=compression_enabled,
            compression_algorithm=content_encoding or None,
            cache_enabled=cache_enabled,
            cache_control=cache_control or None,
            last_modified=last_modified or None,
            content_freshness_days=content_freshness_days,
            indexable=indexable,
            indexability_reason=indexability_reason,
            health_score=health_score,
            title=title,
            title_tags_count=title_tags_count,
            title_len=len(title),
            meta_description=description.strip(),
            meta_description_tags_count=meta_description_tags_count,
            description_len=len(description.strip()),
            charset_declared=charset_declared,
            viewport_declared=bool(viewport),
            multiple_meta_robots=multiple_meta_robots,
            canonical=canonical.strip(),
            canonical_status=canonical_status,
            meta_robots=robots,
            x_robots_tag=x_robots_tag or None,
            breadcrumbs=breadcrumbs,
            structured_data=structured_data_total,
            structured_data_detail=structured_data_detail,
            structured_types=structured_types,
            structured_errors_count=len(structured_error_codes),
            structured_error_codes=structured_error_codes,
            schema_count=schema_count,
            hreflang_count=hreflang_count,
            hreflang_langs=hreflang_langs[:25],
            hreflang_has_x_default=hreflang_has_x_default,
            hreflang_targets=hreflang_targets,
            mobile_friendly_hint=("width=device-width" in viewport) if viewport else None,
            word_count=len(words),
            unique_word_count=unique_word_count,
            keyword_stuffing_score=keyword_stuffing_score,
            top_keywords=top_keywords,
            keyword_density_profile=keyword_density_profile,
            lexical_diversity=lexical_diversity,
            unique_percent=self._unique_percent(body_text),
            readability_score=readability_score,
            avg_sentence_length=avg_sentence_length,
            avg_word_length=avg_word_length,
            complex_words_percent=complex_words_percent,
            content_density=content_density,
            boilerplate_percent=boilerplate_percent,
            toxicity_score=toxicity_score,
            filler_ratio=filler_ratio,
            heading_distribution=heading_distribution,
            h_hierarchy=h_hierarchy,
            h_errors=h_errors,
            h_details=h_details,
            semantic_tags_count=semantic_tags_count,
            html_quality_score=round(
                max(0.0, min(100.0, 50.0 + min(30.0, semantic_tags_count * 2.5) + min(20.0, content_density * 10.0))),
                1,
            ),
            deprecated_tags=deprecated_tags,
            hidden_content=hidden_content,
            cta_count=cta_count,
            cta_text_quality=cta_text_quality,
            lists_count=lists_count,
            tables_count=tables_count,
            faq_count=faq_count,
            h1_count=h1_count,
            h1_text=h1_text,
            images_count=len(images),
            images_without_alt=images_without_alt,
            images_no_alt=images_without_alt,
            images_modern_format_count=images_modern_format_count,
            images_external_count=images_external_count,
            image_duplicate_src_count=image_duplicate_src_count,
            generic_alt_count=generic_alt_count,
            decorative_non_empty_alt_count=decorative_non_empty_alt_count,
            images_optimization={
                "total": len(images),
                "no_alt": images_without_alt,
                "no_width_height": images_no_width_height,
                "no_lazy_load": images_no_lazy_load,
            },
            outgoing_internal_links=len(internal_links),
            outgoing_external_links=external_links,
            external_nofollow_links=external_nofollow,
            external_follow_links=external_follow,
            follow_links_total=follow_links_total,
            nofollow_links_total=nofollow_links_total,
            total_links=total_links,
            weak_anchor_ratio=round((weak_anchor_count / anchor_total), 3) if anchor_total else 0.0,
            anchor_text_quality_score=round(max(0.0, 100.0 - (((weak_anchor_count / anchor_total) * 100.0) if anchor_total else 0.0)), 1),
            ai_markers_count=ai_markers_count,
            ai_markers_list=ai_markers_list,
            ai_marker_sample=ai_marker_sample or None,
            ai_markers_density_1k=ai_markers_density_1k,
            ai_risk_score=ai_risk_score,
            ai_risk_level=ai_risk_level,
            ai_false_positive_guard=ai_false_positive_guard,
            page_type=page_type,
            filler_phrases=filler_phrases,
            url_params_count=url_params_count,
            path_depth=path_depth,
            crawl_budget_risk=crawl_budget_risk,
            csp_present=csp_present,
            hsts_present=hsts_present,
            x_frame_options_present=x_frame_options_present,
            referrer_policy_present=referrer_policy_present,
            permissions_policy_present=permissions_policy_present,
            mixed_content_count=mixed_content_count,
            security_headers_score=security_headers_score,
            og_tags=og_tags,
            js_dependence=js_dependence,
            has_main_tag=has_main_tag,
            cloaking_detected=cloaking_detected,
            has_contact_info=has_contact_info,
            has_legal_docs=has_legal_docs,
            has_author_info=has_author_info,
            has_reviews=has_reviews,
            trust_badges=trust_badges,
            trust_score=trust_score,
            eeat_components=eeat_components,
            eeat_score=eeat_score,
            compression=compression_enabled,
            all_issues=[i.code for i in issues],
            issues=issues,
        )
        return row, internal_links, body_text, weak_anchor_count, anchor_total

    def _compute_pagerank(self, graph: Dict[str, Set[str]]) -> Dict[str, float]:
        nodes = list(graph.keys())
        n = len(nodes)
        if n == 0:
            return {}
        damping = 0.85
        scores = {u: 1.0 / n for u in nodes}
        for _ in range(20):
            new_scores = {u: (1.0 - damping) / n for u in nodes}
            for u in nodes:
                outgoing = graph[u]
                if outgoing:
                    share = scores[u] / len(outgoing)
                    for v in outgoing:
                        new_scores[v] += damping * share
                else:
                    share = scores[u] / n
                    for v in nodes:
                        new_scores[v] += damping * share
            scores = new_scores
        max_score = max(scores.values()) if scores else 1.0
        return {u: round((s / max_score) * 100.0, 2) for u, s in scores.items()}

    def _compute_tfidf_scores(self, page_texts: Dict[str, str], top_n: int = 10) -> Dict[str, Dict[str, float]]:
        if not page_texts:
            return {}
        word_doc_count: Counter = Counter()
        for text in page_texts.values():
            words = set(self._tokenize_long(text, min_len=4))
            word_doc_count.update(words)

        total_docs = len(page_texts)
        result: Dict[str, Dict[str, float]] = {}
        for page_url, text in page_texts.items():
            words = self._tokenize_long(text, min_len=4)
            if not words:
                result[page_url] = {}
                continue
            word_counts = Counter(words)
            tf_idf: Dict[str, float] = {}
            for word, freq in word_counts.most_common(50):
                tf = freq / len(words) if words else 0.0
                doc_freq = word_doc_count.get(word, 0)
                idf = math.log(total_docs / max(1, doc_freq)) if total_docs > 0 else 0.0
                score = tf * idf
                if score > 0.0001:
                    tf_idf[word] = round(score, 6)
            sorted_terms = dict(sorted(tf_idf.items(), key=lambda x: x[1], reverse=True)[:top_n])
            result[page_url] = sorted_terms
        return result

    def _build_semantic_linking_map(
        self, rows: List[NormalizedSiteAuditRow]
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[str]]]:
        if not rows:
            return {}, {}
        keyword_map: Dict[str, Set[str]] = {}
        for row in rows:
            if row.tf_idf_keywords:
                keyword_map[row.url] = set(row.tf_idf_keywords.keys())
            else:
                keyword_map[row.url] = set(row.top_keywords or [])

        topic_clusters: Dict[str, List[str]] = defaultdict(list)
        semantic_by_url: Dict[str, List[Dict[str, Any]]] = {}
        for src in rows:
            src_keywords = keyword_map.get(src.url, set())
            semantic: List[Dict[str, Any]] = []
            for tgt in rows:
                if tgt.url == src.url:
                    continue
                common = src_keywords & keyword_map.get(tgt.url, set())
                if common:
                    common_sorted = sorted(common)
                    semantic.append(
                        {
                            "target_url": tgt.url,
                            "target_title": tgt.title or "",
                            "matching_keywords": common_sorted,
                            "relevance_score": len(common_sorted),
                            "suggested_anchor": common_sorted[0] if common_sorted else "",
                        }
                    )
            semantic_sorted = sorted(semantic, key=lambda x: x["relevance_score"], reverse=True)
            semantic_by_url[src.url] = semantic_sorted
            src.topic_hub = len(semantic_sorted) >= 3
            if semantic_sorted:
                cluster = (semantic_sorted[0].get("matching_keywords") or ["misc"])[0]
            elif src.tf_idf_keywords:
                cluster = next(iter(src.tf_idf_keywords.keys()), "misc")
            else:
                cluster = src.top_keywords[0] if src.top_keywords else "misc"
            src.topic_label = cluster
            topic_clusters[cluster].append(src.url)
        return semantic_by_url, topic_clusters

    def _apply_linking_scores(
        self,
        rows: List[NormalizedSiteAuditRow],
        incoming_counts: Counter,
    ) -> None:
        if not rows:
            return
        for row in rows:
            row.orphan_page = int(incoming_counts.get(row.url, 0)) == 0
            pa = float(row.pagerank or 0.0)
            sem_count = len(row.semantic_links or [])
            incoming = int(incoming_counts.get(row.url, 0))
            anchor_quality = int(min(100, pa * 20 + sem_count * 10 + min(50, incoming * 2)))
            row.anchor_text_quality_score = anchor_quality

            outgoing_internal = int(row.outgoing_internal_links or 0)
            score = 0.0
            score += min(45.0, pa * 45.0)
            score += min(25.0, anchor_quality * 0.25)
            score += min(15.0, incoming * 2.0)
            score += min(10.0, sem_count * 2.0)
            if row.orphan_page:
                score -= 10.0
            if outgoing_internal == 0:
                score -= 5.0
            row.link_quality_score = float(int(max(0.0, min(100.0, score))))

    def _calculate_site_health_scores(self, rows: List[NormalizedSiteAuditRow], incoming_counts: Counter) -> None:
        if not rows:
            return
        for row in rows:
            score = 0.0
            if row.indexable:
                score += 20.0

            score += 5.0 if row.is_https else 0.0
            score += 5.0 if row.mobile_friendly_hint else 0.0
            score += 4.0 if row.compression_enabled else 0.0
            score += 4.0 if (row.cache_control and row.cache_control != "not set") else 0.0
            if row.canonical_status == "self":
                score += 4.0
            elif row.canonical_status == "other":
                score += 2.0
            elif row.canonical_status == "external":
                score += 1.0
            score += min(6.0, float(row.html_quality_score or 0.0) / 100.0 * 6.0)
            if row.response_time_ms is None:
                score += 1.0
            elif row.response_time_ms <= 800:
                score += 4.0
            elif row.response_time_ms <= 1500:
                score += 2.0
            elif row.response_time_ms <= 3000:
                score += 1.0
            score += 2.0 if int(row.structured_data or 0) > 0 else 0.0

            words = int(row.word_count or 0)
            score += 10.0 if words >= 300 else (words / 300.0) * 10.0
            score += min(8.0, float(row.unique_percent or 0.0) / 100.0 * 8.0)
            score += min(5.0, float(row.readability_score or 0.0) / 100.0 * 5.0)
            tox = float(row.toxicity_score or 0.0)
            if tox <= 20:
                score += 4.0
            elif tox <= 40:
                score += 2.0
            elif tox <= 60:
                score += 1.0
            freshness = row.content_freshness_days
            if freshness is None:
                score += 1.0
            elif freshness <= 180:
                score += 3.0
            elif freshness <= 365:
                score += 2.0
            elif freshness <= 730:
                score += 1.0

            title_len = int(row.title_len or 0)
            if 30 <= title_len <= 60:
                score += 5.0
            elif 20 <= title_len <= 70:
                score += 3.0
            desc_len = int(row.description_len or 0)
            if 50 <= desc_len <= 160:
                score += 3.0
            elif 30 <= desc_len <= 170:
                score += 1.0
            if int(row.h1_count or 0) == 1:
                score += 2.0

            no_alt = int(row.images_no_alt or 0)
            score += max(0.0, 2.0 - min(2.0, float(no_alt)))

            incoming = int(incoming_counts.get(row.url, 0))
            score += min(6.0, incoming * 1.5)
            if not row.orphan_page:
                score += 2.0
            if int(row.outgoing_internal_links or 0) > 0:
                score += 2.0

            if int(row.duplicate_title_count or 0) > 1:
                score -= 2.0
            if int(row.duplicate_description_count or 0) > 1:
                score -= 1.0

            row.health_score = round(max(0.0, min(100.0, score)), 1)

    def run(
        self,
        url: str,
        mode: str = "quick",
        max_pages: int = 5,
        batch_urls: List[str] | None = None,
        batch_mode: bool = False,
        extended_hreflang_checks: bool = False,
        progress_callback: Optional[Callable[[int, str, Optional[Dict[str, Any]]], None]] = None,
    ) -> NormalizedSiteAuditPayload:
        def notify(progress: int, message: str, meta: Optional[Dict[str, Any]] = None) -> None:
            if callable(progress_callback):
                progress_callback(progress, message, meta)

        selected_mode = "full" if mode == "full" else "quick"
        page_limit = max(1, min(int(max_pages or 5), 5000))
        timeout = 12

        start_url = self._normalize_url(url)
        base_host = urlparse(start_url).netloc
        if not base_host:
            raise ValueError("Invalid URL for Site Audit Pro")

        prepared_batch_urls: List[str] = []
        if batch_urls:
            seen_batch: Set[str] = set()
            for raw in batch_urls:
                normalized = self._normalize_url(raw)
                if not normalized or normalized in seen_batch:
                    continue
                seen_batch.add(normalized)
                prepared_batch_urls.append(normalized)

        effective_batch_mode = bool(batch_mode and prepared_batch_urls)
        queue: Deque[str] = deque(prepared_batch_urls if effective_batch_mode else [start_url])
        visited: Set[str] = set()
        depth_by_url: Dict[str, int] = {}
        if effective_batch_mode:
            for u in prepared_batch_urls:
                depth_by_url[self._normalize_url(u)] = 0
        else:
            depth_by_url[self._normalize_url(start_url)] = 0
        rows: List[NormalizedSiteAuditRow] = []
        titles_by_url: Dict[str, str] = {}
        descriptions_by_url: Dict[str, str] = {}
        title_counter: Counter = Counter()
        desc_counter: Counter = Counter()
        crawl_errors: List[str] = []
        link_graph: Dict[str, Set[str]] = defaultdict(set)
        incoming_counts: Counter = Counter()
        page_texts: Dict[str, str] = {}
        anchor_quality_raw: Dict[str, Tuple[int, int]] = {}

        session = requests.Session()
        session.headers.update({"User-Agent": "SEO-Tools-SiteAuditPro/0.1"})
        total_target = len(prepared_batch_urls) if effective_batch_mode else page_limit
        total_target = max(1, total_target)

        while queue and len(visited) < page_limit:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            current_norm = self._normalize_url(current)
            current_depth = int(depth_by_url.get(current_norm, 0))

            try:
                response = session.get(current, timeout=timeout, allow_redirects=True)
                final_url = self._normalize_url(response.url or current)
                reason = str(getattr(response, "reason", "") or "").strip()
                status_line = f"{response.status_code} {reason}".strip()
                response_time_ms = int(
                    max(
                        0.0,
                        float(getattr(getattr(response, "elapsed", None), "total_seconds", lambda: 0.0)()) * 1000.0,
                    )
                )
                html_size_bytes = len((response.text or "").encode("utf-8", errors="ignore"))
                row, links, page_text, weak_anchor_count, anchor_total = self._build_row(
                    source_url=current,
                    final_url=final_url,
                    status_code=response.status_code,
                    status_line=status_line,
                    html=response.text or "",
                    base_host=base_host,
                    headers=dict(getattr(response, "headers", {}) or {}),
                    response_time_ms=response_time_ms,
                    redirect_count=len(getattr(response, "history", []) or []),
                    html_size_bytes=html_size_bytes,
                    detailed_checks=(selected_mode == "full"),
                )
                rows.append(row)
                depth_by_url[self._normalize_url(row.url)] = min(depth_by_url.get(self._normalize_url(row.url), current_depth), current_depth)
                depth_by_url[self._normalize_url(final_url)] = min(depth_by_url.get(self._normalize_url(final_url), current_depth), current_depth)
                if row.title:
                    normalized_title = row.title.strip().lower()
                    titles_by_url[row.url] = normalized_title
                    title_counter[normalized_title] += 1
                if row.meta_description:
                    normalized_desc = row.meta_description.strip().lower()
                    descriptions_by_url[row.url] = normalized_desc
                    desc_counter[normalized_desc] += 1
                page_texts[row.url] = page_text
                anchor_quality_raw[row.url] = (weak_anchor_count, anchor_total)
                link_graph[row.url] = set(links)
                for link in links:
                    incoming_counts[link] += 1
                    link_norm = self._normalize_url(link)
                    if link_norm not in depth_by_url:
                        depth_by_url[link_norm] = current_depth + 1
                    if (not effective_batch_mode) and link not in visited and len(visited) + len(queue) < page_limit * 2:
                        queue.append(link)
            except Exception as exc:
                crawl_errors.append(f"{current}: {exc}")
                rows.append(
                    NormalizedSiteAuditRow(
                        url=current,
                        status_code=None,
                        status_line=None,
                        indexable=False,
                        health_score=0.0,
                        issues=[
                            SiteAuditProIssue(
                                severity="critical",
                                code="request_failed",
                                title="Failed to fetch page",
                                details=str(exc),
                            )
                        ],
                    )
                )
                link_graph[current] = set()
                page_texts[current] = ""
                anchor_quality_raw[current] = (0, 0)

            processed_pages = len(visited)
            loop_progress = 25 + int((processed_pages / total_target) * 45)
            loop_progress = max(25, min(70, loop_progress))
            notify(
                loop_progress,
                f"Processed pages: {processed_pages}/{total_target}",
                {
                    "processed_pages": processed_pages,
                    "total_pages": total_target,
                    "queue_size": len(queue),
                    "batch_mode": effective_batch_mode,
                    "current_url": current,
                },
            )

        duplicate_titles = {t for t, count in title_counter.items() if t and count > 1}
        duplicate_desc = {t for t, count in desc_counter.items() if t and count > 1}
        for row in rows:
            row_title = titles_by_url.get(row.url, "")
            row_desc = descriptions_by_url.get(row.url, "")
            row.duplicate_title_count = title_counter.get(row_title, 0) if row_title else 0
            row.duplicate_description_count = desc_counter.get(row_desc, 0) if row_desc else 0
            if row_title in duplicate_titles:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="duplicate_title",
                        title="Duplicate title detected",
                    )
                )
            if row_desc in duplicate_desc:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="duplicate_meta_description",
                        title="Duplicate meta description detected",
                    )
                )
            row_norm = self._normalize_url(row.final_url or row.url)
            row.click_depth = depth_by_url.get(row_norm, depth_by_url.get(self._normalize_url(row.url)))
            if (selected_mode == "full") and (not effective_batch_mode) and row.click_depth is not None and row.click_depth > 3:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="deep_click_depth",
                        title="Page is too deep in click depth",
                        details=f"Click depth: {row.click_depth}",
                    )
                )

        start_norm = self._normalize_url(start_url)
        homepage_row = None
        for row in rows:
            if self._normalize_url(row.url) == start_norm or self._normalize_url(row.final_url or "") == start_norm:
                homepage_row = row
                break
        if homepage_row and (selected_mode == "full"):
            if not homepage_row.csp_present:
                homepage_row.issues.append(
                    SiteAuditProIssue(severity="warning", code="security_missing_csp", title="Homepage missing CSP header")
                )
            if homepage_row.is_https and not homepage_row.hsts_present:
                homepage_row.issues.append(
                    SiteAuditProIssue(severity="warning", code="security_missing_hsts", title="Homepage missing HSTS header")
                )
            if not homepage_row.x_frame_options_present:
                homepage_row.issues.append(
                    SiteAuditProIssue(severity="info", code="security_missing_xfo", title="Homepage missing X-Frame-Options header")
                )
            if not homepage_row.referrer_policy_present:
                homepage_row.issues.append(
                    SiteAuditProIssue(severity="info", code="security_missing_referrer_policy", title="Homepage missing Referrer-Policy header")
                )
            if not homepage_row.permissions_policy_present:
                homepage_row.issues.append(
                    SiteAuditProIssue(severity="info", code="security_missing_permissions_policy", title="Homepage missing Permissions-Policy header")
                )
            if int(homepage_row.mixed_content_count or 0) > 0:
                homepage_row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="security_mixed_content_homepage",
                        title="Homepage contains mixed content links",
                        details=f"Mixed content refs: {homepage_row.mixed_content_count}",
                    )
                )

        self._apply_canonical_and_hreflang_checks(
            rows=rows,
            start_url=start_url,
            extended_hreflang_checks=extended_hreflang_checks,
        )

        simhash_by_url: Dict[str, int] = {}
        row_by_url: Dict[str, NormalizedSiteAuditRow] = {}
        for row in rows:
            row_by_url[row.url] = row
            text = page_texts.get(row.url, "")
            if int(row.word_count or 0) < 80:
                continue
            simhash_by_url[row.url] = self._simhash64(text)

        near_dup_map: Dict[str, Set[str]] = defaultdict(set)
        candidate_urls = list(simhash_by_url.keys())
        for i in range(len(candidate_urls)):
            u1 = candidate_urls[i]
            h1 = simhash_by_url[u1]
            for j in range(i + 1, len(candidate_urls)):
                u2 = candidate_urls[j]
                h2 = simhash_by_url[u2]
                if self._hamming64(h1, h2) <= 6:
                    near_dup_map[u1].add(u2)
                    near_dup_map[u2].add(u1)

        for url_key, near_set in near_dup_map.items():
            row = row_by_url.get(url_key)
            if not row:
                continue
            row.near_duplicate_count = len(near_set)
            row.near_duplicate_urls = sorted(near_set)[:10]
            row.issues.append(
                SiteAuditProIssue(
                    severity="warning",
                    code="near_duplicate_content",
                    title="Near-duplicate content detected",
                    details=f"Similar pages: {len(near_set)}",
                )
            )

        all_urls = [r.url for r in rows]
        allowed = set(all_urls)
        normalized_graph: Dict[str, Set[str]] = {}
        for u in all_urls:
            normalized_graph[u] = {v for v in link_graph.get(u, set()) if v in allowed}

        pagerank_scores = self._compute_pagerank(normalized_graph)
        tfidf_scores = self._compute_tfidf_scores(page_texts, top_n=10)

        for row in rows:
            row.incoming_internal_links = int(incoming_counts.get(row.url, 0))
            row.pagerank = pagerank_scores.get(row.url, 0.0)
            row.tf_idf_keywords = tfidf_scores.get(row.url, {})
            row.top_terms = list(row.tf_idf_keywords.keys())[:10]
            row.topic_label = row.top_terms[0] if row.top_terms else (row.top_keywords[0] if row.top_keywords else "misc")
            weak_count, anchor_total = anchor_quality_raw.get(row.url, (0, 0))
            row.weak_anchor_ratio = round((weak_count / anchor_total), 3) if anchor_total else 0.0
        semantic_by_source, topic_clusters = self._build_semantic_linking_map(rows)
        for row in rows:
            row.semantic_links = semantic_by_source.get(row.url, [])

        self._apply_linking_scores(rows=rows, incoming_counts=incoming_counts)
        self._calculate_site_health_scores(rows=rows, incoming_counts=incoming_counts)

        for row in rows:
            if row.incoming_internal_links == 0 and row.outgoing_internal_links == 0:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="orphan_or_isolated_page",
                        title="Page appears isolated in internal link graph",
                    )
                )
            outgoing_total = (row.outgoing_internal_links or 0) + (row.outgoing_external_links or 0)
            if row.orphan_page:
                row.recommendation = "Add internal links from relevant hub/category pages."
            elif (row.images_without_alt or 0) > 0:
                row.recommendation = "Add descriptive alt text for images."
            elif row.weak_anchor_ratio and row.weak_anchor_ratio > 0.3:
                row.recommendation = "Replace weak anchors with intent-rich descriptive anchors."
            elif row.health_score is not None and row.health_score < 80:
                row.recommendation = "Resolve technical and on-page issues to raise health score."
            elif outgoing_total == 0:
                row.recommendation = "Add contextual internal links to improve crawl paths."
            else:
                row.recommendation = "Maintain page quality and monitor regressions."
            row.all_issues = [issue.code for issue in row.issues]

        semantic_suggestions: List[Dict[str, str]] = []
        for topic, urls in topic_clusters.items():
            if len(urls) < 2:
                continue
            base = urls[0]
            linked = normalized_graph.get(base, set())
            for candidate in urls[1:]:
                if candidate not in linked:
                    semantic_suggestions.append(
                        {
                            "source_url": base,
                            "target_url": candidate,
                            "topic": topic,
                            "reason": "Shared topic without internal link",
                        }
                    )
                if len(semantic_suggestions) >= 200:
                    break
            if len(semantic_suggestions) >= 200:
                break

        severity_counts = {"critical": 0, "warning": 0, "info": 0}
        for row in rows:
            for issue in row.issues:
                sev = (issue.severity or "info").lower()
                if sev not in severity_counts:
                    continue
                severity_counts[sev] += 1

        issues_total = sum(severity_counts.values())
        avg_score = round(sum((r.health_score or 0.0) for r in rows) / len(rows), 1) if rows else 0.0

        summary = SiteAuditProSummary(
            total_pages=len(rows),
            internal_pages=len(rows),
            issues_total=issues_total,
            critical_issues=severity_counts["critical"],
            warning_issues=severity_counts["warning"],
            info_issues=severity_counts["info"],
            score=avg_score,
            mode=selected_mode,
        )

        crawl_budget_summary = {
            "high_risk_urls": sum(1 for r in rows if (r.crawl_budget_risk or "") == "high"),
            "medium_risk_urls": sum(1 for r in rows if (r.crawl_budget_risk or "") == "medium"),
            "parameterized_urls": sum(1 for r in rows if int(r.url_params_count or 0) > 0),
            "deep_path_urls": sum(1 for r in rows if int(r.path_depth or 0) >= 4),
        }
        homepage_security = {}
        if homepage_row:
            homepage_security = {
                "url": homepage_row.url,
                "security_headers_score": homepage_row.security_headers_score,
                "csp_present": homepage_row.csp_present,
                "hsts_present": homepage_row.hsts_present,
                "x_frame_options_present": homepage_row.x_frame_options_present,
                "referrer_policy_present": homepage_row.referrer_policy_present,
                "permissions_policy_present": homepage_row.permissions_policy_present,
                "mixed_content_count": homepage_row.mixed_content_count,
            }

        artifacts: Dict[str, Any] = {
            "migration_stage": "adapter_lightweight_crawl",
            "max_pages_requested": max_pages,
            "max_pages_scanned": len(rows),
            "batch_mode": effective_batch_mode,
            "batch_urls_requested": len(prepared_batch_urls),
            "crawl_errors": crawl_errors[:50],
            "crawl_budget_summary": crawl_budget_summary,
            "homepage_security": homepage_security,
            "topic_clusters_count": len(topic_clusters),
            "semantic_suggestions": semantic_suggestions,
            "notes": [
                "Lightweight crawl adapter is active",
                "Full seopro calculation parity is pending",
            ],
        }
        if effective_batch_mode:
            artifacts["notes"].append("Batch URL mode active: only provided URLs were scanned")

        return NormalizedSiteAuditPayload(
            mode=selected_mode,
            summary=summary,
            rows=rows,
            artifacts=artifacts,
        )

    @staticmethod
    def to_public_results(normalized: NormalizedSiteAuditPayload) -> Dict[str, Any]:
        pages = [row.model_dump() for row in normalized.rows]
        issues = [
            {**issue.model_dump(), "url": row.url}
            for row in normalized.rows
            for issue in row.issues
        ]
        pagerank = sorted(
            [{"url": row.url, "score": row.pagerank or 0.0} for row in normalized.rows],
            key=lambda x: x["score"],
            reverse=True,
        )
        tf_idf = [{"url": row.url, "top_terms": row.top_terms} for row in normalized.rows]
        duplicate_title_groups: Dict[str, List[str]] = defaultdict(list)
        duplicate_desc_groups: Dict[str, List[str]] = defaultdict(list)
        topic_clusters: Dict[str, List[str]] = defaultdict(list)
        for row in normalized.rows:
            title = (row.title or "").strip().lower()
            desc = (row.meta_description or "").strip().lower()
            if row.duplicate_title_count > 1 and title:
                duplicate_title_groups[title].append(row.url)
            if row.duplicate_description_count > 1 and desc:
                duplicate_desc_groups[desc].append(row.url)
            topic_clusters[(row.topic_label or "misc")].append(row.url)

        total_pages = max(1, len(normalized.rows))
        orphan_pages = sum(1 for row in normalized.rows if row.orphan_page)
        topic_hubs = sum(1 for row in normalized.rows if row.topic_hub)
        pages_without_alt = sum(1 for row in normalized.rows if (row.images_without_alt or 0) > 0)
        non_https_pages = sum(1 for row in normalized.rows if row.is_https is False)
        avg_response_time = round(
            sum((row.response_time_ms or 0) for row in normalized.rows) / total_pages,
            1,
        )
        avg_readability = round(
            sum((row.readability_score or 0.0) for row in normalized.rows) / total_pages,
            1,
        )
        avg_link_quality = round(
            sum((row.link_quality_score or 0.0) for row in normalized.rows) / total_pages,
            1,
        )
        avg_perf_light = round(
            sum((row.perf_light_score or 0.0) for row in normalized.rows) / total_pages,
            1,
        )

        pipeline = {
            "pagerank": pagerank,
            "tf_idf": tf_idf,
            "duplicates": {
                "title_groups": [{"value": k, "urls": v} for k, v in duplicate_title_groups.items()],
                "description_groups": [{"value": k, "urls": v} for k, v in duplicate_desc_groups.items()],
            },
            "site_health": {
                "average_health_score": normalized.summary.score,
                "critical_issues": normalized.summary.critical_issues,
                "warning_issues": normalized.summary.warning_issues,
            },
            "semantic_linking_map": normalized.artifacts.get("semantic_suggestions", []),
            "anchor_text_quality": {
                "average_weak_anchor_ratio": round(
                    sum((row.weak_anchor_ratio or 0.0) for row in normalized.rows) / max(1, len(normalized.rows)),
                    3,
                ),
                "pages_with_weak_anchors": sum(1 for row in normalized.rows if (row.weak_anchor_ratio or 0.0) > 0.2),
            },
            "topic_clusters": [{"topic": k, "urls": v, "count": len(v)} for k, v in topic_clusters.items()],
            "link_quality_scores": [{"url": row.url, "score": row.link_quality_score} for row in normalized.rows],
            "metrics": {
                "avg_response_time_ms": avg_response_time,
                "avg_readability_score": avg_readability,
                "avg_link_quality_score": avg_link_quality,
                "avg_perf_light_score": avg_perf_light,
                "orphan_pages": orphan_pages,
                "topic_hubs": topic_hubs,
                "pages_without_alt": pages_without_alt,
                "non_https_pages": non_https_pages,
                "crawl_budget_high_risk": sum(1 for row in normalized.rows if (row.crawl_budget_risk or "") == "high"),
                "crawl_budget_medium_risk": sum(1 for row in normalized.rows if (row.crawl_budget_risk or "") == "medium"),
            },
        }
        return {
            "engine": "site_pro_adapter_v0",
            "mode": normalized.mode,
            "summary": normalized.summary.model_dump(),
            "pages": pages,
            "issues": issues,
            "issues_count": normalized.summary.issues_total,
            "pipeline": pipeline,
            "artifacts": normalized.artifacts,
        }


