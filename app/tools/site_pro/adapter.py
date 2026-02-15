"""Adapter bridge for future seopro.py migration."""
from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import math
import re
from typing import Any, Deque, Dict, List, Set, Tuple
from urllib.parse import urljoin, urldefrag, urlparse

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
    AI_MARKER_RE = re.compile(r"\b(ai|chatgpt|generated|llm|neural)\b", re.I)

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
        return bool(re.search(r"(\+?\d[\d\s\-\(\)]{7,}\d)|(@)|(\bcontact\b)|(\bphone\b)", raw))

    def _detect_legal_docs(self, text: str) -> bool:
        raw = (text or "").lower()
        return any(token in raw for token in ("privacy", "terms", "policy", "cookies", "gdpr"))

    def _detect_author_info(self, soup: BeautifulSoup, text: str) -> bool:
        raw = (text or "").lower()
        if soup.find(attrs={"rel": re.compile("author", re.I)}):
            return True
        return any(token in raw for token in ("author", "written by", "editor"))

    def _detect_reviews(self, soup: BeautifulSoup, text: str) -> bool:
        raw = (text or "").lower()
        if soup.find(attrs={"itemprop": re.compile("review|rating", re.I)}):
            return True
        return any(token in raw for token in ("review", "rating", "testimonial"))

    def _detect_trust_badges(self, text: str) -> bool:
        raw = (text or "").lower()
        return any(token in raw for token in ("secure", "verified", "ssl", "guarantee", "trusted"))

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
        for m in re.finditer(rf"\b{re.escape(marker)}\b", raw, flags=re.I):
            start = max(0, m.start() - 70)
            end = min(len(raw), m.end() + 70)
            snippet = re.sub(r"\s+", " ", raw[start:end].strip())
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= 2:
                break
        return " ... ".join(snippets)

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
        html: str,
        base_host: str,
        headers: Dict[str, Any],
        response_time_ms: int,
        redirect_count: int,
        html_size_bytes: int,
    ) -> Tuple[NormalizedSiteAuditRow, List[str], str, int, int]:
        soup = BeautifulSoup(html or "", "html.parser")
        body_text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
        title = (soup.title.string if soup.title and soup.title.string else "").strip()
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = (desc_tag.get("content") if desc_tag else "") or ""
        robots_tag = soup.find("meta", attrs={"name": "robots"})
        robots = ((robots_tag.get("content") if robots_tag else "") or "").lower()
        viewport_tag = soup.find("meta", attrs={"name": "viewport"})
        viewport = ((viewport_tag.get("content") if viewport_tag else "") or "").lower()
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
        hreflang_count = len(
            [
                tag
                for tag in soup.find_all("link", href=True)
                if (tag.get("rel") and "alternate" in [str(x).lower() for x in tag.get("rel")])
                and bool((tag.get("hreflang") or "").strip())
            ]
        )
        dom_nodes_count = len(soup.find_all(True))
        h1_count = len(soup.find_all("h1"))
        h1_text = (soup.find("h1").get_text(" ", strip=True)[:120] if soup.find("h1") else "")
        images = soup.find_all("img")
        images_without_alt = sum(1 for img in images if not (img.get("alt") or "").strip())
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
        ai_markers_count = len(self.AI_MARKER_RE.findall(body_text))
        ai_markers_list = sorted(set(self.AI_MARKER_RE.findall(body_text)))[:20]
        ai_marker_sample = self._ai_marker_sample(body_text, ai_markers_list)
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
            response_time_ms=response_time_ms,
            html_size_bytes=html_size_bytes,
            content_kb=round((html_size_bytes or 0) / 1024.0, 1),
            dom_nodes_count=dom_nodes_count,
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
            title_len=len(title),
            meta_description=description.strip(),
            description_len=len(description.strip()),
            canonical=canonical.strip(),
            canonical_status=canonical_status,
            meta_robots=robots,
            x_robots_tag=x_robots_tag or None,
            breadcrumbs=breadcrumbs,
            structured_data=structured_data_total,
            structured_data_detail=structured_data_detail,
            structured_types=structured_types,
            schema_count=schema_count,
            hreflang_count=hreflang_count,
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
            filler_phrases=filler_phrases,
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

    def run(self, url: str, mode: str = "quick", max_pages: int = 5) -> NormalizedSiteAuditPayload:
        selected_mode = "full" if mode == "full" else "quick"
        page_limit = max(1, min(int(max_pages or 5), 5000))
        timeout = 12

        start_url = self._normalize_url(url)
        base_host = urlparse(start_url).netloc
        if not base_host:
            raise ValueError("Invalid URL for Site Audit Pro")

        queue: Deque[str] = deque([start_url])
        visited: Set[str] = set()
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

        while queue and len(visited) < page_limit:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            try:
                response = session.get(current, timeout=timeout, allow_redirects=True)
                final_url = self._normalize_url(response.url or current)
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
                    html=response.text or "",
                    base_host=base_host,
                    headers=dict(getattr(response, "headers", {}) or {}),
                    response_time_ms=response_time_ms,
                    redirect_count=len(getattr(response, "history", []) or []),
                    html_size_bytes=html_size_bytes,
                )
                rows.append(row)
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
                    if link not in visited and len(visited) + len(queue) < page_limit * 2:
                        queue.append(link)
            except Exception as exc:
                crawl_errors.append(f"{current}: {exc}")
                rows.append(
                    NormalizedSiteAuditRow(
                        url=current,
                        status_code=None,
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

        artifacts: Dict[str, Any] = {
            "migration_stage": "adapter_lightweight_crawl",
            "max_pages_requested": max_pages,
            "max_pages_scanned": len(rows),
            "crawl_errors": crawl_errors[:50],
            "topic_clusters_count": len(topic_clusters),
            "semantic_suggestions": semantic_suggestions,
            "notes": [
                "Lightweight crawl adapter is active",
                "Full seopro calculation parity is pending",
            ],
        }

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
                "orphan_pages": orphan_pages,
                "topic_hubs": topic_hubs,
                "pages_without_alt": pages_without_alt,
                "non_https_pages": non_https_pages,
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


