"""Single-page OnPage audit service."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
import re

import requests
from bs4 import BeautifulSoup


_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+", re.IGNORECASE)
_SENTENCE_SPLIT_RE = re.compile(r"[.!?]+")

_STOPWORDS_RU = {
    "и", "в", "во", "на", "с", "со", "по", "под", "над", "к", "ко", "у", "о", "об", "от", "до", "для",
    "не", "ни", "это", "как", "а", "но", "или", "ли", "же", "бы", "что", "чтобы", "из", "за", "при",
    "мы", "вы", "они", "он", "она", "оно", "их", "его", "ее", "наш", "ваш", "этот", "эта", "эти",
}
_STOPWORDS_EN = {
    "the", "a", "an", "and", "or", "but", "if", "for", "to", "of", "in", "on", "at", "from", "with",
    "by", "as", "is", "are", "was", "were", "be", "been", "it", "this", "that", "these", "those",
    "we", "you", "they", "he", "she", "them", "our", "your",
}


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _ensure_url(raw_url: str) -> str:
    url = _norm_text(raw_url)
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


def _tokens(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _keyword_presence(haystack: str, keyword: str) -> bool:
    if not haystack or not keyword:
        return False
    return keyword.lower() in haystack.lower()


class OnPageAuditServiceV1:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def _collect_visible_text(self, soup: BeautifulSoup) -> str:
        for tag in soup(["script", "style", "noscript", "template"]):
            tag.extract()
        body = soup.body or soup
        text = body.get_text(" ", strip=True)
        return re.sub(r"\s+", " ", text).strip()

    def _top_ngrams(self, tokens: List[str], n: int, limit: int = 20) -> List[Dict[str, Any]]:
        if n <= 1 or len(tokens) < n:
            return []
        total = max(1, len(tokens) - n + 1)
        grams = Counter(" ".join(tokens[i:i + n]) for i in range(0, len(tokens) - n + 1))
        rows: List[Dict[str, Any]] = []
        for gram, count in grams.most_common(limit):
            rows.append({"term": gram, "count": count, "pct": round(count / total * 100.0, 3)})
        return rows

    def _keyword_rows(
        self,
        text: str,
        title: str,
        description: str,
        h1_values: List[str],
        keywords: List[str],
        total_words: int,
        warn_density: float,
        critical_density: float,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        text_l = text.lower()
        h1_joined = " | ".join(h1_values).lower()
        for kw in keywords:
            kw_norm = _norm_text(kw).lower()
            if not kw_norm:
                continue
            pattern = re.compile(rf"\b{re.escape(kw_norm)}\b", re.IGNORECASE)
            occurrences = len(pattern.findall(text_l))
            density = (occurrences / total_words * 100.0) if total_words > 0 else 0.0
            status = "ok"
            if density >= critical_density:
                status = "critical"
            elif density >= warn_density or occurrences == 0:
                status = "warning"
            rows.append(
                {
                    "keyword": kw_norm,
                    "occurrences": occurrences,
                    "density_pct": round(density, 3),
                    "in_title": _keyword_presence(title, kw_norm),
                    "in_description": _keyword_presence(description, kw_norm),
                    "in_h1": _keyword_presence(h1_joined, kw_norm),
                    "status": status,
                }
            )
        return rows

    def _build_issues(
        self,
        *,
        title: str,
        description: str,
        h1_values: List[str],
        title_len: int,
        description_len: int,
        total_words: int,
        min_word_count: int,
        title_min_len: int,
        title_max_len: int,
        description_min_len: int,
        description_max_len: int,
        h1_required: bool,
        h1_max_count: int,
        keyword_rows: List[Dict[str, Any]],
        top_terms: List[Dict[str, Any]],
        technical: Dict[str, Any],
        links: Dict[str, Any],
        media: Dict[str, Any],
        readability: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []

        def add(severity: str, code: str, title_text: str, details: str) -> None:
            issues.append({"severity": severity, "code": code, "title": title_text, "details": details})

        if not title:
            add("critical", "title_missing", "Title is missing", "Add unique <title> for the page.")
        elif title_len < title_min_len or title_len > title_max_len:
            add(
                "warning",
                "title_length_out_of_range",
                "Title length out of range",
                f"Current title length: {title_len}. Target: {title_min_len}-{title_max_len}.",
            )

        if not description:
            add("warning", "description_missing", "Description is missing", "Add meta description.")
        elif description_len < description_min_len or description_len > description_max_len:
            add(
                "warning",
                "description_length_out_of_range",
                "Description length out of range",
                f"Current description length: {description_len}. Target: {description_min_len}-{description_max_len}.",
            )

        if h1_required and not h1_values:
            add("critical", "h1_missing", "H1 is missing", "Add one relevant H1 heading.")
        if h1_values and len(h1_values) > h1_max_count:
            add("warning", "h1_multiple", "Multiple H1 headings", f"H1 count: {len(h1_values)} (limit: {h1_max_count}).")

        if total_words < min_word_count:
            add(
                "warning",
                "low_word_count",
                "Low content volume",
                f"Word count: {total_words}. Recommended minimum: {min_word_count}.",
            )

        for row in keyword_rows:
            if row["status"] == "critical":
                add(
                    "critical",
                    "keyword_stuffing",
                    "Potential keyword stuffing",
                    f"Keyword '{row['keyword']}' density is {row['density_pct']}%.",
                )
            elif row["status"] == "warning" and row["occurrences"] == 0:
                add(
                    "warning",
                    "keyword_missing",
                    "Keyword not found in content",
                    f"Keyword '{row['keyword']}' is missing in page text.",
                )
            elif row["status"] == "warning":
                add(
                    "warning",
                    "keyword_density_high",
                    "Elevated keyword density",
                    f"Keyword '{row['keyword']}' density is {row['density_pct']}%.",
                )

        if top_terms:
            top = top_terms[0]
            if top.get("pct", 0) >= 8:
                add(
                    "critical",
                    "top_term_spam",
                    "Excessive repetition of one term",
                    f"Term '{top.get('term')}' takes {top.get('pct')}% of all words.",
                )
            elif top.get("pct", 0) >= 6:
                add(
                    "warning",
                    "top_term_repetition",
                    "High term repetition",
                    f"Term '{top.get('term')}' takes {top.get('pct')}% of all words.",
                )

        if technical.get("noindex"):
            add("critical", "meta_noindex", "Meta robots contains noindex", "Remove noindex if page should rank.")
        if technical.get("nofollow"):
            add("warning", "meta_nofollow", "Meta robots contains nofollow", "Verify if nofollow is intentional.")
        if not technical.get("canonical_href"):
            add("warning", "canonical_missing", "Canonical is missing", "Add rel=canonical to avoid duplicates.")
        elif not technical.get("canonical_is_self"):
            add("warning", "canonical_not_self", "Canonical points to another URL", "Check canonical target relevance.")
        if not technical.get("viewport"):
            add("warning", "viewport_missing", "Viewport meta is missing", "Add viewport for mobile rendering.")

        images_total = int(media.get("images_total", 0))
        images_missing_alt = int(media.get("images_missing_alt", 0))
        if images_total > 0:
            missing_alt_pct = images_missing_alt / max(1, images_total) * 100.0
            if missing_alt_pct >= 30:
                add(
                    "critical",
                    "images_alt_missing_critical",
                    "Many images are missing alt attributes",
                    f"Missing alt: {images_missing_alt}/{images_total} ({round(missing_alt_pct, 1)}%).",
                )
            elif missing_alt_pct > 0:
                add(
                    "warning",
                    "images_alt_missing",
                    "Some images are missing alt attributes",
                    f"Missing alt: {images_missing_alt}/{images_total} ({round(missing_alt_pct, 1)}%).",
                )

        external_links = int(links.get("external_links", 0))
        internal_links = int(links.get("internal_links", 0))
        empty_anchors = int(links.get("empty_anchor_links", 0))
        if internal_links == 0:
            add("warning", "internal_links_missing", "No internal links", "Add internal links to improve crawlability.")
        if external_links >= 50:
            add("warning", "external_links_many", "Too many external links", f"External links found: {external_links}.")
        if empty_anchors > 0:
            add("warning", "empty_anchor_links", "Empty/weak anchor texts found", f"Links with empty anchor: {empty_anchors}.")

        lexical_diversity = float(readability.get("lexical_diversity", 0.0))
        long_sentence_ratio = float(readability.get("long_sentence_ratio", 0.0))
        if lexical_diversity < 0.25:
            add(
                "warning",
                "low_lexical_diversity",
                "Low lexical diversity",
                f"Lexical diversity is {round(lexical_diversity, 3)}. Improve semantic variety.",
            )
        if long_sentence_ratio > 0.35:
            add(
                "warning",
                "long_sentences_excess",
                "Too many long sentences",
                f"Long sentence ratio is {round(long_sentence_ratio * 100, 1)}%.",
            )

        return issues

    def run(
        self,
        *,
        url: str,
        keywords: Optional[List[str]] = None,
        language: str = "auto",
        min_word_count: int = 250,
        keyword_density_warn_pct: float = 3.0,
        keyword_density_critical_pct: float = 5.0,
        title_min_len: int = 30,
        title_max_len: int = 60,
        description_min_len: int = 120,
        description_max_len: int = 160,
        h1_required: bool = True,
        h1_max_count: int = 1,
    ) -> Dict[str, Any]:
        clean_url = _ensure_url(url)
        if not clean_url:
            return {
                "task_type": "onpage_audit",
                "url": url,
                "completed_at": datetime.utcnow().isoformat(),
                "results": {
                    "engine": "onpage-v1",
                    "issues": [{"severity": "critical", "code": "invalid_url", "title": "Invalid URL", "details": "Specify valid URL."}],
                    "issues_count": 1,
                    "summary": {"critical_issues": 1, "warning_issues": 0, "info_issues": 0, "score": 0},
                    "score": 0,
                    "recommendations": ["Check URL format and try again."],
                },
            }

        try:
            response = requests.get(clean_url, timeout=self.timeout, headers={"User-Agent": "Mozilla/5.0"})
            final_url = response.url or clean_url
            status_code = response.status_code
            raw_html = response.text
            soup = BeautifulSoup(raw_html, "html.parser")
        except Exception as exc:
            return {
                "task_type": "onpage_audit",
                "url": clean_url,
                "completed_at": datetime.utcnow().isoformat(),
                "results": {
                    "engine": "onpage-v1",
                    "issues": [{"severity": "critical", "code": "fetch_error", "title": "Failed to fetch page", "details": str(exc)}],
                    "issues_count": 1,
                    "summary": {"critical_issues": 1, "warning_issues": 0, "info_issues": 0, "score": 0},
                    "score": 0,
                    "recommendations": ["Check page accessibility and try again."],
                },
            }

        page_lang = _norm_text(soup.html.get("lang") if soup.html else "")
        if language == "auto":
            if page_lang.lower().startswith("ru"):
                language = "ru"
            elif page_lang.lower().startswith("en"):
                language = "en"
            else:
                language = "ru"

        title = _norm_text(soup.title.string if soup.title else "")
        description = ""
        desc_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if desc_tag:
            description = _norm_text(desc_tag.get("content"))

        h1_values = [_norm_text(h.get_text(" ", strip=True)) for h in soup.find_all("h1")]
        h1_values = [x for x in h1_values if x]

        visible_text = self._collect_visible_text(soup)
        all_tokens = _tokens(visible_text)
        total_words = len(all_tokens)
        unique_words = len(set(all_tokens))
        char_count = len(visible_text)

        sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(visible_text) if s.strip()]
        sentence_lengths = [len(_tokens(s)) for s in sentences]
        avg_sentence_len = round(sum(sentence_lengths) / max(1, len(sentence_lengths)), 2)
        long_sentence_ratio = sum(1 for ln in sentence_lengths if ln >= 25) / max(1, len(sentence_lengths))
        lexical_diversity = unique_words / max(1, total_words)

        stopwords = _STOPWORDS_RU if language == "ru" else _STOPWORDS_EN
        content_tokens = [t for t in all_tokens if t not in stopwords and len(t) > 2]
        content_counts = Counter(content_tokens)
        top_terms: List[Dict[str, Any]] = []
        for term, count in content_counts.most_common(20):
            pct = round((count / total_words * 100.0), 3) if total_words > 0 else 0.0
            top_terms.append({"term": term, "count": count, "pct": pct})

        bigrams = self._top_ngrams(content_tokens, n=2, limit=20)
        trigrams = self._top_ngrams(content_tokens, n=3, limit=20)

        keyword_list = [_norm_text(k) for k in (keywords or []) if _norm_text(k)]
        keyword_rows = self._keyword_rows(
            text=visible_text,
            title=title,
            description=description,
            h1_values=h1_values,
            keywords=keyword_list,
            total_words=total_words,
            warn_density=keyword_density_warn_pct,
            critical_density=keyword_density_critical_pct,
        )

        parsed_final = urlparse(final_url)
        final_domain = parsed_final.netloc.lower()
        base_url = f"{parsed_final.scheme}://{parsed_final.netloc}"

        links_total = 0
        internal_links = 0
        external_links = 0
        nofollow_links = 0
        empty_anchor_links = 0
        for a in soup.find_all("a"):
            href = _norm_text(a.get("href"))
            if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            links_total += 1
            anchor = _norm_text(a.get_text(" ", strip=True))
            if len(anchor) < 2:
                empty_anchor_links += 1
            rel_values = [str(x).lower() for x in (a.get("rel") or [])]
            if "nofollow" in rel_values:
                nofollow_links += 1
            full_href = urljoin(base_url, href)
            href_domain = urlparse(full_href).netloc.lower()
            if href_domain == final_domain:
                internal_links += 1
            else:
                external_links += 1

        images = soup.find_all("img")
        images_total = len(images)
        images_missing_alt = sum(1 for img in images if not _norm_text(img.get("alt")))

        canonical_href = ""
        canonical_tag = soup.find("link", attrs={"rel": re.compile(r"canonical", re.I)})
        if canonical_tag:
            canonical_href = _norm_text(canonical_tag.get("href"))
        canonical_abs = urljoin(base_url, canonical_href) if canonical_href else ""
        canonical_is_self = False
        if canonical_abs:
            c = urlparse(canonical_abs)
            f = urlparse(final_url)
            canonical_is_self = (c.scheme, c.netloc, c.path.rstrip("/")) == (f.scheme, f.netloc, f.path.rstrip("/"))

        robots_content = ""
        robots_tag = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
        if robots_tag:
            robots_content = _norm_text(robots_tag.get("content")).lower()
        noindex = "noindex" in robots_content
        nofollow = "nofollow" in robots_content

        viewport_tag = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
        viewport = _norm_text(viewport_tag.get("content")) if viewport_tag else ""
        hreflang_count = len(soup.find_all("link", attrs={"hreflang": True}))
        schema_count = len(soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.I)}))

        technical = {
            "canonical_href": canonical_href,
            "canonical_abs": canonical_abs,
            "canonical_is_self": canonical_is_self,
            "robots": robots_content,
            "noindex": noindex,
            "nofollow": nofollow,
            "viewport": viewport,
            "lang": page_lang,
            "hreflang_count": hreflang_count,
            "schema_count": schema_count,
        }
        links = {
            "links_total": links_total,
            "internal_links": internal_links,
            "external_links": external_links,
            "nofollow_links": nofollow_links,
            "empty_anchor_links": empty_anchor_links,
        }
        media = {
            "images_total": images_total,
            "images_missing_alt": images_missing_alt,
            "images_with_alt": max(0, images_total - images_missing_alt),
        }
        readability = {
            "sentences_count": len(sentences),
            "avg_sentence_len": avg_sentence_len,
            "long_sentence_ratio": round(long_sentence_ratio, 4),
            "lexical_diversity": round(lexical_diversity, 4),
        }

        issues = self._build_issues(
            title=title,
            description=description,
            h1_values=h1_values,
            title_len=len(title),
            description_len=len(description),
            total_words=total_words,
            min_word_count=_safe_int(min_word_count, 250),
            title_min_len=_safe_int(title_min_len, 30),
            title_max_len=_safe_int(title_max_len, 60),
            description_min_len=_safe_int(description_min_len, 120),
            description_max_len=_safe_int(description_max_len, 160),
            h1_required=bool(h1_required),
            h1_max_count=max(1, _safe_int(h1_max_count, 1)),
            keyword_rows=keyword_rows,
            top_terms=top_terms,
            technical=technical,
            links=links,
            media=media,
            readability=readability,
        )

        critical_count = sum(1 for i in issues if i.get("severity") == "critical")
        warning_count = sum(1 for i in issues if i.get("severity") == "warning")
        info_count = sum(1 for i in issues if i.get("severity") == "info")
        score = max(0, 100 - critical_count * 20 - warning_count * 7 - info_count * 3)

        recommendations: List[str] = []
        for issue in issues:
            sev = issue.get("severity")
            if sev not in ("critical", "warning"):
                continue
            details = _norm_text(issue.get("details"))
            if details and details not in recommendations:
                recommendations.append(details)
        if not recommendations:
            recommendations.append("No critical on-page issues detected.")

        headings = {
            "h1_count": len(h1_values),
            "h2_count": len(soup.find_all("h2")),
            "h3_count": len(soup.find_all("h3")),
            "h4_count": len(soup.find_all("h4")),
            "h5_count": len(soup.find_all("h5")),
            "h6_count": len(soup.find_all("h6")),
        }

        spam_signals = [
            {
                "severity": "critical" if i.get("code") in ("keyword_stuffing", "top_term_spam") else "warning",
                "code": i.get("code"),
                "title": i.get("title"),
                "details": i.get("details"),
            }
            for i in issues
            if i.get("code") in ("keyword_stuffing", "keyword_density_high", "top_term_spam", "top_term_repetition")
        ]

        return {
            "task_type": "onpage_audit",
            "url": clean_url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "engine": "onpage-v1",
                "status_code": status_code,
                "final_url": final_url,
                "language": language,
                "settings": {
                    "keywords": keyword_list,
                    "min_word_count": _safe_int(min_word_count, 250),
                    "keyword_density_warn_pct": _safe_float(keyword_density_warn_pct, 3.0),
                    "keyword_density_critical_pct": _safe_float(keyword_density_critical_pct, 5.0),
                    "title_min_len": _safe_int(title_min_len, 30),
                    "title_max_len": _safe_int(title_max_len, 60),
                    "description_min_len": _safe_int(description_min_len, 120),
                    "description_max_len": _safe_int(description_max_len, 160),
                    "h1_required": bool(h1_required),
                    "h1_max_count": max(1, _safe_int(h1_max_count, 1)),
                },
                "content": {
                    "word_count": total_words,
                    "unique_word_count": unique_words,
                    "char_count": char_count,
                    "text_sample": visible_text[:500],
                },
                "title": {"text": title, "length": len(title)},
                "description": {"text": description, "length": len(description)},
                "h1": {"count": len(h1_values), "values": h1_values},
                "headings": headings,
                "keywords": keyword_rows,
                "top_terms": top_terms,
                "ngrams": {"bigrams": bigrams, "trigrams": trigrams},
                "technical": technical,
                "links": links,
                "media": media,
                "readability": readability,
                "spam_signals": spam_signals,
                "issues": issues,
                "issues_count": len(issues),
                "summary": {
                    "critical_issues": critical_count,
                    "warning_issues": warning_count,
                    "info_issues": info_count,
                    "score": score,
                },
                "score": score,
                "recommendations": recommendations[:30],
                "meta": {
                    "domain": urlparse(final_url).netloc,
                    "fetched_at": datetime.utcnow().isoformat(),
                },
            },
        }

