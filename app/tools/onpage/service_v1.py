"""Single-page OnPage audit service."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
import json
import math
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

    def _heading_structure(self, soup: BeautifulSoup) -> Dict[str, Any]:
        heading_tags = []
        for level in range(1, 7):
            for node in soup.find_all(f"h{level}"):
                heading_tags.append(
                    {
                        "level": level,
                        "tag": f"h{level}",
                        "text": _norm_text(node.get_text(" ", strip=True)),
                    }
                )
        heading_tags = [h for h in heading_tags if h["text"]]
        counts = {f"h{i}_count": len([h for h in heading_tags if h["level"] == i]) for i in range(1, 7)}
        sequence = [h["level"] for h in heading_tags]
        level_skips = 0
        for i in range(1, len(sequence)):
            if sequence[i] - sequence[i - 1] > 1:
                level_skips += 1
        duplicates = sum(1 for c in Counter([h["text"].lower() for h in heading_tags]).values() if c > 1)
        return {
            "headings": heading_tags,
            "counts": counts,
            "outline_levels": sequence,
            "level_skips": level_skips,
            "duplicate_heading_texts": duplicates,
        }

    def _schema_analysis(self, soup: BeautifulSoup) -> Dict[str, Any]:
        schema_types: Counter = Counter()
        json_ld_blocks = soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.I)})
        json_ld_valid = 0
        for block in json_ld_blocks:
            raw = _norm_text(block.string or block.get_text(" ", strip=True))
            if not raw:
                continue
            try:
                payload = json.loads(raw)
                json_ld_valid += 1
            except Exception:
                continue

            stack = payload if isinstance(payload, list) else [payload]
            while stack:
                item = stack.pop()
                if isinstance(item, dict):
                    t = item.get("@type")
                    if isinstance(t, list):
                        for tv in t:
                            if _norm_text(tv):
                                schema_types[_norm_text(tv)] += 1
                    elif _norm_text(t):
                        schema_types[_norm_text(t)] += 1
                    for v in item.values():
                        if isinstance(v, dict):
                            stack.append(v)
                        elif isinstance(v, list):
                            for vv in v:
                                if isinstance(vv, dict):
                                    stack.append(vv)
        microdata_items = len(soup.select("[itemscope]"))
        rdfa_items = len(soup.select("[typeof]"))
        return {
            "json_ld_blocks": len(json_ld_blocks),
            "json_ld_valid_blocks": json_ld_valid,
            "microdata_items": microdata_items,
            "rdfa_items": rdfa_items,
            "types": [{"type": k, "count": v} for k, v in schema_types.most_common(30)],
            "types_count": len(schema_types),
        }

    def _opengraph_analysis(self, soup: BeautifulSoup) -> Dict[str, Any]:
        og: Dict[str, str] = {}
        for tag in soup.find_all("meta", attrs={"property": re.compile(r"^og:", re.I)}):
            prop = _norm_text(tag.get("property")).lower()
            content = _norm_text(tag.get("content"))
            if prop and content and prop not in og:
                og[prop] = content
        required = ["og:title", "og:description", "og:type", "og:url", "og:image"]
        missing = [x for x in required if x not in og]
        return {
            "tags": og,
            "tags_count": len(og),
            "required_missing": missing,
            "required_present_count": len(required) - len(missing),
        }

    def _ai_insights(
        self,
        *,
        visible_text: str,
        total_words: int,
        content_tokens: List[str],
        sentence_lengths: List[int],
        links: Dict[str, Any],
    ) -> Dict[str, Any]:
        text_l = visible_text.lower()
        ai_markers = [
            "as an ai", "as a language model", "in conclusion", "it is important to note",
            "в заключение", "важно отметить", "как ии", "как языковая модель",
            "следует отметить", "подводя итог", "таким образом",
        ]
        hedge_words = {
            "maybe", "perhaps", "likely", "possibly", "generally", "often", "can",
            "возможно", "вероятно", "может", "обычно", "часто", "как правило",
        }
        template_phrases = [
            "in today's world", "unlock the power", "seamless experience",
            "в современном мире", "лучшее решение", "высокое качество",
        ]

        ai_marker_hits = sum(text_l.count(m) for m in ai_markers)
        ai_marker_density_1k = round(ai_marker_hits * 1000.0 / max(1, total_words), 2)

        hedge_hits = sum(1 for t in content_tokens if t in hedge_words)
        hedging_ratio = round(hedge_hits / max(1, len(content_tokens)), 4)

        template_hits = sum(text_l.count(p) for p in template_phrases)
        template_repetition = round(template_hits * 1000.0 / max(1, total_words), 2)

        mean_len = sum(sentence_lengths) / max(1, len(sentence_lengths))
        variance = sum((x - mean_len) ** 2 for x in sentence_lengths) / max(1, len(sentence_lengths))
        stddev = math.sqrt(variance)
        burstiness_cv = round(stddev / max(1.0, mean_len), 4)

        freq = Counter(content_tokens)
        probs = [c / max(1, len(content_tokens)) for c in freq.values() if c > 0]
        entropy = -sum(p * math.log(p, 2) for p in probs) if probs else 0.0
        max_entropy = math.log(max(2, len(freq)), 2) if freq else 1.0
        perplexity_proxy = round(1.0 - (entropy / max(0.0001, max_entropy)), 4)

        # Rough entity extraction by capitalized tokens in original text.
        entity_candidates = re.findall(r"\b[А-ЯЁA-Z][а-яёa-zA-Z\-]{2,}\b", visible_text)
        entities_unique = len(set(entity_candidates))
        entity_depth_1k = round(entities_unique * 1000.0 / max(1, total_words), 2)

        numbers_count = len(re.findall(r"\b\d+(?:[.,]\d+)?\b", visible_text))
        percent_count = len(re.findall(r"\b\d+(?:[.,]\d+)?\s*%", visible_text))
        date_like_count = len(re.findall(r"\b(?:19|20)\d{2}\b", visible_text))
        claim_specificity_score = round(
            min(100.0, ((numbers_count + percent_count + date_like_count * 2) * 100.0) / max(1, total_words / 25.0)),
            1,
        )

        author_signals = ["author", "editor", "reviewed by", "эксперт", "автор", "редактор", "обновлено"]
        author_signal_score = round(min(100.0, sum(1 for s in author_signals if s in text_l) * 20.0), 1)

        source_tokens = ["source", "according to", "study", "исследование", "источник", "по данным"]
        source_mentions = sum(text_l.count(s) for s in source_tokens)
        external_links = int(links.get("external_links", 0))
        source_attribution_score = round(min(100.0, source_mentions * 10.0 + external_links * 5.0), 1)

        ai_risk = (
            min(100.0, ai_marker_density_1k * 5.0) * 0.22
            + min(100.0, hedging_ratio * 400.0) * 0.12
            + min(100.0, template_repetition * 8.0) * 0.14
            + min(100.0, perplexity_proxy * 100.0) * 0.18
            + min(100.0, max(0.0, (0.35 - burstiness_cv) * 220.0)) * 0.14
            + min(100.0, max(0.0, 80.0 - claim_specificity_score)) * 0.10
            + min(100.0, max(0.0, 70.0 - source_attribution_score)) * 0.06
            + min(100.0, max(0.0, 60.0 - author_signal_score)) * 0.04
        )
        ai_risk_composite = round(max(0.0, min(100.0, ai_risk)), 1)

        return {
            "ai_marker_hits": ai_marker_hits,
            "ai_marker_density_1k": ai_marker_density_1k,
            "hedging_ratio": hedging_ratio,
            "template_repetition": template_repetition,
            "burstiness_cv": burstiness_cv,
            "perplexity_proxy": perplexity_proxy,
            "entity_depth_1k": entity_depth_1k,
            "entities_unique": entities_unique,
            "claim_specificity_score": claim_specificity_score,
            "author_signal_score": author_signal_score,
            "source_attribution_score": source_attribution_score,
            "ai_risk_composite": ai_risk_composite,
        }

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
        ngrams: Dict[str, Any],
        spam_metrics: Dict[str, Any],
        heading_analysis: Dict[str, Any],
        schema_analysis: Dict[str, Any],
        opengraph: Dict[str, Any],
        content_profile: Dict[str, Any],
        ai_insights: Dict[str, Any],
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
            if row["occurrences"] > 0 and not row.get("in_title"):
                add(
                    "warning",
                    "keyword_not_in_title",
                    "Keyword missing in title",
                    f"Keyword '{row['keyword']}' is not used in title.",
                )
            if row["occurrences"] > 0 and not row.get("in_h1"):
                add(
                    "warning",
                    "keyword_not_in_h1",
                    "Keyword missing in H1",
                    f"Keyword '{row['keyword']}' is not used in H1.",
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
        if heading_analysis.get("level_skips", 0) > 0:
            add(
                "warning",
                "heading_level_skips",
                "Heading hierarchy has skipped levels",
                f"Detected heading level skips: {heading_analysis.get('level_skips', 0)}.",
            )
        if heading_analysis.get("duplicate_heading_texts", 0) > 0:
            add(
                "warning",
                "duplicate_headings",
                "Duplicate heading texts found",
                f"Duplicate heading texts: {heading_analysis.get('duplicate_heading_texts', 0)}.",
            )
        if schema_analysis.get("json_ld_blocks", 0) == 0 and schema_analysis.get("microdata_items", 0) == 0:
            add("warning", "schema_missing", "Structured data is missing", "Add JSON-LD or microdata schema markup.")
        elif schema_analysis.get("json_ld_blocks", 0) > 0 and schema_analysis.get("json_ld_valid_blocks", 0) == 0:
            add("warning", "schema_invalid_jsonld", "JSON-LD detected but not parsed", "Validate JSON-LD syntax.")
        if opengraph.get("required_missing"):
            add(
                "warning",
                "opengraph_missing_required",
                "OpenGraph required tags are missing",
                f"Missing OG tags: {', '.join(opengraph.get('required_missing', []))}.",
            )

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

        top_bigram_pct = float(spam_metrics.get("top_bigram_pct", 0.0))
        top_trigram_pct = float(spam_metrics.get("top_trigram_pct", 0.0))
        duplicate_sentence_ratio = float(spam_metrics.get("duplicate_sentence_ratio", 0.0))
        uppercase_ratio = float(spam_metrics.get("uppercase_ratio", 0.0))
        punctuation_ratio = float(spam_metrics.get("punctuation_ratio", 0.0))
        stopword_ratio = float(spam_metrics.get("stopword_ratio", 0.0))
        content_html_ratio = float(spam_metrics.get("content_html_ratio", 0.0))

        if top_bigram_pct >= 4.0:
            add("critical", "bigram_spam", "Bigram repetition spam", f"Top bigram share is {round(top_bigram_pct, 2)}%.")
        elif top_bigram_pct >= 2.5:
            add("warning", "bigram_repetition_high", "High bigram repetition", f"Top bigram share is {round(top_bigram_pct, 2)}%.")

        if top_trigram_pct >= 2.5:
            add("critical", "trigram_spam", "Trigram repetition spam", f"Top trigram share is {round(top_trigram_pct, 2)}%.")
        elif top_trigram_pct >= 1.6:
            add("warning", "trigram_repetition_high", "High trigram repetition", f"Top trigram share is {round(top_trigram_pct, 2)}%.")

        if duplicate_sentence_ratio >= 0.2:
            add(
                "critical",
                "duplicate_sentences_high",
                "High duplicate sentence ratio",
                f"Duplicate sentences ratio is {round(duplicate_sentence_ratio * 100, 1)}%.",
            )
        elif duplicate_sentence_ratio >= 0.1:
            add(
                "warning",
                "duplicate_sentences_warning",
                "Duplicate sentence ratio is elevated",
                f"Duplicate sentences ratio is {round(duplicate_sentence_ratio * 100, 1)}%.",
            )

        if uppercase_ratio > 0.35:
            add("warning", "uppercase_spam_signal", "Too many uppercase letters", f"Uppercase ratio is {round(uppercase_ratio * 100, 1)}%.")
        if punctuation_ratio > 0.12:
            add("warning", "punctuation_spam_signal", "Too many punctuation marks", f"Punctuation ratio is {round(punctuation_ratio * 100, 1)}%.")
        if stopword_ratio < 0.2:
            add("warning", "stopword_ratio_low", "Low stopword ratio", f"Stopword ratio is {round(stopword_ratio * 100, 1)}%.")
        if content_html_ratio < 0.1:
            add("warning", "content_html_ratio_low", "Low text-to-HTML ratio", f"Content/HTML ratio is {round(content_html_ratio, 3)}.")
        if content_profile.get("wateriness_pct", 0) > 30:
            add(
                "warning",
                "wateriness_high",
                "Wateriness above recommended range",
                f"Wateriness is {content_profile.get('wateriness_pct', 0)}%.",
            )
        if content_profile.get("nausea_index", 0) > 30:
            add(
                "critical",
                "nausea_high",
                "Nausea above recommended range",
                f"Nausea is {content_profile.get('nausea_index', 0)}.",
            )
        elif content_profile.get("nausea_index", 0) > 15:
            add(
                "warning",
                "nausea_elevated",
                "Nausea is elevated",
                f"Nausea is {content_profile.get('nausea_index', 0)}.",
            )

        ai_risk = float(ai_insights.get("ai_risk_composite", 0.0))
        if ai_risk >= 75:
            add("critical", "ai_risk_high", "High AI-pattern risk", f"AI risk composite is {ai_risk}.")
        elif ai_risk >= 55:
            add("warning", "ai_risk_elevated", "Elevated AI-pattern risk", f"AI risk composite is {ai_risk}.")
        if float(ai_insights.get("ai_marker_density_1k", 0.0)) >= 8.0:
            add(
                "warning",
                "ai_marker_density_high",
                "AI marker density is high",
                f"AI marker density is {ai_insights.get('ai_marker_density_1k', 0.0)} per 1000 words.",
            )
        if float(ai_insights.get("template_repetition", 0.0)) >= 6.0:
            add(
                "warning",
                "template_repetition_high",
                "Template phrase repetition is high",
                f"Template repetition is {ai_insights.get('template_repetition', 0.0)} per 1000 words.",
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
                    "summary": {
                        "critical_issues": 1,
                        "warning_issues": 0,
                        "info_issues": 0,
                        "score": 0,
                        "spam_score": 0,
                        "keyword_coverage_score": 0,
                        "keyword_coverage_pct": 0,
                    },
                    "score": 0,
                    "scores": {"onpage_score": 0, "spam_score": 0, "keyword_coverage_score": 0},
                    "ai_insights": {
                        "ai_marker_density_1k": 0,
                        "hedging_ratio": 0,
                        "template_repetition": 0,
                        "burstiness_cv": 0,
                        "perplexity_proxy": 0,
                        "entity_depth_1k": 0,
                        "claim_specificity_score": 0,
                        "author_signal_score": 0,
                        "source_attribution_score": 0,
                        "ai_risk_composite": 0,
                    },
                    "keyword_coverage": {
                        "keywords_total": 0,
                        "present_keywords": 0,
                        "coverage_pct": 0,
                        "title_coverage_pct": 0,
                        "h1_coverage_pct": 0,
                    },
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
                    "summary": {
                        "critical_issues": 1,
                        "warning_issues": 0,
                        "info_issues": 0,
                        "score": 0,
                        "spam_score": 0,
                        "keyword_coverage_score": 0,
                        "keyword_coverage_pct": 0,
                    },
                    "score": 0,
                    "scores": {"onpage_score": 0, "spam_score": 0, "keyword_coverage_score": 0},
                    "ai_insights": {
                        "ai_marker_density_1k": 0,
                        "hedging_ratio": 0,
                        "template_repetition": 0,
                        "burstiness_cv": 0,
                        "perplexity_proxy": 0,
                        "entity_depth_1k": 0,
                        "claim_specificity_score": 0,
                        "author_signal_score": 0,
                        "source_attribution_score": 0,
                        "ai_risk_composite": 0,
                    },
                    "keyword_coverage": {
                        "keywords_total": 0,
                        "present_keywords": 0,
                        "coverage_pct": 0,
                        "title_coverage_pct": 0,
                        "h1_coverage_pct": 0,
                    },
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
        heading_analysis = self._heading_structure(soup)

        visible_text = self._collect_visible_text(soup)
        all_tokens = _tokens(visible_text)
        total_words = len(all_tokens)
        unique_words = len(set(all_tokens))
        char_count = len(visible_text)
        clean_char_count = len(re.sub(r"[\W_]+", "", visible_text, flags=re.UNICODE))

        sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(visible_text) if s.strip()]
        sentence_lengths = [len(_tokens(s)) for s in sentences]
        avg_sentence_len = round(sum(sentence_lengths) / max(1, len(sentence_lengths)), 2)
        long_sentence_ratio = sum(1 for ln in sentence_lengths if ln >= 25) / max(1, len(sentence_lengths))
        lexical_diversity = unique_words / max(1, total_words)
        sentence_norm = [re.sub(r"\s+", " ", s.lower()) for s in sentences if len(_tokens(s)) >= 6]
        sentence_counts = Counter(sentence_norm)
        duplicate_sentences = sum(c - 1 for c in sentence_counts.values() if c > 1)
        duplicate_sentence_ratio = duplicate_sentences / max(1, len(sentence_norm))

        stopwords = _STOPWORDS_RU if language == "ru" else _STOPWORDS_EN
        content_tokens = [t for t in all_tokens if t not in stopwords and len(t) > 2]
        content_counts = Counter(content_tokens)
        core_vocabulary = len(set(content_tokens))
        top_terms: List[Dict[str, Any]] = []
        for term, count in content_counts.most_common(20):
            pct = round((count / total_words * 100.0), 3) if total_words > 0 else 0.0
            top_terms.append({"term": term, "count": count, "pct": pct})

        bigrams = self._top_ngrams(content_tokens, n=2, limit=20)
        trigrams = self._top_ngrams(content_tokens, n=3, limit=20)
        top_bigram_pct = float((bigrams[0] or {}).get("pct", 0.0)) if bigrams else 0.0
        top_trigram_pct = float((trigrams[0] or {}).get("pct", 0.0)) if trigrams else 0.0

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
        anchor_token_counts: Counter = Counter()
        for a in soup.find_all("a"):
            href = _norm_text(a.get("href"))
            if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            links_total += 1
            anchor = _norm_text(a.get_text(" ", strip=True))
            if len(anchor) < 2:
                empty_anchor_links += 1
            for tk in _tokens(anchor):
                if len(tk) > 2:
                    anchor_token_counts[tk] += 1
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
        schema_analysis = self._schema_analysis(soup)
        schema_count = int(schema_analysis.get("json_ld_blocks", 0) or 0)
        opengraph = self._opengraph_analysis(soup)

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
        stopword_count = max(0, total_words - len(content_tokens))
        letters = [ch for ch in visible_text if ch.isalpha()]
        uppercase_letters = [ch for ch in letters if ch.isupper()]
        punctuation_count = len([ch for ch in visible_text if ch in "!?.,;:"])
        text_html_pct = round((char_count / max(1, len(raw_html))) * 100.0, 2)
        wateriness_pct = round((stopword_count / max(1, total_words)) * 100.0, 1)
        nausea_index = round(float((top_terms[0] or {}).get("pct", 0.0)) if top_terms else 0.0, 1)

        def _band(value: float, good_min: float, good_max: float, ok_min: float, ok_max: float) -> str:
            if good_min <= value <= good_max:
                return "good"
            if ok_min <= value <= ok_max:
                return "acceptable"
            return "bad"

        text_length_band = _band(float(char_count), 1500, 35000, 500, 45000)
        wateriness_band = _band(float(wateriness_pct), 0, 15, 16, 30)
        nausea_band = _band(float(nausea_index), 5, 15, 0, 30)
        text_html_band = _band(float(text_html_pct), 51, 100, 11, 50)

        spam_metrics = {
            "stopword_ratio": round(stopword_count / max(1, total_words), 4),
            "content_html_ratio": round(char_count / max(1, len(raw_html)), 4),
            "content_html_pct": text_html_pct,
            "uppercase_ratio": round(len(uppercase_letters) / max(1, len(letters)), 4),
            "punctuation_ratio": round(punctuation_count / max(1, char_count), 4),
            "duplicate_sentences": duplicate_sentences,
            "duplicate_sentence_ratio": round(duplicate_sentence_ratio, 4),
            "top_bigram_pct": round(top_bigram_pct, 3),
            "top_trigram_pct": round(top_trigram_pct, 3),
        }
        link_anchor_terms = [
            {"term": term, "count": count}
            for term, count in anchor_token_counts.most_common(10)
        ]
        content_profile = {
            "text_length": char_count,
            "clean_text_length": clean_char_count,
            "word_count": total_words,
            "vocabulary": unique_words,
            "core_vocabulary": core_vocabulary,
            "wateriness_pct": wateriness_pct,
            "nausea_index": nausea_index,
            "text_html_pct": text_html_pct,
            "ratings": {
                "text_length": text_length_band,
                "wateriness": wateriness_band,
                "nausea": nausea_band,
                "text_html": text_html_band,
            },
            "top_link_terms": link_anchor_terms,
        }
        ai_insights = self._ai_insights(
            visible_text=visible_text,
            total_words=total_words,
            content_tokens=content_tokens,
            sentence_lengths=sentence_lengths,
            links=links,
        )

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
            ngrams={"bigrams": bigrams, "trigrams": trigrams},
            spam_metrics=spam_metrics,
            heading_analysis=heading_analysis,
            schema_analysis=schema_analysis,
            opengraph=opengraph,
            content_profile=content_profile,
            ai_insights=ai_insights,
        )

        critical_count = sum(1 for i in issues if i.get("severity") == "critical")
        warning_count = sum(1 for i in issues if i.get("severity") == "warning")
        info_count = sum(1 for i in issues if i.get("severity") == "info")
        score = max(0, 100 - critical_count * 20 - warning_count * 7 - info_count * 3)

        present_keywords = sum(1 for row in keyword_rows if int(row.get("occurrences", 0)) > 0)
        keywords_total = len(keyword_rows)
        keyword_coverage_pct = (present_keywords / max(1, keywords_total) * 100.0) if keywords_total else 100.0
        keyword_title_coverage_pct = (
            sum(1 for row in keyword_rows if row.get("in_title")) / max(1, keywords_total) * 100.0
            if keywords_total else 100.0
        )
        keyword_h1_coverage_pct = (
            sum(1 for row in keyword_rows if row.get("in_h1")) / max(1, keywords_total) * 100.0
            if keywords_total else 100.0
        )

        spam_score = 100.0
        spam_score -= min(30.0, spam_metrics.get("top_bigram_pct", 0.0) * 4.0)
        spam_score -= min(25.0, spam_metrics.get("top_trigram_pct", 0.0) * 8.0)
        spam_score -= min(20.0, float(spam_metrics.get("duplicate_sentence_ratio", 0.0)) * 100.0 * 0.8)
        spam_score -= min(10.0, max(0.0, (float(spam_metrics.get("uppercase_ratio", 0.0)) - 0.18) * 100.0))
        spam_score -= min(10.0, max(0.0, (float(spam_metrics.get("punctuation_ratio", 0.0)) - 0.07) * 120.0))
        spam_issue_codes = {
            "keyword_stuffing",
            "keyword_density_high",
            "top_term_spam",
            "top_term_repetition",
            "bigram_spam",
            "bigram_repetition_high",
            "trigram_spam",
            "trigram_repetition_high",
            "duplicate_sentences_high",
            "duplicate_sentences_warning",
            "uppercase_spam_signal",
            "punctuation_spam_signal",
        }
        spam_issue_count = sum(1 for i in issues if i.get("code") in spam_issue_codes)
        spam_score -= min(20.0, spam_issue_count * 3.0)
        spam_score = max(0.0, min(100.0, spam_score))

        keyword_coverage_score = round(
            max(
                0.0,
                min(
                    100.0,
                    (keyword_coverage_pct * 0.6) + (keyword_title_coverage_pct * 0.2) + (keyword_h1_coverage_pct * 0.2),
                ),
            ),
            1,
        )
        score = round((score * 0.7) + (spam_score * 0.2) + (keyword_coverage_score * 0.1), 1)

        recommendations: List[str] = []
        for issue in issues:
            sev = issue.get("severity")
            if sev not in ("critical", "warning"):
                continue
            details = _norm_text(issue.get("details"))
            if details and details not in recommendations:
                recommendations.append(details)
        if spam_score < 70:
            recommendations.append("Reduce repetitive phrases and normalize n-gram distribution.")
        if keyword_coverage_score < 70 and keywords_total > 0:
            recommendations.append("Improve keyword coverage in content, title and H1.")
        if float(ai_insights.get("ai_risk_composite", 0.0)) >= 55:
            recommendations.append("Increase specificity, sources and author evidence to reduce AI-pattern risk.")
        if not recommendations:
            recommendations.append("No critical on-page issues detected.")

        headings = heading_analysis.get("counts", {})
        headings["outline_levels"] = heading_analysis.get("outline_levels", [])
        headings["level_skips"] = heading_analysis.get("level_skips", 0)
        headings["duplicate_heading_texts"] = heading_analysis.get("duplicate_heading_texts", 0)

        parameter_values = [
            {"parameter": "Длина текста", "value": char_count, "status": text_length_band},
            {"parameter": "Чистая длина текста", "value": clean_char_count, "status": "info"},
            {"parameter": "Всего слов", "value": total_words, "status": "info"},
            {"parameter": "Словарь", "value": unique_words, "status": "info"},
            {"parameter": "Словарь ядра", "value": core_vocabulary, "status": "info"},
            {"parameter": "Водность", "value": f"{wateriness_pct}%", "status": wateriness_band},
            {"parameter": "Тошнота", "value": nausea_index, "status": nausea_band},
            {"parameter": "Text/HTML", "value": f"{text_html_pct}%", "status": text_html_band},
            {"parameter": "H1", "value": headings.get("h1_count", 0), "status": "info"},
            {"parameter": "H2", "value": headings.get("h2_count", 0), "status": "info"},
            {"parameter": "H3", "value": headings.get("h3_count", 0), "status": "info"},
            {"parameter": "H4", "value": headings.get("h4_count", 0), "status": "info"},
            {"parameter": "H5", "value": headings.get("h5_count", 0), "status": "info"},
            {"parameter": "H6", "value": headings.get("h6_count", 0), "status": "info"},
            {"parameter": "Schema types", "value": schema_analysis.get("types_count", 0), "status": "info"},
            {"parameter": "OpenGraph tags", "value": opengraph.get("tags_count", 0), "status": "info"},
            {"parameter": "AI Marker Density/1k", "value": ai_insights.get("ai_marker_density_1k", 0), "status": "bad" if ai_insights.get("ai_marker_density_1k", 0) >= 8 else ("acceptable" if ai_insights.get("ai_marker_density_1k", 0) >= 4 else "good")},
            {"parameter": "Hedging Ratio", "value": ai_insights.get("hedging_ratio", 0), "status": "bad" if ai_insights.get("hedging_ratio", 0) >= 0.12 else ("acceptable" if ai_insights.get("hedging_ratio", 0) >= 0.07 else "good")},
            {"parameter": "Template Repetition", "value": ai_insights.get("template_repetition", 0), "status": "bad" if ai_insights.get("template_repetition", 0) >= 6 else ("acceptable" if ai_insights.get("template_repetition", 0) >= 3 else "good")},
            {"parameter": "Burstiness CV", "value": ai_insights.get("burstiness_cv", 0), "status": "bad" if ai_insights.get("burstiness_cv", 0) < 0.22 else ("acceptable" if ai_insights.get("burstiness_cv", 0) < 0.30 else "good")},
            {"parameter": "Perplexity Proxy", "value": ai_insights.get("perplexity_proxy", 0), "status": "bad" if ai_insights.get("perplexity_proxy", 0) >= 0.72 else ("acceptable" if ai_insights.get("perplexity_proxy", 0) >= 0.58 else "good")},
            {"parameter": "Entity Depth/1k", "value": ai_insights.get("entity_depth_1k", 0), "status": "bad" if ai_insights.get("entity_depth_1k", 0) < 6 else ("acceptable" if ai_insights.get("entity_depth_1k", 0) < 12 else "good")},
            {"parameter": "Claim Specificity Score", "value": ai_insights.get("claim_specificity_score", 0), "status": "bad" if ai_insights.get("claim_specificity_score", 0) < 35 else ("acceptable" if ai_insights.get("claim_specificity_score", 0) < 65 else "good")},
            {"parameter": "Author Signal Score", "value": ai_insights.get("author_signal_score", 0), "status": "bad" if ai_insights.get("author_signal_score", 0) < 20 else ("acceptable" if ai_insights.get("author_signal_score", 0) < 45 else "good")},
            {"parameter": "Source Attribution Score", "value": ai_insights.get("source_attribution_score", 0), "status": "bad" if ai_insights.get("source_attribution_score", 0) < 25 else ("acceptable" if ai_insights.get("source_attribution_score", 0) < 55 else "good")},
            {"parameter": "AI Risk Composite", "value": ai_insights.get("ai_risk_composite", 0), "status": "bad" if ai_insights.get("ai_risk_composite", 0) >= 75 else ("acceptable" if ai_insights.get("ai_risk_composite", 0) >= 55 else "good")},
        ]

        spam_signals = [
            {
                "severity": "critical" if i.get("code") in ("keyword_stuffing", "top_term_spam", "bigram_spam", "trigram_spam", "duplicate_sentences_high") else "warning",
                "code": i.get("code"),
                "title": i.get("title"),
                "details": i.get("details"),
            }
            for i in issues
            if i.get("code") in (
                "keyword_stuffing",
                "keyword_density_high",
                "top_term_spam",
                "top_term_repetition",
                "bigram_spam",
                "bigram_repetition_high",
                "trigram_spam",
                "trigram_repetition_high",
                "duplicate_sentences_high",
                "duplicate_sentences_warning",
                "uppercase_spam_signal",
                "punctuation_spam_signal",
            )
        ]

        def _issue_category(code: str) -> str:
            c = (code or "").lower()
            if any(x in c for x in ("link", "anchor")):
                return "links"
            if any(x in c for x in ("schema", "open", "og", "structured")):
                return "schema"
            if any(x in c for x in ("keyword", "term", "water", "nausea", "content", "lexical", "sentence")):
                return "content"
            if any(x in c for x in ("canonical", "robots", "viewport", "h1", "title", "description", "heading")):
                return "technical"
            if any(x in c for x in ("ai_", "template", "burst", "perplex", "source", "author")):
                return "ai"
            return "other"

        severity_weight = {"critical": 3, "warning": 2, "info": 1}
        heatmap: Dict[str, Dict[str, Any]] = {}
        for cat in ("technical", "content", "ai", "links", "schema", "other"):
            cat_issues = [i for i in issues if _issue_category(i.get("code", "")) == cat]
            score_cat = max(0, 100 - sum(severity_weight.get(str(i.get("severity", "info")).lower(), 1) * 8 for i in cat_issues))
            heatmap[cat] = {
                "issues": len(cat_issues),
                "score": score_cat,
                "critical": sum(1 for i in cat_issues if i.get("severity") == "critical"),
                "warning": sum(1 for i in cat_issues if i.get("severity") == "warning"),
            }

        def _queue_item(issue: Dict[str, Any]) -> Dict[str, Any]:
            sev = str(issue.get("severity", "info")).lower()
            impact = 90 if sev == "critical" else (65 if sev == "warning" else 35)
            code_l = str(issue.get("code", "")).lower()
            effort = 50
            if any(x in code_l for x in ("title", "description", "h1", "open", "og", "schema_missing")):
                effort = 25
            elif any(x in code_l for x in ("canonical", "robots", "viewport")):
                effort = 35
            elif any(x in code_l for x in ("duplicate", "template", "ai_risk", "nausea", "wateriness")):
                effort = 60
            priority_score = round((impact * 0.7) + ((100 - effort) * 0.3), 1)
            bucket = "Now" if priority_score >= 75 else ("Next" if priority_score >= 55 else "Later")
            return {
                "code": issue.get("code", ""),
                "title": issue.get("title", ""),
                "severity": sev,
                "impact": impact,
                "effort": effort,
                "priority_score": priority_score,
                "bucket": bucket,
            }

        priority_queue = sorted([_queue_item(i) for i in issues], key=lambda x: x["priority_score"], reverse=True)[:20]

        targets = [
            {
                "metric": "Overall Score",
                "current": score,
                "target": 85,
                "delta": round(85 - float(score), 1),
            },
            {
                "metric": "Spam Score",
                "current": round(spam_score, 1),
                "target": 80,
                "delta": round(80 - float(spam_score), 1),
            },
            {
                "metric": "Keyword Coverage %",
                "current": round(keyword_coverage_pct, 1),
                "target": 75,
                "delta": round(75 - float(keyword_coverage_pct), 1),
            },
            {
                "metric": "AI Risk Composite",
                "current": ai_insights.get("ai_risk_composite", 0),
                "target": 40,
                "delta": round(float(ai_insights.get("ai_risk_composite", 0)) - 40, 1),
            },
            {
                "metric": "Text/HTML %",
                "current": text_html_pct,
                "target": 12,
                "delta": round(12 - float(text_html_pct), 1),
            },
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
                "heading_structure": heading_analysis,
                "keywords": keyword_rows,
                "top_terms": top_terms,
                "ngrams": {"bigrams": bigrams, "trigrams": trigrams},
                "spam_metrics": spam_metrics,
                "content_profile": content_profile,
                "technical": technical,
                "schema": schema_analysis,
                "opengraph": opengraph,
                "links": links,
                "link_anchor_terms": link_anchor_terms,
                "media": media,
                "readability": readability,
                "ai_insights": ai_insights,
                "spam_signals": spam_signals,
                "heatmap": heatmap,
                "priority_queue": priority_queue,
                "targets": targets,
                "parameter_values": parameter_values,
                "issues": issues,
                "issues_count": len(issues),
                "summary": {
                    "critical_issues": critical_count,
                    "warning_issues": warning_count,
                    "info_issues": info_count,
                    "score": score,
                    "spam_score": round(spam_score, 1),
                    "keyword_coverage_score": keyword_coverage_score,
                    "keyword_coverage_pct": round(keyword_coverage_pct, 1),
                    "ai_risk_composite": ai_insights.get("ai_risk_composite", 0),
                },
                "score": score,
                "scores": {
                    "onpage_score": score,
                    "spam_score": round(spam_score, 1),
                    "keyword_coverage_score": keyword_coverage_score,
                    "ai_risk_composite": ai_insights.get("ai_risk_composite", 0),
                },
                "keyword_coverage": {
                    "keywords_total": keywords_total,
                    "present_keywords": present_keywords,
                    "coverage_pct": round(keyword_coverage_pct, 1),
                    "title_coverage_pct": round(keyword_title_coverage_pct, 1),
                    "h1_coverage_pct": round(keyword_h1_coverage_pct, 1),
                },
                "recommendations": recommendations[:30],
                "meta": {
                    "domain": urlparse(final_url).netloc,
                    "fetched_at": datetime.utcnow().isoformat(),
                },
            },
        }
