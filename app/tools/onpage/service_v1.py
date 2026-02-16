"""Single-page OnPage audit service."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import re

import requests
from bs4 import BeautifulSoup


_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-Я0-9]+", re.IGNORECASE)

_STOPWORDS_RU = {
    "и", "в", "во", "на", "с", "со", "по", "под", "над", "к", "ко", "у", "о", "об", "от", "до", "для",
    "не", "ни", "это", "как", "а", "но", "или", "ли", "же", "бы", "что", "чтобы", "из", "за", "при",
    "мы", "вы", "они", "он", "она", "оно", "их", "его", "ее", "их", "наш", "ваш", "этот", "эта", "эти",
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
        text = re.sub(r"\s+", " ", text).strip()
        return text

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
            elif density >= warn_density:
                status = "warning"
            elif occurrences == 0:
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
    ) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []

        def add(severity: str, code: str, title_text: str, details: str) -> None:
            issues.append(
                {
                    "severity": severity,
                    "code": code,
                    "title": title_text,
                    "details": details,
                }
            )

        if not title:
            add("critical", "title_missing", "Title отсутствует", "Добавьте уникальный <title> для страницы.")
        elif title_len < title_min_len or title_len > title_max_len:
            add(
                "warning",
                "title_length_out_of_range",
                "Title имеет нецелевую длину",
                f"Длина title: {title_len}. Рекомендуемый диапазон: {title_min_len}-{title_max_len}.",
            )

        if not description:
            add("warning", "description_missing", "Description отсутствует", "Добавьте meta description.")
        elif description_len < description_min_len or description_len > description_max_len:
            add(
                "warning",
                "description_length_out_of_range",
                "Description имеет нецелевую длину",
                f"Длина description: {description_len}. Рекомендуемый диапазон: {description_min_len}-{description_max_len}.",
            )

        if h1_required and not h1_values:
            add("critical", "h1_missing", "H1 отсутствует", "Добавьте один релевантный H1.")
        if h1_values and len(h1_values) > h1_max_count:
            add("warning", "h1_multiple", "Найдено несколько H1", f"Найдено H1: {len(h1_values)} (лимит: {h1_max_count}).")

        if total_words < min_word_count:
            add(
                "warning",
                "low_word_count",
                "Недостаточный объем контента",
                f"Слов на странице: {total_words}. Минимум: {min_word_count}.",
            )

        for row in keyword_rows:
            if row["status"] == "critical":
                add(
                    "critical",
                    "keyword_stuffing",
                    "Вероятный переспам ключа",
                    f"Ключ '{row['keyword']}' имеет плотность {row['density_pct']}%.",
                )
            elif row["status"] == "warning" and row["occurrences"] == 0:
                add(
                    "warning",
                    "keyword_missing",
                    "Ключ отсутствует в тексте",
                    f"Ключ '{row['keyword']}' не найден на странице.",
                )
            elif row["status"] == "warning":
                add(
                    "warning",
                    "keyword_density_high",
                    "Повышенная плотность ключа",
                    f"Ключ '{row['keyword']}' имеет плотность {row['density_pct']}%.",
                )

        if top_terms:
            top = top_terms[0]
            if top.get("pct", 0) >= 8:
                add(
                    "critical",
                    "top_term_spam",
                    "Слишком частое повторение слова",
                    f"Слово '{top.get('term')}' занимает {top.get('pct')}% текста.",
                )
            elif top.get("pct", 0) >= 6:
                add(
                    "warning",
                    "top_term_repetition",
                    "Высокая повторяемость слова",
                    f"Слово '{top.get('term')}' занимает {top.get('pct')}% текста.",
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
                    "issues": [{"severity": "critical", "code": "invalid_url", "title": "Некорректный URL", "details": "Укажите валидный URL."}],
                    "issues_count": 1,
                    "summary": {"critical_issues": 1, "warning_issues": 0, "info_issues": 0, "score": 0},
                    "score": 0,
                    "recommendations": ["Проверьте формат URL и повторите аудит."],
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
                    "issues": [{"severity": "critical", "code": "fetch_error", "title": "Ошибка загрузки страницы", "details": str(exc)}],
                    "issues_count": 1,
                    "summary": {"critical_issues": 1, "warning_issues": 0, "info_issues": 0, "score": 0},
                    "score": 0,
                    "recommendations": ["Проверьте доступность страницы и повторите аудит."],
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

        stopwords = _STOPWORDS_RU if language == "ru" else _STOPWORDS_EN
        content_tokens = [t for t in all_tokens if t not in stopwords and len(t) > 2]
        content_counts = Counter(content_tokens)
        top_terms = []
        for term, count in content_counts.most_common(20):
            pct = round((count / total_words * 100.0), 3) if total_words > 0 else 0.0
            top_terms.append({"term": term, "count": count, "pct": pct})

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
        )

        critical_count = sum(1 for i in issues if i.get("severity") == "critical")
        warning_count = sum(1 for i in issues if i.get("severity") == "warning")
        info_count = sum(1 for i in issues if i.get("severity") == "info")
        score = max(0, 100 - critical_count * 20 - warning_count * 8 - info_count * 3)

        recommendations: List[str] = []
        for issue in issues:
            sev = issue.get("severity")
            if sev not in ("critical", "warning"):
                continue
            details = _norm_text(issue.get("details"))
            if details and details not in recommendations:
                recommendations.append(details)
        if not recommendations:
            recommendations.append("Критичных on-page проблем не обнаружено.")

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
                "recommendations": recommendations[:20],
                "meta": {
                    "domain": urlparse(final_url).netloc,
                    "fetched_at": datetime.utcnow().isoformat(),
                },
            },
        }

