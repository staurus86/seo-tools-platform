"""Adapter bridge for future seopro.py migration."""
from __future__ import annotations

from collections import Counter, defaultdict, deque
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

    STOP_WORDS = {
        "the", "and", "for", "that", "this", "with", "from", "your", "you", "are", "was", "were",
        "about", "into", "http", "https", "www", "com", "как", "это", "для", "что", "или", "при",
        "site", "page", "seo",
    }
    WEAK_ANCHORS = {"click here", "here", "read more", "more", "подробнее", "тут", "link"}
    FILLER_WORDS = {
        "very", "really", "basically", "actually", "simply", "just", "literally", "maybe", "perhaps",
        "просто", "очень", "как бы", "наверное", "возможно",
    }
    TOXIC_WORDS = {"hate", "stupid", "idiot", "trash", "scam", "ненавижу", "тупой", "мусор"}
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
        tokens = re.findall(r"[a-zA-Zа-яА-Я0-9]{3,}", (text or "").lower())
        return [t for t in tokens if t not in self.STOP_WORDS]

    def _readability_score(self, text: str) -> float:
        # Lightweight readability heuristic: shorter sentences and moderate word length score higher.
        raw = (text or "").strip()
        if not raw:
            return 0.0
        sentences = [s for s in re.split(r"[.!?]+", raw) if s.strip()]
        words = re.findall(r"[a-zA-Zа-яА-Я0-9]+", raw)
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
        raw = re.findall(r"[a-zA-Zа-яА-Я0-9]+", (text or "").lower())
        if not raw:
            return 0.0
        filler = sum(1 for t in raw if t in self.FILLER_WORDS)
        return round(filler / len(raw), 4)

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
    ) -> Tuple[NormalizedSiteAuditRow, List[str], List[str], int, int]:
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
        schema_count = len(
            [
                tag
                for tag in soup.find_all("script")
                if ((tag.get("type") or "").lower().strip() == "application/ld+json")
            ]
        )
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
        images = soup.find_all("img")
        images_without_alt = sum(1 for img in images if not (img.get("alt") or "").strip())
        words = self._tokenize(body_text)
        ai_markers_count = len(self.AI_MARKER_RE.findall(body_text))
        unique_word_count = len(set(words))
        lexical_diversity = round(unique_word_count / max(1, len(words)), 3) if words else 0.0
        readability_score = self._readability_score(body_text)
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
        is_https = urlparse(final_url).scheme.lower() == "https"

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

        health_score = max(0.0, round(100.0 - penalty, 1))
        row = NormalizedSiteAuditRow(
            url=source_url,
            final_url=final_url,
            status_code=status_code,
            response_time_ms=response_time_ms,
            html_size_bytes=html_size_bytes,
            dom_nodes_count=dom_nodes_count,
            redirect_count=redirect_count,
            is_https=is_https,
            compression_enabled=compression_enabled,
            cache_enabled=cache_enabled,
            indexable=("noindex" not in robots and status_code < 400),
            health_score=health_score,
            title=title,
            meta_description=description.strip(),
            canonical=canonical.strip(),
            meta_robots=robots,
            schema_count=schema_count,
            hreflang_count=hreflang_count,
            mobile_friendly_hint=("width=device-width" in viewport) if viewport else None,
            word_count=len(words),
            unique_word_count=unique_word_count,
            lexical_diversity=lexical_diversity,
            readability_score=readability_score,
            toxicity_score=toxicity_score,
            filler_ratio=filler_ratio,
            h1_count=h1_count,
            images_count=len(images),
            images_without_alt=images_without_alt,
            outgoing_internal_links=len(internal_links),
            outgoing_external_links=external_links,
            external_nofollow_links=external_nofollow,
            external_follow_links=external_follow,
            weak_anchor_ratio=round((weak_anchor_count / anchor_total), 3) if anchor_total else 0.0,
            ai_markers_count=ai_markers_count,
            issues=issues,
        )
        return row, internal_links, words, weak_anchor_count, anchor_total

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

    def _compute_top_tfidf_terms(self, page_tokens: Dict[str, List[str]], top_n: int = 8) -> Dict[str, List[str]]:
        doc_count = len(page_tokens) or 1
        df = Counter()
        for terms in page_tokens.values():
            df.update(set(terms))
        per_page: Dict[str, List[str]] = {}
        for url, terms in page_tokens.items():
            if not terms:
                per_page[url] = []
                continue
            tf = Counter(terms)
            scores: List[Tuple[str, float]] = []
            for term, freq in tf.items():
                idf = math.log((1 + doc_count) / (1 + df[term])) + 1.0
                scores.append((term, freq * idf))
            scores.sort(key=lambda x: x[1], reverse=True)
            per_page[url] = [term for term, _ in scores[:top_n]]
        return per_page

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
        page_tokens: Dict[str, List[str]] = {}
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
                row, links, tokens, weak_anchor_count, anchor_total = self._build_row(
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
                page_tokens[row.url] = tokens
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
                page_tokens[current] = []
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
        tfidf_terms = self._compute_top_tfidf_terms(page_tokens, top_n=8)

        topic_clusters: Dict[str, List[str]] = defaultdict(list)
        for row in rows:
            row.incoming_internal_links = int(incoming_counts.get(row.url, 0))
            row.pagerank = pagerank_scores.get(row.url, 0.0)
            row.top_terms = tfidf_terms.get(row.url, [])
            row.topic_label = row.top_terms[0] if row.top_terms else "misc"
            weak_count, anchor_total = anchor_quality_raw.get(row.url, (0, 0))
            row.weak_anchor_ratio = round((weak_count / anchor_total), 3) if anchor_total else 0.0
            density_penalty = min(20.0, row.weak_anchor_ratio * 30.0) if row.weak_anchor_ratio is not None else 0.0
            link_score = 100.0
            link_score -= density_penalty
            outgoing_total = (row.outgoing_internal_links or 0) + (row.outgoing_external_links or 0)
            nofollow_ratio = (
                (row.external_nofollow_links or 0) / max(1, row.outgoing_external_links or 0)
                if (row.outgoing_external_links or 0) > 0
                else 0.0
            )
            if row.outgoing_internal_links == 0 and row.incoming_internal_links > 0:
                link_score -= 10
            row.orphan_page = row.incoming_internal_links == 0
            if row.incoming_internal_links == 0 and row.outgoing_internal_links == 0:
                row.issues.append(
                    SiteAuditProIssue(
                        severity="warning",
                        code="orphan_or_isolated_page",
                        title="Page appears isolated in internal link graph",
                    )
                )
                link_score -= 15
            if (row.outgoing_external_links or 0) >= 5 and (row.outgoing_internal_links or 0) == 0:
                link_score -= 8
            if nofollow_ratio > 0.8 and (row.outgoing_external_links or 0) >= 3:
                link_score -= 5
            if row.response_time_ms and row.response_time_ms > 2500:
                link_score -= min(8.0, ((row.response_time_ms - 2500) / 1000.0) * 2.0)
            if (row.redirect_count or 0) > 1:
                link_score -= min(6.0, (row.redirect_count - 1) * 2.0)
            row.link_quality_score = round(max(0.0, min(100.0, link_score)), 1)
            row.topic_hub = bool(
                (row.pagerank or 0.0) >= 60.0
                and (row.outgoing_internal_links or 0) >= 3
                and row.incoming_internal_links >= 1
            )
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
            topic_clusters[row.topic_label].append(row.url)

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
