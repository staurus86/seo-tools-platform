"""Adapter bridge for future seopro.py migration."""
from __future__ import annotations

from collections import Counter, deque
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

    def _build_row(self, page_url: str, status_code: int, html: str) -> Tuple[NormalizedSiteAuditRow, List[str], str]:
        soup = BeautifulSoup(html or "", "html.parser")
        title = (soup.title.string if soup.title and soup.title.string else "").strip()
        desc_tag = soup.find("meta", attrs={"name": "description"})
        description = (desc_tag.get("content") if desc_tag else "") or ""
        robots_tag = soup.find("meta", attrs={"name": "robots"})
        robots = ((robots_tag.get("content") if robots_tag else "") or "").lower()
        canonical_tag = soup.find("link", attrs={"rel": lambda x: x and "canonical" in str(x).lower()})
        canonical = (canonical_tag.get("href") if canonical_tag else "") or ""

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

        health_score = max(0.0, round(100.0 - penalty, 1))
        row = NormalizedSiteAuditRow(
            url=page_url,
            status_code=status_code,
            indexable=("noindex" not in robots and status_code < 400),
            health_score=health_score,
            issues=issues,
        )
        return row, self._extract_internal_links(page_url, soup, urlparse(page_url).netloc), title

    def run(self, url: str, mode: str = "quick", max_pages: int = 100) -> NormalizedSiteAuditPayload:
        selected_mode = "full" if mode == "full" else "quick"
        page_limit = max(1, min(int(max_pages or 100), 5000))
        timeout = 12

        start_url = self._normalize_url(url)
        base_host = urlparse(start_url).netloc
        if not base_host:
            raise ValueError("Invalid URL for Site Audit Pro")

        queue: Deque[str] = deque([start_url])
        visited: Set[str] = set()
        rows: List[NormalizedSiteAuditRow] = []
        titles_by_url: Dict[str, str] = {}
        title_counter: Counter = Counter()
        crawl_errors: List[str] = []

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
                row, links, title = self._build_row(final_url, response.status_code, response.text or "")
                rows.append(row)
                if title:
                    normalized_title = title.strip().lower()
                    titles_by_url[row.url] = normalized_title
                    title_counter[normalized_title] += 1
                for link in links:
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

        duplicate_titles = {t for t, count in title_counter.items() if t and count > 1}
        if duplicate_titles:
            for row in rows:
                row_title = titles_by_url.get(row.url, "")
                if row_title in duplicate_titles:
                    row.issues.append(
                        SiteAuditProIssue(
                            severity="warning",
                            code="duplicate_title",
                            title="Duplicate title detected",
                        )
                    )

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
        return {
            "engine": "site_pro_adapter_v0",
            "mode": normalized.mode,
            "summary": normalized.summary.model_dump(),
            "pages": [row.model_dump() for row in normalized.rows],
            "issues": [
                {**issue.model_dump(), "url": row.url}
                for row in normalized.rows
                for issue in row.issues
            ],
            "issues_count": normalized.summary.issues_total,
            "artifacts": normalized.artifacts,
        }
