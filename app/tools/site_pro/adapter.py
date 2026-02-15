"""Adapter bridge for future seopro.py migration."""
from __future__ import annotations

from typing import Any, Dict

from .schema import (
    NormalizedSiteAuditPayload,
    NormalizedSiteAuditRow,
    SiteAuditProSummary,
)


class SiteAuditProAdapter:
    """
    Transitional adapter.
    Current behavior returns a deterministic normalized skeleton so API/UI wiring
    can be shipped before full seopro function-level porting.
    """

    def run(self, url: str, mode: str = "quick", max_pages: int = 100) -> NormalizedSiteAuditPayload:
        selected_mode = "full" if mode == "full" else "quick"

        summary = SiteAuditProSummary(
            total_pages=1,
            internal_pages=1,
            issues_total=0,
            critical_issues=0,
            warning_issues=0,
            info_issues=0,
            score=100.0,
            mode=selected_mode,
        )

        row = NormalizedSiteAuditRow(
            url=url,
            status_code=200,
            indexable=True,
            health_score=100.0,
            issues=[],
        )

        payload = NormalizedSiteAuditPayload(
            mode=selected_mode,
            summary=summary,
            rows=[row],
            artifacts={
                "migration_stage": "skeleton",
                "notes": [
                    "site_audit_pro endpoint is active",
                    "seopro calculation pipeline port is pending",
                ],
                "max_pages_requested": max_pages,
            },
        )
        return payload

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
