"""Schema for Site Audit Pro normalized result."""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


AuditMode = Literal["quick", "full"]


class SiteAuditProIssue(BaseModel):
    severity: Literal["critical", "warning", "info"] = "info"
    code: str
    title: str
    details: Optional[str] = None


class SiteAuditProSummary(BaseModel):
    total_pages: int = 0
    internal_pages: int = 0
    issues_total: int = 0
    critical_issues: int = 0
    warning_issues: int = 0
    info_issues: int = 0
    score: Optional[float] = None
    mode: AuditMode = "quick"


class SiteAuditProResult(BaseModel):
    task_type: str = "site_audit_pro"
    url: str
    mode: AuditMode = "quick"
    completed_at: str
    results: Dict[str, Any] = Field(default_factory=dict)


class NormalizedSiteAuditRow(BaseModel):
    url: str
    status_code: Optional[int] = None
    indexable: Optional[bool] = None
    health_score: Optional[float] = None
    title: Optional[str] = None
    meta_description: Optional[str] = None
    canonical: Optional[str] = None
    word_count: Optional[int] = None
    h1_count: Optional[int] = None
    images_count: Optional[int] = None
    images_without_alt: Optional[int] = None
    outgoing_internal_links: int = 0
    incoming_internal_links: int = 0
    outgoing_external_links: int = 0
    pagerank: Optional[float] = None
    topic_label: Optional[str] = None
    top_terms: List[str] = Field(default_factory=list)
    duplicate_title_count: int = 0
    duplicate_description_count: int = 0
    weak_anchor_ratio: Optional[float] = None
    link_quality_score: Optional[float] = None
    ai_markers_count: int = 0
    issues: List[SiteAuditProIssue] = Field(default_factory=list)


class NormalizedSiteAuditPayload(BaseModel):
    mode: AuditMode = "quick"
    summary: SiteAuditProSummary = Field(default_factory=SiteAuditProSummary)
    rows: List[NormalizedSiteAuditRow] = Field(default_factory=list)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
