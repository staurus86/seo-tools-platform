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
    final_url: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[int] = None
    html_size_bytes: Optional[int] = None
    dom_nodes_count: Optional[int] = None
    redirect_count: int = 0
    is_https: Optional[bool] = None
    compression_enabled: Optional[bool] = None
    cache_enabled: Optional[bool] = None
    indexable: Optional[bool] = None
    health_score: Optional[float] = None
    title: Optional[str] = None
    meta_description: Optional[str] = None
    canonical: Optional[str] = None
    meta_robots: Optional[str] = None
    schema_count: int = 0
    hreflang_count: int = 0
    mobile_friendly_hint: Optional[bool] = None
    word_count: Optional[int] = None
    unique_word_count: Optional[int] = None
    lexical_diversity: Optional[float] = None
    readability_score: Optional[float] = None
    toxicity_score: Optional[float] = None
    filler_ratio: Optional[float] = None
    h1_count: Optional[int] = None
    images_count: Optional[int] = None
    images_without_alt: Optional[int] = None
    external_nofollow_links: int = 0
    external_follow_links: int = 0
    outgoing_internal_links: int = 0
    incoming_internal_links: int = 0
    outgoing_external_links: int = 0
    orphan_page: Optional[bool] = None
    topic_hub: Optional[bool] = None
    pagerank: Optional[float] = None
    topic_label: Optional[str] = None
    top_terms: List[str] = Field(default_factory=list)
    duplicate_title_count: int = 0
    duplicate_description_count: int = 0
    weak_anchor_ratio: Optional[float] = None
    link_quality_score: Optional[float] = None
    ai_markers_count: int = 0
    recommendation: Optional[str] = None
    issues: List[SiteAuditProIssue] = Field(default_factory=list)


class NormalizedSiteAuditPayload(BaseModel):
    mode: AuditMode = "quick"
    summary: SiteAuditProSummary = Field(default_factory=SiteAuditProSummary)
    rows: List[NormalizedSiteAuditRow] = Field(default_factory=list)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
