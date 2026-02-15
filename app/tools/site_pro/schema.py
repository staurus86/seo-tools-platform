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
    issues: List[SiteAuditProIssue] = Field(default_factory=list)


class NormalizedSiteAuditPayload(BaseModel):
    mode: AuditMode = "quick"
    summary: SiteAuditProSummary = Field(default_factory=SiteAuditProSummary)
    rows: List[NormalizedSiteAuditRow] = Field(default_factory=list)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
