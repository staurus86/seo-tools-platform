"""Pydantic schemas for LLM Crawler Simulation."""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator


ALLOWED_PROFILES = {
    "generic-bot",
    "search-bot",
    "ai-bot",
    "gptbot",
    "chatgpt-user",
    "claudebot",
    "perplexitybot",
    "google-extended",
    "ccbot",
}


class LlmCrawlerOptions(BaseModel):
    renderJs: bool = False
    timeoutMs: int = 20000
    profile: List[str] = Field(
        default_factory=lambda: [
            "generic-bot",
            "search-bot",
            "ai-bot",
            "gptbot",
            "google-extended",
            "perplexitybot",
        ]
    )
    showHeaders: bool = False
    include_raw_html: bool = False
    include_rendered_html: bool = False
    runCloaking: bool = False

    @field_validator("timeoutMs", mode="before")
    @classmethod
    def _normalize_timeout(cls, value: Any) -> int:
        try:
            raw = int(value)
        except Exception:
            raw = 20000
        return max(3000, min(raw, 120000))

    @field_validator("profile", mode="before")
    @classmethod
    def _normalize_profile(cls, value: Any) -> List[str]:
        if value is None:
            return ["generic-bot", "search-bot", "ai-bot"]
        if isinstance(value, str):
            parts = [x.strip().lower() for x in value.replace(";", ",").split(",") if x.strip()]
        elif isinstance(value, list):
            parts = [str(x).strip().lower() for x in value if str(x).strip()]
        else:
            parts = []
        result: List[str] = []
        seen = set()
        for item in parts:
            if item not in ALLOWED_PROFILES or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result or ["generic-bot", "search-bot", "ai-bot"]


class LlmCrawlerRunRequest(BaseModel):
    url: str | None = None
    urls: List[str] | None = None
    sitemap_url: str | None = None
    mode: str = Field(default="single_url")
    options: LlmCrawlerOptions = Field(default_factory=LlmCrawlerOptions)

    @field_validator("url", mode="before")
    @classmethod
    def _strip_url(cls, value: Any) -> str:
        return str(value or "").strip()


class LlmCrawlerJobResponse(BaseModel):
    jobId: str


class LlmCrawlerJobStatusResponse(BaseModel):
    status: str
    progress: int
    result: Dict[str, Any] | None = None
    error: str | None = None
    requestId: str | None = None
    jobId: str | None = None
    status_message: str | None = None
    render_status: Dict[str, Any] | None = None
    cloaking: Dict[str, Any] | None = None
    eeat_score: Dict[str, Any] | None = None
    entity_graph: Dict[str, Any] | None = None
    citation_probability: float | None = None
    js_dependency_score: float | None = None
    llm_ingestion: Dict[str, Any] | None = None
    vector_quality_score: float | None = None
    ai_understanding_score: float | None = None
    trust_signal_score: float | None = None
    content_loss_percent: float | None = None
    projected_score_after_fixes: float | None = None
    citation_breakdown: Dict[str, Any] | None = None
    discoverability_score: float | None = None
    ai_answer_preview: Dict[str, Any] | None = None
    topic_fallback_used: bool | None = None
    preview_mode: str | None = None
    chunk_ranking_debug: List[Dict[str, Any]] | None = None
    metrics_bytes: Dict[str, Any] | None = None
    chunk_dedupe: Dict[str, Any] | None = None
    page_type: str | None = None
    page_type_confidence: float | None = None
    page_type_reasons: List[str] | None = None
    noise_breakdown: Dict[str, Any] | None = None
    main_content_confidence: Dict[str, Any] | None = None
    content_segments: List[Dict[str, Any]] | None = None
    ai_blocks: Dict[str, Any] | None = None
    critical_blocks: List[Dict[str, Any]] | None = None
    ai_directives: Dict[str, Any] | None = None
    improvement_library: Dict[str, Any] | None = None
    detection_issues: List[str] | None = None
    detectors: Dict[str, Any] | None = None
    quality_gates: Dict[str, Any] | None = None
    recommendation_diagnostics: Dict[str, Any] | None = None
    segmentation: Dict[str, Any] | None = None
    content_extraction: Dict[str, Any] | None = None
    navigation_detection: Dict[str, Any] | None = None
    ads_detection: Dict[str, Any] | None = None
    structured_data: Dict[str, Any] | None = None
    page_classification: Dict[str, Any] | None = None
    js_dependency: Dict[str, Any] | None = None
    utility_detection: Dict[str, Any] | None = None
    main_content_analysis: Dict[str, Any] | None = None
    content_quality: Dict[str, Any] | None = None
    retrieval: Dict[str, Any] | None = None
    entities: Dict[str, Any] | None = None
    citation_model: Dict[str, Any] | None = None
    validation: Dict[str, Any] | None = None
