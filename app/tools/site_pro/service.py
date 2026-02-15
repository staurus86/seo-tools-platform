"""Site Audit Pro orchestration service."""
from __future__ import annotations

from datetime import datetime, timezone
import os
import time
from typing import Any, Callable, Dict, Optional

from .adapter import SiteAuditProAdapter
from .artifacts import SiteProArtifactStore

ProgressCallback = Optional[Callable[[int, str], None]]


class SiteAuditProService:
    def __init__(self) -> None:
        self.adapter = SiteAuditProAdapter()

    def run(
        self,
        *,
        url: str,
        task_id: str,
        mode: str = "quick",
        max_pages: int = 5,
        progress_callback: ProgressCallback = None,
    ) -> Dict[str, Any]:
        def notify(progress: int, message: str) -> None:
            if callable(progress_callback):
                progress_callback(progress, message)

        selected_mode = "full" if mode == "full" else "quick"
        started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.perf_counter()

        notify(5, "Preparing Site Audit Pro")
        notify(20, "Collecting crawl scope")
        notify(45, "Running scoring pipeline")
        normalized = self.adapter.run(url=url, mode=selected_mode, max_pages=max_pages)
        notify(75, "Building normalized report payload")
        public_results = self.adapter.to_public_results(normalized)
        notify(85, "Preparing deep artifacts")
        self._attach_chunked_artifacts(
            task_id=task_id,
            mode=selected_mode,
            public_results=public_results,
        )
        notify(95, "Finalizing Site Audit Pro result")
        duration_ms = int((time.perf_counter() - t0) * 1000)

        summary = public_results.get("summary", {}) if isinstance(public_results, dict) else {}

        return {
            "task_type": "site_audit_pro",
            "url": url,
            "mode": selected_mode,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "results": public_results,
            "meta": {
                "task_id": task_id,
                "service": "site_pro_service_v0",
                "started_at": started_at,
                "duration_ms": duration_ms,
                "pages_scanned": summary.get("total_pages", 0),
                "issues_total": summary.get("issues_total", 0),
            },
        }

    def _attach_chunked_artifacts(self, *, task_id: str, mode: str, public_results: Dict[str, Any]) -> None:
        """
        Persist heavy deep arrays as chunked JSONL and attach manifest to results.
        This keeps task payload predictable while preserving full details for download.
        """
        if not isinstance(public_results, dict):
            return

        pipeline = public_results.get("pipeline", {}) or {}
        issues = public_results.get("issues", []) or []
        semantic = pipeline.get("semantic_linking_map", []) or []
        pages = public_results.get("pages", []) or []

        # In quick mode we still emit artifacts when payload arrays are non-trivial.
        should_emit = (mode == "full") or (len(issues) > 100) or (len(semantic) > 100) or (len(pages) > 200)
        if not should_emit:
            return

        store = SiteProArtifactStore(task_id=task_id)
        manifest = {
            "task_id": task_id,
            "base_dir": str(store.root_dir),
            "chunks": [],
        }

        issue_rows = [
            {
                "url": row.get("url"),
                "severity": row.get("severity"),
                "code": row.get("code"),
                "title": row.get("title"),
                "details": row.get("details"),
            }
            for row in issues
        ]
        semantic_rows = [
            {
                "source_url": row.get("source_url"),
                "target_url": row.get("target_url"),
                "topic": row.get("topic"),
                "reason": row.get("reason"),
            }
            for row in semantic
        ]
        page_rows = [
            {
                "url": row.get("url"),
                "status_code": row.get("status_code"),
                "health_score": row.get("health_score"),
                "topic_label": row.get("topic_label"),
                "recommendation": row.get("recommendation"),
            }
            for row in pages
        ]

        manifest["chunks"].append(store.write_chunked_jsonl(name="issues", rows=issue_rows, chunk_size=500))
        manifest["chunks"].append(store.write_chunked_jsonl(name="semantic_map", rows=semantic_rows, chunk_size=500))
        manifest["chunks"].append(store.write_chunked_jsonl(name="pages", rows=page_rows, chunk_size=1000))

        results_artifacts = public_results.setdefault("artifacts", {})
        results_artifacts["chunk_manifest"] = manifest
        self._compact_inline_payload(public_results)

    @staticmethod
    def _int_setting(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None:
            try:
                from app.config import settings

                raw = getattr(settings, name, None)
            except Exception:
                raw = None
        try:
            value = int(raw if raw is not None else default)
        except Exception:
            value = int(default)
        return max(1, value)

    def _compact_inline_payload(self, public_results: Dict[str, Any]) -> None:
        """
        Keep task payload small when chunk artifacts are available.
        Full records stay downloadable via manifest links.
        """
        issues_limit = self._int_setting("SITE_AUDIT_PRO_INLINE_ISSUES_LIMIT", 200)
        semantic_limit = self._int_setting("SITE_AUDIT_PRO_INLINE_SEMANTIC_LIMIT", 200)
        pages_limit = self._int_setting("SITE_AUDIT_PRO_INLINE_PAGES_LIMIT", 500)

        issues = list(public_results.get("issues", []) or [])
        pages = list(public_results.get("pages", []) or [])
        pipeline = public_results.get("pipeline", {}) or {}
        semantic = list(pipeline.get("semantic_linking_map", []) or [])

        omitted = {
            "issues": max(0, len(issues) - issues_limit),
            "semantic_linking_map": max(0, len(semantic) - semantic_limit),
            "pages": max(0, len(pages) - pages_limit),
        }

        public_results["issues"] = issues[:issues_limit]
        public_results["pages"] = pages[:pages_limit]
        pipeline["semantic_linking_map"] = semantic[:semantic_limit]
        public_results["pipeline"] = pipeline

        artifacts = public_results.setdefault("artifacts", {})
        artifacts["payload_compacted"] = any(v > 0 for v in omitted.values())
        artifacts["inline_limits"] = {
            "issues": issues_limit,
            "semantic_linking_map": semantic_limit,
            "pages": pages_limit,
        }
        artifacts["omitted_counts"] = omitted
