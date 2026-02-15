"""Site Audit Pro orchestration service."""
from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Callable, Dict, Optional

from .adapter import SiteAuditProAdapter

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
        started_at = datetime.utcnow().isoformat()
        t0 = time.perf_counter()

        notify(5, "Preparing Site Audit Pro")
        notify(20, "Collecting crawl scope")
        notify(45, "Running scoring pipeline")
        normalized = self.adapter.run(url=url, mode=selected_mode, max_pages=max_pages)
        notify(75, "Building normalized report payload")
        public_results = self.adapter.to_public_results(normalized)
        notify(95, "Finalizing Site Audit Pro result")
        duration_ms = int((time.perf_counter() - t0) * 1000)

        summary = public_results.get("summary", {}) if isinstance(public_results, dict) else {}

        return {
            "task_type": "site_audit_pro",
            "url": url,
            "mode": selected_mode,
            "completed_at": datetime.utcnow().isoformat(),
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
