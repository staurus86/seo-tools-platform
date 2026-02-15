"""Site Audit Pro orchestration service."""
from __future__ import annotations

from datetime import datetime
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

        notify(5, "Preparing Site Audit Pro")
        notify(20, "Collecting crawl scope")
        notify(45, "Running scoring pipeline")
        normalized = self.adapter.run(url=url, mode=selected_mode, max_pages=max_pages)
        notify(75, "Building normalized report payload")
        public_results = self.adapter.to_public_results(normalized)
        notify(95, "Finalizing Site Audit Pro result")

        return {
            "task_type": "site_audit_pro",
            "url": url,
            "mode": selected_mode,
            "completed_at": datetime.utcnow().isoformat(),
            "results": public_results,
            "meta": {
                "task_id": task_id,
                "service": "site_pro_service_v0",
            },
        }
