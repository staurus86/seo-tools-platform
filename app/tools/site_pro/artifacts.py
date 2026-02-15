"""Chunked artifact storage for Site Audit Pro deep payloads."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List


def _reports_root() -> Path:
    try:
        from app.config import settings

        configured = str(getattr(settings, "REPORTS_DIR", "") or "").strip()
        if configured in {"", ".", "./", ".\\"}:
            return Path("reports_output")
        return Path(configured)
    except Exception:
        return Path(os.getenv("REPORTS_DIR", "reports_output"))


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value or "artifact").strip("_") or "artifact"


class SiteProArtifactStore:
    def __init__(self, task_id: str, base_url: str = "/api/site-pro-artifacts") -> None:
        self.task_id = task_id
        self.base_url = base_url.rstrip("/")
        self.root_dir = _reports_root() / "site_pro" / _safe_name(task_id)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def write_chunked_jsonl(
        self,
        *,
        name: str,
        rows: List[Dict[str, Any]],
        chunk_size: int,
    ) -> Dict[str, Any]:
        safe_name = _safe_name(name)
        files: List[Dict[str, Any]] = []
        if not rows:
            return {"kind": safe_name, "total_records": 0, "chunk_size": chunk_size, "files": files}

        for idx in range(0, len(rows), max(1, int(chunk_size))):
            part = idx // max(1, int(chunk_size)) + 1
            batch = rows[idx : idx + max(1, int(chunk_size))]
            filename = f"{safe_name}_part-{part:03d}.jsonl"
            path = self.root_dir / filename
            with path.open("w", encoding="utf-8", newline="\n") as f:
                for row in batch:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            files.append(
                {
                    "filename": filename,
                    "path": str(path),
                    "records": len(batch),
                    "download_url": f"{self.base_url}/{self.task_id}/{filename}",
                }
            )

        return {
            "kind": safe_name,
            "total_records": len(rows),
            "chunk_size": chunk_size,
            "files": files,
        }
