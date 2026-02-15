"""Task artifact cleanup helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Set
import shutil

from app.config import settings


def _collect_paths(value: Any, out: Set[Path]) -> None:
    if isinstance(value, dict):
        for v in value.values():
            _collect_paths(v, out)
        return
    if isinstance(value, list):
        for item in value:
            _collect_paths(item, out)
        return
    if isinstance(value, str):
        lower = value.lower()
        if lower.startswith("http://") or lower.startswith("https://"):
            return
        if any(ext in lower for ext in (".png", ".jpg", ".jpeg", ".webp", ".docx", ".xlsx", ".txt", ".csv", ".json", ".jsonl", ".ndjson")):
            out.add(Path(value))
            return
        reports_token = str(settings.REPORTS_DIR).replace("\\", "/").lower()
        value_token = value.replace("\\", "/").lower()
        if reports_token in value_token and "." not in Path(value).name:
            out.add(Path(value))


def extract_artifact_paths(task_data: Dict[str, Any]) -> List[Path]:
    candidates: Set[Path] = set()
    _collect_paths(task_data, candidates)
    task_id = str(task_data.get("task_id") or "").strip()
    if task_id:
        for ext in ("docx", "xlsx", "txt", "csv", "json"):
            candidates.add(Path(settings.REPORTS_DIR) / f"{task_id}.{ext}")
        candidates.add(Path(settings.REPORTS_DIR) / "mobile" / task_id)
    return sorted(candidates)


def delete_task_artifacts(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Delete artifacts referenced by task payload and return cleanup summary."""
    reports_root = Path(settings.REPORTS_DIR).resolve()
    deleted_files = 0
    deleted_dirs = 0
    skipped = 0
    removed_items: List[Path] = []

    for path in extract_artifact_paths(task_data):
        try:
            p = path if path.is_absolute() else Path.cwd() / path
            resolved = p.resolve()
            if reports_root not in resolved.parents and resolved != reports_root:
                skipped += 1
                continue
            if resolved.is_file():
                resolved.unlink(missing_ok=True)
                deleted_files += 1
                removed_items.append(resolved)
            elif resolved.is_dir():
                shutil.rmtree(resolved, ignore_errors=True)
                deleted_dirs += 1
                removed_items.append(resolved)
        except Exception:
            skipped += 1

    for removed in removed_items:
        for parent in removed.parents:
            if parent == reports_root:
                break
            try:
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
            except Exception:
                break

    return {
        "deleted_files": deleted_files,
        "deleted_dirs": deleted_dirs,
        "skipped": skipped,
    }
