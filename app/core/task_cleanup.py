"""Task artifact cleanup helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Set
import shutil
from datetime import datetime, timedelta, timezone

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


def prune_stale_report_artifacts(max_age_days: int | None = None) -> Dict[str, Any]:
    """
    Delete stale files under REPORTS_DIR according to age policy and clean empty folders.
    Intended for periodic maintenance (cron/manual trigger).
    """
    days = int(max_age_days if max_age_days is not None else settings.MAX_REPORT_AGE_DAYS)
    days = max(1, days)
    reports_root = Path(settings.REPORTS_DIR).resolve()
    if not reports_root.exists():
        return {
            "reports_root": str(reports_root),
            "max_age_days": days,
            "deleted_files": 0,
            "deleted_dirs": 0,
            "skipped": 0,
            "scanned_files": 0,
            "scanned_dirs": 0,
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    allowed_ext = {".png", ".jpg", ".jpeg", ".webp", ".docx", ".xlsx", ".txt", ".csv", ".json", ".jsonl", ".ndjson"}
    deleted_files = 0
    deleted_dirs = 0
    skipped = 0
    scanned_files = 0
    scanned_dirs = 0

    for path in reports_root.rglob("*"):
        try:
            resolved = path.resolve()
            if reports_root not in resolved.parents and resolved != reports_root:
                skipped += 1
                continue
            if resolved.is_file():
                scanned_files += 1
                if resolved.suffix.lower() not in allowed_ext:
                    continue
                mtime = datetime.fromtimestamp(resolved.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    resolved.unlink(missing_ok=True)
                    deleted_files += 1
            elif resolved.is_dir():
                scanned_dirs += 1
        except Exception:
            skipped += 1

    # Remove empty directories bottom-up.
    for path in sorted([p for p in reports_root.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
        try:
            resolved = path.resolve()
            if resolved == reports_root:
                continue
            if reports_root not in resolved.parents:
                skipped += 1
                continue
            if not any(resolved.iterdir()):
                resolved.rmdir()
                deleted_dirs += 1
        except Exception:
            skipped += 1

    return {
        "reports_root": str(reports_root),
        "max_age_days": days,
        "deleted_files": deleted_files,
        "deleted_dirs": deleted_dirs,
        "skipped": skipped,
        "scanned_files": scanned_files,
        "scanned_dirs": scanned_dirs,
    }
