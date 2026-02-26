"""
Shared task storage utilities — Redis + in-memory fallback.

All tool endpoints import from here to read/write task results.
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any

# Redis-based storage for task results
_redis_client = None
_redis_available = True

# Memory fallback storage (used when Redis is unavailable)
task_results_memory: Dict[str, Any] = {}


def get_redis_client():
    global _redis_client, _redis_available
    if _redis_client is None and _redis_available:
        try:
            import redis
            from app.config import settings
            _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
            _redis_client.ping()
            print("[API] Redis connection established for task results")
        except Exception as e:
            print(f"[API] Redis unavailable for task results: {e}")
            _redis_available = False
            _redis_client = None
    return _redis_client


def get_task_result(task_id: str) -> Optional[Dict[str, Any]]:
    """Get task result from Redis or memory fallback."""
    redis_client = get_redis_client()
    if redis_client:
        try:
            data = redis_client.get(f"task:{task_id}")
            if data:
                return json.loads(data)
        except Exception as e:
            print(f"[API] Error getting task from Redis: {e}")

    # Fallback to memory (for development without Redis)
    return task_results_memory.get(task_id)


def _save_task_payload(task_id: str, data: Dict[str, Any]) -> None:
    """Persist task payload in Redis (24h TTL) or memory fallback."""
    redis_client = get_redis_client()
    if redis_client:
        try:
            redis_client.setex(f"task:{task_id}", 86400, json.dumps(data))
            return
        except Exception as e:
            print(f"[API] Error saving task in Redis: {e}")
    task_results_memory[task_id] = data


def create_task_result(task_id: str, task_type: str, url: str, result: Dict[str, Any]):
    """Store task result in Redis with 24 hour TTL."""
    data = {
        "task_id": task_id,
        "task_type": task_type,
        "url": url,
        "status": "SUCCESS",
        "progress": 100,
        "status_message": "Completed",
        "error": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": result,
        "completed_at": datetime.utcnow().isoformat(),
    }
    _save_task_payload(task_id, data)
    print(f"[API] Task {task_id} stored")


def create_task_pending(
    task_id: str, task_type: str, url: str, status_message: str = "Queued"
) -> None:
    """Create task record in pending state."""
    now = datetime.utcnow().isoformat()
    data = {
        "task_id": task_id,
        "task_type": task_type,
        "url": url,
        "status": "PENDING",
        "progress": 0,
        "progress_meta": {},
        "status_message": status_message,
        "error": None,
        "created_at": now,
        "completed_at": None,
        "result": None,
    }
    _save_task_payload(task_id, data)


def update_task_state(
    task_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[int] = None,
    status_message: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress_meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Update task fields while preserving existing payload."""
    task = get_task_result(task_id)
    if not task:
        return
    if status is not None:
        task["status"] = status
    if progress is not None:
        task["progress"] = max(0, min(100, int(progress)))
    if status_message is not None:
        task["status_message"] = status_message
    if result is not None:
        task["result"] = result
    if error is not None:
        task["error"] = error
    if progress_meta is not None:
        task["progress_meta"] = progress_meta
    if status in ("SUCCESS", "FAILURE"):
        task["completed_at"] = datetime.utcnow().isoformat()
    _save_task_payload(task_id, task)


def append_task_artifact(task_id: str, artifact_path: str, kind: str = "report") -> None:
    """Attach generated artifact path to task payload for future cleanup."""
    task = get_task_result(task_id)
    if not task:
        return
    bucket = task.setdefault("artifacts", {})
    by_kind = bucket.setdefault(kind, [])
    if artifact_path not in by_kind:
        by_kind.append(artifact_path)
    _save_task_payload(task_id, task)


def delete_task_result(task_id: str) -> bool:
    """Delete task result from Redis/memory storage."""
    deleted = False
    redis_client = get_redis_client()
    if redis_client:
        try:
            deleted = bool(redis_client.delete(f"task:{task_id}")) or deleted
        except Exception as e:
            print(f"[API] Error deleting task from Redis: {e}")
    if task_id in task_results_memory:
        del task_results_memory[task_id]
        deleted = True
    return deleted
